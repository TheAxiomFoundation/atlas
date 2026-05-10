"""Massachusetts General Laws source-first corpus adapter."""

from __future__ import annotations

import html
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import quote, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

MASSACHUSETTS_GENERAL_LAWS_BASE_URL = "https://malegislature.gov"
MASSACHUSETTS_GENERAL_LAWS_INDEX_PATH = "Laws/GeneralLaws"
MASSACHUSETTS_GENERAL_LAWS_SOURCE_FORMAT = "massachusetts-general-laws-html"
MASSACHUSETTS_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_INDEX_RELATIVE_PATH = "pages/Laws/GeneralLaws/index.html"
_TITLE_ONCLICK_RE = re.compile(
    r"accordionAjaxLoad\(\s*'(?P<part_id>\d+)'\s*,\s*'(?P<title_id>\d+)'\s*,\s*'(?P<title>[IVXLCDM]+)'\s*\)",
    re.I,
)
_PART_RE = re.compile(r"^Part\s+(?P<part>[IVXLCDM]+)$", re.I)
_TITLE_RE = re.compile(r"^Title\s+(?P<title>[IVXLCDM]+)$", re.I)
_CHAPTER_RE = re.compile(r"^Chapter\s+(?P<chapter>[0-9A-Z.-]+)$", re.I)
_SECTION_RE = re.compile(r"^Section\s+(?P<section>.+)$", re.I)
_CHAPTER_SECTION_HREF_RE = re.compile(
    r"/Laws/GeneralLaws/Part(?P<part>[IVXLCDM]+)/Title(?P<title>[IVXLCDM]+)/Chapter(?P<chapter>[^/]+)/Section(?P<section>[^/?#]+)",
    re.I,
)
_EXPLICIT_REF_RE = re.compile(
    r"(?:M\.?\s*G\.?\s*L\.?\s*)?(?:c\.|chapter)\s*(?P<chapter>\d+[A-Z]?)\s*,?\s*(?:\u00a7|section)\s*(?P<section>\d+[A-Z]?)",
    re.I,
)
_THIS_CHAPTER_REF_RE = re.compile(
    r"(?:\u00a7|section)\s*(?P<section>\d+[A-Z]?)\s+of\s+(?:this|said)\s+chapter",
    re.I,
)


@dataclass(frozen=True)
class MassachusettsPart:
    """One Massachusetts General Laws part from the official index."""

    code: str
    heading: str
    href: str
    ordinal: int
    chapter_range: str | None = None

    @property
    def source_id(self) -> str:
        return f"part-{_slug(self.code)}"

    @property
    def citation_path(self) -> str:
        return f"us-ma/statute/part-{_slug(self.code)}"

    @property
    def legal_identifier(self) -> str:
        return f"M.G.L. Part {self.code}"


@dataclass(frozen=True)
class MassachusettsTitle:
    """One title listed on a Massachusetts part page."""

    part_code: str
    part_heading: str
    part_citation_path: str
    roman: str
    title_id: str
    part_id: str
    heading: str
    ordinal: int
    chapter_range: str | None = None

    @property
    def source_id(self) -> str:
        return f"part-{_slug(self.part_code)}-title-{_slug(self.roman)}"

    @property
    def citation_path(self) -> str:
        return f"{self.part_citation_path}/title-{_slug(self.roman)}"

    @property
    def legal_identifier(self) -> str:
        return f"M.G.L. Part {self.part_code}, Title {self.roman}"

    @property
    def relative_path(self) -> str:
        return f"ajax/GetChaptersForTitle/part-{self.part_id}-title-{self.title_id}-{self.roman}.html"


@dataclass(frozen=True)
class MassachusettsChapter:
    """One Massachusetts General Laws chapter."""

    part_code: str
    part_heading: str
    title_roman: str
    title_heading: str
    title_citation_path: str
    number: str
    heading: str
    href: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"chapter-{_slug(self.number)}"

    @property
    def citation_path(self) -> str:
        return f"{self.title_citation_path}/chapter-{_slug(self.number)}"

    @property
    def legal_identifier(self) -> str:
        return f"M.G.L. c. {self.number}"

    @property
    def relative_path(self) -> str:
        return _page_relative_path(self.href)


@dataclass(frozen=True)
class MassachusettsSectionTarget:
    """One section link from a Massachusetts chapter page."""

    part_code: str
    part_heading: str
    title_roman: str
    title_heading: str
    chapter_number: str
    chapter_heading: str
    chapter_citation_path: str
    section: str
    heading: str
    href: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"{self.chapter_number}-{_slug(self.section)}"

    @property
    def citation_path(self) -> str:
        return f"us-ma/statute/{_slug(self.chapter_number)}/{_slug(self.section)}"

    @property
    def legal_identifier(self) -> str:
        return f"M.G.L. c. {self.chapter_number}, \u00a7 {self.section}"

    @property
    def relative_path(self) -> str:
        return _page_relative_path(self.href)


@dataclass(frozen=True)
class MassachusettsParsedSection:
    """Parsed Massachusetts section body."""

    heading: str
    body: str | None
    references_to: tuple[str, ...]
    status: str | None = None


@dataclass(frozen=True)
class MassachusettsProvision:
    """Normalized Massachusetts part, title, chapter, or section node."""

    kind: str
    source_id: str
    display_number: str
    citation_path: str
    legal_identifier: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int | None
    part_code: str | None = None
    part_heading: str | None = None
    title_roman: str | None = None
    title_heading: str | None = None
    chapter_number: str | None = None
    chapter_heading: str | None = None
    section: str | None = None
    references_to: tuple[str, ...] = ()
    status: str | None = None


@dataclass(frozen=True)
class _MassachusettsSource:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _RecordedSource:
    source_url: str
    source_path: str
    sha256: str


class _MassachusettsFetcher:
    def __init__(
        self,
        *,
        base_url: str,
        source_dir: Path | None,
        download_dir: Path | None,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.base_url = _base_url(base_url)
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._request_lock = Lock()
        self._last_request_at = 0.0

    def fetch_index(self) -> _MassachusettsSource:
        return self._fetch(
            relative_path=_INDEX_RELATIVE_PATH,
            source_url=urljoin(self.base_url, MASSACHUSETTS_GENERAL_LAWS_INDEX_PATH),
        )

    def fetch_part(self, part: MassachusettsPart) -> _MassachusettsSource:
        return self._fetch(
            relative_path=_page_relative_path(part.href),
            source_url=urljoin(self.base_url, part.href),
        )

    def fetch_title_chapters(self, title: MassachusettsTitle) -> _MassachusettsSource:
        request = requests.Request(
            "GET",
            urljoin(self.base_url, "/Laws/GeneralLaws/GetChaptersForTitle"),
            params={
                "partId": title.part_id,
                "titleId": title.title_id,
                "title": title.roman,
            },
        )
        prepared = request.prepare()
        if prepared.url is None:
            raise ValueError(f"failed to build Massachusetts title URL for {title.roman}")
        return self._fetch(relative_path=title.relative_path, source_url=prepared.url)

    def fetch_chapter(self, chapter: MassachusettsChapter) -> _MassachusettsSource:
        return self._fetch(
            relative_path=chapter.relative_path,
            source_url=urljoin(self.base_url, chapter.href),
        )

    def fetch_section(self, target: MassachusettsSectionTarget) -> _MassachusettsSource:
        return self._fetch(
            relative_path=target.relative_path,
            source_url=urljoin(self.base_url, target.href),
        )

    def _fetch(self, *, relative_path: str, source_url: str) -> _MassachusettsSource:
        if self.source_dir is not None:
            return _MassachusettsSource(
                relative_path=relative_path,
                source_url=source_url,
                data=_read_source_file(self.source_dir, relative_path),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return _MassachusettsSource(
                    relative_path=relative_path,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )

        data = self._download(source_url)
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return _MassachusettsSource(
            relative_path=relative_path,
            source_url=source_url,
            data=data,
        )

    def _download(self, source_url: str) -> bytes:
        last_error: BaseException | None = None
        for attempt in range(self.request_attempts):
            with self._request_lock:
                elapsed = time.monotonic() - self._last_request_at
                if elapsed < self.request_delay_seconds:
                    time.sleep(self.request_delay_seconds - elapsed)
                self._last_request_at = time.monotonic()
            try:
                response = requests.get(
                    source_url,
                    headers={"User-Agent": MASSACHUSETTS_USER_AGENT},
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.content
            except requests.RequestException as exc:
                last_error = exc
                if attempt + 1 < self.request_attempts:
                    time.sleep(min(2.0 * (attempt + 1), 10.0))
        raise RuntimeError(
            f"failed to fetch Massachusetts source {source_url}: {last_error}"
        ) from last_error


def extract_massachusetts_general_laws(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_part: str | None = None,
    only_title: str | None = None,
    only_chapter: str | None = None,
    limit: int | None = None,
    workers: int = 8,
    download_dir: str | Path | None = None,
    base_url: str = MASSACHUSETTS_GENERAL_LAWS_BASE_URL,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
) -> StateStatuteExtractReport:
    """Snapshot official Massachusetts General Laws sources and extract provisions."""
    jurisdiction = "us-ma"
    part_filter = _optional_filter(only_part)
    title_filter = _optional_filter(only_title)
    chapter_filter = _optional_filter(only_chapter)
    run_id = _massachusetts_run_id(
        version,
        only_part=part_filter,
        only_title=title_filter,
        only_chapter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _MassachusettsFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    source_paths: list[Path] = []
    source_by_relative: dict[str, _RecordedSource] = {}
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    errors: list[str] = []

    index_source = fetcher.fetch_index()
    index_recorded = _record_source(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        source=index_source,
        source_by_relative=source_by_relative,
        source_paths=source_paths,
    )
    parts = tuple(
        part
        for part in parse_massachusetts_parts(index_source.data)
        if part_filter is None
        or _same_filter(part.code, part_filter)
        or _same_filter(part.heading, part_filter)
    )
    if not parts:
        raise ValueError(f"no Massachusetts part sources selected for filter: {only_part!r}")

    chapter_targets: list[MassachusettsChapter] = []
    section_targets: list[MassachusettsSectionTarget] = []

    for part in parts:
        added = _append_provision(
            _part_provision(part),
            index_recorded,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            records=records,
            items=items,
            seen=seen,
        )
        if added:
            title_count += 1

        part_source = fetcher.fetch_part(part)
        part_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=part_source,
            source_by_relative=source_by_relative,
            source_paths=source_paths,
        )
        titles = tuple(
            title
            for title in parse_massachusetts_titles(part_source.data, part=part)
            if title_filter is None
            or _same_filter(title.roman, title_filter)
            or _same_filter(title.heading, title_filter)
        )
        for title in titles:
            added = _append_provision(
                _title_provision(title),
                part_recorded,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                records=records,
                items=items,
                seen=seen,
            )
            if added:
                container_count += 1

            chapters_source = fetcher.fetch_title_chapters(title)
            _record_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                source=chapters_source,
                source_by_relative=source_by_relative,
                source_paths=source_paths,
            )
            chapters = tuple(
                chapter
                for chapter in parse_massachusetts_chapters(chapters_source.data, title=title)
                if chapter_filter is None
                or _same_filter(chapter.number, chapter_filter)
                or _same_filter(chapter.heading, chapter_filter)
            )
            chapter_targets.extend(chapters)

    if not chapter_targets:
        raise ValueError(
            "no Massachusetts chapter sources selected for filters: "
            f"part={only_part!r}, title={only_title!r}, chapter={only_chapter!r}"
        )

    section_budget = max(0, limit) if limit is not None else None
    chapter_pages = _fetch_chapter_pages(
        fetcher,
        chapter_targets,
        workers=max(1, workers),
    )
    for chapter, source, targets, error in chapter_pages:
        if error is not None:
            errors.append(f"chapter {chapter.number}: {error}")
            continue
        if source is None or targets is None:
            continue
        chapter_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=source,
            source_by_relative=source_by_relative,
            source_paths=source_paths,
        )
        added = _append_provision(
            _chapter_provision(chapter),
            chapter_recorded,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            records=records,
            items=items,
            seen=seen,
        )
        if added:
            container_count += 1

        for target in targets:
            if section_budget is not None and len(section_targets) >= section_budget:
                break
            section_targets.append(target)
        if section_budget is not None and len(section_targets) >= section_budget:
            break

    fetched_sections = _fetch_section_pages(
        fetcher,
        section_targets,
        workers=max(1, workers),
    )
    for target, source, parsed, error in fetched_sections:
        if error is not None:
            errors.append(f"{target.chapter_number} {target.section}: {error}")
            continue
        if source is None or parsed is None:
            continue
        section_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=source,
            source_by_relative=source_by_relative,
            source_paths=source_paths,
        )
        added = _append_provision(
            _section_provision(target, parsed),
            section_recorded,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            records=records,
            items=items,
            seen=seen,
        )
        if added:
            section_count += 1

    if not records:
        raise ValueError("no Massachusetts provisions extracted")
    if errors and section_count == 0:
        raise ValueError(f"no Massachusetts sections extracted: {errors[:5]}")

    inventory_path = store.inventory_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
    coverage_path = store.coverage_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return StateStatuteExtractReport(
        jurisdiction=jurisdiction,
        title_count=title_count,
        container_count=container_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        errors=tuple(errors),
    )


def parse_massachusetts_parts(data: str | bytes) -> tuple[MassachusettsPart, ...]:
    """Parse the official General Laws index into part targets."""
    soup = BeautifulSoup(data, "html.parser")
    parts: list[MassachusettsPart] = []
    for ordinal, link in enumerate(
        soup.select("ul.generalLawsList a[href*='/Laws/GeneralLaws/Part']"),
        start=1,
    ):
        if not isinstance(link, Tag):
            continue
        part_text = _clean_text(_selector_text(link, ".part"))
        match = _PART_RE.match(part_text)
        if not match:
            continue
        href = str(link.get("href") or "").strip()
        heading = _clean_text(_selector_text(link, ".partTitle"))
        chapter_range = _clean_text(_selector_text(link, ".chapters")) or None
        parts.append(
            MassachusettsPart(
                code=match.group("part").upper(),
                heading=heading,
                href=href,
                ordinal=ordinal,
                chapter_range=chapter_range,
            )
        )
    return tuple(parts)


def parse_massachusetts_titles(
    data: str | bytes,
    *,
    part: MassachusettsPart,
) -> tuple[MassachusettsTitle, ...]:
    """Parse title panels from one official part page."""
    soup = BeautifulSoup(data, "html.parser")
    titles: list[MassachusettsTitle] = []
    for panel in soup.select("div.panel.panel-default"):
        if not isinstance(panel, Tag):
            continue
        link = panel.select_one(".glTitle a[onclick]")
        if not isinstance(link, Tag):
            continue
        onclick = str(link.get("onclick") or "")
        onclick_match = _TITLE_ONCLICK_RE.search(onclick)
        title_match = _TITLE_RE.match(_clean_text(link.get_text(" ", strip=True)))
        if onclick_match is None or title_match is None:
            continue
        heading_links = [
            candidate
            for candidate in panel.select(".panel-heading h4.panel-title a")
            if isinstance(candidate, Tag)
        ]
        heading = ""
        for candidate in heading_links:
            text = _clean_text(candidate.get_text(" ", strip=True))
            if text and not _TITLE_RE.match(text) and not text.lower().startswith("chapters"):
                heading = text
                break
        chapter_range = _clean_text(panel.select_one(".titleChapters").get_text(" ", strip=True)) if panel.select_one(".titleChapters") else None
        titles.append(
            MassachusettsTitle(
                part_code=part.code,
                part_heading=part.heading,
                part_citation_path=part.citation_path,
                roman=title_match.group("title").upper(),
                title_id=onclick_match.group("title_id"),
                part_id=onclick_match.group("part_id"),
                heading=heading,
                ordinal=len(titles) + 1,
                chapter_range=chapter_range,
            )
        )
    return tuple(titles)


def parse_massachusetts_chapters(
    data: str | bytes,
    *,
    title: MassachusettsTitle,
) -> tuple[MassachusettsChapter, ...]:
    """Parse official title chapter fragments."""
    soup = BeautifulSoup(data, "html.parser")
    chapters: list[MassachusettsChapter] = []
    for link in soup.select("ul.generalLawsList a[href*='/Chapter']"):
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "").strip()
        chapter_text = _clean_text(_selector_text(link, ".chapter"))
        chapter_match = _CHAPTER_RE.match(chapter_text)
        if chapter_match is None:
            continue
        heading = _clean_text(_selector_text(link, ".chapterTitle"))
        chapters.append(
            MassachusettsChapter(
                part_code=title.part_code,
                part_heading=title.part_heading,
                title_roman=title.roman,
                title_heading=title.heading,
                title_citation_path=title.citation_path,
                number=chapter_match.group("chapter").upper(),
                heading=heading,
                href=href,
                ordinal=len(chapters) + 1,
            )
        )
    return tuple(chapters)


def parse_massachusetts_chapter_page(
    data: str | bytes,
    *,
    chapter: MassachusettsChapter,
) -> tuple[MassachusettsSectionTarget, ...]:
    """Parse section targets from one official chapter page."""
    soup = BeautifulSoup(data, "html.parser")
    targets: list[MassachusettsSectionTarget] = []
    seen: set[str] = set()
    for link in soup.select("ul.generalLawsList a[href*='/Section']"):
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "").strip()
        if not href:
            continue
        section_text = _clean_text(_selector_text(link, ".section"))
        section_match = _SECTION_RE.match(section_text)
        if section_match is None:
            continue
        section = _clean_section(section_match.group("section"))
        if not section or href in seen:
            continue
        seen.add(href)
        heading = _clean_text(_selector_text(link, ".sectionTitle"))
        targets.append(
            MassachusettsSectionTarget(
                part_code=chapter.part_code,
                part_heading=chapter.part_heading,
                title_roman=chapter.title_roman,
                title_heading=chapter.title_heading,
                chapter_number=chapter.number,
                chapter_heading=chapter.heading,
                chapter_citation_path=chapter.citation_path,
                section=section,
                heading=heading,
                href=href,
                ordinal=len(targets) + 1,
            )
        )
    return tuple(targets)


def parse_massachusetts_section(
    data: str | bytes,
    *,
    target: MassachusettsSectionTarget,
) -> MassachusettsParsedSection:
    """Parse one official Massachusetts General Laws section page."""
    soup = BeautifulSoup(data, "html.parser")
    heading_node = soup.find("h2", id="skipTo")
    if not isinstance(heading_node, Tag):
        raise ValueError(f"missing section heading for chapter {target.chapter_number} section {target.section}")
    heading_text = _clean_text(heading_node.get_text(" ", strip=True))
    heading = target.heading
    match = re.match(r"Section\s+.+?:\s*(?P<heading>.+)$", heading_text, re.I)
    if match:
        heading = _clean_text(match.group("heading"))

    paragraphs: list[str] = []
    for sibling in heading_node.find_next_siblings():
        if not isinstance(sibling, Tag):
            continue
        if sibling.name == "p":
            text = _clean_text(sibling.get_text(" ", strip=True))
            if text:
                paragraphs.append(text)
        elif sibling.name in {"script", "style"}:
            continue
    body = "\n".join(paragraphs).strip() or None
    if body is not None:
        body = _strip_section_marker(body, target.section)
    return MassachusettsParsedSection(
        heading=heading,
        body=body,
        references_to=_references_from_body(body, target=target),
        status=_status_from_body(body, heading),
    )


def _fetch_chapter_pages(
    fetcher: _MassachusettsFetcher,
    chapters: list[MassachusettsChapter],
    *,
    workers: int,
) -> list[
    tuple[
        MassachusettsChapter,
        _MassachusettsSource | None,
        tuple[MassachusettsSectionTarget, ...] | None,
        Exception | None,
    ]
]:
    def fetch_one(
        chapter: MassachusettsChapter,
    ) -> tuple[
        MassachusettsChapter,
        _MassachusettsSource | None,
        tuple[MassachusettsSectionTarget, ...] | None,
        Exception | None,
    ]:
        try:
            source = fetcher.fetch_chapter(chapter)
            return chapter, source, parse_massachusetts_chapter_page(source.data, chapter=chapter), None
        except Exception as exc:  # noqa: BLE001
            return chapter, None, None, exc

    if workers <= 1:
        return [fetch_one(chapter) for chapter in chapters]
    ordered: dict[
        int,
        tuple[
            MassachusettsChapter,
            _MassachusettsSource | None,
            tuple[MassachusettsSectionTarget, ...] | None,
            Exception | None,
        ],
    ] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, chapter): index for index, chapter in enumerate(chapters)}
        for future in as_completed(futures):
            ordered[futures[future]] = future.result()
    return [ordered[index] for index in range(len(chapters))]


def _fetch_section_pages(
    fetcher: _MassachusettsFetcher,
    targets: list[MassachusettsSectionTarget],
    *,
    workers: int,
) -> list[
    tuple[
        MassachusettsSectionTarget,
        _MassachusettsSource | None,
        MassachusettsParsedSection | None,
        Exception | None,
    ]
]:
    def fetch_one(
        target: MassachusettsSectionTarget,
    ) -> tuple[
        MassachusettsSectionTarget,
        _MassachusettsSource | None,
        MassachusettsParsedSection | None,
        Exception | None,
    ]:
        try:
            source = fetcher.fetch_section(target)
            return target, source, parse_massachusetts_section(source.data, target=target), None
        except Exception as exc:  # noqa: BLE001
            return target, None, None, exc

    if workers <= 1:
        return [fetch_one(target) for target in targets]
    ordered: dict[
        int,
        tuple[
            MassachusettsSectionTarget,
            _MassachusettsSource | None,
            MassachusettsParsedSection | None,
            Exception | None,
        ],
    ] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, target): index for index, target in enumerate(targets)}
        for future in as_completed(futures):
            ordered[futures[future]] = future.result()
    return [ordered[index] for index in range(len(targets))]


def _record_source(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    source: _MassachusettsSource,
    source_by_relative: dict[str, _RecordedSource],
    source_paths: list[Path],
) -> _RecordedSource:
    existing = source_by_relative.get(source.relative_path)
    if existing is not None:
        return existing
    relative_name = f"{MASSACHUSETTS_GENERAL_LAWS_SOURCE_FORMAT}/{source.relative_path}"
    artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        relative_name,
    )
    sha256 = store.write_bytes(artifact_path, source.data)
    source_paths.append(artifact_path)
    recorded = _RecordedSource(
        source_url=source.source_url,
        source_path=_state_source_key(jurisdiction, run_id, relative_name),
        sha256=sha256,
    )
    source_by_relative[source.relative_path] = recorded
    return recorded


def _append_provision(
    provision: MassachusettsProvision,
    source: _RecordedSource,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
    records: list[ProvisionRecord],
    items: list[SourceInventoryItem],
    seen: set[str],
) -> bool:
    if provision.citation_path in seen:
        return False
    seen.add(provision.citation_path)
    metadata = _metadata(provision)
    items.append(
        SourceInventoryItem(
            citation_path=provision.citation_path,
            source_url=source.source_url,
            source_path=source.source_path,
            source_format=MASSACHUSETTS_GENERAL_LAWS_SOURCE_FORMAT,
            sha256=source.sha256,
            metadata=metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(provision.citation_path),
            jurisdiction="us-ma",
            document_class=DocumentClass.STATUTE.value,
            citation_path=provision.citation_path,
            body=provision.body,
            heading=provision.heading,
            citation_label=provision.legal_identifier,
            version=version,
            source_url=source.source_url,
            source_path=source.source_path,
            source_id=provision.source_id,
            source_format=MASSACHUSETTS_GENERAL_LAWS_SOURCE_FORMAT,
            source_as_of=source_as_of,
            expression_date=expression_date,
            parent_citation_path=provision.parent_citation_path,
            parent_id=(
                deterministic_provision_id(provision.parent_citation_path)
                if provision.parent_citation_path
                else None
            ),
            level=provision.level,
            ordinal=provision.ordinal,
            kind=provision.kind,
            legal_identifier=provision.legal_identifier,
            identifiers=_identifiers(provision),
            metadata=metadata,
        )
    )
    return True


def _part_provision(part: MassachusettsPart) -> MassachusettsProvision:
    return MassachusettsProvision(
        kind="part",
        source_id=part.source_id,
        display_number=part.code,
        citation_path=part.citation_path,
        legal_identifier=part.legal_identifier,
        heading=part.heading,
        body=None,
        parent_citation_path=None,
        level=0,
        ordinal=part.ordinal,
        part_code=part.code,
        part_heading=part.heading,
    )


def _title_provision(title: MassachusettsTitle) -> MassachusettsProvision:
    return MassachusettsProvision(
        kind="title",
        source_id=title.source_id,
        display_number=title.roman,
        citation_path=title.citation_path,
        legal_identifier=title.legal_identifier,
        heading=title.heading,
        body=None,
        parent_citation_path=title.part_citation_path,
        level=1,
        ordinal=title.ordinal,
        part_code=title.part_code,
        part_heading=title.part_heading,
        title_roman=title.roman,
        title_heading=title.heading,
    )


def _chapter_provision(chapter: MassachusettsChapter) -> MassachusettsProvision:
    return MassachusettsProvision(
        kind="chapter",
        source_id=chapter.source_id,
        display_number=chapter.number,
        citation_path=chapter.citation_path,
        legal_identifier=chapter.legal_identifier,
        heading=chapter.heading,
        body=None,
        parent_citation_path=chapter.title_citation_path,
        level=2,
        ordinal=chapter.ordinal,
        part_code=chapter.part_code,
        part_heading=chapter.part_heading,
        title_roman=chapter.title_roman,
        title_heading=chapter.title_heading,
        chapter_number=chapter.number,
        chapter_heading=chapter.heading,
    )


def _section_provision(
    target: MassachusettsSectionTarget,
    parsed: MassachusettsParsedSection,
) -> MassachusettsProvision:
    return MassachusettsProvision(
        kind="section",
        source_id=target.source_id,
        display_number=target.section,
        citation_path=target.citation_path,
        legal_identifier=target.legal_identifier,
        heading=parsed.heading,
        body=parsed.body,
        parent_citation_path=target.chapter_citation_path,
        level=3,
        ordinal=target.ordinal,
        part_code=target.part_code,
        part_heading=target.part_heading,
        title_roman=target.title_roman,
        title_heading=target.title_heading,
        chapter_number=target.chapter_number,
        chapter_heading=target.chapter_heading,
        section=target.section,
        references_to=parsed.references_to,
        status=parsed.status,
    )


def _metadata(provision: MassachusettsProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "display_number": provision.display_number,
    }
    if provision.part_code:
        metadata["part"] = provision.part_code
    if provision.part_heading:
        metadata["part_heading"] = provision.part_heading
    if provision.title_roman:
        metadata["title"] = provision.title_roman
    if provision.title_heading:
        metadata["title_heading"] = provision.title_heading
    if provision.chapter_number:
        metadata["chapter"] = provision.chapter_number
    if provision.chapter_heading:
        metadata["chapter_heading"] = provision.chapter_heading
    if provision.section:
        metadata["section"] = provision.section
    if provision.parent_citation_path:
        metadata["parent_citation_path"] = provision.parent_citation_path
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.status:
        metadata["status"] = provision.status
    return metadata


def _identifiers(provision: MassachusettsProvision) -> dict[str, str]:
    identifiers = {"massachusetts:source_id": provision.source_id}
    if provision.part_code:
        identifiers["massachusetts:part"] = provision.part_code
    if provision.title_roman:
        identifiers["massachusetts:title"] = provision.title_roman
    if provision.chapter_number:
        identifiers["massachusetts:chapter"] = provision.chapter_number
    if provision.section:
        identifiers["massachusetts:section"] = provision.section
    return identifiers


def _references_from_body(
    body: str | None,
    *,
    target: MassachusettsSectionTarget,
) -> tuple[str, ...]:
    if not body:
        return ()
    refs: list[str] = []
    for match in _EXPLICIT_REF_RE.finditer(body):
        refs.append(
            f"us-ma/statute/{_slug(match.group('chapter'))}/{_slug(match.group('section'))}"
        )
    for match in _THIS_CHAPTER_REF_RE.finditer(body):
        refs.append(f"us-ma/statute/{_slug(target.chapter_number)}/{_slug(match.group('section'))}")
    refs = [ref for ref in refs if ref != target.citation_path]
    return tuple(dict.fromkeys(refs))


def _status_from_body(body: str | None, heading: str | None) -> str | None:
    text = " ".join(value for value in (heading, body) if value).strip().lower()
    if text.startswith("repealed") or " repealed," in text[:200]:
        return "repealed"
    return None


def _strip_section_marker(body: str, section: str) -> str:
    section_pattern = re.compile(
        rf"^Section\s+{re.escape(section)}\.\s*",
        re.I,
    )
    return section_pattern.sub("", body).strip()


def _selector_text(node: Tag, selector: str) -> str:
    selected = node.select_one(selector)
    if selected is None:
        return ""
    return selected.get_text(" ", strip=True)


def _clean_section(value: str) -> str:
    text = _clean_text(value)
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    return text.strip(" .")


def _clean_text(value: str | None) -> str:
    if value is None:
        return ""
    text = html.unescape(value)
    text = text.replace("\xa0", " ").replace("\u2011", "-")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    return re.sub(r"[ \t\r\f\v]+", " ", text).strip()


def _optional_filter(value: str | None) -> str | None:
    return _clean_text(value) if value is not None and str(value).strip() else None


def _same_filter(value: str, expected: str) -> bool:
    return _slug(value) == _slug(expected)


def _slug(value: str) -> str:
    return re.sub(r"[^0-9a-z]+", "-", value.lower()).strip("-")


def _page_relative_path(href: str) -> str:
    parsed = urlparse(href)
    path = parsed.path.strip("/")
    if not path:
        return _INDEX_RELATIVE_PATH
    parts = [_quote_path_segment(unquote(part)) for part in path.split("/") if part]
    return "pages/" + "/".join(parts) + ".html"


def _quote_path_segment(value: str) -> str:
    return quote(value, safe="-._~")


def _read_source_file(source_dir: Path, relative_path: str) -> bytes:
    candidates = [
        source_dir / relative_path,
        source_dir / MASSACHUSETTS_GENERAL_LAWS_SOURCE_FORMAT / relative_path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.read_bytes()
    raise FileNotFoundError(f"missing Massachusetts source file: {relative_path}")


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _base_url(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _massachusetts_run_id(
    version: str,
    *,
    only_part: str | None,
    only_title: str | None,
    only_chapter: str | None,
    limit: int | None,
) -> str:
    if only_part is None and only_title is None and only_chapter is None and limit is None:
        return version
    parts = [version, "us-ma"]
    if only_part is not None:
        parts.append(f"part-{_slug(only_part)}")
    if only_title is not None:
        parts.append(f"title-{_slug(only_title)}")
    if only_chapter is not None:
        parts.append(f"chapter-{_slug(only_chapter)}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value
