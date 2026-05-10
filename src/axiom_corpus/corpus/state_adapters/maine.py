"""Maine Revised Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

MAINE_REVISED_STATUTES_BASE_URL = "https://legislature.maine.gov/statutes/"
MAINE_REVISED_STATUTES_INDEX = "homepage.html"
MAINE_REVISED_STATUTES_SOURCE_FORMAT = "maine-revised-statutes-html"
MAINE_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_TITLE_RE = re.compile(r"^TITLE\s+(?P<title>\d+[A-Z-]*):\s*(?P<heading>.+)$", re.I)
_PART_RE = re.compile(r"^Part\s+(?P<part>\d+[A-Z-]*):\s*(?P<heading>.+)$", re.I)
_CHAPTER_RE = re.compile(r"Chapter\s+(?P<chapter>[0-9A-Z-]+):\s*(?P<heading>.+)", re.I)
_SECTION_LINK_RE = re.compile(
    r"(?P<title>\d+[A-Z-]*)\s+\u00a7(?P<section>[0-9A-Z-]+)\.\s*(?P<heading>.+)",
    re.I,
)
_TITLE_PAGE_RE = re.compile(r"title(?P<title>\d+[A-Z-]*)ch0sec0(?:-\d+)?\.html$", re.I)
_CHAPTER_PAGE_RE = re.compile(
    r"title(?P<title>\d+[A-Z-]*)ch(?P<chapter>[0-9A-Z-]+)sec0(?P<variant>-\d+)?\.html$",
    re.I,
)
_SECTION_PAGE_RE = re.compile(
    r"title(?P<title>\d+[A-Z-]*)sec(?P<section>[0-9A-Z-]+)\.html$",
    re.I,
)


@dataclass(frozen=True)
class MaineTitle:
    """One Maine Revised Statutes title from the official title index."""

    number: str
    heading: str
    relative_path: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"title-{_slug(self.number)}"

    @property
    def citation_path(self) -> str:
        return f"us-me/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"{self.number} M.R.S."


@dataclass(frozen=True)
class MainePart:
    """One part listed on a Maine title page."""

    title: str
    part: str
    heading: str
    parent_citation_path: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"title-{_slug(self.title)}-part-{_slug(self.part)}"

    @property
    def citation_path(self) -> str:
        return f"us-me/statute/title-{_slug(self.title)}/part-{_slug(self.part)}"

    @property
    def legal_identifier(self) -> str:
        return f"{self.title} M.R.S. Part {self.part}"


@dataclass(frozen=True)
class MaineChapter:
    """One chapter listed on a Maine title page."""

    title: str
    chapter_id: str
    display_chapter: str
    heading: str
    relative_path: str
    parent_citation_path: str
    level: int
    ordinal: int
    section_range: str | None = None
    status: str | None = None

    @property
    def source_id(self) -> str:
        return f"title-{_slug(self.title)}-chapter-{_slug(self.chapter_id)}"

    @property
    def citation_path(self) -> str:
        return f"us-me/statute/title-{_slug(self.title)}/chapter-{_slug(self.chapter_id)}"

    @property
    def legal_identifier(self) -> str:
        return f"{self.title} M.R.S. Chapter {self.display_chapter}"


@dataclass(frozen=True)
class MaineSectionTarget:
    """One section link from a Maine chapter page."""

    title: str
    section_id: str
    display_section: str
    heading: str
    relative_path: str
    parent_citation_path: str
    ordinal: int
    status: str | None = None

    @property
    def source_id(self) -> str:
        return f"{self.title}-{self.section_id}"

    @property
    def citation_path(self) -> str:
        return f"us-me/statute/{self.title}/{self.section_id}"

    @property
    def legal_identifier(self) -> str:
        return f"{self.title} M.R.S. \u00a7 {self.display_section}"


@dataclass(frozen=True)
class MaineTitleDocument:
    """Parsed official Maine title page."""

    title_heading: str
    parts: tuple[MainePart, ...]
    chapters: tuple[MaineChapter, ...]


@dataclass(frozen=True)
class MaineParsedSection:
    """Parsed Maine section body."""

    display_section: str
    heading: str
    body: str | None
    references_to: tuple[str, ...]
    source_history: tuple[str, ...]
    notes: tuple[str, ...]
    status: str | None = None


@dataclass(frozen=True)
class MaineProvision:
    """Normalized Maine title, part, chapter, or section node."""

    kind: str
    title: str
    source_id: str
    display_number: str
    citation_path: str
    legal_identifier: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int | None
    references_to: tuple[str, ...] = ()
    source_history: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    status: str | None = None


@dataclass(frozen=True)
class _MaineSourcePage:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _RecordedSource:
    source_url: str
    source_path: str
    sha256: str


class _MaineFetcher:
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

    def fetch(self, relative_path: str) -> _MaineSourcePage:
        normalized = _normalize_relative_path(relative_path)
        source_url = self.base_url if normalized == MAINE_REVISED_STATUTES_INDEX else urljoin(
            self.base_url,
            normalized,
        )
        if self.source_dir is not None:
            return _MaineSourcePage(
                relative_path=normalized,
                source_url=source_url,
                data=_read_source_file(self.source_dir, normalized),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            if cached_path.exists():
                return _MaineSourcePage(
                    relative_path=normalized,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )

        data = self._download(source_url)
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            _write_cache_bytes(cached_path, data)
        return _MaineSourcePage(relative_path=normalized, source_url=source_url, data=data)

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
                    headers={"User-Agent": MAINE_USER_AGENT},
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.content
            except requests.RequestException as exc:
                last_error = exc
                if attempt + 1 < self.request_attempts:
                    time.sleep(min(2.0 * (attempt + 1), 10.0))
        raise RuntimeError(f"failed to fetch Maine source {source_url}: {last_error}") from last_error


def extract_maine_revised_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    only_chapter: str | int | None = None,
    limit: int | None = None,
    workers: int = 8,
    download_dir: str | Path | None = None,
    base_url: str = MAINE_REVISED_STATUTES_BASE_URL,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
) -> StateStatuteExtractReport:
    """Snapshot official Maine Revised Statutes HTML and extract provisions."""
    jurisdiction = "us-me"
    title_filter = _optional_filter(only_title)
    chapter_filter = _optional_filter(only_chapter)
    run_id = _maine_run_id(
        version,
        only_title=title_filter,
        only_chapter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _MaineFetcher(
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

    index_page = fetcher.fetch(MAINE_REVISED_STATUTES_INDEX)
    _record_source_page(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        page=index_page,
        source_by_relative=source_by_relative,
        source_paths=source_paths,
    )
    titles = tuple(
        title
        for title in parse_maine_title_index(index_page.data)
        if title_filter is None or _same_filter(title.number, title_filter)
    )
    if not titles:
        raise ValueError(f"no Maine title sources selected for filter: {only_title!r}")

    section_targets: list[MaineSectionTarget] = []
    for title in titles:
        title_page = fetcher.fetch(title.relative_path)
        title_source = _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=title_page,
            source_by_relative=source_by_relative,
            source_paths=source_paths,
        )
        document = parse_maine_title_page(title_page.data, title=title)
        title_provision = _title_provision(title, heading=document.title_heading)
        added = _append_provision(
            title_provision,
            title_source,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            records=records,
            items=items,
            seen=seen,
        )
        if added:
            title_count += 1
        for part in document.parts:
            added = _append_provision(
                _part_provision(part),
                title_source,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                records=records,
                items=items,
                seen=seen,
            )
            if added:
                container_count += 1
        for chapter in document.chapters:
            if chapter_filter is not None and not (
                _same_filter(chapter.display_chapter, chapter_filter)
                or _same_filter(chapter.chapter_id, chapter_filter)
            ):
                continue
            added = _append_provision(
                _chapter_provision(chapter),
                title_source,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                records=records,
                items=items,
                seen=seen,
            )
            if added:
                container_count += 1
            try:
                chapter_page = fetcher.fetch(chapter.relative_path)
                _record_source_page(
                    store,
                    jurisdiction=jurisdiction,
                    run_id=run_id,
                    page=chapter_page,
                    source_by_relative=source_by_relative,
                    source_paths=source_paths,
                )
                section_targets.extend(
                    parse_maine_chapter_page(chapter_page.data, chapter=chapter)
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{chapter.relative_path}: {exc}")
            if limit is not None and len(section_targets) >= limit:
                break
        if limit is not None and len(section_targets) >= limit:
            break

    if limit is not None:
        section_targets = section_targets[:limit]
    fetched_sections = _fetch_section_pages(
        fetcher,
        section_targets,
        workers=max(1, workers),
    )
    for target, page, parsed, error in fetched_sections:
        if error is not None:
            errors.append(f"{target.relative_path}: {error}")
            continue
        if page is None or parsed is None:
            continue
        section_source = _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=page,
            source_by_relative=source_by_relative,
            source_paths=source_paths,
        )
        added = _append_provision(
            _section_provision(target, parsed),
            section_source,
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
        raise ValueError("no Maine provisions extracted")
    if errors and section_count == 0:
        raise ValueError(f"no Maine sections extracted: {errors[:5]}")

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


def parse_maine_title_index(html: str | bytes) -> tuple[MaineTitle, ...]:
    """Parse the official Maine title list page."""
    soup = BeautifulSoup(html, "lxml")
    titles: list[MaineTitle] = []
    for ordinal, link in enumerate(soup.select("ul.title_list a[href]"), start=1):
        text = _clean_text(link.get_text(" ", strip=True))
        match = _TITLE_RE.match(text)
        if not match:
            continue
        relative_path = _relative_from_href(link.get("href", ""), MAINE_REVISED_STATUTES_INDEX)
        titles.append(
            MaineTitle(
                number=_clean_title(match.group("title")),
                heading=_clean_heading(match.group("heading")),
                relative_path=relative_path,
                ordinal=ordinal,
            )
        )
    return tuple(titles)


def parse_maine_title_page(html: str | bytes, *, title: MaineTitle) -> MaineTitleDocument:
    """Parse one official Maine title table of contents page."""
    soup = BeautifulSoup(html, "lxml")
    title_heading = _parse_title_heading(soup, title)
    parts: list[MainePart] = []
    chapters: list[MaineChapter] = []
    title_parent = title.citation_path

    part_nodes = list(soup.select(".MRSPart_toclist"))
    if part_nodes:
        for part_ordinal, part_node in enumerate(part_nodes, start=1):
            part_header = part_node.select_one(".heading_part")
            parsed_part = _parse_part_heading(
                _clean_text(part_header.get_text(" ", strip=True)) if part_header else "",
                title=title,
                ordinal=part_ordinal,
            )
            if parsed_part is not None:
                parts.append(parsed_part)
                parent = parsed_part.citation_path
                level = 2
            else:
                parent = title_parent
                level = 1
            for chapter_node in part_node.select(".MRSChapter_toclist"):
                chapter = _parse_chapter_node(
                    chapter_node,
                    title=title,
                    parent_citation_path=parent,
                    level=level,
                    ordinal=len(chapters) + 1,
                )
                if chapter is not None:
                    chapters.append(chapter)
    else:
        for chapter_node in soup.select(".MRSChapter_toclist"):
            chapter = _parse_chapter_node(
                chapter_node,
                title=title,
                parent_citation_path=title_parent,
                level=1,
                ordinal=len(chapters) + 1,
            )
            if chapter is not None:
                chapters.append(chapter)
    return MaineTitleDocument(
        title_heading=title_heading,
        parts=tuple(parts),
        chapters=tuple(chapters),
    )


def parse_maine_chapter_page(
    html: str | bytes,
    *,
    chapter: MaineChapter,
) -> tuple[MaineSectionTarget, ...]:
    """Parse a Maine chapter table of contents page into section targets."""
    soup = BeautifulSoup(html, "lxml")
    targets: list[MaineSectionTarget] = []
    for ordinal, section_node in enumerate(soup.select(".MRSSection_toclist"), start=1):
        link = section_node.select_one("a[href]")
        if link is None:
            continue
        link_text = _clean_text(link.get_text(" ", strip=True))
        match = _SECTION_LINK_RE.search(link_text)
        source_section = _section_id_from_href(link.get("href", ""), chapter.relative_path)
        if match is not None:
            display_section = _clean_section(match.group("section"))
            heading = _clean_heading(match.group("heading"))
            target_title = _clean_title(match.group("title"))
        elif source_section is not None:
            display_section = source_section
            heading = _clean_heading(link_text)
            target_title = chapter.title
        else:
            continue
        relative_path = _relative_from_href(link.get("href", ""), chapter.relative_path)
        targets.append(
            MaineSectionTarget(
                title=target_title,
                section_id=source_section or display_section,
                display_section=display_section,
                heading=heading,
                relative_path=relative_path,
                parent_citation_path=chapter.citation_path,
                ordinal=ordinal,
                status=_status_from_text(link_text, section_node),
            )
        )
    return tuple(targets)


def parse_maine_section(
    html: str | bytes,
    *,
    target: MaineSectionTarget | None = None,
) -> MaineParsedSection:
    """Parse one official Maine section page."""
    soup = BeautifulSoup(html, "lxml")
    section_node = soup.select_one(".MRSSection") or soup.select_one(".section-content") or soup
    heading_node = section_node.select_one(".heading_section") if isinstance(section_node, Tag) else None
    display_section = target.display_section if target is not None else ""
    heading = target.heading if target is not None else ""
    if heading_node is not None:
        parsed_section, parsed_heading = _parse_section_heading(
            _clean_text(heading_node.get_text(" ", strip=True))
        )
        display_section = parsed_section or display_section
        heading = parsed_heading or heading

    body_lines: list[str] = []
    notes: list[str] = []
    history: list[str] = []
    status_notes: list[str] = []
    if isinstance(section_node, Tag):
        for blip in section_node.select(".headnote_blip"):
            text = _clean_text(blip.get_text(" ", strip=True))
            if text:
                status_notes.append(text)
        for subsection in section_node.select(".MRSSubSection"):
            text_node = subsection.select_one(".mrs-text") or subsection
            text = _clean_text(text_node.get_text(" ", strip=True))
            if text:
                body_lines.append(text)
            for item in subsection.select(".bhistory"):
                text = _clean_text(item.get_text(" ", strip=True))
                if text:
                    history.append(text)
        for note in section_node.select(".note"):
            text = _clean_text(note.get_text(" ", strip=True))
            if text:
                notes.append(text)
        for hist in section_node.select(".qhistory"):
            text = _clean_text(hist.get_text(" ", strip=True))
            text = re.sub(r"^SECTION HISTORY\s*", "", text, flags=re.I).strip()
            if text:
                history.append(text)
    if not body_lines and status_notes:
        body_lines.extend(status_notes)
    status = _section_status(status_notes, target.status if target is not None else None)
    references_to = _references_from_links(section_node, target=target) if isinstance(
        section_node,
        Tag,
    ) else ()
    return MaineParsedSection(
        display_section=display_section,
        heading=heading,
        body="\n".join(body_lines).strip() or None,
        references_to=references_to,
        source_history=tuple(dict.fromkeys(history)),
        notes=tuple(dict.fromkeys(notes + status_notes)),
        status=status,
    )


def _fetch_section_pages(
    fetcher: _MaineFetcher,
    targets: list[MaineSectionTarget],
    *,
    workers: int,
) -> list[tuple[MaineSectionTarget, _MaineSourcePage | None, MaineParsedSection | None, Exception | None]]:
    def fetch_one(target: MaineSectionTarget) -> tuple[
        MaineSectionTarget,
        _MaineSourcePage | None,
        MaineParsedSection | None,
        Exception | None,
    ]:
        try:
            page = fetcher.fetch(target.relative_path)
            return target, page, parse_maine_section(page.data, target=target), None
        except Exception as exc:  # noqa: BLE001
            return target, None, None, exc

    if workers <= 1:
        return [fetch_one(target) for target in targets]
    results: list[
        tuple[MaineSectionTarget, _MaineSourcePage | None, MaineParsedSection | None, Exception | None]
    ] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, target): index for index, target in enumerate(targets)}
        ordered: dict[int, tuple[
            MaineSectionTarget,
            _MaineSourcePage | None,
            MaineParsedSection | None,
            Exception | None,
        ]] = {}
        for future in as_completed(futures):
            ordered[futures[future]] = future.result()
    for index in range(len(targets)):
        results.append(ordered[index])
    return results


def _record_source_page(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    page: _MaineSourcePage,
    source_by_relative: dict[str, _RecordedSource],
    source_paths: list[Path],
) -> _RecordedSource:
    existing = source_by_relative.get(page.relative_path)
    if existing is not None:
        return existing
    relative_name = f"{MAINE_REVISED_STATUTES_SOURCE_FORMAT}/{page.relative_path}"
    artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        relative_name,
    )
    sha256 = store.write_bytes(artifact_path, page.data)
    source_paths.append(artifact_path)
    recorded = _RecordedSource(
        source_url=page.source_url,
        source_path=_state_source_key(jurisdiction, run_id, relative_name),
        sha256=sha256,
    )
    source_by_relative[page.relative_path] = recorded
    return recorded


def _append_provision(
    provision: MaineProvision,
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
            source_format=MAINE_REVISED_STATUTES_SOURCE_FORMAT,
            sha256=source.sha256,
            metadata=metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(provision.citation_path),
            jurisdiction="us-me",
            document_class=DocumentClass.STATUTE.value,
            citation_path=provision.citation_path,
            body=provision.body,
            heading=provision.heading,
            citation_label=provision.legal_identifier,
            version=version,
            source_url=source.source_url,
            source_path=source.source_path,
            source_id=provision.source_id,
            source_format=MAINE_REVISED_STATUTES_SOURCE_FORMAT,
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
            identifiers={
                "maine:title": provision.title,
                f"maine:{provision.kind}": provision.display_number,
                "maine:source_id": provision.source_id,
            },
            metadata=metadata,
        )
    )
    return True


def _title_provision(title: MaineTitle, *, heading: str) -> MaineProvision:
    return MaineProvision(
        kind="title",
        title=title.number,
        source_id=title.source_id,
        display_number=title.number,
        citation_path=title.citation_path,
        legal_identifier=title.legal_identifier,
        heading=heading,
        body=None,
        parent_citation_path=None,
        level=0,
        ordinal=title.ordinal,
    )


def _part_provision(part: MainePart) -> MaineProvision:
    return MaineProvision(
        kind="part",
        title=part.title,
        source_id=part.source_id,
        display_number=part.part,
        citation_path=part.citation_path,
        legal_identifier=part.legal_identifier,
        heading=part.heading,
        body=None,
        parent_citation_path=part.parent_citation_path,
        level=1,
        ordinal=part.ordinal,
    )


def _chapter_provision(chapter: MaineChapter) -> MaineProvision:
    notes = (f"Section range: {chapter.section_range}",) if chapter.section_range else ()
    return MaineProvision(
        kind="chapter",
        title=chapter.title,
        source_id=chapter.source_id,
        display_number=chapter.display_chapter,
        citation_path=chapter.citation_path,
        legal_identifier=chapter.legal_identifier,
        heading=chapter.heading,
        body=None,
        parent_citation_path=chapter.parent_citation_path,
        level=chapter.level,
        ordinal=chapter.ordinal,
        notes=notes,
        status=chapter.status,
    )


def _section_provision(target: MaineSectionTarget, parsed: MaineParsedSection) -> MaineProvision:
    return MaineProvision(
        kind="section",
        title=target.title,
        source_id=target.source_id,
        display_number=parsed.display_section or target.display_section,
        citation_path=target.citation_path,
        legal_identifier=target.legal_identifier,
        heading=parsed.heading or target.heading,
        body=parsed.body,
        parent_citation_path=target.parent_citation_path,
        level=3,
        ordinal=target.ordinal,
        references_to=parsed.references_to,
        source_history=parsed.source_history,
        notes=parsed.notes,
        status=parsed.status or target.status,
    )


def _parse_title_heading(soup: BeautifulSoup, title: MaineTitle) -> str:
    node = soup.select_one(".title_heading div")
    text = _clean_text(node.get_text(" ", strip=True)) if node else ""
    match = _TITLE_RE.match(text)
    return _clean_heading(match.group("heading")) if match else title.heading


def _parse_part_heading(text: str, *, title: MaineTitle, ordinal: int) -> MainePart | None:
    match = _PART_RE.match(text)
    if not match:
        return None
    return MainePart(
        title=title.number,
        part=_clean_title(match.group("part")),
        heading=_clean_heading(match.group("heading")),
        parent_citation_path=title.citation_path,
        ordinal=ordinal,
    )


def _parse_chapter_node(
    node: Tag,
    *,
    title: MaineTitle,
    parent_citation_path: str,
    level: int,
    ordinal: int,
) -> MaineChapter | None:
    link = node.select_one("a[href]")
    if link is None:
        return None
    link_text = _clean_text(link.get_text(" ", strip=True))
    match = _CHAPTER_RE.search(link_text)
    href = link.get("href", "")
    chapter_id = _chapter_id_from_href(href, title.relative_path)
    if match is None and chapter_id is None:
        return None
    display_chapter = (
        _clean_title(match.group("chapter")) if match is not None else str(chapter_id)
    )
    heading = _clean_heading(match.group("heading")) if match is not None else link_text
    return MaineChapter(
        title=title.number,
        chapter_id=chapter_id or display_chapter,
        display_chapter=display_chapter,
        heading=heading,
        relative_path=_relative_from_href(href, title.relative_path),
        parent_citation_path=parent_citation_path,
        level=level,
        ordinal=ordinal,
        section_range=_section_range_from_node(node),
        status=_status_from_text(link_text, node),
    )


def _parse_section_heading(text: str) -> tuple[str | None, str | None]:
    match = re.match(r"^\u00a7(?P<section>[0-9A-Z-]+)\.\s*(?P<heading>.+)$", text, re.I)
    if not match:
        return None, _clean_heading(text) if text else None
    return _clean_section(match.group("section")), _clean_heading(match.group("heading"))


def _references_from_links(node: Tag, *, target: MaineSectionTarget | None) -> tuple[str, ...]:
    refs: list[str] = []
    own = target.citation_path if target is not None else None
    for link in node.select("a[href]"):
        href = link.get("href", "")
        parsed = _section_ref_from_href(href, current_path=target.relative_path if target else "")
        if parsed is None or parsed == own:
            continue
        refs.append(parsed)
    return tuple(dict.fromkeys(refs))


def _section_ref_from_href(href: str, *, current_path: str) -> str | None:
    relative = _relative_from_href(href, current_path or MAINE_REVISED_STATUTES_INDEX)
    name = Path(relative).name
    match = _SECTION_PAGE_RE.search(name)
    if not match:
        return None
    return f"us-me/statute/{_clean_title(match.group('title'))}/{_clean_section(match.group('section'))}"


def _chapter_id_from_href(href: str, current_path: str) -> str | None:
    name = Path(_relative_from_href(href, current_path)).name
    match = _CHAPTER_PAGE_RE.search(name)
    if not match:
        return None
    variant = match.group("variant") or ""
    return _clean_title(f"{match.group('chapter')}{variant}")


def _section_id_from_href(href: str, current_path: str) -> str | None:
    name = Path(_relative_from_href(href, current_path)).name
    match = _SECTION_PAGE_RE.search(name)
    return _clean_section(match.group("section")) if match else None


def _section_range_from_node(node: Tag) -> str | None:
    link = node.select_one("a")
    text = _clean_text(node.get_text(" ", strip=True))
    if link is not None:
        text = text.replace(_clean_text(link.get_text(" ", strip=True)), "", 1).strip()
    text = text.lstrip("\u00a0 ").strip()
    return text or None


def _status_from_text(text: str, node: Tag | None = None) -> str | None:
    values = [text]
    if node is not None:
        values.extend(str(value) for value in node.get("class", []))
    joined = " ".join(values).lower()
    if "repealed" in joined:
        return "repealed"
    if "effective until" in joined:
        return "effective-until"
    return None


def _section_status(status_notes: list[str], fallback: str | None) -> str | None:
    joined = " ".join(status_notes).lower()
    if "repealed" in joined:
        return "repealed"
    if "effective until" in joined:
        return "effective-until"
    return fallback


def _metadata(provision: MaineProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "title": provision.title,
        "display_number": provision.display_number,
    }
    if provision.parent_citation_path:
        metadata["parent_citation_path"] = provision.parent_citation_path
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.source_history:
        metadata["source_history"] = list(provision.source_history)
    if provision.notes:
        metadata["notes"] = list(provision.notes)
    if provision.status:
        metadata["status"] = provision.status
    return metadata


def _relative_from_href(href: str, current_path: str) -> str:
    href = href.strip()
    if not href:
        return _normalize_relative_path(current_path)
    parsed = urlparse(href)
    if parsed.scheme and parsed.netloc:
        path = parsed.path.lstrip("/")
        if path.startswith("statutes/"):
            path = path[len("statutes/") :]
        return _normalize_relative_path(path)
    base = f"https://example.test/{_normalize_relative_path(current_path)}"
    path = urlparse(urljoin(base, href)).path.lstrip("/")
    return _normalize_relative_path(path)


def _normalize_relative_path(value: str) -> str:
    normalized = value.replace("\\", "/").strip().lstrip("/")
    if normalized in {"", ".", "./"}:
        return MAINE_REVISED_STATUTES_INDEX
    parts = [part for part in normalized.split("/") if part not in {"", "."}]
    out: list[str] = []
    for part in parts:
        if part == "..":
            if out:
                out.pop()
            continue
        out.append(part)
    return "/".join(out)


def _read_source_file(source_dir: Path, relative_path: str) -> bytes:
    candidates = [
        source_dir / relative_path,
        source_dir / MAINE_REVISED_STATUTES_SOURCE_FORMAT / relative_path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.read_bytes()
    raise FileNotFoundError(f"missing Maine source file: {relative_path}")


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _base_url(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _maine_run_id(
    version: str,
    *,
    only_title: str | None,
    only_chapter: str | None,
    limit: int | None,
) -> str:
    if only_title is None and only_chapter is None and limit is None:
        return version
    parts = [version, "us-me"]
    if only_title is not None:
        parts.append(f"title-{_slug(only_title)}")
    if only_chapter is not None:
        parts.append(f"chapter-{_slug(only_chapter)}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _optional_filter(value: str | int | None) -> str | None:
    return _clean_title(str(value)) if value is not None else None


def _same_filter(value: str, expected: str) -> bool:
    return _slug(value) == _slug(expected)


def _clean_title(value: str) -> str:
    return value.strip().upper()


def _clean_section(value: str) -> str:
    return value.strip().upper()


def _clean_heading(value: str) -> str:
    return _clean_text(value).strip(" :.")


def _clean_text(value: str) -> str:
    text = value.replace("\xa0", " ").replace("\u2011", "-")
    text = text.replace("\u2013", "-").replace("\u2014", "--")
    return re.sub(r"\s+", " ", text).strip()


def _slug(value: str) -> str:
    return re.sub(r"[^0-9a-z]+", "-", value.lower()).strip("-")


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value
