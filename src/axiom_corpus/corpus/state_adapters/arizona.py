"""Arizona Revised Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

ARIZONA_ARS_BASE_URL = "https://www.azleg.gov"
ARIZONA_TITLE_INDEX_URL = f"{ARIZONA_ARS_BASE_URL}/arstitle/"
ARIZONA_TITLE_INDEX_SOURCE_FORMAT = "arizona-ars-title-index-html"
ARIZONA_TITLE_DETAIL_SOURCE_FORMAT = "arizona-ars-title-detail-html"
ARIZONA_SECTION_SOURCE_FORMAT = "arizona-ars-section-html"
ARIZONA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_SECTION_RE = re.compile(r"^(?P<title>\d+)-(?P<section>[0-9A-Z]+(?:\.[0-9A-Z]+)?)$", re.I)
_SECTION_REFERENCE_RE = re.compile(
    r"(?:A\.?R\.?S\.?|ARS|sections?|section|\u00a7+)\s+"
    r"(?P<section>\d{1,2}-[0-9A-Z]+(?:\.[0-9A-Z]+)?)",
    re.I,
)
_HREF_REFERENCE_RE = re.compile(r"/ars/\d+/[0-9A-Z]+\.htm", re.I)


@dataclass(frozen=True)
class ArizonaTitle:
    """One Arizona Revised Statutes title from the official title index."""

    number: str
    heading: str
    source_url: str | None
    ordinal: int
    repealed: bool = False

    @property
    def citation_path(self) -> str:
        return f"us-az/statute/title-{self.number}"

    @property
    def source_id(self) -> str:
        return f"title-{self.number}"

    @property
    def legal_identifier(self) -> str:
        return f"Arizona Revised Statutes Title {self.number}"


@dataclass(frozen=True)
class ArizonaChapter:
    """One chapter listed in an official Arizona title detail page."""

    title_number: str
    chapter: str
    heading: str
    ordinal: int
    section_range: str | None = None

    @property
    def citation_path(self) -> str:
        return f"us-az/statute/title-{self.title_number}/chapter-{self.chapter}"

    @property
    def source_id(self) -> str:
        return f"title-{self.title_number}-chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"A.R.S. Title {self.title_number}, Chapter {self.chapter}"


@dataclass(frozen=True)
class ArizonaArticle:
    """One article listed in an official Arizona title detail page."""

    title_number: str
    chapter: str
    article: str
    heading: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return (
            f"us-az/statute/title-{self.title_number}/chapter-{self.chapter}/"
            f"article-{self.article.lower()}"
        )

    @property
    def source_id(self) -> str:
        return f"title-{self.title_number}-chapter-{self.chapter}-article-{self.article.lower()}"

    @property
    def legal_identifier(self) -> str:
        return f"A.R.S. Title {self.title_number}, Chapter {self.chapter}, Article {self.article}"


@dataclass(frozen=True)
class ArizonaSectionTarget:
    """One section link from an official Arizona title detail page."""

    title_number: str
    chapter: str
    article: ArizonaArticle | None
    section: str
    heading: str
    source_url: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-az/statute/{self.section}"

    @property
    def source_id(self) -> str:
        return self.section

    @property
    def legal_identifier(self) -> str:
        return f"A.R.S. \u00a7 {self.section}"


@dataclass(frozen=True)
class ArizonaTitleDocument:
    """Parsed official Arizona title-detail page."""

    chapters: tuple[ArizonaChapter, ...]
    articles: tuple[ArizonaArticle, ...]
    sections: tuple[ArizonaSectionTarget, ...]


@dataclass(frozen=True)
class ArizonaParsedSection:
    """Parsed Arizona section body."""

    heading: str
    body: str | None
    references_to: tuple[str, ...]
    status: str | None = None


@dataclass(frozen=True)
class _ArizonaSource:
    relative_path: str
    source_url: str
    source_format: str
    data: bytes


@dataclass(frozen=True)
class _RecordedSource:
    source_url: str
    source_path: str
    source_format: str
    sha256: str


@dataclass(frozen=True)
class _ArizonaSectionFetchResult:
    target: ArizonaSectionTarget
    source: _ArizonaSource | None = None
    parsed: ArizonaParsedSection | None = None
    error: BaseException | None = None


class _ArizonaFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        base_url: str,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.base_url = base_url.rstrip("/")
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_title_index(self) -> _ArizonaSource:
        relative_path = f"{ARIZONA_TITLE_INDEX_SOURCE_FORMAT}/index.html"
        data = self._fetch(relative_path, f"{self.base_url}/arstitle/")
        return _ArizonaSource(
            relative_path=relative_path,
            source_url=f"{self.base_url}/arstitle/",
            source_format=ARIZONA_TITLE_INDEX_SOURCE_FORMAT,
            data=data,
        )

    def fetch_title_detail(self, title: ArizonaTitle) -> _ArizonaSource:
        relative_path = f"{ARIZONA_TITLE_DETAIL_SOURCE_FORMAT}/title-{title.number}.html"
        source_url = title.source_url or f"{self.base_url}/arsDetail?title={title.number}"
        data = self._fetch(relative_path, source_url)
        return _ArizonaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=ARIZONA_TITLE_DETAIL_SOURCE_FORMAT,
            data=data,
        )

    def fetch_section(self, target: ArizonaSectionTarget) -> _ArizonaSource:
        relative_path = (
            f"{ARIZONA_SECTION_SOURCE_FORMAT}/title-{target.title_number}/"
            f"{_section_file_stem(target.section)}.html"
        )
        data = self._fetch(relative_path, target.source_url)
        return _ArizonaSource(
            relative_path=relative_path,
            source_url=target.source_url,
            source_format=ARIZONA_SECTION_SOURCE_FORMAT,
            data=data,
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_arizona_source(
            source_url,
            fetcher=self,
            request_delay_seconds=self.request_delay_seconds,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
        )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            _write_cache_bytes(cached_path, data)
        return data

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            wait_seconds = self.request_delay_seconds - elapsed
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self._last_request_at = time.monotonic()


def extract_arizona_revised_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = ARIZONA_ARS_BASE_URL,
    request_delay_seconds: float = 0.03,
    timeout_seconds: float = 30.0,
    request_attempts: int = 3,
    workers: int = 8,
) -> StateStatuteExtractReport:
    """Snapshot official Arizona Revised Statutes HTML and extract provisions."""
    jurisdiction = "us-az"
    title_filter = _title_filter(only_title)
    run_id = _arizona_run_id(version, title_filter=title_filter, limit=limit)
    source_as_of_text = source_as_of or str(version)
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _ArizonaFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        base_url=base_url,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    errors: list[str] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0

    title_index_source = fetcher.fetch_title_index()
    title_index_path, title_index_recorded = _record_source(
        store,
        jurisdiction,
        run_id,
        title_index_source,
    )
    source_paths.append(title_index_path)
    titles = parse_arizona_title_index(title_index_source.data, base_url=base_url)
    if title_filter is not None:
        titles = tuple(title for title in titles if title.number == title_filter)

    for title in titles:
        if limit is not None and section_count >= limit:
            break
        if title.citation_path not in seen:
            seen.add(title.citation_path)
            title_count += 1
            _append_record(
                items,
                records,
                jurisdiction=jurisdiction,
                citation_path=title.citation_path,
                version=run_id,
                source_url=title.source_url or title_index_source.source_url,
                source_path=title_index_recorded.source_path,
                source_format=title_index_recorded.source_format,
                source_id=title.source_id,
                sha256=title_index_recorded.sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="title",
                body=None,
                heading=title.heading,
                legal_identifier=title.legal_identifier,
                parent_citation_path=None,
                level=0,
                ordinal=title.ordinal,
                identifiers={"arizona:title": title.number},
                metadata={
                    "kind": "title",
                    "title": title.number,
                    "repealed": title.repealed,
                },
            )
        if title.source_url is None:
            continue

        title_source = fetcher.fetch_title_detail(title)
        title_path, title_recorded = _record_source(store, jurisdiction, run_id, title_source)
        source_paths.append(title_path)
        document = parse_arizona_title_detail(title_source.data, title=title, base_url=base_url)

        for chapter in document.chapters:
            if chapter.citation_path in seen:
                continue
            seen.add(chapter.citation_path)
            container_count += 1
            _append_record(
                items,
                records,
                jurisdiction=jurisdiction,
                citation_path=chapter.citation_path,
                version=run_id,
                source_url=title_source.source_url,
                source_path=title_recorded.source_path,
                source_format=title_recorded.source_format,
                source_id=chapter.source_id,
                sha256=title_recorded.sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="chapter",
                body=None,
                heading=chapter.heading,
                legal_identifier=chapter.legal_identifier,
                parent_citation_path=title.citation_path,
                level=1,
                ordinal=chapter.ordinal,
                identifiers={
                    "arizona:title": title.number,
                    "arizona:chapter": chapter.chapter,
                },
                metadata={
                    "kind": "chapter",
                    "title": title.number,
                    "chapter": chapter.chapter,
                    **({"section_range": chapter.section_range} if chapter.section_range else {}),
                },
            )

        for article in document.articles:
            if article.citation_path in seen:
                continue
            seen.add(article.citation_path)
            container_count += 1
            _append_record(
                items,
                records,
                jurisdiction=jurisdiction,
                citation_path=article.citation_path,
                version=run_id,
                source_url=title_source.source_url,
                source_path=title_recorded.source_path,
                source_format=title_recorded.source_format,
                source_id=article.source_id,
                sha256=title_recorded.sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="article",
                body=None,
                heading=article.heading,
                legal_identifier=article.legal_identifier,
                parent_citation_path=(
                    f"us-az/statute/title-{title.number}/chapter-{article.chapter}"
                ),
                level=2,
                ordinal=article.ordinal,
                identifiers={
                    "arizona:title": title.number,
                    "arizona:chapter": article.chapter,
                    "arizona:article": article.article,
                },
                metadata={
                    "kind": "article",
                    "title": title.number,
                    "chapter": article.chapter,
                    "article": article.article,
                },
            )

        selected_sections = [
            section
            for section in document.sections
            if limit is None or section_count < limit
        ]
        if limit is not None:
            selected_sections = selected_sections[: max(0, limit - section_count)]
        for result in _fetch_arizona_section_results(
            fetcher,
            selected_sections,
            workers=workers,
        ):
            target = result.target
            if result.error is not None:
                errors.append(f"section {target.section}: {result.error}")
                continue
            assert result.source is not None
            assert result.parsed is not None
            if target.citation_path in seen:
                errors.append(f"duplicate citation path: {target.citation_path}")
                continue
            section_path, section_recorded = _record_source(
                store,
                jurisdiction,
                run_id,
                result.source,
            )
            source_paths.append(section_path)
            seen.add(target.citation_path)
            section_count += 1
            parent = (
                target.article.citation_path
                if target.article is not None
                else f"us-az/statute/title-{title.number}/chapter-{target.chapter}"
            )
            _append_record(
                items,
                records,
                jurisdiction=jurisdiction,
                citation_path=target.citation_path,
                version=run_id,
                source_url=result.source.source_url,
                source_path=section_recorded.source_path,
                source_format=section_recorded.source_format,
                source_id=target.source_id,
                sha256=section_recorded.sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="section",
                body=result.parsed.body,
                heading=result.parsed.heading or target.heading,
                legal_identifier=target.legal_identifier,
                parent_citation_path=parent,
                level=3 if target.article is not None else 2,
                ordinal=target.ordinal,
                identifiers={
                    "arizona:title": title.number,
                    "arizona:chapter": target.chapter,
                    "arizona:section": target.section,
                },
                metadata={
                    "kind": "section",
                    "title": title.number,
                    "chapter": target.chapter,
                    "section": target.section,
                    **(
                        {"article": target.article.article}
                        if target.article is not None
                        else {}
                    ),
                    **({"status": result.parsed.status} if result.parsed.status else {}),
                    "references_to": list(result.parsed.references_to),
                },
            )
        if limit is not None and section_count >= limit:
            break

    if not records:
        raise ValueError("no Arizona provisions extracted")

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


def parse_arizona_title_index(
    html: str | bytes,
    *,
    base_url: str = ARIZONA_ARS_BASE_URL,
) -> tuple[ArizonaTitle, ...]:
    """Parse the official Arizona title index."""
    soup = BeautifulSoup(_decode(html), "lxml")
    rows = soup.select("#arsTable tr")
    titles: list[ArizonaTitle] = []
    ordinal = 0
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        title_text = _clean_text(cells[1])
        title_match = re.search(r"Title\s+(?P<title>\d+)", title_text, re.I)
        if title_match is None:
            continue
        ordinal += 1
        number = title_match.group("title")
        heading = _strip_terminal_period(_clean_text(cells[2]))
        link = cells[1].find("a", href=True)
        source_url = urljoin(base_url, str(link["href"])) if isinstance(link, Tag) else None
        titles.append(
            ArizonaTitle(
                number=number,
                heading=heading,
                source_url=source_url,
                ordinal=ordinal,
                repealed="REPEALED" in heading.upper(),
            )
        )
    return tuple(titles)


def parse_arizona_title_detail(
    html: str | bytes,
    *,
    title: ArizonaTitle,
    base_url: str = ARIZONA_ARS_BASE_URL,
) -> ArizonaTitleDocument:
    """Parse one official Arizona title-detail page."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapters: list[ArizonaChapter] = []
    articles: list[ArizonaArticle] = []
    sections: list[ArizonaSectionTarget] = []
    section_ordinal = 0
    for chapter_node in soup.select("div.accordion"):
        h5 = chapter_node.find("h5")
        if not isinstance(h5, Tag):
            continue
        chapter_link = h5.find("a")
        chapter_match = re.search(r"Chapter\s+(?P<chapter>[0-9A-Z]+)", _clean_text(chapter_link or ""))
        if chapter_match is None:
            continue
        chapter = chapter_match.group("chapter")
        heading_parts = [
            _clean_text(child)
            for child in h5.find_all("div")
            if _clean_text(child) and not _clean_text(child).startswith("Sec:")
        ]
        section_range = None
        for child in h5.find_all("div"):
            child_text = _clean_text(child)
            if child_text.startswith("Sec:"):
                section_range = child_text.removeprefix("Sec:").strip()
        heading = heading_parts[0] if heading_parts else f"Chapter {chapter}"
        chapters.append(
            ArizonaChapter(
                title_number=title.number,
                chapter=chapter,
                heading=heading,
                ordinal=len(chapters) + 1,
                section_range=section_range,
            )
        )
        for article_node in chapter_node.select("div.article"):
            article_link = article_node.find("a")
            article_match = re.search(
                r"Article\s+(?P<article>[0-9A-Z]+)",
                _clean_text(article_link or ""),
                re.I,
            )
            article: ArizonaArticle | None = None
            if article_match is not None:
                heading_node = article_node.find("span")
                article = ArizonaArticle(
                    title_number=title.number,
                    chapter=chapter,
                    article=article_match.group("article").upper(),
                    heading=(
                        _strip_terminal_period(_clean_text(heading_node))
                        if isinstance(heading_node, Tag)
                        else ""
                    ),
                    ordinal=len(articles) + 1,
                )
                articles.append(article)
            for row in article_node.find_all("ul"):
                stat_link = row.find("a", class_="stat", href=True)
                if not isinstance(stat_link, Tag):
                    continue
                label = _clean_text(stat_link)
                if _SECTION_RE.match(label) is None:
                    continue
                href = str(stat_link["href"])
                section_url = _section_source_url(href, base_url=base_url)
                if section_url is None:
                    continue
                heading_node = row.find("li", class_="colright")
                section_ordinal += 1
                sections.append(
                    ArizonaSectionTarget(
                        title_number=title.number,
                        chapter=chapter,
                        article=article,
                        section=label.upper(),
                        heading=_strip_terminal_period(_clean_text(heading_node or "")),
                        source_url=section_url,
                        ordinal=section_ordinal,
                    )
                )
    return ArizonaTitleDocument(
        chapters=tuple(chapters),
        articles=tuple(articles),
        sections=tuple(sections),
    )


def parse_arizona_section(html: str | bytes, *, fallback_heading: str | None = None) -> ArizonaParsedSection:
    """Parse one official printable Arizona section HTML document."""
    soup = BeautifulSoup(_decode(html), "lxml")
    title_text = _clean_text(soup.find("title") or "")
    title_heading = ""
    if " - " in title_text:
        title_heading = title_text.split(" - ", 1)[1]
    body = soup.find("body")
    paragraphs = body.find_all("p", recursive=False) if isinstance(body, Tag) else soup.find_all("p")
    heading = fallback_heading or title_heading
    body_parts: list[str] = []
    for index, paragraph in enumerate(paragraphs):
        text = _clean_text(paragraph)
        if not text:
            continue
        if index == 0 and _SECTION_RE.match(text.split(".", 1)[0].strip()):
            heading_node = paragraph.find("u")
            if isinstance(heading_node, Tag):
                heading = _clean_text(heading_node)
            continue
        body_parts.append(text)
    body_text = _normalize_body("\n\n".join(body_parts))
    status = "repealed" if _is_repealed(heading, body_text) else None
    return ArizonaParsedSection(
        heading=_strip_terminal_period(heading),
        body=body_text,
        references_to=tuple(_extract_references(_decode(html))),
        status=status,
    )


def _fetch_arizona_section_results(
    fetcher: _ArizonaFetcher,
    sections: list[ArizonaSectionTarget],
    *,
    workers: int,
) -> list[_ArizonaSectionFetchResult]:
    if not sections:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_arizona_section_result(fetcher, section) for section in sections]
    results: list[_ArizonaSectionFetchResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_arizona_section_result, fetcher, section): section
            for section in sections
        }
        for future in as_completed(future_map):
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(
                    _ArizonaSectionFetchResult(target=future_map[future], error=exc)
                )
    order = {section.citation_path: index for index, section in enumerate(sections)}
    return sorted(results, key=lambda result: order[result.target.citation_path])


def _fetch_arizona_section_result(
    fetcher: _ArizonaFetcher,
    section: ArizonaSectionTarget,
) -> _ArizonaSectionFetchResult:
    try:
        source = fetcher.fetch_section(section)
        parsed = parse_arizona_section(source.data, fallback_heading=section.heading)
        return _ArizonaSectionFetchResult(target=section, source=source, parsed=parsed)
    except BaseException as exc:  # pragma: no cover
        return _ArizonaSectionFetchResult(target=section, error=exc)


def _append_record(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    *,
    jurisdiction: str,
    citation_path: str,
    version: str,
    source_url: str,
    source_path: str,
    source_format: str,
    source_id: str,
    sha256: str,
    source_as_of: str,
    expression_date: str,
    kind: str,
    body: str | None,
    heading: str | None,
    legal_identifier: str,
    parent_citation_path: str | None,
    level: int,
    ordinal: int | None,
    identifiers: dict[str, str],
    metadata: dict[str, Any],
) -> None:
    items.append(
        SourceInventoryItem(
            citation_path=citation_path,
            source_url=source_url,
            source_path=source_path,
            source_format=source_format,
            sha256=sha256,
            metadata=metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(citation_path),
            jurisdiction=jurisdiction,
            document_class=DocumentClass.STATUTE.value,
            citation_path=citation_path,
            body=body,
            heading=heading,
            citation_label=legal_identifier,
            version=version,
            source_url=source_url,
            source_path=source_path,
            source_id=source_id,
            source_format=source_format,
            source_as_of=source_as_of,
            expression_date=expression_date,
            parent_citation_path=parent_citation_path,
            parent_id=(
                deterministic_provision_id(parent_citation_path)
                if parent_citation_path
                else None
            ),
            level=level,
            ordinal=ordinal,
            kind=kind,
            legal_identifier=legal_identifier,
            identifiers=identifiers,
            metadata=metadata,
        )
    )


def _record_source(
    store: CorpusArtifactStore,
    jurisdiction: str,
    run_id: str,
    source: _ArizonaSource,
) -> tuple[Path, _RecordedSource]:
    path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        source.relative_path,
    )
    sha = store.write_bytes(path, source.data)
    return path, _RecordedSource(
        source_url=source.source_url,
        source_path=_store_relative_path(store, path),
        source_format=source.source_format,
        sha256=sha,
    )


def _download_arizona_source(
    source_url: str,
    *,
    fetcher: _ArizonaFetcher,
    request_delay_seconds: float,
    timeout_seconds: float,
    request_attempts: int,
) -> bytes:
    last_error: BaseException | None = None
    for attempt in range(1, request_attempts + 1):
        try:
            fetcher.wait_for_request_slot()
            result = subprocess.run(
                [
                    "curl",
                    "--fail",
                    "--location",
                    "--silent",
                    "--show-error",
                    "--max-time",
                    str(max(1, int(timeout_seconds))),
                    "--user-agent",
                    ARIZONA_USER_AGENT,
                    source_url,
                ],
                check=False,
                capture_output=True,
                timeout=timeout_seconds + 5,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr.decode("utf-8", errors="replace"))
            return result.stdout
        except (RuntimeError, subprocess.SubprocessError) as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds, 0.25) * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"Arizona source request failed: {source_url}")


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _section_source_url(href: str, *, base_url: str) -> str | None:
    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)
    if parsed.path.lower().endswith(".htm") and "/ars/" in parsed.path.lower():
        return absolute
    query = parse_qs(parsed.query)
    doc_names = query.get("docName") or query.get("docname")
    if not doc_names:
        return None
    doc_name = doc_names[0]
    if "/ars/" not in doc_name.lower() or not doc_name.lower().endswith(".htm"):
        return None
    return doc_name


def _section_file_stem(section: str) -> str:
    title, rest = section.split("-", 1)
    return f"{int(title)}-{rest.lower()}"


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title|Title)[-\s]*", "", text)
    return str(int(text)) if text.isdigit() else text.upper()


def _arizona_run_id(version: str, *, title_filter: str | None, limit: int | None) -> str:
    if title_filter is None and limit is None:
        return version
    parts = [version, "us-az"]
    if title_filter is not None:
        parts.append(f"title-{title_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _is_repealed(heading: str, body: str | None) -> bool:
    text = f"{heading}\n{body or ''}"
    return bool(re.search(r"\b(repealed|renumbered)\b", text, re.I))


def _extract_references(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    refs: list[str] = []
    text = soup.get_text(" ", strip=True)
    refs.extend(
        f"us-az/statute/{match.group('section').upper()}"
        for match in _SECTION_REFERENCE_RE.finditer(text)
    )
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        if _HREF_REFERENCE_RE.search(href):
            parsed = urlparse(href)
            parts = parsed.path.strip("/").split("/")
            if len(parts) >= 3:
                title = parts[-2]
                section = parts[-1].removesuffix(".htm")
                refs.append(f"us-az/statute/{int(title)}-{section.lstrip('0').upper()}")
    return _dedupe_preserve_order(refs)


def _normalize_body(text: str) -> str | None:
    normalized = text.replace("\xa0", " ")
    normalized = re.sub(r"[ \t\r\f\v]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    normalized = normalized.strip()
    return normalized or None


def _decode(value: str | bytes) -> str:
    if isinstance(value, str):
        return value
    return value.decode("utf-8", errors="replace")


def _clean_text(value: Any) -> str:
    text = value.get_text(" ", strip=True) if hasattr(value, "get_text") else str(value)
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _strip_terminal_period(value: str) -> str:
    return value.strip().removesuffix(".").strip()


def _store_relative_path(store: CorpusArtifactStore, path: Path) -> str:
    try:
        return path.relative_to(store.root).as_posix()
    except ValueError:
        return path.as_posix()


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
