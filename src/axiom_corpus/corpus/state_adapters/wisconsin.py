"""Wisconsin Statutes source-first corpus adapter."""

from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

WISCONSIN_STATUTES_BASE_URL = "https://docs.legis.wisconsin.gov"
WISCONSIN_STATUTES_TOC_PATH = "/statutes/prefaces/toc"
WISCONSIN_STATUTES_TOC_URL = f"{WISCONSIN_STATUTES_BASE_URL}{WISCONSIN_STATUTES_TOC_PATH}"
WISCONSIN_STATUTES_SOURCE_FORMAT = "wisconsin-statutes-html"
WISCONSIN_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_CHAPTER_RE = re.compile(r"^(?P<label>\d+[A-Z]?)\.\s*(?P<heading>.+)$", re.I)
_CHAPTER_TITLE_RE = re.compile(r"^Statutes\s+ch\.\s+(?P<label>\d+[A-Z]?)$", re.I)
_SUBCHAPTER_CITE_RE = re.compile(
    r"^statutes/subch\.\s+(?P<label>[IVXLCDM]+)\s+of\s+ch\.\s+(?P<chapter>\d+[A-Z]?)$",
    re.I,
)
_SECTION_CITE_RE = re.compile(r"^statutes/(?P<label>\d+[A-Z]?\.\d+[A-Z0-9.]*)", re.I)
_SECTION_TEXT_RE = re.compile(r"\b(?P<label>\d+[A-Z]?\.\d+[A-Z0-9.]*)(?:\s*\([^)]+\))*\b", re.I)


@dataclass(frozen=True)
class WisconsinSource:
    """One official Wisconsin Legislature source page stored in the corpus."""

    source_url: str
    source_path: str
    source_format: str
    sha256: str
    source_document_id: str


@dataclass(frozen=True)
class WisconsinChapterLink:
    """One chapter discovered from the official Wisconsin Statutes TOC."""

    label: str
    heading: str
    source_url: str
    relative_path: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-wi/statute/chapter-{_path_segment(self.label)}"

    @property
    def legal_identifier(self) -> str:
        return f"Wis. Stat. ch. {self.label}"


@dataclass(frozen=True)
class WisconsinSubchapter:
    """One subchapter within a Wisconsin Statutes chapter."""

    chapter: str
    label: str
    heading: str | None
    ordinal: int
    source: WisconsinSource

    @property
    def citation_path(self) -> str:
        return f"us-wi/statute/chapter-{_path_segment(self.chapter)}/subchapter-{_path_segment(self.label)}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-wi/statute/chapter-{_path_segment(self.chapter)}"

    @property
    def legal_identifier(self) -> str:
        return f"Wis. Stat. ch. {self.chapter}, subch. {self.label}"


@dataclass
class _WisconsinSectionBuilder:
    label: str
    heading: str | None
    chapter: str
    subchapter_label: str | None
    subchapter_heading: str | None
    parent_citation_path: str
    level: int
    ordinal: int
    source: WisconsinSource
    lines: list[str] = field(default_factory=list)
    history: list[str] = field(default_factory=list)
    references_to: list[str] = field(default_factory=list)
    _reference_seen: set[str] = field(default_factory=set)

    @property
    def citation_path(self) -> str:
        return f"us-wi/statute/{self.label}"

    @property
    def legal_identifier(self) -> str:
        return f"Wis. Stat. {self.label}"

    def add_line(self, line: str | None) -> None:
        if line:
            self.lines.append(line)

    def add_history(self, text: str | None) -> None:
        if text and text not in self.history:
            self.history.append(text)

    def add_reference(self, label: str) -> None:
        target = f"us-wi/statute/{label}"
        if target == self.citation_path or target in self._reference_seen:
            return
        self._reference_seen.add(target)
        self.references_to.append(target)


@dataclass(frozen=True)
class _SourcePage:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _FetchResult:
    key: str
    page: _SourcePage | None = None
    error: BaseException | None = None


class _WisconsinFetcher:
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
        self._last_request_at = 0.0
        self._request_lock = Lock()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": WISCONSIN_USER_AGENT})

    def fetch(self, source_url: str, relative_path: str) -> _SourcePage:
        data = self._fetch_bytes(source_url, relative_path)
        return _SourcePage(relative_path=relative_path, source_url=source_url, data=data)

    def wait_for_request_slot(self) -> None:
        if self.request_delay_seconds <= 0:
            return
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            self._last_request_at = time.monotonic()

    def _fetch_bytes(self, source_url: str, relative_path: str) -> bytes:
        if self.source_dir is not None:
            path = self.source_dir / relative_path
            if path.exists():
                return path.read_bytes()
        if self.download_dir is not None:
            path = self.download_dir / relative_path
            if path.exists():
                return path.read_bytes()
        data = _download_wisconsin_source(source_url, fetcher=self)
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return data


def extract_wisconsin_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_url: str = WISCONSIN_STATUTES_TOC_URL,
    base_url: str = WISCONSIN_STATUTES_BASE_URL,
    source_as_of: str | None = None,
    expression_date: str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 90.0,
    request_attempts: int = 3,
    workers: int = 8,
) -> StateStatuteExtractReport:
    """Snapshot official Wisconsin Statutes HTML and extract normalized provisions."""
    jurisdiction = "us-wi"
    chapter_filter = _normalize_chapter_filter(only_title)
    run_id = _wisconsin_run_id(version, only_title=chapter_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = expression_date or source_as_of_text
    fetcher = _WisconsinFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    toc_relative = _toc_relative_path()
    toc_page = fetcher.fetch(source_url, toc_relative)
    source_paths: list[Path] = []
    records: list[ProvisionRecord] = []
    items: list[SourceInventoryItem] = []
    seen: set[str] = set()

    toc_source = _write_source(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        page=toc_page,
        source_paths=source_paths,
        source_document_id="statutes-toc",
    )
    publication_note = extract_wisconsin_publication_note(toc_page.data)
    chapter_links = [
        chapter
        for chapter in parse_wisconsin_chapter_links(
            toc_page.data,
            base_url=fetcher.base_url,
        )
        if chapter_filter is None or _normalize_chapter_filter(chapter.label) == chapter_filter
    ]
    if not chapter_links:
        raise ValueError(f"no Wisconsin Statutes chapters selected for filter: {only_title!r}")
    if limit is not None:
        chapter_links = chapter_links[: max(0, limit)]

    chapter_pages = _fetch_chapter_pages(fetcher, chapter_links, workers=workers)
    section_count = 0
    subchapter_count = 0
    for chapter_link, chapter_page in zip(chapter_links, chapter_pages, strict=True):
        chapter_source = _write_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=chapter_page,
            source_paths=source_paths,
            source_document_id=f"chapter-{chapter_link.label}",
        )
        _append_chapter_record(
            chapter_link,
            source=toc_source,
            seen=seen,
            records=records,
            items=items,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            publication_note=publication_note,
        )
        subchapters, sections = parse_wisconsin_chapter_page(
            chapter_page.data,
            chapter=chapter_link,
            source=chapter_source,
        )
        for subchapter in subchapters:
            _append_subchapter_record(
                subchapter,
                seen=seen,
                records=records,
                items=items,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                publication_note=publication_note,
            )
        for section in sections:
            _append_section_record(
                section,
                seen=seen,
                records=records,
                items=items,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                publication_note=publication_note,
            )
        subchapter_count += len(subchapters)
        section_count += len(sections)

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
        title_count=len(chapter_links),
        container_count=subchapter_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def parse_wisconsin_chapter_links(
    html_data: str | bytes,
    *,
    base_url: str = WISCONSIN_STATUTES_BASE_URL,
) -> tuple[WisconsinChapterLink, ...]:
    """Parse statute chapter links from the official Wisconsin Statutes TOC."""
    soup = BeautifulSoup(_html_text(html_data), "lxml")
    chapters: list[WisconsinChapterLink] = []
    seen: set[str] = set()
    for entry in soup.select("div.qstoc_entry"):
        anchor = entry.find("a", href=True, title=True)
        if not isinstance(anchor, Tag):
            continue
        title_match = _CHAPTER_TITLE_RE.match(str(anchor.get("title") or ""))
        if title_match is None:
            continue
        text = _clean_text(anchor.get_text(" "))
        text_match = _CHAPTER_RE.match(text)
        if text_match is None:
            continue
        label = text_match.group("label")
        if label in seen:
            continue
        seen.add(label)
        source_url = _chapter_url(base_url, label)
        chapters.append(
            WisconsinChapterLink(
                label=label,
                heading=text_match.group("heading"),
                source_url=source_url,
                relative_path=_chapter_relative_path(label),
                ordinal=len(chapters) + 1,
            )
        )
    return tuple(chapters)


def extract_wisconsin_publication_note(html_data: str | bytes) -> str | None:
    """Extract the official publication/current-through note when present."""
    text = _clean_text(BeautifulSoup(_html_text(html_data), "lxml").get_text(" "))
    match = re.search(
        r"2023-24 Wisconsin Statutes updated through .*?\(Published\s+4-3-26\)",
        text,
        flags=re.I,
    )
    if match:
        return _clean_text(match.group(0))
    match = re.search(
        r"Updated through 2025 Wisconsin Act 103.*?April 3, 2026",
        text,
        flags=re.I,
    )
    return _clean_text(match.group(0)) if match else None


def parse_wisconsin_chapter_page(
    html_data: str | bytes,
    *,
    chapter: WisconsinChapterLink,
    source: WisconsinSource,
) -> tuple[tuple[WisconsinSubchapter, ...], tuple[_WisconsinSectionBuilder, ...]]:
    """Parse subchapters and section bodies from one official chapter HTML page."""
    soup = BeautifulSoup(_html_text(html_data), "lxml")
    document = soup.find("div", id="document")
    root = document if isinstance(document, Tag) else soup
    subchapters: list[WisconsinSubchapter] = []
    sections: list[_WisconsinSectionBuilder] = []
    by_label: dict[str, _WisconsinSectionBuilder] = {}
    current_subchapter: WisconsinSubchapter | None = None
    pending_subchapter_label: str | None = None

    for div in root.find_all("div"):
        if not isinstance(div, Tag):
            continue
        classes = set(div.get("class") or [])
        if "qsnum_subchap" in classes:
            pending_subchapter_label = _subchapter_label(div, chapter=chapter.label)
            continue
        if "qstitle_subchap" in classes and pending_subchapter_label:
            current_subchapter = WisconsinSubchapter(
                chapter=chapter.label,
                label=pending_subchapter_label,
                heading=_clean_text(div.get_text(" ")) or None,
                ordinal=len(subchapters) + 1,
                source=source,
            )
            subchapters.append(current_subchapter)
            pending_subchapter_label = None
            continue

        section_label = _clean_text(div.get("data-section"))
        if not section_label:
            continue
        if "qsatxt_1sect" in classes:
            parent = current_subchapter.citation_path if current_subchapter else chapter.citation_path
            builder = _WisconsinSectionBuilder(
                label=section_label,
                heading=_section_heading(div),
                chapter=chapter.label,
                subchapter_label=current_subchapter.label if current_subchapter else None,
                subchapter_heading=current_subchapter.heading if current_subchapter else None,
                parent_citation_path=parent,
                level=2 if current_subchapter else 1,
                ordinal=len(sections) + 1,
                source=source,
            )
            builder.add_line(_statute_text_line(div, section_intro=True))
            _collect_references(div, section=builder)
            by_label[section_label] = builder
            sections.append(builder)
            continue
        builder = by_label.get(section_label)
        if builder is None:
            continue
        if any(item.startswith("qsatxt_") for item in classes):
            builder.add_line(_statute_text_line(div, section_intro=False))
            _collect_references(div, section=builder)
        elif "qsnote_history" in classes:
            builder.add_history(_note_text(div))
            _collect_references(div, section=builder)
    return tuple(subchapters), tuple(sections)


def _fetch_chapter_pages(
    fetcher: _WisconsinFetcher,
    chapters: list[WisconsinChapterLink],
    *,
    workers: int,
) -> tuple[_SourcePage, ...]:
    if workers <= 1 or len(chapters) <= 1:
        results = [
            _FetchResult(
                chapter.source_url,
                page=fetcher.fetch(chapter.source_url, chapter.relative_path),
            )
            for chapter in chapters
        ]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(fetcher.fetch, chapter.source_url, chapter.relative_path): chapter
                for chapter in chapters
            }
            for future in as_completed(futures):
                chapter = futures[future]
                try:
                    results.append(_FetchResult(chapter.source_url, page=future.result()))
                except BaseException as exc:
                    results.append(_FetchResult(chapter.source_url, error=exc))
    errors = [f"{result.key}: {result.error}" for result in results if result.error]
    if errors:
        raise RuntimeError("; ".join(errors[:5]))
    by_url = {result.key: result.page for result in results if result.page is not None}
    return tuple(by_url[chapter.source_url] for chapter in chapters if chapter.source_url in by_url)


def _download_wisconsin_source(source_url: str, *, fetcher: _WisconsinFetcher) -> bytes:
    last_error: BaseException | None = None
    for attempt in range(1, fetcher.request_attempts + 1):
        try:
            fetcher.wait_for_request_slot()
            response = fetcher._session.get(source_url, timeout=fetcher.timeout_seconds)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt < fetcher.request_attempts:
                time.sleep(_retry_delay(exc, attempt=attempt))
    if last_error is not None:
        raise last_error
    raise ValueError(f"Wisconsin source request failed: {source_url}")


def _write_source(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    page: _SourcePage,
    source_paths: list[Path],
    source_document_id: str,
) -> WisconsinSource:
    path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, page.relative_path)
    sha256 = store.write_bytes(path, page.data)
    source_paths.append(path)
    return WisconsinSource(
        source_url=page.source_url,
        source_path=_store_relative_path(store, path),
        source_format=WISCONSIN_STATUTES_SOURCE_FORMAT,
        sha256=sha256,
        source_document_id=source_document_id,
    )


def _append_chapter_record(
    chapter: WisconsinChapterLink,
    *,
    source: WisconsinSource,
    seen: set[str],
    records: list[ProvisionRecord],
    items: list[SourceInventoryItem],
    version: str,
    source_as_of: str,
    expression_date: str,
    publication_note: str | None,
) -> None:
    metadata = _base_metadata(source, publication_note=publication_note)
    _append_record(
        records,
        items,
        seen=seen,
        record=ProvisionRecord(
            id=deterministic_provision_id(chapter.citation_path),
            jurisdiction="us-wi",
            document_class=DocumentClass.STATUTE.value,
            citation_path=chapter.citation_path,
            heading=chapter.heading,
            citation_label=chapter.legal_identifier,
            version=version,
            source_url=source.source_url,
            source_path=source.source_path,
            source_id=f"chapter-{chapter.label}",
            source_format=source.source_format,
            source_as_of=source_as_of,
            expression_date=expression_date,
            level=0,
            ordinal=chapter.ordinal,
            kind="chapter",
            legal_identifier=chapter.legal_identifier,
            identifiers={"wisconsin:chapter": chapter.label},
            metadata=metadata,
        ),
        source=source,
        metadata=metadata,
    )


def _append_subchapter_record(
    subchapter: WisconsinSubchapter,
    *,
    seen: set[str],
    records: list[ProvisionRecord],
    items: list[SourceInventoryItem],
    version: str,
    source_as_of: str,
    expression_date: str,
    publication_note: str | None,
) -> None:
    parent_id = deterministic_provision_id(subchapter.parent_citation_path)
    metadata = _base_metadata(subchapter.source, publication_note=publication_note)
    _append_record(
        records,
        items,
        seen=seen,
        record=ProvisionRecord(
            id=deterministic_provision_id(subchapter.citation_path),
            jurisdiction="us-wi",
            document_class=DocumentClass.STATUTE.value,
            citation_path=subchapter.citation_path,
            heading=subchapter.heading,
            citation_label=subchapter.legal_identifier,
            version=version,
            source_url=subchapter.source.source_url,
            source_path=subchapter.source.source_path,
            source_id=f"chapter-{subchapter.chapter}-subchapter-{subchapter.label}",
            source_format=subchapter.source.source_format,
            source_as_of=source_as_of,
            expression_date=expression_date,
            parent_citation_path=subchapter.parent_citation_path,
            parent_id=parent_id,
            level=1,
            ordinal=subchapter.ordinal,
            kind="subchapter",
            legal_identifier=subchapter.legal_identifier,
            identifiers={
                "wisconsin:chapter": subchapter.chapter,
                "wisconsin:subchapter": subchapter.label,
            },
            metadata=metadata,
        ),
        source=subchapter.source,
        metadata=metadata,
    )


def _append_section_record(
    section: _WisconsinSectionBuilder,
    *,
    seen: set[str],
    records: list[ProvisionRecord],
    items: list[SourceInventoryItem],
    version: str,
    source_as_of: str,
    expression_date: str,
    publication_note: str | None,
) -> None:
    parent_id = deterministic_provision_id(section.parent_citation_path)
    metadata = _base_metadata(section.source, publication_note=publication_note)
    if section.subchapter_label:
        metadata["subchapter"] = section.subchapter_label
    if section.subchapter_heading:
        metadata["subchapter_heading"] = section.subchapter_heading
    if section.history:
        metadata["history"] = section.history
    if section.references_to:
        metadata["references_to"] = section.references_to
    _append_record(
        records,
        items,
        seen=seen,
        record=ProvisionRecord(
            id=deterministic_provision_id(section.citation_path),
            jurisdiction="us-wi",
            document_class=DocumentClass.STATUTE.value,
            citation_path=section.citation_path,
            body="\n".join(section.lines) if section.lines else None,
            heading=section.heading,
            citation_label=section.legal_identifier,
            version=version,
            source_url=section.source.source_url,
            source_path=section.source.source_path,
            source_id=section.label,
            source_format=section.source.source_format,
            source_as_of=source_as_of,
            expression_date=expression_date,
            parent_citation_path=section.parent_citation_path,
            parent_id=parent_id,
            level=section.level,
            ordinal=section.ordinal,
            kind="section",
            legal_identifier=section.legal_identifier,
            identifiers={
                "wisconsin:chapter": section.chapter,
                "wisconsin:section": section.label,
            },
            metadata=metadata,
        ),
        source=section.source,
        metadata=metadata,
    )


def _append_record(
    records: list[ProvisionRecord],
    items: list[SourceInventoryItem],
    *,
    seen: set[str],
    record: ProvisionRecord,
    source: WisconsinSource,
    metadata: dict[str, Any],
) -> None:
    if record.citation_path in seen:
        return
    seen.add(record.citation_path)
    records.append(record)
    items.append(
        SourceInventoryItem(
            citation_path=record.citation_path,
            source_url=source.source_url,
            source_path=source.source_path,
            source_format=source.source_format,
            sha256=source.sha256,
            metadata=metadata,
        )
    )


def _base_metadata(
    source: WisconsinSource,
    *,
    publication_note: str | None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source_authority": "Wisconsin Legislative Reference Bureau",
        "source_document_id": source.source_document_id,
    }
    if publication_note:
        metadata["publication_note"] = publication_note
    return metadata


def _section_heading(div: Tag) -> str | None:
    title = div.select_one(".qstitle_sect")
    if title is None:
        return None
    return _clean_text(title.get_text(" ")) or None


def _statute_text_line(div: Tag, *, section_intro: bool) -> str | None:
    clone = BeautifulSoup(str(div), "lxml").find("div")
    if not isinstance(clone, Tag):
        return None
    for ref in clone.select(".reference"):
        ref.decompose()
    if section_intro:
        for selector in (".qsnum_sect", ".qstitle_sect"):
            for element in clone.select(selector):
                element.decompose()
    return _clean_text(clone.get_text(" ")) or None


def _note_text(div: Tag) -> str | None:
    clone = BeautifulSoup(str(div), "lxml").find("div")
    if not isinstance(clone, Tag):
        return None
    for ref in clone.select(".reference"):
        ref.decompose()
    return _clean_text(clone.get_text(" ")) or None


def _collect_references(div: Tag, *, section: _WisconsinSectionBuilder) -> None:
    for anchor in div.find_all("a"):
        if not isinstance(anchor, Tag):
            continue
        rel_value = _clean_text(anchor.get("rel")[0] if isinstance(anchor.get("rel"), list) else anchor.get("rel"))
        href_value = _clean_text(anchor.get("href"))
        for value in (rel_value, href_value.removeprefix("/document/")):
            label = _section_label_from_cite(value)
            if label:
                section.add_reference(label)
    cites = _data_cites(div)
    for cite in cites:
        label = _section_label_from_cite(cite)
        if label:
            section.add_reference(label)
    for match in _SECTION_TEXT_RE.finditer(div.get_text(" ")):
        section.add_reference(match.group("label"))


def _data_cites(div: Tag) -> tuple[str, ...]:
    raw = div.get("data-cites")
    if not raw:
        return ()
    try:
        values = json.loads(str(raw))
    except json.JSONDecodeError:
        return ()
    return tuple(str(value) for value in values if isinstance(value, str))


def _section_label_from_cite(value: str) -> str | None:
    match = _SECTION_CITE_RE.match(value)
    if match is None:
        return None
    return _strip_subsection(match.group("label"))


def _strip_subsection(label: str) -> str:
    return label.split("(", 1)[0]


def _subchapter_label(div: Tag, *, chapter: str) -> str | None:
    for cite in _data_cites(div):
        match = _SUBCHAPTER_CITE_RE.match(cite)
        if match and match.group("chapter") == chapter:
            return match.group("label").upper()
    text = _clean_text(div.get_text(" "))
    match = re.search(r"SUBCHAPTER\s+(?P<label>[IVXLCDM]+)", text, flags=re.I)
    return match.group("label").upper() if match else None


def _chapter_url(base_url: str, label: str) -> str:
    quoted = quote(label, safe="")
    return urljoin(_base_url(base_url), f"/statutes/statutes/{quoted}?view=section")


def _toc_relative_path() -> str:
    return f"{WISCONSIN_STATUTES_SOURCE_FORMAT}/statutes/prefaces/toc.html"


def _chapter_relative_path(label: str) -> str:
    return f"{WISCONSIN_STATUTES_SOURCE_FORMAT}/statutes/statutes/{_path_segment(label)}.html"


def _base_url(value: str) -> str:
    return value.rstrip("/") + "/"


def _retry_delay(exc: BaseException, *, attempt: int) -> float:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except ValueError:
                pass
        if exc.response.status_code == 429:
            return min(60.0, 5.0 * attempt)
        if exc.response.status_code >= 500:
            return min(90.0, 5.0 * attempt)
    return 0.5 * attempt


def _normalize_chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower().startswith("chapter "):
        text = text.split(None, 1)[1]
    return text.upper()


def _wisconsin_run_id(version: str, *, only_title: str | None, limit: int | None) -> str:
    parts = [version]
    if only_title:
        parts.append(f"chapter-{_path_segment(only_title)}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _path_segment(value: str) -> str:
    return re.sub(r"[^a-z0-9.]+", "-", value.lower()).strip("-")


def _html_text(html_data: str | bytes) -> str:
    if isinstance(html_data, bytes):
        return html_data.decode("utf-8", errors="replace")
    return html_data


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)


def _store_relative_path(store: CorpusArtifactStore, path: Path) -> str:
    try:
        return str(path.relative_to(store.root))
    except ValueError:
        return str(path)


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)
