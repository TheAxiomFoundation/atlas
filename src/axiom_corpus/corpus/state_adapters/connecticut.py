"""Connecticut General Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import urljoin

import requests
import urllib3
from bs4 import BeautifulSoup
from bs4.element import Tag
from urllib3.exceptions import InsecureRequestWarning

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

CONNECTICUT_CURRENT_BASE_URL = "https://www.cga.ct.gov/current/pub/"
CONNECTICUT_SUPPLEMENT_BASE_URL = "https://www.cga.ct.gov/2026/sup/"
CONNECTICUT_CURRENT_SOURCE_FORMAT = "connecticut-general-statutes-current-html"
CONNECTICUT_SUPPLEMENT_SOURCE_FORMAT = "connecticut-general-statutes-supplement-html"
CONNECTICUT_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_TITLE_FILE_RE = re.compile(r"^title_(?P<title>\d+[a-z]?)\.htm$", re.I)
_CHAPTER_FILE_RE = re.compile(r"^chap_(?P<chapter>\d+[a-z]*)\.htm$", re.I)
_CHAPTER_TEXT_RE = re.compile(r"^Chapter\s+(?P<chapter>\d+[A-Za-z]*)\b", re.I)
_SECTION_ID_RE = re.compile(r"^secs?_(?P<section>.+)$", re.I)
_SECTION_HEADER_RE = re.compile(
    r"^Secs?\.\s+(?P<section>[^.]+)\.\s*(?P<heading>.*)$",
    re.I | re.S,
)
_SECTION_REF_RE = re.compile(
    r"\b(?:section|sections|sec\.|secs\.)\s+"
    r"(?P<section>\d+[a-z]?-\d+[a-z]*(?:\([^)]+\))?)",
    re.I,
)
_STATUS_RE = re.compile(
    r"\b(repealed|reserved|transferred|obsolete|expired|omitted|conditionally repealed)\b",
    re.I,
)


@dataclass(frozen=True)
class ConnecticutTitle:
    """One title row from the official Connecticut title index."""

    number: str
    heading: str | None
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int
    source_set: str
    active: bool = True
    status: str | None = None

    @property
    def source_id(self) -> str:
        return f"title-{self.number}"

    @property
    def citation_path(self) -> str:
        return f"us-ct/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"Conn. Gen. Stat. title {self.number}"


@dataclass(frozen=True)
class ConnecticutChapter:
    """One chapter row from an official Connecticut title page."""

    title_number: str
    title_heading: str | None
    chapter: str
    heading: str | None
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int
    source_set: str
    section_range: str | None = None
    status: str | None = None

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter}"

    @property
    def citation_path(self) -> str:
        return f"us-ct/statute/{self.source_id}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-ct/statute/title-{self.title_number}"

    @property
    def legal_identifier(self) -> str:
        return f"Conn. Gen. Stat. ch. {self.chapter}"


@dataclass(frozen=True)
class ConnecticutSection:
    """One section parsed from an official Connecticut chapter page."""

    title_number: str
    title_heading: str | None
    chapter: str
    chapter_heading: str | None
    section: str
    heading: str | None
    body: str | None
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int
    source_set: str
    references_to: tuple[str, ...]
    source_history: tuple[str, ...] = ()
    amendment_history: tuple[str, ...] = ()
    cross_references: tuple[str, ...] = ()
    annotations: tuple[str, ...] = ()
    status: str | None = None

    @property
    def source_id(self) -> str:
        return self.section

    @property
    def citation_path(self) -> str:
        return f"us-ct/statute/{self.section}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-ct/statute/chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        marker = "§§" if "-and-" in self.section or "-to-" in self.section else "§"
        return f"Conn. Gen. Stat. {marker} {self.section.replace('-and-', ' and ')}"


@dataclass(frozen=True)
class _ConnecticutSource:
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


class _ConnecticutFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        current_base_url: str,
        supplement_base_url: str | None,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
        verify_ssl: bool,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.current_base_url = _base_url(current_base_url)
        self.supplement_base_url = _base_url(supplement_base_url) if supplement_base_url else None
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self.verify_ssl = verify_ssl
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_title_index(self, source_set: str) -> _ConnecticutSource:
        return self._fetch_named(source_set, "titles.htm", "titles.html")

    def fetch_title(self, source_set: str, title: str) -> _ConnecticutSource:
        filename = f"title_{_title_file_token(title)}.htm"
        return self._fetch_named(source_set, filename, f"title-{title}.html")

    def fetch_chapter(self, source_set: str, chapter: str) -> _ConnecticutSource:
        filename = f"chap_{_chapter_file_token(chapter)}.htm"
        return self._fetch_named(source_set, filename, f"chapter-{chapter}.html")

    def has_source_set(self, source_set: str) -> bool:
        return self._base_url_for(source_set) is not None

    def _fetch_named(
        self,
        source_set: str,
        source_name: str,
        cache_name: str,
    ) -> _ConnecticutSource:
        source_format = _source_format_for(source_set)
        relative_path = f"{source_format}/{cache_name}"
        base_url = self._base_url_for(source_set)
        if base_url is None:
            raise ValueError(f"Connecticut source set is not configured: {source_set}")
        source_url = urljoin(base_url, source_name)
        data = self._fetch(relative_path, source_url)
        return _ConnecticutSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=source_format,
            data=data,
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_connecticut_source(
            source_url,
            fetcher=self,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
            verify_ssl=self.verify_ssl,
        )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            _write_cache_bytes(cached_path, data)
        return data

    def _base_url_for(self, source_set: str) -> str | None:
        if source_set == "current":
            return self.current_base_url
        if source_set == "supplement":
            return self.supplement_base_url
        raise ValueError(f"unknown Connecticut source set: {source_set}")

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            self._last_request_at = time.monotonic()


def extract_connecticut_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    only_chapter: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    current_base_url: str = CONNECTICUT_CURRENT_BASE_URL,
    supplement_base_url: str | None = CONNECTICUT_SUPPLEMENT_BASE_URL,
    include_supplement: bool = True,
    request_delay_seconds: float = 0.05,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
    verify_ssl: bool = True,
) -> StateStatuteExtractReport:
    """Snapshot official Connecticut statute HTML and extract provisions.

    Connecticut publishes the revised General Statutes and a separate annual
    supplement. When enabled, supplement sections replace same-citation base
    sections and add newly published sections.
    """
    jurisdiction = "us-ct"
    title_filter = _title_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter)
    run_id = _connecticut_run_id(
        version,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _ConnecticutFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        current_base_url=current_base_url,
        supplement_base_url=supplement_base_url if include_supplement else None,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
        verify_ssl=verify_ssl,
    )

    source_paths: list[Path] = []
    items_by_path: dict[str, SourceInventoryItem] = {}
    records_by_path: dict[str, ProvisionRecord] = {}
    order: list[str] = []
    errors: list[str] = []
    remaining_sections = limit

    for source_set in ("current", "supplement"):
        if source_set == "supplement" and not include_supplement:
            continue
        if not fetcher.has_source_set(source_set):
            continue
        try:
            remaining_sections = _extract_source_set(
                store,
                fetcher=fetcher,
                jurisdiction=jurisdiction,
                run_id=run_id,
                version=run_id,
                source_set=source_set,
                title_filter=title_filter,
                chapter_filter=chapter_filter,
                remaining_sections=remaining_sections,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                source_paths=source_paths,
                items_by_path=items_by_path,
                records_by_path=records_by_path,
                order=order,
                errors=errors,
            )
        except (OSError, requests.RequestException, ValueError) as exc:
            if source_set == "supplement":
                errors.append(f"supplement: {exc}")
                continue
            raise

    records = [records_by_path[path] for path in order if path in records_by_path]
    items = [items_by_path[path] for path in order if path in items_by_path]
    if not records:
        raise ValueError("no Connecticut General Statutes provisions extracted")

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

    title_count = sum(1 for record in records if record.kind == "title")
    chapter_count = sum(1 for record in records if record.kind == "chapter")
    section_count = sum(1 for record in records if record.kind == "section")
    return StateStatuteExtractReport(
        jurisdiction=jurisdiction,
        title_count=title_count,
        container_count=title_count + chapter_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        errors=tuple(errors),
    )


def _extract_source_set(
    store: CorpusArtifactStore,
    *,
    fetcher: _ConnecticutFetcher,
    jurisdiction: str,
    run_id: str,
    version: str,
    source_set: str,
    title_filter: str | None,
    chapter_filter: str | None,
    remaining_sections: int | None,
    source_as_of: str,
    expression_date: str,
    source_paths: list[Path],
    items_by_path: dict[str, SourceInventoryItem],
    records_by_path: dict[str, ProvisionRecord],
    order: list[str],
    errors: list[str],
) -> int | None:
    title_index_source = fetcher.fetch_title_index(source_set)
    title_index_recorded = _record_source(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        source=title_index_source,
    )
    source_paths.append(_source_artifact_path(store, jurisdiction, run_id, title_index_source))
    titles = parse_connecticut_title_index(
        title_index_source.data,
        source=title_index_recorded,
        source_set=source_set,
        base_url=fetcher._base_url_for(source_set) or CONNECTICUT_CURRENT_BASE_URL,
    )
    if title_filter is not None:
        titles = tuple(title for title in titles if title.number == title_filter)
    if source_set == "current" and not titles:
        raise ValueError(f"no Connecticut titles selected for filter: {title_filter!r}")

    for title in titles:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        _upsert(
            items_by_path,
            records_by_path,
            order,
            _title_inventory_item(title),
            _title_record(
                title,
                version=version,
                source_as_of=source_as_of,
                expression_date=expression_date,
            ),
            replace_existing=source_set == "supplement" and title.citation_path not in records_by_path,
        )
        if not title.active:
            continue

        title_source = fetcher.fetch_title(source_set, title.number)
        title_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=title_source,
        )
        source_paths.append(_source_artifact_path(store, jurisdiction, run_id, title_source))
        chapters = parse_connecticut_title_page(
            title_source.data,
            title=title,
            source=title_recorded,
            source_set=source_set,
            base_url=fetcher._base_url_for(source_set) or CONNECTICUT_CURRENT_BASE_URL,
        )
        if chapter_filter is not None:
            chapters = tuple(chapter for chapter in chapters if chapter.chapter == chapter_filter)

        for chapter in chapters:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            _upsert(
                items_by_path,
                records_by_path,
                order,
                _chapter_inventory_item(chapter),
                _chapter_record(
                    chapter,
                    version=version,
                    source_as_of=source_as_of,
                    expression_date=expression_date,
                ),
                replace_existing=source_set == "supplement" and chapter.citation_path not in records_by_path,
            )
            chapter_source = fetcher.fetch_chapter(source_set, chapter.chapter)
            chapter_recorded = _record_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                source=chapter_source,
            )
            source_paths.append(_source_artifact_path(store, jurisdiction, run_id, chapter_source))
            sections = parse_connecticut_chapter_page(
                chapter_source.data,
                chapter=chapter,
                source=chapter_recorded,
                source_set=source_set,
            )
            if remaining_sections is not None:
                sections = sections[:remaining_sections]
            for section in sections:
                if section.citation_path in records_by_path and source_set == "current":
                    continue
                _upsert(
                    items_by_path,
                    records_by_path,
                    order,
                    _section_inventory_item(section),
                    _section_record(
                        section,
                        version=version,
                        source_as_of=source_as_of,
                        expression_date=expression_date,
                    ),
                    replace_existing=source_set == "supplement",
                )
                if remaining_sections is not None:
                    remaining_sections -= 1
                    if remaining_sections <= 0:
                        break
    return remaining_sections


def parse_connecticut_title_index(
    html: str | bytes,
    *,
    source: _RecordedSource,
    source_set: str = "current",
    base_url: str = CONNECTICUT_CURRENT_BASE_URL,
) -> tuple[ConnecticutTitle, ...]:
    """Parse the official Connecticut title index."""
    soup = BeautifulSoup(_decode(html), "lxml")
    titles: list[ConnecticutTitle] = []
    seen: set[str] = set()
    for row in soup.find_all("tr"):
        designator = row.find("span", class_="toc_ttl_desig")
        if not isinstance(designator, Tag):
            continue
        match = re.match(r"Title\s+(?P<title>\d+[A-Za-z]?)$", _clean_text(designator), re.I)
        if match is None:
            continue
        number = _normalize_title(match.group("title"))
        if number in seen:
            continue
        seen.add(number)
        link = designator.find_parent("a", href=True)
        if not isinstance(link, Tag):
            link = row.find("a", href=_TITLE_FILE_RE)
        href = str(link["href"]) if isinstance(link, Tag) else ""
        heading_node = row.find("span", class_="toc_ttl_name")
        heading = _clean_text(heading_node) if isinstance(heading_node, Tag) else None
        row_text = _clean_text(row)
        titles.append(
            ConnecticutTitle(
                number=number,
                heading=heading,
                source_url=urljoin(base_url, href) if href else source.source_url,
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                ordinal=len(titles) + 1,
                source_set=source_set,
                active=bool(href),
                status=_status_from_text(row_text),
            )
        )
    return tuple(titles)


def parse_connecticut_title_page(
    html: str | bytes,
    *,
    title: ConnecticutTitle,
    source: _RecordedSource,
    source_set: str = "current",
    base_url: str = CONNECTICUT_CURRENT_BASE_URL,
) -> tuple[ConnecticutChapter, ...]:
    """Parse chapter rows from one official Connecticut title page."""
    soup = BeautifulSoup(_decode(html), "lxml")
    title_heading = _heading_text(soup, "h1", "title-name") or title.heading
    chapters: list[ConnecticutChapter] = []
    seen: set[str] = set()
    for row in soup.find_all("tr"):
        link = row.find("a", class_="toc_ch_link", href=True)
        if not isinstance(link, Tag):
            continue
        href = str(link["href"])
        href_match = _CHAPTER_FILE_RE.match(href)
        if href_match is None:
            continue
        chapter = href_match.group("chapter").lower()
        if chapter in seen:
            continue
        seen.add(chapter)
        heading = _clean_text(link) if link.find_parent("td") is not row.find("td") else None
        if re.match(r"^Chapter\b", heading or "", re.I):
            heading = None
        cells = row.find_all("td")
        if len(cells) > 1:
            second_link = cells[1].find("a", class_="toc_ch_link")
            if isinstance(second_link, Tag):
                heading = _clean_text(second_link)
            elif heading is None:
                heading = _clean_text(cells[1])
        range_node = row.find("span", class_="toc_rng_secs")
        section_range = _clean_text(range_node) if isinstance(range_node, Tag) else None
        chapters.append(
            ConnecticutChapter(
                title_number=title.number,
                title_heading=title_heading,
                chapter=chapter,
                heading=heading,
                source_url=urljoin(base_url, href),
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                ordinal=len(chapters) + 1,
                source_set=source_set,
                section_range=section_range,
                status=_status_from_text(_clean_text(row)),
            )
        )
    return tuple(chapters)


def parse_connecticut_chapter_page(
    html: str | bytes,
    *,
    chapter: ConnecticutChapter,
    source: _RecordedSource,
    source_set: str = "current",
) -> tuple[ConnecticutSection, ...]:
    """Parse section records from one official Connecticut chapter page."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapter_heading = _heading_text(soup, "h2", "chap-name") or chapter.heading
    sections: list[ConnecticutSection] = []
    for span in soup.find_all("span", class_="catchln", id=True):
        if not isinstance(span, Tag):
            continue
        section = _section_from_anchor(str(span.get("id", "")))
        if not section:
            continue
        content_nodes = _section_content_nodes(span)
        heading = _section_heading(span, section)
        body_parts: list[str] = []
        source_history: list[str] = []
        amendment_history: list[str] = []
        cross_refs: list[str] = []
        annotations: list[str] = []
        for node in content_nodes:
            if not isinstance(node, Tag):
                continue
            if _is_nav_table(node):
                continue
            text = _node_text_without_heading(node, span)
            if not text:
                continue
            classes = {str(value) for value in node.get("class", [])}
            if classes & {"source", "source-first"}:
                source_history.append(text)
            elif classes & {"history", "history-first"}:
                amendment_history.append(text)
            elif classes & {"cross-ref", "cross-ref-first"}:
                cross_refs.append(text)
            elif classes & {"annotation", "annotation-first"}:
                annotations.append(text)
            else:
                body_parts.append(text)
        body = _join_paragraphs(body_parts)
        notes_text = "\n".join([body or "", *source_history, *amendment_history, *cross_refs])
        references_to = _extract_references(content_nodes, notes_text, current=section)
        status = _status_from_text(" ".join(part for part in [heading or "", body or ""] if part))
        sections.append(
            ConnecticutSection(
                title_number=chapter.title_number,
                title_heading=chapter.title_heading,
                chapter=chapter.chapter,
                chapter_heading=chapter_heading,
                section=section,
                heading=heading,
                body=body,
                source_url=f"{source.source_url}#sec_{section}",
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                ordinal=len(sections) + 1,
                source_set=source_set,
                references_to=references_to,
                source_history=tuple(source_history),
                amendment_history=tuple(amendment_history),
                cross_references=tuple(cross_refs),
                annotations=tuple(annotations),
                status=status,
            )
        )
    return tuple(sections)


def _title_inventory_item(title: ConnecticutTitle) -> SourceInventoryItem:
    metadata: dict[str, Any] = {
        "kind": "title",
        "title": title.number,
        "heading": title.heading,
        "source_id": title.source_id,
        "source_set": title.source_set,
        "active": title.active,
    }
    if title.status:
        metadata["status"] = title.status
    return SourceInventoryItem(
        citation_path=title.citation_path,
        source_url=title.source_url,
        source_path=title.source_path,
        source_format=title.source_format,
        sha256=title.sha256,
        metadata=metadata,
    )


def _chapter_inventory_item(chapter: ConnecticutChapter) -> SourceInventoryItem:
    metadata: dict[str, Any] = {
        "kind": "chapter",
        "title": chapter.title_number,
        "chapter": chapter.chapter,
        "heading": chapter.heading,
        "source_id": chapter.source_id,
        "source_set": chapter.source_set,
        "parent_citation_path": chapter.parent_citation_path,
    }
    if chapter.section_range:
        metadata["section_range"] = chapter.section_range
    if chapter.status:
        metadata["status"] = chapter.status
    return SourceInventoryItem(
        citation_path=chapter.citation_path,
        source_url=chapter.source_url,
        source_path=chapter.source_path,
        source_format=chapter.source_format,
        sha256=chapter.sha256,
        metadata=metadata,
    )


def _section_inventory_item(section: ConnecticutSection) -> SourceInventoryItem:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": section.title_number,
        "chapter": section.chapter,
        "section": section.section,
        "heading": section.heading,
        "source_id": section.source_id,
        "source_set": section.source_set,
        "parent_citation_path": section.parent_citation_path,
        "references_to": list(section.references_to),
    }
    _add_section_metadata(metadata, section)
    return SourceInventoryItem(
        citation_path=section.citation_path,
        source_url=section.source_url,
        source_path=section.source_path,
        source_format=section.source_format,
        sha256=section.sha256,
        metadata=metadata,
    )


def _title_record(
    title: ConnecticutTitle,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    metadata: dict[str, Any] = {
        "title": title.number,
        "source_set": title.source_set,
        "active": title.active,
    }
    if title.status:
        metadata["status"] = title.status
    return ProvisionRecord(
        id=deterministic_provision_id(title.citation_path),
        jurisdiction="us-ct",
        document_class=DocumentClass.STATUTE.value,
        citation_path=title.citation_path,
        citation_label=title.legal_identifier,
        heading=title.heading,
        body=None,
        version=version,
        source_url=title.source_url,
        source_path=title.source_path,
        source_id=title.source_id,
        source_format=title.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        level=0,
        ordinal=title.ordinal,
        kind="title",
        legal_identifier=title.legal_identifier,
        identifiers={"connecticut:title": title.number},
        metadata=metadata,
    )


def _chapter_record(
    chapter: ConnecticutChapter,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    metadata: dict[str, Any] = {
        "title": chapter.title_number,
        "chapter": chapter.chapter,
        "source_set": chapter.source_set,
    }
    if chapter.section_range:
        metadata["section_range"] = chapter.section_range
    if chapter.status:
        metadata["status"] = chapter.status
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-ct",
        document_class=DocumentClass.STATUTE.value,
        citation_path=chapter.citation_path,
        citation_label=chapter.legal_identifier,
        heading=chapter.heading,
        body=None,
        version=version,
        source_url=chapter.source_url,
        source_path=chapter.source_path,
        source_id=chapter.source_id,
        source_format=chapter.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=chapter.parent_citation_path,
        parent_id=deterministic_provision_id(chapter.parent_citation_path),
        level=1,
        ordinal=chapter.ordinal,
        kind="chapter",
        legal_identifier=chapter.legal_identifier,
        identifiers={
            "connecticut:title": chapter.title_number,
            "connecticut:chapter": chapter.chapter,
        },
        metadata=metadata,
    )


def _section_record(
    section: ConnecticutSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    metadata: dict[str, Any] = {
        "title": section.title_number,
        "chapter": section.chapter,
        "section": section.section,
        "source_set": section.source_set,
        "references_to": list(section.references_to),
    }
    _add_section_metadata(metadata, section)
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-ct",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        citation_label=section.legal_identifier,
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=section.source_url,
        source_path=section.source_path,
        source_id=section.source_id,
        source_format=section.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.parent_citation_path,
        parent_id=deterministic_provision_id(section.parent_citation_path),
        level=2,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=section.legal_identifier,
        identifiers={
            "connecticut:title": section.title_number,
            "connecticut:chapter": section.chapter,
            "connecticut:section": section.section,
        },
        metadata=metadata,
    )


def _add_section_metadata(metadata: dict[str, Any], section: ConnecticutSection) -> None:
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.amendment_history:
        metadata["amendment_history"] = list(section.amendment_history)
    if section.cross_references:
        metadata["cross_references"] = list(section.cross_references)
    if section.annotations:
        metadata["annotations"] = list(section.annotations)
    if section.status:
        metadata["status"] = section.status


def _upsert(
    items_by_path: dict[str, SourceInventoryItem],
    records_by_path: dict[str, ProvisionRecord],
    order: list[str],
    item: SourceInventoryItem,
    record: ProvisionRecord,
    *,
    replace_existing: bool,
) -> None:
    if record.citation_path not in records_by_path:
        order.append(record.citation_path)
    if record.citation_path in records_by_path and not replace_existing:
        return
    items_by_path[record.citation_path] = item
    records_by_path[record.citation_path] = record


def _record_source(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    source: _ConnecticutSource,
) -> _RecordedSource:
    artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        source.relative_path,
    )
    sha256 = store.write_bytes(artifact_path, source.data)
    return _RecordedSource(
        source_url=source.source_url,
        source_path=_state_source_key(jurisdiction, run_id, source.relative_path),
        source_format=source.source_format,
        sha256=sha256,
    )


def _source_artifact_path(
    store: CorpusArtifactStore,
    jurisdiction: str,
    run_id: str,
    source: _ConnecticutSource,
) -> Path:
    return store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, source.relative_path)


def _download_connecticut_source(
    source_url: str,
    *,
    fetcher: _ConnecticutFetcher,
    timeout_seconds: float,
    request_attempts: int,
    verify_ssl: bool,
) -> bytes:
    headers = {"User-Agent": CONNECTICUT_USER_AGENT}
    if not verify_ssl:
        urllib3.disable_warnings(InsecureRequestWarning)
    last_error: requests.RequestException | None = None
    for attempt in range(1, request_attempts + 1):
        fetcher.wait_for_request_slot()
        try:
            response = requests.get(
                source_url,
                headers=headers,
                timeout=timeout_seconds,
                verify=verify_ssl,
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= request_attempts:
                break
            time.sleep(min(2.0 * attempt, 10.0))
    assert last_error is not None
    raise last_error


def _section_content_nodes(start_span: Tag) -> tuple[Tag, ...]:
    parent = start_span.find_parent(["p", "div"])
    if not isinstance(parent, Tag):
        return ()
    nodes: list[Tag] = []
    current: Tag | None = parent
    while isinstance(current, Tag):
        if current is not parent and current.find("span", class_="catchln", id=True):
            break
        if current.name in {"p", "table"}:
            nodes.append(current)
        next_sibling = current.find_next_sibling()
        current = next_sibling if isinstance(next_sibling, Tag) else None
    return tuple(nodes)


def _node_text_without_heading(node: Tag, heading_span: Tag) -> str | None:
    text = _clean_text(node)
    if not text:
        return None
    if node.find("span", class_="catchln") is heading_span:
        heading = _clean_text(heading_span)
        if text.startswith(heading):
            text = text[len(heading) :].strip()
    return text or None


def _section_heading(span: Tag, section: str) -> str | None:
    text = _clean_text(span)
    match = _SECTION_HEADER_RE.match(text)
    if match is None:
        return None
    heading = match.group("heading").strip()
    return heading.rstrip(".") or None


def _section_from_anchor(anchor: str) -> str | None:
    match = _SECTION_ID_RE.match(anchor)
    if match is None:
        return None
    raw = match.group("section")
    raw = raw.replace("_and_", "-and-").replace("_to_", "-to-").replace("_", "-")
    raw = raw.replace("--", "-").strip("-")
    return raw.lower() or None


def _extract_references(nodes: tuple[Tag, ...], text: str, *, current: str) -> tuple[str, ...]:
    refs: set[str] = set()
    for node in nodes:
        for link in node.find_all("a", href=True):
            href = str(link["href"])
            if "#sec_" not in href and "#secs_" not in href:
                continue
            fragment = href.rsplit("#", 1)[-1]
            ref = _section_from_anchor(fragment)
            if ref and ref != current:
                refs.add(f"us-ct/statute/{ref}")
    for match in _SECTION_REF_RE.finditer(text):
        ref = match.group("section").lower()
        ref = re.sub(r"\(.+\)$", "", ref)
        if ref and ref != current:
            refs.add(f"us-ct/statute/{ref}")
    return tuple(sorted(refs))


def _is_nav_table(node: Tag) -> bool:
    return node.name == "table" and "nav_tbl" in {str(value) for value in node.get("class", [])}


def _heading_text(soup: BeautifulSoup, name: str, class_name: str) -> str | None:
    node = soup.find(name, class_=class_name)
    if not isinstance(node, Tag):
        return None
    text = _clean_text(node)
    return text or None


def _status_from_text(text: str) -> str | None:
    match = _STATUS_RE.search(text)
    if match is None:
        return None
    return match.group(1).lower()


def _source_format_for(source_set: str) -> str:
    if source_set == "current":
        return CONNECTICUT_CURRENT_SOURCE_FORMAT
    if source_set == "supplement":
        return CONNECTICUT_SUPPLEMENT_SOURCE_FORMAT
    raise ValueError(f"unknown Connecticut source set: {source_set}")


def _title_file_token(title: str) -> str:
    match = re.match(r"^(?P<number>\d+)(?P<suffix>[a-z]?)$", str(title).lower())
    if match is None:
        return str(title).lower()
    return f"{int(match.group('number')):02d}{match.group('suffix')}"


def _chapter_file_token(chapter: str) -> str:
    match = re.match(r"^(?P<number>\d+)(?P<suffix>[a-z]*)$", str(chapter).lower())
    if match is None:
        return str(chapter).lower()
    return f"{int(match.group('number')):03d}{match.group('suffix')}"


def _normalize_title(title: str) -> str:
    match = re.match(r"^(?P<number>\d+)(?P<suffix>[a-z]?)$", str(title).strip().lower())
    if match is None:
        return str(title).strip().lower()
    return f"{int(match.group('number'))}{match.group('suffix')}"


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    text = re.sub(r"^title[-\s]*", "", text)
    return _normalize_title(text)


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    text = re.sub(r"^chapter[-\s]*", "", text)
    return text


def _connecticut_run_id(
    version: str,
    *,
    title_filter: str | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    parts = [version]
    if title_filter is not None:
        parts.append(f"title-{title_filter}")
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _decode(html: str | bytes) -> str:
    if isinstance(html, str):
        return html
    return html.decode("utf-8-sig", errors="replace")


def _clean_text(node: Tag | str | None) -> str:
    if node is None:
        return ""
    text = node if isinstance(node, str) else node.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s+([,.;:!?)])", r"\1", text)


def _join_paragraphs(parts: list[str]) -> str | None:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    if not cleaned:
        return None
    return "\n\n".join(cleaned)


def _base_url(value: str | None) -> str | None:
    if value is None:
        return None
    return value.rstrip("/") + "/"


def _state_source_key(jurisdiction: str, run_id: str, relative_path: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_path}"


def _write_cache_bytes(path: Path, data: bytes) -> None:
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)
