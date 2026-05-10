"""Florida Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

FLORIDA_STATUTES_BASE_URL = "https://www.leg.state.fl.us/statutes/"
FLORIDA_STATUTES_DEFAULT_YEAR = 2025
FLORIDA_TITLE_INDEX_SOURCE_FORMAT = "florida-statutes-title-index-html"
FLORIDA_CHAPTER_SOURCE_FORMAT = "florida-statutes-chapter-html"
FLORIDA_BULK_ZIP_SOURCE_FORMAT = "florida-statutes-bulk-zip"
FLORIDA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

FLORIDA_TITLE_ROMANS = (
    "I",
    "II",
    "III",
    "IV",
    "V",
    "VI",
    "VII",
    "VIII",
    "IX",
    "X",
    "XI",
    "XII",
    "XIII",
    "XIV",
    "XV",
    "XVI",
    "XVII",
    "XVIII",
    "XIX",
    "XX",
    "XXI",
    "XXII",
    "XXIII",
    "XXIV",
    "XXV",
    "XXVI",
    "XXVII",
    "XXVIII",
    "XXIX",
    "XXX",
    "XXXI",
    "XXXII",
    "XXXIII",
    "XXXIV",
    "XXXV",
    "XXXVI",
    "XXXVII",
    "XXXVIII",
    "XXXIX",
    "XL",
    "XLI",
    "XLII",
    "XLIII",
    "XLIV",
    "XLV",
    "XLVI",
    "XLVII",
    "XLVIII",
    "XLIX",
)

_CHAPTER_CONTENT_RE = re.compile(
    r"^(?P<range>\d{4}-\d{4})/(?P<padded>\d{4})/(?P=padded)ContentsIndex\.html$",
    re.I,
)
_SECTION_NUMBER_RE = re.compile(r"(?P<section>\d+[A-Z]?(?:\.\d+[A-Z]?)*)", re.I)
_REFERENCE_HREF_RE = re.compile(r"/Sections/(?P<padded>\d{4}\.[0-9A-Z]+)\.html", re.I)
_REFERENCE_TEXT_RE = re.compile(r"\b(?:s\.|ss\.|section|sections)\s+(?P<section>\d+[A-Z]?(?:\.\d+[A-Z]?)*)", re.I)


@dataclass(frozen=True)
class FloridaTitle:
    """One Florida Statutes title from the official title index."""

    roman: str
    heading: str | None
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"title-{self.roman.lower()}"

    @property
    def citation_path(self) -> str:
        return f"us-fl/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"Florida Statutes Title {self.roman}"


@dataclass(frozen=True)
class FloridaChapterTarget:
    """One whole-chapter page target discovered from a title index page."""

    title_roman: str
    chapter: str
    chapter_path: str
    source_url: str
    ordinal: int


@dataclass(frozen=True)
class FloridaChapter:
    """Chapter metadata parsed from an official whole-chapter page."""

    title_roman: str
    title_heading: str | None
    chapter: str
    heading: str | None
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter}"

    @property
    def citation_path(self) -> str:
        return f"us-fl/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"Fla. Stat. ch. {self.chapter}"


@dataclass(frozen=True)
class FloridaPart:
    """Part container parsed from an official whole-chapter page."""

    title_roman: str
    chapter: str
    part: str
    heading: str | None
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter}-part-{self.part.lower()}"

    @property
    def citation_path(self) -> str:
        return f"us-fl/statute/chapter-{self.chapter}/part-{self.part.lower()}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-fl/statute/chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"Fla. Stat. ch. {self.chapter}, part {self.part}"


@dataclass(frozen=True)
class FloridaSection:
    """Section text parsed from an official whole-chapter page."""

    title_roman: str
    chapter: str
    section: str
    heading: str | None
    body: str | None
    parent_citation_path: str
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int
    references_to: tuple[str, ...]
    source_history: tuple[str, ...]
    notes: tuple[str, ...]
    status: str | None = None

    @property
    def source_id(self) -> str:
        return self.section

    @property
    def citation_path(self) -> str:
        return f"us-fl/statute/{self.section}"

    @property
    def legal_identifier(self) -> str:
        return f"Fla. Stat. § {self.section}"


@dataclass(frozen=True)
class FloridaChapterDocument:
    """Parsed official whole-chapter page."""

    chapter: FloridaChapter
    parts: tuple[FloridaPart, ...]
    sections: tuple[FloridaSection, ...]


@dataclass(frozen=True)
class _FloridaSource:
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


class _FloridaFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        base_url: str,
        source_year: int,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.base_url = base_url.rstrip("/") + "/"
        self.source_year = source_year
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_title_index(self, title_roman: str) -> _FloridaSource:
        relative_path = f"{FLORIDA_TITLE_INDEX_SOURCE_FORMAT}/title-{title_roman}.html"
        source_url = self._url(
            {
                "App_mode": "Display_Index",
                "Title_Request": title_roman,
                "StatuteYear": str(self.source_year),
            }
        )
        data = self._fetch(relative_path, source_url)
        return _FloridaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=FLORIDA_TITLE_INDEX_SOURCE_FORMAT,
            data=data,
        )

    def fetch_chapter(self, chapter_path: str) -> _FloridaSource:
        normalized = _normalize_chapter_path(chapter_path)
        relative_path = f"{FLORIDA_CHAPTER_SOURCE_FORMAT}/{normalized}"
        source_url = self._url(
            {
                "App_mode": "Display_Statute",
                "URL": normalized,
                "StatuteYear": str(self.source_year),
            }
        )
        data = self._fetch(relative_path, source_url)
        return _FloridaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=FLORIDA_CHAPTER_SOURCE_FORMAT,
            data=data,
        )

    def _url(self, params: dict[str, str]) -> str:
        return urljoin(self.base_url, "index.cfm") + "?" + urlencode(params)

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_florida_source(
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
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            self._last_request_at = time.monotonic()


def extract_florida_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_year: int = FLORIDA_STATUTES_DEFAULT_YEAR,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    source_zip: str | Path | None = None,
    source_zip_url: str | None = None,
    base_url: str = FLORIDA_STATUTES_BASE_URL,
    request_delay_seconds: float = 0.05,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
) -> StateStatuteExtractReport:
    """Snapshot official Florida Statutes whole-chapter HTML and extract provisions."""
    _ = source_zip_url
    jurisdiction = "us-fl"
    title_filter = _title_filter(only_title)
    run_id = _florida_run_id(
        version,
        title_filter=title_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _FloridaFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        base_url=base_url,
        source_year=source_year,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    source_paths: list[Path] = []
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    remaining_sections = limit
    if source_zip is not None:
        zip_path = Path(source_zip)
        zip_bytes = zip_path.read_bytes()
        relative_path = f"{FLORIDA_BULK_ZIP_SOURCE_FORMAT}/{zip_path.name}"
        artifact_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, relative_path)
        store.write_bytes(artifact_path, zip_bytes)
        source_paths.append(artifact_path)

    selected_titles = (
        tuple(title for title in FLORIDA_TITLE_ROMANS if title_filter is None or title == title_filter)
    )
    if not selected_titles:
        raise ValueError(f"no Florida Statutes titles selected for filter: {only_title!r}")

    chapter_targets: list[FloridaChapterTarget] = []
    for title_ordinal, title_roman in enumerate(selected_titles, start=1):
        title_source = fetcher.fetch_title_index(title_roman)
        recorded_title = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=title_source,
        )
        source_paths.append(
            store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, title_source.relative_path)
        )
        title = _parse_florida_title_index(
            title_source.data,
            title_roman=title_roman,
            title_ordinal=title_ordinal,
            source=recorded_title,
        )
        _append_unique(
            seen,
            items,
            records,
            _title_inventory_item(title),
            _title_record(
                title,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            ),
        )
        title_count += 1
        container_count += 1
        chapter_targets.extend(_parse_florida_chapter_targets(title_source.data, title_roman=title_roman))

    for chapter_ordinal, target in enumerate(chapter_targets, start=1):
        if remaining_sections is not None and remaining_sections <= 0:
            break
        chapter_source = fetcher.fetch_chapter(target.chapter_path)
        recorded_chapter = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=chapter_source,
        )
        source_paths.append(
            store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                chapter_source.relative_path,
            )
        )
        document = _parse_florida_chapter_page(
            chapter_source.data,
            target=target,
            ordinal=chapter_ordinal,
            source=recorded_chapter,
        )

        _append_unique(
            seen,
            items,
            records,
            _chapter_inventory_item(document.chapter),
            _chapter_record(
                document.chapter,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            ),
        )
        container_count += 1

        for part in document.parts:
            _append_unique(
                seen,
                items,
                records,
                _part_inventory_item(part),
                _part_record(
                    part,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                ),
            )
            container_count += 1

        for section in document.sections:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            added = _append_unique(
                seen,
                items,
                records,
                _section_inventory_item(section),
                _section_record(
                    section,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                ),
            )
            if added:
                section_count += 1
                if remaining_sections is not None:
                    remaining_sections -= 1

    if not records:
        raise ValueError("no Florida Statutes provisions extracted")

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
    )


def _parse_florida_title_index(
    data: bytes,
    *,
    title_roman: str,
    title_ordinal: int,
    source: _RecordedSource,
) -> FloridaTitle:
    soup = BeautifulSoup(data, "html.parser")
    anchor = soup.find("a", attrs={"name": f"Title{title_roman}"})
    heading: str | None = None
    if isinstance(anchor, Tag):
        row = anchor.find_parent("tr")
        if isinstance(row, Tag):
            cells = row.find_all("td", recursive=False)
            if len(cells) >= 2:
                heading = _clean_text(cells[1].get_text(" ", strip=True))
    return FloridaTitle(
        roman=title_roman,
        heading=heading,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        ordinal=title_ordinal,
    )


def _parse_florida_chapter_targets(
    data: bytes,
    *,
    title_roman: str,
) -> tuple[FloridaChapterTarget, ...]:
    soup = BeautifulSoup(data, "html.parser")
    targets: list[FloridaChapterTarget] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        query = parse_qs(urlparse(href).query)
        url_values = query.get("URL")
        if not url_values:
            continue
        content_path = url_values[0]
        match = _CHAPTER_CONTENT_RE.match(content_path)
        if match is None:
            continue
        padded = match.group("padded")
        chapter = str(int(padded))
        chapter_path = f"{match.group('range')}/{padded}/{padded}.html"
        if chapter_path in seen:
            continue
        seen.add(chapter_path)
        targets.append(
            FloridaChapterTarget(
                title_roman=title_roman,
                chapter=chapter,
                chapter_path=chapter_path,
                source_url=_florida_statute_url(chapter_path),
                ordinal=len(targets) + 1,
            )
        )
    return tuple(targets)


def _parse_florida_chapter_page(
    data: bytes,
    *,
    target: FloridaChapterTarget,
    ordinal: int,
    source: _RecordedSource,
) -> FloridaChapterDocument:
    soup = BeautifulSoup(data, "html.parser")
    chapter_div = soup.find("div", class_="Chapter")
    if not isinstance(chapter_div, Tag):
        raise ValueError(f"Florida chapter page has no Chapter div: {source.source_url}")

    title_roman = _strip_prefix(_node_text(chapter_div, "TitleNumber"), "TITLE ") or target.title_roman
    title_heading = _node_text(chapter_div, "TitleName")
    chapter = _strip_prefix(_node_text(chapter_div, "ChapterNumber"), "CHAPTER ") or target.chapter
    chapter_heading = _node_text(chapter_div, "ChapterName")
    chapter_record = FloridaChapter(
        title_roman=title_roman,
        title_heading=title_heading,
        chapter=chapter,
        heading=chapter_heading,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        ordinal=ordinal,
    )

    parts: list[FloridaPart] = []
    sections: list[FloridaSection] = []
    section_ordinal = 0
    direct_sections = chapter_div.find_all("div", class_="Section", recursive=False)
    for section_div in direct_sections:
        section_ordinal += 1
        sections.append(
            _parse_florida_section(
                section_div,
                title_roman=title_roman,
                chapter=chapter,
                parent_citation_path=chapter_record.citation_path,
                source=source,
                ordinal=section_ordinal,
            )
        )

    for part_div in chapter_div.find_all("div", class_="Part", recursive=False):
        part = _parse_florida_part(
            part_div,
            title_roman=title_roman,
            chapter=chapter,
            source=source,
            ordinal=len(parts) + 1,
        )
        parts.append(part)
        for section_div in part_div.find_all("div", class_="Section", recursive=False):
            section_ordinal += 1
            sections.append(
                _parse_florida_section(
                    section_div,
                    title_roman=title_roman,
                    chapter=chapter,
                    parent_citation_path=part.citation_path,
                    source=source,
                    ordinal=section_ordinal,
                )
            )

    return FloridaChapterDocument(
        chapter=chapter_record,
        parts=tuple(parts),
        sections=tuple(sections),
    )


def _parse_florida_part(
    part_div: Tag,
    *,
    title_roman: str,
    chapter: str,
    source: _RecordedSource,
    ordinal: int,
) -> FloridaPart:
    part_number = _node_text(part_div, "PartNumber") or f"PART {ordinal}"
    part = _strip_prefix(part_number, "PART ") or part_number
    heading_node = part_div.find("span", class_="PartTitle") or part_div.find(
        "span",
        class_="PartName",
    )
    heading = (
        _clean_text(heading_node.get_text(" ", strip=True))
        if isinstance(heading_node, Tag)
        else None
    )
    return FloridaPart(
        title_roman=title_roman,
        chapter=chapter,
        part=part,
        heading=heading,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        ordinal=ordinal,
    )


def _parse_florida_section(
    section_div: Tag,
    *,
    title_roman: str,
    chapter: str,
    parent_citation_path: str,
    source: _RecordedSource,
    ordinal: int,
) -> FloridaSection:
    section_number = _node_text(section_div, "SectionNumber") or ""
    match = _SECTION_NUMBER_RE.search(section_number)
    if match is None:
        raise ValueError(f"Florida section without numeric section id in {source.source_url}")
    section = _clean_section_number(match.group("section"))
    heading = _node_text(section_div, "CatchlineText") or _node_text(section_div, "Catchline")
    heading = _clean_heading(heading)
    body_node = section_div.find(class_="SectionBody", recursive=False)
    body = _clean_text(body_node.get_text(" ", strip=True)) if isinstance(body_node, Tag) else None
    history = tuple(
        _clean_text(node.get_text(" ", strip=True))
        for node in section_div.find_all("div", class_="History", recursive=False)
    )
    notes = tuple(
        _clean_text(node.get_text(" ", strip=True))
        for node in section_div.find_all("div", class_="Note", recursive=False)
    )
    body_text = body or ""
    status = "repealed" if re.search(r"\b(repealed|expired)\b", heading or "", re.I) else None
    return FloridaSection(
        title_roman=title_roman,
        chapter=chapter,
        section=section,
        heading=heading,
        body=body,
        parent_citation_path=parent_citation_path,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        ordinal=ordinal,
        references_to=_extract_references(section_div, full_text=body_text),
        source_history=tuple(value for value in history if value),
        notes=tuple(value for value in notes if value),
        status=status,
    )


def _title_inventory_item(title: FloridaTitle) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=title.citation_path,
        source_url=title.source_url,
        source_path=title.source_path,
        source_format=title.source_format,
        sha256=title.sha256,
        metadata={
            "kind": "title",
            "title": title.roman,
            "heading": title.heading,
            "source_id": title.source_id,
        },
    )


def _chapter_inventory_item(chapter: FloridaChapter) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=chapter.citation_path,
        source_url=chapter.source_url,
        source_path=chapter.source_path,
        source_format=chapter.source_format,
        sha256=chapter.sha256,
        metadata={
            "kind": "chapter",
            "title": chapter.title_roman,
            "chapter": chapter.chapter,
            "heading": chapter.heading,
            "source_id": chapter.source_id,
            "parent_citation_path": f"us-fl/statute/title-{chapter.title_roman.lower()}",
        },
    )


def _part_inventory_item(part: FloridaPart) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=part.citation_path,
        source_url=part.source_url,
        source_path=part.source_path,
        source_format=part.source_format,
        sha256=part.sha256,
        metadata={
            "kind": "part",
            "title": part.title_roman,
            "chapter": part.chapter,
            "part": part.part,
            "heading": part.heading,
            "source_id": part.source_id,
            "parent_citation_path": part.parent_citation_path,
        },
    )


def _section_inventory_item(section: FloridaSection) -> SourceInventoryItem:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": section.title_roman,
        "chapter": section.chapter,
        "section": section.section,
        "heading": section.heading,
        "source_id": section.source_id,
        "parent_citation_path": section.parent_citation_path,
        "references_to": list(section.references_to),
    }
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.notes:
        metadata["notes"] = list(section.notes)
    if section.status:
        metadata["status"] = section.status
    return SourceInventoryItem(
        citation_path=section.citation_path,
        source_url=section.source_url,
        source_path=section.source_path,
        source_format=section.source_format,
        sha256=section.sha256,
        metadata=metadata,
    )


def _title_record(
    title: FloridaTitle,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(title.citation_path),
        jurisdiction="us-fl",
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
        parent_citation_path=None,
        parent_id=None,
        level=0,
        ordinal=title.ordinal,
        kind="title",
        legal_identifier=title.legal_identifier,
        identifiers={"fl:title": title.roman},
        metadata={"title": title.roman},
    )


def _chapter_record(
    chapter: FloridaChapter,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    parent = f"us-fl/statute/title-{chapter.title_roman.lower()}"
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-fl",
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
        parent_citation_path=parent,
        parent_id=deterministic_provision_id(parent),
        level=1,
        ordinal=chapter.ordinal,
        kind="chapter",
        legal_identifier=chapter.legal_identifier,
        identifiers={"fl:title": chapter.title_roman, "fl:chapter": chapter.chapter},
        metadata={"title": chapter.title_roman, "chapter": chapter.chapter},
    )


def _part_record(
    part: FloridaPart,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(part.citation_path),
        jurisdiction="us-fl",
        document_class=DocumentClass.STATUTE.value,
        citation_path=part.citation_path,
        citation_label=part.legal_identifier,
        heading=part.heading,
        body=None,
        version=version,
        source_url=part.source_url,
        source_path=part.source_path,
        source_id=part.source_id,
        source_format=part.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=part.parent_citation_path,
        parent_id=deterministic_provision_id(part.parent_citation_path),
        level=2,
        ordinal=part.ordinal,
        kind="part",
        legal_identifier=part.legal_identifier,
        identifiers={
            "fl:title": part.title_roman,
            "fl:chapter": part.chapter,
            "fl:part": part.part,
        },
        metadata={"title": part.title_roman, "chapter": part.chapter, "part": part.part},
    )


def _section_record(
    section: FloridaSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    metadata: dict[str, Any] = {
        "title": section.title_roman,
        "chapter": section.chapter,
        "section": section.section,
        "references_to": list(section.references_to),
    }
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.notes:
        metadata["notes"] = list(section.notes)
    if section.status:
        metadata["status"] = section.status
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-fl",
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
        level=3 if "/part-" in section.parent_citation_path else 2,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=section.legal_identifier,
        identifiers={
            "fl:title": section.title_roman,
            "fl:chapter": section.chapter,
            "fl:section": section.section,
        },
        metadata=metadata,
    )


def _append_unique(
    seen: set[str],
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    item: SourceInventoryItem,
    record: ProvisionRecord,
) -> bool:
    if record.citation_path in seen:
        return False
    seen.add(record.citation_path)
    items.append(item)
    records.append(record)
    return True


def _record_source(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    source: _FloridaSource,
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


def _download_florida_source(
    source_url: str,
    *,
    fetcher: _FloridaFetcher,
    request_delay_seconds: float,
    timeout_seconds: float,
    request_attempts: int,
) -> bytes:
    headers = {"User-Agent": FLORIDA_USER_AGENT}
    last_error: requests.RequestException | None = None
    for attempt in range(1, request_attempts + 1):
        fetcher.wait_for_request_slot()
        try:
            response = requests.get(source_url, headers=headers, timeout=timeout_seconds)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= request_attempts:
                break
            time.sleep(max(request_delay_seconds, 0.2) * attempt)
    assert last_error is not None
    raise last_error


def _write_cache_bytes(path: Path, data: bytes) -> None:
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _florida_statute_url(chapter_path: str, *, source_year: int = FLORIDA_STATUTES_DEFAULT_YEAR) -> str:
    params = {
        "App_mode": "Display_Statute",
        "URL": chapter_path,
        "StatuteYear": str(source_year),
    }
    return urljoin(FLORIDA_STATUTES_BASE_URL, "index.cfm") + "?" + urlencode(params)


def _normalize_chapter_path(value: str) -> str:
    cleaned = value.strip().lstrip("/")
    if not re.fullmatch(r"\d{4}-\d{4}/\d{4}/\d{4}\.html", cleaned):
        raise ValueError(f"invalid Florida chapter path: {value!r}")
    return cleaned


def _node_text(root: Tag, class_name: str) -> str | None:
    node = root.find(class_=class_name)
    if not isinstance(node, Tag):
        return None
    return _clean_text(node.get_text(" ", strip=True)) or None


def _strip_prefix(value: str | None, prefix: str) -> str | None:
    if value is None:
        return None
    if value.upper().startswith(prefix.upper()):
        return value[len(prefix) :].strip()
    return value.strip()


def _clean_heading(value: str | None) -> str | None:
    if value is None:
        return None
    text = _clean_text(value)
    text = re.sub(r"\s*[\u2014-]\s*$", "", text)
    return text or None


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u2003", " ")).strip()


def _clean_section_number(value: str) -> str:
    text = value.strip().replace("\u2003", "")
    text = re.sub(r"^0+(?=\d)", "", text)
    return text


def _extract_references(section_div: Tag, *, full_text: str) -> tuple[str, ...]:
    refs: set[str] = set()
    for link in section_div.find_all("a", href=True):
        match = _REFERENCE_HREF_RE.search(str(link["href"]))
        if match:
            refs.add(f"us-fl/statute/{_clean_section_number(match.group('padded'))}")
    for match in _REFERENCE_TEXT_RE.finditer(full_text):
        refs.add(f"us-fl/statute/{_clean_section_number(match.group('section'))}")
    return tuple(sorted(refs))


def _title_filter(value: str | None) -> str | None:
    if value is None:
        return None
    token = str(value).strip().upper().removeprefix("TITLE ").removeprefix("TITLE-")
    if token not in FLORIDA_TITLE_ROMANS:
        raise ValueError(f"unsupported Florida Statutes title filter: {value!r}")
    return token


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _florida_run_id(
    version: str,
    *,
    title_filter: str | None,
    limit: int | None,
) -> str:
    parts = [version]
    if title_filter:
        parts.append(f"title-{title_filter.lower()}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _state_source_key(jurisdiction: str, run_id: str, relative: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative}"
