"""Iowa Code source-first corpus adapter."""

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
from urllib.parse import urlencode, urljoin

import fitz
import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

IOWA_CODE_DEFAULT_YEAR = 2026
IOWA_CODE_BASE_URL = "https://www.legis.iowa.gov"
IOWA_TITLE_INDEX_SOURCE_FORMAT = "iowa-code-title-index-html"
IOWA_CHAPTER_INDEX_SOURCE_FORMAT = "iowa-code-chapter-index-html"
IOWA_SECTION_INDEX_SOURCE_FORMAT = "iowa-code-section-index-html"
IOWA_SECTION_PDF_SOURCE_FORMAT = "iowa-code-section-pdf"
IOWA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_TITLE_RE = re.compile(
    r"^\s*Title\s+(?P<roman>[IVXLCDM]+)\s*-\s*(?P<heading>.+?)"
    r"(?:\s*\(Ch\.\s*(?P<chapter_range>[^)]+)\))?\s*$",
    re.I,
)
_CHAPTER_RE = re.compile(
    r"^\s*Chapter\s+(?P<chapter>\d+[A-Z]*)\s*-\s*(?P<heading>.+?)\s*$",
    re.I,
)
_SECTION_LISTING_RE = re.compile(
    r"^\s*\u00a7?\s*(?P<section>\d+[A-Z]*(?:\.[0-9A-Z]+)+)\s*-\s*(?P<heading>.+?)\s*$",
    re.I,
)
_SECTION_NUMBER_RE = re.compile(r"\b\d+[A-Z]*(?:\.[0-9A-Z]+)+\b", re.I)
_SECTION_WORD_REFERENCE_RE = re.compile(
    rf"\bsections?\s+(?P<ref>{_SECTION_NUMBER_RE.pattern})",
    re.I,
)
_SECTION_SYMBOL_REFERENCE_RE = re.compile(
    rf"\u00a7+\s*(?P<ref>{_SECTION_NUMBER_RE.pattern})",
    re.I,
)


@dataclass(frozen=True)
class IowaTitle:
    """One Iowa Code title from the official title index."""

    roman: str
    heading: str
    chapter_range: str | None
    source_url: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"title-{self.roman.lower()}"

    @property
    def citation_path(self) -> str:
        return f"us-ia/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"Iowa Code Title {self.roman}"


@dataclass(frozen=True)
class IowaChapter:
    """One Iowa Code chapter from a title chapter index."""

    title_roman: str
    title_heading: str
    chapter: str
    heading: str
    source_url: str
    pdf_url: str | None
    rtf_url: str | None
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter}"

    @property
    def citation_path(self) -> str:
        return f"us-ia/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"Iowa Code ch. {self.chapter}"


@dataclass(frozen=True)
class IowaSectionListing:
    """One section listing row from an official Iowa chapter section index."""

    title_roman: str
    chapter: str
    section: str
    heading: str
    pdf_url: str
    rtf_url: str | None
    ordinal: int

    @property
    def source_id(self) -> str:
        return self.section

    @property
    def citation_path(self) -> str:
        return f"us-ia/statute/{self.section}"

    @property
    def legal_identifier(self) -> str:
        return f"Iowa Code \u00a7 {self.section}"


@dataclass(frozen=True)
class IowaParsedSectionText:
    """Normalized text and notes parsed from one Iowa section PDF."""

    body: str | None
    source_history: tuple[str, ...]
    source_notes: tuple[str, ...]
    references_to: tuple[str, ...]
    status: str | None


@dataclass(frozen=True)
class _IowaSource:
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
class _IowaSectionFetchResult:
    section: IowaSectionListing
    source: _IowaSource | None = None
    parsed_text: IowaParsedSectionText | None = None
    error: BaseException | None = None


class _IowaFetcher:
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
        self.base_url = base_url.rstrip("/")
        self.source_year = source_year
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_title_index(self) -> _IowaSource:
        relative_path = f"{IOWA_TITLE_INDEX_SOURCE_FORMAT}/{self.source_year}.html"
        source_url = f"{self.base_url}/law/iowaCode?{urlencode({'year': self.source_year})}"
        data = self._fetch(relative_path, source_url)
        return _IowaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=IOWA_TITLE_INDEX_SOURCE_FORMAT,
            data=data,
        )

    def fetch_chapter_index(self, title: IowaTitle) -> _IowaSource:
        relative_path = f"{IOWA_CHAPTER_INDEX_SOURCE_FORMAT}/title-{title.roman}.html"
        source_url = f"{self.base_url}/law/iowaCode/chapters?{urlencode({'title': title.roman, 'year': self.source_year})}"
        data = self._fetch(relative_path, source_url)
        return _IowaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=IOWA_CHAPTER_INDEX_SOURCE_FORMAT,
            data=data,
        )

    def fetch_section_index(self, chapter: IowaChapter) -> _IowaSource:
        relative_path = f"{IOWA_SECTION_INDEX_SOURCE_FORMAT}/chapter-{chapter.chapter}.html"
        source_url = (
            f"{self.base_url}/law/iowaCode/sections?"
            f"{urlencode({'codeChapter': chapter.chapter, 'year': self.source_year})}"
        )
        data = self._fetch(relative_path, source_url)
        return _IowaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=IOWA_SECTION_INDEX_SOURCE_FORMAT,
            data=data,
        )

    def fetch_section_pdf(self, section: IowaSectionListing) -> _IowaSource:
        relative_path = (
            f"{IOWA_SECTION_PDF_SOURCE_FORMAT}/chapter-{section.chapter}/"
            f"{section.section}.pdf"
        )
        data = self._fetch(relative_path, section.pdf_url)
        return _IowaSource(
            relative_path=relative_path,
            source_url=section.pdf_url,
            source_format=IOWA_SECTION_PDF_SOURCE_FORMAT,
            data=data,
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_iowa_source(
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


def extract_iowa_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_year: int = IOWA_CODE_DEFAULT_YEAR,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    only_chapter: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    request_delay_seconds: float = 0.05,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
    workers: int = 1,
    base_url: str = IOWA_CODE_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official Iowa Code indexes/PDFs and extract provisions."""
    jurisdiction = "us-ia"
    title_filter = _title_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter)
    run_id = _iowa_run_id(
        version,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or str(version)
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _IowaFetcher(
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
    errors: list[str] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    remaining_sections = limit

    title_index = fetcher.fetch_title_index()
    title_index_path, title_index_recorded = _record_source(
        store,
        jurisdiction,
        run_id,
        title_index,
    )
    source_paths.append(title_index_path)
    titles = parse_iowa_title_index(title_index.data, base_url=base_url)
    if title_filter is not None:
        titles = tuple(title for title in titles if title.roman == title_filter)
    if not titles:
        raise ValueError(f"no Iowa titles selected for filter: {only_title!r}")

    for title in titles:
        if remaining_sections is not None and remaining_sections <= 0:
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
                source_url=title.source_url,
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
                identifiers={"iowa:title": title.roman},
                metadata={
                    "kind": "title",
                    "source_year": source_year,
                    **(
                        {"chapter_range": title.chapter_range}
                        if title.chapter_range is not None
                        else {}
                    ),
                },
            )

        chapter_index = fetcher.fetch_chapter_index(title)
        chapter_index_path, chapter_index_recorded = _record_source(
            store,
            jurisdiction,
            run_id,
            chapter_index,
        )
        source_paths.append(chapter_index_path)
        chapters = parse_iowa_chapter_index(chapter_index.data, title=title, base_url=base_url)
        if chapter_filter is not None:
            chapters = tuple(chapter for chapter in chapters if chapter.chapter == chapter_filter)
        for chapter in chapters:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            if chapter.citation_path not in seen:
                seen.add(chapter.citation_path)
                container_count += 1
                _append_record(
                    items,
                    records,
                    jurisdiction=jurisdiction,
                    citation_path=chapter.citation_path,
                    version=run_id,
                    source_url=chapter.source_url,
                    source_path=chapter_index_recorded.source_path,
                    source_format=chapter_index_recorded.source_format,
                    source_id=chapter.source_id,
                    sha256=chapter_index_recorded.sha256,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    kind="chapter",
                    body=None,
                    heading=chapter.heading,
                    legal_identifier=chapter.legal_identifier,
                    parent_citation_path=title.citation_path,
                    level=1,
                    ordinal=chapter.ordinal,
                    identifiers={"iowa:title": title.roman, "iowa:chapter": chapter.chapter},
                    metadata={
                        "kind": "chapter",
                        "source_year": source_year,
                        "title": title.roman,
                        "chapter": chapter.chapter,
                        **({"chapter_pdf_url": chapter.pdf_url} if chapter.pdf_url else {}),
                        **({"chapter_rtf_url": chapter.rtf_url} if chapter.rtf_url else {}),
                    },
                )

            section_index = fetcher.fetch_section_index(chapter)
            section_index_path, section_index_recorded = _record_source(
                store,
                jurisdiction,
                run_id,
                section_index,
            )
            source_paths.append(section_index_path)
            sections = parse_iowa_section_index(
                section_index.data,
                chapter=chapter,
                base_url=base_url,
            )
            if not sections:
                continue
            selected_sections: list[IowaSectionListing] = []
            for section in sections:
                if (
                    remaining_sections is not None
                    and len(selected_sections) >= remaining_sections
                ):
                    break
                if section.citation_path in seen:
                    errors.append(f"duplicate citation path: {section.citation_path}")
                    continue
                selected_sections.append(section)
            for result in _fetch_iowa_section_pdf_results(
                fetcher,
                selected_sections,
                source_year=source_year,
                workers=workers,
            ):
                section = result.section
                if result.error is not None:
                    errors.append(f"section {section.section}: {result.error}")
                    continue
                assert result.source is not None
                assert result.parsed_text is not None
                pdf_path, pdf_recorded = _record_source(
                    store,
                    jurisdiction,
                    run_id,
                    result.source,
                )
                source_paths.append(pdf_path)
                parsed_text = result.parsed_text

                seen.add(section.citation_path)
                section_count += 1
                metadata: dict[str, Any] = {
                    "kind": "section",
                    "source_year": source_year,
                    "title": section.title_roman,
                    "chapter": section.chapter,
                    "section": section.section,
                    "section_index_source_path": section_index_recorded.source_path,
                    "section_index_sha256": section_index_recorded.sha256,
                }
                if section.rtf_url:
                    metadata["section_rtf_url"] = section.rtf_url
                if parsed_text.references_to:
                    metadata["references_to"] = list(parsed_text.references_to)
                if parsed_text.source_history:
                    metadata["source_history"] = list(parsed_text.source_history)
                if parsed_text.source_notes:
                    metadata["source_notes"] = list(parsed_text.source_notes)
                if parsed_text.status:
                    metadata["status"] = parsed_text.status
                _append_record(
                    items,
                    records,
                    jurisdiction=jurisdiction,
                    citation_path=section.citation_path,
                    version=run_id,
                    source_url=section.pdf_url,
                    source_path=pdf_recorded.source_path,
                    source_format=pdf_recorded.source_format,
                    source_id=section.source_id,
                    sha256=pdf_recorded.sha256,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    kind="section",
                    body=parsed_text.body,
                    heading=section.heading,
                    legal_identifier=section.legal_identifier,
                    parent_citation_path=chapter.citation_path,
                    level=2,
                    ordinal=section.ordinal,
                    identifiers={
                        "iowa:title": section.title_roman,
                        "iowa:chapter": section.chapter,
                        "iowa:section": section.section,
                    },
                    metadata=metadata,
                )
                if remaining_sections is not None:
                    remaining_sections -= 1

    if not records:
        raise ValueError("no Iowa provisions extracted")

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


def _fetch_iowa_section_pdf_results(
    fetcher: _IowaFetcher,
    sections: list[IowaSectionListing],
    *,
    source_year: int,
    workers: int,
) -> list[_IowaSectionFetchResult]:
    if not sections:
        return []
    if workers <= 1 or len(sections) == 1:
        return [
            _fetch_one_iowa_section_pdf(fetcher, section, source_year=source_year)
            for section in sections
        ]
    results: dict[int, _IowaSectionFetchResult] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(
                _fetch_one_iowa_section_pdf,
                fetcher,
                section,
                source_year=source_year,
            ): index
            for index, section in enumerate(sections)
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return [results[index] for index in range(len(sections))]


def _fetch_one_iowa_section_pdf(
    fetcher: _IowaFetcher,
    section: IowaSectionListing,
    *,
    source_year: int,
) -> _IowaSectionFetchResult:
    try:
        source = fetcher.fetch_section_pdf(section)
        parsed_text = parse_iowa_section_pdf(
            source.data,
            section=section.section,
            heading=section.heading,
            source_year=source_year,
        )
        return _IowaSectionFetchResult(section=section, source=source, parsed_text=parsed_text)
    except (requests.RequestException, OSError, ValueError) as exc:
        return _IowaSectionFetchResult(section=section, error=exc)


def parse_iowa_title_index(
    html: str | bytes,
    *,
    base_url: str = IOWA_CODE_BASE_URL,
) -> tuple[IowaTitle, ...]:
    """Parse the official Iowa Code title index."""
    soup = BeautifulSoup(_decode(html), "lxml")
    rows = soup.select("#iacList tbody tr") or soup.find_all("tr")
    titles: list[IowaTitle] = []
    ordinal = 0
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        title_text = _clean_text(cells[0])
        match = _TITLE_RE.match(title_text)
        link = row.find("a", href=re.compile(r"/law/iowaCode/chapters\?", re.I))
        if match is None or link is None:
            continue
        ordinal += 1
        titles.append(
            IowaTitle(
                roman=match.group("roman").upper(),
                heading=match.group("heading").strip(),
                chapter_range=(
                    match.group("chapter_range").strip()
                    if match.group("chapter_range")
                    else None
                ),
                source_url=urljoin(base_url, str(link["href"])),
                ordinal=ordinal,
            )
        )
    return tuple(titles)


def parse_iowa_chapter_index(
    html: str | bytes,
    *,
    title: IowaTitle,
    base_url: str = IOWA_CODE_BASE_URL,
) -> tuple[IowaChapter, ...]:
    """Parse one official Iowa Code title chapter index."""
    soup = BeautifulSoup(_decode(html), "lxml")
    rows = soup.select("#iacList tbody tr") or soup.find_all("tr")
    chapters: list[IowaChapter] = []
    ordinal = 0
    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue
        chapter_text = _clean_text(cells[0])
        match = _CHAPTER_RE.match(chapter_text)
        sections_link = row.find("a", href=re.compile(r"/law/iowaCode/sections\?", re.I))
        if match is None or sections_link is None:
            continue
        heading = match.group("heading").strip()
        if heading.upper() == "RESERVED":
            continue
        ordinal += 1
        pdf_url: str | None = None
        rtf_url: str | None = None
        for link in row.find_all("a", href=True):
            href = str(link["href"])
            if href.lower().endswith(".pdf") and "/docs/code/" in href and pdf_url is None:
                pdf_url = urljoin(base_url, href)
            if href.lower().endswith(".rtf") and "/docs/code/" in href and rtf_url is None:
                rtf_url = urljoin(base_url, href)
        chapters.append(
            IowaChapter(
                title_roman=title.roman,
                title_heading=title.heading,
                chapter=match.group("chapter").upper(),
                heading=heading,
                source_url=urljoin(base_url, str(sections_link["href"])),
                pdf_url=pdf_url,
                rtf_url=rtf_url,
                ordinal=ordinal,
            )
        )
    return tuple(chapters)


def parse_iowa_section_index(
    html: str | bytes,
    *,
    chapter: IowaChapter,
    base_url: str = IOWA_CODE_BASE_URL,
) -> tuple[IowaSectionListing, ...]:
    """Parse one official Iowa Code chapter section index."""
    soup = BeautifulSoup(_decode(html), "lxml")
    rows = soup.select("#iacList tbody tr") or soup.find_all("tr")
    sections: list[IowaSectionListing] = []
    ordinal = 0
    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue
        section_text = _clean_text(cells[0])
        match = _SECTION_LISTING_RE.match(section_text)
        if match is None:
            continue
        pdf_url: str | None = None
        rtf_url: str | None = None
        for link in row.find_all("a", href=True):
            href = str(link["href"])
            if href.lower().endswith(".pdf") and "/docs/code/" in href and pdf_url is None:
                pdf_url = urljoin(base_url, href)
            if href.lower().endswith(".rtf") and "/docs/code/" in href and rtf_url is None:
                rtf_url = urljoin(base_url, href)
        if pdf_url is None:
            continue
        ordinal += 1
        sections.append(
            IowaSectionListing(
                title_roman=chapter.title_roman,
                chapter=chapter.chapter,
                section=match.group("section").upper(),
                heading=match.group("heading").strip(),
                pdf_url=pdf_url,
                rtf_url=rtf_url,
                ordinal=ordinal,
            )
        )
    return tuple(sections)


def parse_iowa_section_pdf(
    data: bytes,
    *,
    section: str,
    heading: str | None = None,
    source_year: int = IOWA_CODE_DEFAULT_YEAR,
) -> IowaParsedSectionText:
    """Extract section body text and official notes from one Iowa section PDF."""
    lines = _pdf_text_lines(data, section=section, source_year=source_year)
    content_lines = _drop_section_heading(lines, section=section)
    body_lines, note_lines = _split_body_and_notes(content_lines)
    body = _join_statute_lines(body_lines)
    notes = _join_note_lines(note_lines)
    source_history, source_notes = _split_history_notes(notes)
    reference_text = "\n".join(line for line in [body, *source_notes] if line)
    references_to = _references_to(reference_text, current_section=section)
    status = _section_status(heading, body, notes)
    return IowaParsedSectionText(
        body=body,
        source_history=source_history,
        source_notes=source_notes,
        references_to=references_to,
        status=status,
    )


def _pdf_text_lines(data: bytes, *, section: str, source_year: int) -> list[str]:
    try:
        document = fitz.open(stream=data, filetype="pdf")
    except Exception as exc:  # pragma: no cover - PyMuPDF exception classes vary.
        raise ValueError(f"invalid Iowa section PDF: {exc}") from exc
    lines: list[str] = []
    for page in document:
        for raw_line in page.get_text("text").splitlines():
            line = _clean_text(raw_line)
            if not line:
                continue
            if _is_iowa_pdf_running_text(line, section=section, source_year=source_year):
                continue
            lines.append(line)
    return lines


def _is_iowa_pdf_running_text(line: str, *, section: str, source_year: int) -> bool:
    if line.isdigit():
        return True
    if re.match(r"^[A-Z][a-z]{2}\s+[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4}$", line):
        return True
    if re.fullmatch(rf"Iowa Code {source_year}, Section {re.escape(section)}.*", line):
        return True
    if re.fullmatch(rf"\u00a7{re.escape(section)}, .+", line):
        return True
    return bool(re.fullmatch(rf".+,\s*\u00a7{re.escape(section)}", line))


def _drop_section_heading(lines: list[str], *, section: str) -> list[str]:
    remaining: list[str] = []
    dropped = False
    for line in lines:
        if not dropped and re.match(rf"^{re.escape(section)}\s+\S+", line):
            dropped = True
            continue
        remaining.append(line)
    return remaining


def _split_body_and_notes(lines: list[str]) -> tuple[list[str], list[str]]:
    for index, line in enumerate(lines):
        if _starts_iowa_source_note(line):
            return lines[:index], lines[index:]
    return lines, []


def _starts_iowa_source_note(line: str) -> bool:
    return bool(
        line.startswith("[")
        or re.match(r"^\d{2,4}\s+Acts,\s+ch\b", line)
        or line.startswith("Referred to in")
    )


def _join_statute_lines(lines: list[str]) -> str | None:
    paragraphs = _coalesce_lines(lines)
    text = "\n".join(paragraphs).strip()
    return text or None


def _join_note_lines(lines: list[str]) -> tuple[str, ...]:
    return tuple(_coalesce_lines(lines))


def _coalesce_lines(lines: list[str]) -> list[str]:
    paragraphs: list[str] = []
    current: list[str] = []
    for line in lines:
        if _starts_new_paragraph(line) and current:
            paragraphs.append(" ".join(current).strip())
            current = [line]
        else:
            current.append(line)
    if current:
        paragraphs.append(" ".join(current).strip())
    return [paragraph for paragraph in paragraphs if paragraph]


def _starts_new_paragraph(line: str) -> bool:
    return bool(
        re.match(r"^\d+[A-Z]?\.\s*", line)
        or re.match(r"^[a-z]\.\s*", line)
        or re.match(r"^\([0-9A-Za-z]+\)\s*", line)
        or line.startswith("[")
        or re.match(r"^\d{2,4}\s+Acts,\s+ch\b", line)
        or line.startswith("Referred to in")
        or re.match(r"^\d{4}\s+(?:amendment|amendments|strike)\b", line)
        or line.startswith("Subsection ")
    )


def _split_history_notes(notes: tuple[str, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    histories: list[str] = []
    source_notes: list[str] = []
    in_history = True
    for note in notes:
        if note.startswith("Referred to in") or re.match(r"^\d{4}\s+", note):
            in_history = False
        if in_history:
            histories.append(note)
        else:
            source_notes.append(note)
    return tuple(histories), tuple(source_notes)


def _references_to(text: str, *, current_section: str) -> tuple[str, ...]:
    references: list[str] = []
    for line in text.splitlines():
        referred_match = re.search(r"Referred to in\s+(?P<refs>.+)", line, re.I)
        if referred_match is not None:
            for match in _SECTION_NUMBER_RE.finditer(referred_match.group("refs")):
                _append_reference(
                    references,
                    match.group(0).upper(),
                    current_section=current_section,
                )
        for match in _SECTION_WORD_REFERENCE_RE.finditer(line):
            _append_reference(
                references,
                match.group("ref").upper(),
                current_section=current_section,
            )
        for match in _SECTION_SYMBOL_REFERENCE_RE.finditer(line):
            _append_reference(
                references,
                match.group("ref").upper(),
                current_section=current_section,
            )
    return tuple(_dedupe_preserve_order(references))


def _append_reference(references: list[str], section: str, *, current_section: str) -> None:
    if section != current_section:
        references.append(f"us-ia/statute/{section}")


def _section_status(
    heading: str | None,
    body: str | None,
    source_notes: tuple[str, ...],
) -> str | None:
    text = " ".join(part for part in [heading, body, *source_notes] if part).lower()
    if "repealed" in text:
        return "repealed"
    if "reserved" in text:
        return "reserved"
    return None


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
    source: _IowaSource,
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


def _download_iowa_source(
    source_url: str,
    *,
    fetcher: _IowaFetcher,
    request_delay_seconds: float,
    timeout_seconds: float,
    request_attempts: int,
) -> bytes:
    last_error: requests.RequestException | None = None
    for attempt in range(1, request_attempts + 1):
        try:
            fetcher.wait_for_request_slot()
            response = requests.get(
                source_url,
                headers={"User-Agent": IOWA_USER_AGENT},
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds, 0.25) * attempt)
    assert last_error is not None
    raise last_error


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _iowa_run_id(
    version: str,
    *,
    title_filter: str | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and chapter_filter is None and limit is None:
        return version
    parts = [version, "us-ia"]
    if title_filter is not None:
        parts.append(f"title-{title_filter.lower()}")
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter.lower()}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title|Title)[-\s]*", "", text)
    return text.upper() or None


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:chapter|Chapter)[-\s]*", "", text)
    return text.upper() or None


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _decode(value: str | bytes) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _clean_text(value: Any) -> str:
    text = value.get_text(" ", strip=True) if hasattr(value, "get_text") else str(value)
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


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
