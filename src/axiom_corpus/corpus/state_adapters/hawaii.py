"""Hawaii Revised Statutes source-first corpus adapter."""

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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

HAWAII_HRS_BASE_URL = "https://data.capitol.hawaii.gov/hrscurrent/"
HAWAII_ROOT_INDEX_SOURCE_FORMAT = "hawaii-hrs-root-index-html"
HAWAII_VOLUME_DIRECTORY_SOURCE_FORMAT = "hawaii-hrs-volume-directory-html"
HAWAII_CHAPTER_DIRECTORY_SOURCE_FORMAT = "hawaii-hrs-chapter-directory-html"
HAWAII_CHAPTER_INDEX_SOURCE_FORMAT = "hawaii-hrs-chapter-index-html"
HAWAII_SECTION_SOURCE_FORMAT = "hawaii-hrs-section-html"
HAWAII_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_VOLUME_HREF_RE = re.compile(r"/hrscurrent/Vol(?P<volume>\d{2})_Ch(?P<range>[^/]+)/?$")
_HRS_CHAPTER_DIR_RE = re.compile(r"/(?P<code>HRS(?P<padded>\d{4}[A-Z]?))/?$", re.I)
_HRS_FILE_RE = re.compile(
    r"/HRS(?P<padded>\d{4}[A-Z]?)/(?P<filename>HRS_(?P=padded)-(?P<section>[\d_]+)?\.htm)$",
    re.I,
)
_SECTION_NUMBER_PATTERN = r"\d+[A-Z]?-\d+(?:\.\d+)?[A-Z]?"
_SECTION_RANGE_TAIL_PATTERN = (
    rf"(?:(?:\s+to\s+|\s*,\s*)(?:{_SECTION_NUMBER_PATTERN}|\d+(?:\.\d+)?))*"
)
_SECTION_RANGE_PATTERN = rf"(?P<section>{_SECTION_NUMBER_PATTERN}){_SECTION_RANGE_TAIL_PATTERN}"
_SECTION_HEADING_RE = re.compile(
    rf"^\s*\u00a7?\s*{_SECTION_RANGE_PATTERN}"
    r"\s+(?P<heading>.+?)\.?\s*$",
    re.I | re.S,
)
_SECTION_HEADING_PREFIX_RE = re.compile(
    rf"^\s*\u00a7?\s*{_SECTION_RANGE_PATTERN}\]?"
    r"\s+(?P<heading>.+?\.)(?:\s+(?P<body>.*))?$",
    re.I | re.S,
)
_SECTION_REFERENCE_RE = re.compile(
    rf"(?:\bsections?\s+|\u00a7+\s*)(?P<section>{_SECTION_NUMBER_PATTERN})",
    re.I,
)
_POSSIBLE_HISTORY_RE = re.compile(
    r"^\[\s*(?:L|RL|CC|PC|HRS|am|gen|rep|ren)\b",
    re.I,
)
_INLINE_HISTORY_RE = re.compile(
    r"\s*(?P<history>\[(?:L|RL|CC|PC|HRS|am|gen|rep|ren)\b[^\]]+\])\s*$",
    re.I | re.S,
)
_NOTES_HEADING_RE = re.compile(
    r"^(?:Note|Cross References|Revision Note|Attorney General Opinions|Law Journals and Reviews|"
    r"Case Notes|Rules of Court|Rules|COMMENTARY)\s*$",
    re.I,
)


@dataclass(frozen=True)
class HawaiiVolume:
    """One volume directory from the official current HRS directory."""

    number: str
    chapter_range: str
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"volume-{self.number}"

    @property
    def citation_path(self) -> str:
        return f"us-hi/statute/{self.source_id}"

    @property
    def heading(self) -> str:
        return f"Chapters {self.chapter_range}"

    @property
    def legal_identifier(self) -> str:
        return f"Hawaii Revised Statutes volume {int(self.number)}"


@dataclass(frozen=True)
class HawaiiChapterDirectory:
    """One HRS chapter directory discovered from an official volume directory."""

    volume_number: str
    chapter: str
    padded_chapter: str
    source_url: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter}"

    @property
    def citation_path(self) -> str:
        return f"us-hi/statute/chapter-{self.chapter}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-hi/statute/volume-{self.volume_number}"


@dataclass(frozen=True)
class HawaiiChapter:
    """One HRS chapter parsed from the official chapter contents page."""

    directory: HawaiiChapterDirectory
    heading: str
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    status: str | None = None

    @property
    def chapter(self) -> str:
        return self.directory.chapter

    @property
    def source_id(self) -> str:
        return self.directory.source_id

    @property
    def citation_path(self) -> str:
        return self.directory.citation_path

    @property
    def parent_citation_path(self) -> str:
        return self.directory.parent_citation_path

    @property
    def legal_identifier(self) -> str:
        return f"Haw. Rev. Stat. ch. {self.chapter}"


@dataclass(frozen=True)
class HawaiiSectionListing:
    """One section HTML file listed under an official HRS chapter directory."""

    volume_number: str
    chapter: str
    padded_chapter: str
    filename: str
    source_url: str
    ordinal: int
    fallback_section: str

    @property
    def relative_source_name(self) -> str:
        return (
            f"{HAWAII_SECTION_SOURCE_FORMAT}/volume-{self.volume_number}/"
            f"HRS{self.padded_chapter}/{self.filename}"
        )


@dataclass(frozen=True)
class HawaiiSection:
    """One HRS section parsed from official section HTML."""

    listing: HawaiiSectionListing
    section: str
    heading: str
    body: str | None
    source_history: tuple[str, ...]
    source_notes: tuple[str, ...]
    references_to: tuple[str, ...]
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    part_heading: str | None = None
    status: str | None = None

    @property
    def source_id(self) -> str:
        return self.section

    @property
    def citation_path(self) -> str:
        return f"us-hi/statute/{self.section}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-hi/statute/chapter-{self.listing.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"Haw. Rev. Stat. \u00a7 {self.section}"


@dataclass(frozen=True)
class _HawaiiSource:
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
class _HawaiiSectionFetchResult:
    listing: HawaiiSectionListing
    source: _HawaiiSource | None = None
    error: BaseException | None = None


class _HawaiiFetcher:
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
        self.base_url = base_url.rstrip("/") + "/"
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_root_index(self) -> _HawaiiSource:
        relative_path = f"{HAWAII_ROOT_INDEX_SOURCE_FORMAT}/index.html"
        data = self._fetch(relative_path, self.base_url)
        return _HawaiiSource(
            relative_path=relative_path,
            source_url=self.base_url,
            source_format=HAWAII_ROOT_INDEX_SOURCE_FORMAT,
            data=data,
        )

    def fetch_volume_directory(self, volume: HawaiiVolume) -> _HawaiiSource:
        relative_path = f"{HAWAII_VOLUME_DIRECTORY_SOURCE_FORMAT}/volume-{volume.number}.html"
        data = self._fetch(relative_path, volume.source_url)
        return _HawaiiSource(
            relative_path=relative_path,
            source_url=volume.source_url,
            source_format=HAWAII_VOLUME_DIRECTORY_SOURCE_FORMAT,
            data=data,
        )

    def fetch_chapter_directory(self, directory: HawaiiChapterDirectory) -> _HawaiiSource:
        relative_path = (
            f"{HAWAII_CHAPTER_DIRECTORY_SOURCE_FORMAT}/volume-{directory.volume_number}/"
            f"HRS{directory.padded_chapter}.html"
        )
        data = self._fetch(relative_path, directory.source_url)
        return _HawaiiSource(
            relative_path=relative_path,
            source_url=directory.source_url,
            source_format=HAWAII_CHAPTER_DIRECTORY_SOURCE_FORMAT,
            data=data,
        )

    def fetch_chapter_index(self, directory: HawaiiChapterDirectory) -> _HawaiiSource:
        filename = f"HRS_{directory.padded_chapter}-.htm"
        source_url = urljoin(directory.source_url.rstrip("/") + "/", filename)
        relative_path = (
            f"{HAWAII_CHAPTER_INDEX_SOURCE_FORMAT}/volume-{directory.volume_number}/"
            f"HRS{directory.padded_chapter}.html"
        )
        data = self._fetch(relative_path, source_url)
        return _HawaiiSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=HAWAII_CHAPTER_INDEX_SOURCE_FORMAT,
            data=data,
        )

    def fetch_section(self, listing: HawaiiSectionListing) -> _HawaiiSource:
        data = self._fetch(listing.relative_source_name, listing.source_url)
        return _HawaiiSource(
            relative_path=listing.relative_source_name,
            source_url=listing.source_url,
            source_format=HAWAII_SECTION_SOURCE_FORMAT,
            data=data,
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_hawaii_source(
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


def extract_hawaii_revised_statutes(
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
    base_url: str = HAWAII_HRS_BASE_URL,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
    workers: int = 8,
) -> StateStatuteExtractReport:
    """Snapshot official current Hawaii Revised Statutes HTML and extract provisions."""
    jurisdiction = "us-hi"
    volume_filter = _volume_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter)
    run_id = _hawaii_run_id(
        version,
        volume_filter=volume_filter,
        chapter_filter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _HawaiiFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        base_url=base_url,
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

    root_source = fetcher.fetch_root_index()
    root_recorded = _record_source(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        source=root_source,
    )
    source_paths.append(
        store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            root_source.relative_path,
        )
    )
    volumes = parse_hawaii_root_index(
        root_source.data,
        source=root_recorded,
        base_url=base_url,
    )
    if volume_filter is not None:
        volumes = tuple(volume for volume in volumes if volume.number == volume_filter)
    if not volumes:
        raise ValueError(f"no Hawaii HRS volumes selected for filter: {only_title!r}")

    for volume in volumes:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        if _append_unique(
            seen,
            items,
            records,
            _volume_inventory_item(volume),
            _volume_record(
                volume,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            ),
        ):
            title_count += 1
            container_count += 1

        volume_source = fetcher.fetch_volume_directory(volume)
        _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=volume_source,
        )
        source_paths.append(
            store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                volume_source.relative_path,
            )
        )
        chapter_directories = parse_hawaii_volume_directory(
            volume_source.data,
            volume=volume,
            base_url=base_url,
        )
        if chapter_filter is not None:
            chapter_directories = tuple(
                directory
                for directory in chapter_directories
                if directory.chapter == chapter_filter
            )

        for directory in chapter_directories:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            chapter_directory_source = fetcher.fetch_chapter_directory(directory)
            chapter_directory_recorded = _record_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                source=chapter_directory_source,
            )
            source_paths.append(
                store.source_path(
                    jurisdiction,
                    DocumentClass.STATUTE,
                    run_id,
                    chapter_directory_source.relative_path,
                )
            )
            listings = parse_hawaii_chapter_directory(
                chapter_directory_source.data,
                directory=directory,
                base_url=base_url,
            )

            chapter_index_source = fetcher.fetch_chapter_index(directory)
            chapter_index_recorded = _record_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                source=chapter_index_source,
            )
            source_paths.append(
                store.source_path(
                    jurisdiction,
                    DocumentClass.STATUTE,
                    run_id,
                    chapter_index_source.relative_path,
                )
            )
            chapter = parse_hawaii_chapter_index(
                chapter_index_source.data,
                directory=directory,
                source=chapter_index_recorded,
            )
            if _append_unique(
                seen,
                items,
                records,
                _chapter_inventory_item(chapter),
                _chapter_record(
                    chapter,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    directory_source=chapter_directory_recorded,
                ),
            ):
                container_count += 1

            selected_listings: list[HawaiiSectionListing] = []
            for listing in listings:
                if (
                    remaining_sections is not None
                    and len(selected_listings) >= remaining_sections
                ):
                    break
                selected_listings.append(listing)

            for result in _fetch_hawaii_section_results(
                fetcher,
                selected_listings,
                workers=workers,
            ):
                if result.error is not None:
                    errors.append(
                        f"section {result.listing.fallback_section}: {result.error}"
                    )
                    continue
                assert result.source is not None
                section_recorded = _record_source(
                    store,
                    jurisdiction=jurisdiction,
                    run_id=run_id,
                    source=result.source,
                )
                source_paths.append(
                    store.source_path(
                        jurisdiction,
                        DocumentClass.STATUTE,
                        run_id,
                        result.source.relative_path,
                    )
                )
                try:
                    section = parse_hawaii_section_page(
                        result.source.data,
                        listing=result.listing,
                        source=section_recorded,
                    )
                except ValueError as exc:
                    errors.append(
                        f"section {result.listing.fallback_section}: {exc}"
                    )
                    continue
                if section.citation_path in seen:
                    errors.append(f"duplicate citation path: {section.citation_path}")
                    continue
                if _append_unique(
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
                ):
                    section_count += 1
                    if remaining_sections is not None:
                        remaining_sections -= 1

    if not records:
        raise ValueError("no Hawaii Revised Statutes provisions extracted")

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


def parse_hawaii_root_index(
    html: str | bytes,
    *,
    source: _RecordedSource,
    base_url: str = HAWAII_HRS_BASE_URL,
) -> tuple[HawaiiVolume, ...]:
    """Parse the official current HRS directory root into volume directories."""
    soup = BeautifulSoup(_decode(html), "lxml")
    volumes: list[HawaiiVolume] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        match = _VOLUME_HREF_RE.search(href)
        if match is None:
            continue
        number = match.group("volume")
        if number in seen:
            continue
        seen.add(number)
        volumes.append(
            HawaiiVolume(
                number=number,
                chapter_range=match.group("range"),
                source_url=urljoin(base_url, href),
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                ordinal=len(volumes) + 1,
            )
        )
    return tuple(volumes)


def parse_hawaii_volume_directory(
    html: str | bytes,
    *,
    volume: HawaiiVolume,
    base_url: str = HAWAII_HRS_BASE_URL,
) -> tuple[HawaiiChapterDirectory, ...]:
    """Parse one official HRS volume directory into HRS chapter directories."""
    soup = BeautifulSoup(_decode(html), "lxml")
    directories: list[HawaiiChapterDirectory] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        match = _HRS_CHAPTER_DIR_RE.search(href.rstrip("/"))
        if match is None:
            continue
        padded = match.group("padded").upper()
        chapter = _strip_padded_chapter(padded)
        if chapter in seen:
            continue
        seen.add(chapter)
        directories.append(
            HawaiiChapterDirectory(
                volume_number=volume.number,
                chapter=chapter,
                padded_chapter=padded,
                source_url=urljoin(base_url, href.rstrip("/") + "/"),
                ordinal=len(directories) + 1,
            )
        )
    return tuple(directories)


def parse_hawaii_chapter_directory(
    html: str | bytes,
    *,
    directory: HawaiiChapterDirectory,
    base_url: str = HAWAII_HRS_BASE_URL,
) -> tuple[HawaiiSectionListing, ...]:
    """Parse an official HRS chapter directory listing into section files."""
    soup = BeautifulSoup(_decode(html), "lxml")
    listings: list[HawaiiSectionListing] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        match = _HRS_FILE_RE.search(href)
        if match is None:
            continue
        section_token = match.group("section")
        if not section_token:
            continue
        filename = match.group("filename")
        if filename in seen:
            continue
        seen.add(filename)
        listings.append(
            HawaiiSectionListing(
                volume_number=directory.volume_number,
                chapter=directory.chapter,
                padded_chapter=directory.padded_chapter,
                filename=filename,
                source_url=urljoin(base_url, href),
                ordinal=len(listings) + 1,
                fallback_section=_section_from_filename(
                    directory.chapter,
                    section_token,
                ),
            )
        )
    return tuple(listings)


def parse_hawaii_chapter_index(
    html: str | bytes,
    *,
    directory: HawaiiChapterDirectory,
    source: _RecordedSource,
) -> HawaiiChapter:
    """Parse the official chapter contents file into a chapter container."""
    soup = BeautifulSoup(_decode(html), "lxml")
    text_rows = [_clean_text(tag) for tag in soup.find_all("p")]
    text_rows = [row for row in text_rows if row]
    heading = f"Chapter {directory.chapter}"
    for index, row in enumerate(text_rows):
        if re.fullmatch(rf"CHAPTER\s+{re.escape(directory.chapter)}", row, re.I):
            if index + 1 < len(text_rows):
                heading = _strip_terminal_period(text_rows[index + 1].title())
            break
    status = _status_from_heading(heading)
    return HawaiiChapter(
        directory=directory,
        heading=heading,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        status=status,
    )


def parse_hawaii_section_page(
    html: str | bytes,
    *,
    listing: HawaiiSectionListing,
    source: _RecordedSource,
) -> HawaiiSection:
    """Parse one official HRS section page into normalized section text."""
    soup = BeautifulSoup(_decode(html), "lxml")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    page_links = soup.find(id="pageLinks")
    if isinstance(page_links, Tag):
        page_links.decompose()
    container = soup.find(class_="WordSection1") or soup.body or soup
    paragraphs = _paragraphs(container)
    if not paragraphs:
        raise ValueError("empty section page")

    heading_index = None
    section = listing.fallback_section
    prefix_section = section
    prefix_heading = f"Section {listing.fallback_section}"
    heading = f"Section {listing.fallback_section}"
    for index, (tag, text) in enumerate(paragraphs):
        bold = tag.find("b") if isinstance(tag, Tag) else None
        candidates = [text]
        if isinstance(bold, Tag):
            candidates.insert(0, _clean_text(bold))
        for candidate in candidates:
            match = _match_section_heading(candidate)
            if match is None:
                continue
            prefix_section = match.group("section").upper()
            section = prefix_section
            prefix_heading = _strip_terminal_period(match.group("heading"))
            heading = prefix_heading
            section, heading = _repair_split_section_suffix(
                section=section,
                heading=heading,
                fallback_section=listing.fallback_section,
            )
            if (
                "REPEALED" in heading.upper()
                and listing.fallback_section.upper() != section
            ):
                section = listing.fallback_section.upper()
            heading_index = index
            break
        if heading_index is not None:
            break
    if heading_index is None:
        raise ValueError("missing section heading")

    part_heading = _part_heading_before(paragraphs[:heading_index])
    body_lines: list[str] = []
    source_history: list[str] = []
    source_notes: list[str] = []
    in_notes = False
    for index, (tag, text) in enumerate(paragraphs[heading_index:]):
        if index == 0:
            text = _remove_heading_prefix(
                text,
                section=prefix_section,
                heading=prefix_heading,
            )
        if not text:
            continue
        classes = set(tag.get("class", [])) if isinstance(tag, Tag) else set()
        is_note_class = any(str(class_name).lower().startswith("xnotes") for class_name in classes)
        if is_note_class or _NOTES_HEADING_RE.match(text):
            in_notes = True
            source_notes.append(text)
            continue
        if _POSSIBLE_HISTORY_RE.search(text):
            source_history.append(text)
            continue
        if in_notes:
            source_notes.append(text)
        else:
            body_lines.append(text)
    body = _normalize_body("\n".join(body_lines))
    body, inline_history = _pop_inline_history(body)
    source_history.extend(inline_history)
    if body is not None and re.match(r"^L\s+\d{4}\b", body):
        source_history.append(body)
        body = None
    status = _section_status(heading, body, source_history, source_notes)
    references_to = tuple(
        _extract_references("\n".join([heading, body or "", *source_notes]))
    )
    return HawaiiSection(
        listing=listing,
        section=section,
        heading=heading,
        body=body,
        source_history=tuple(source_history),
        source_notes=tuple(source_notes),
        references_to=references_to,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        part_heading=part_heading,
        status=status,
    )


def _fetch_hawaii_section_results(
    fetcher: _HawaiiFetcher,
    listings: list[HawaiiSectionListing],
    *,
    workers: int,
) -> list[_HawaiiSectionFetchResult]:
    if not listings:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_hawaii_section_result(fetcher, listing) for listing in listings]
    results: list[_HawaiiSectionFetchResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_hawaii_section_result, fetcher, listing): listing
            for listing in listings
        }
        for future in as_completed(future_map):
            listing = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_HawaiiSectionFetchResult(listing=listing, error=exc))
    order = {listing.source_url: index for index, listing in enumerate(listings)}
    return sorted(results, key=lambda result: order[result.listing.source_url])


def _fetch_hawaii_section_result(
    fetcher: _HawaiiFetcher,
    listing: HawaiiSectionListing,
) -> _HawaiiSectionFetchResult:
    try:
        return _HawaiiSectionFetchResult(
            listing=listing,
            source=fetcher.fetch_section(listing),
        )
    except BaseException as exc:  # pragma: no cover
        return _HawaiiSectionFetchResult(listing=listing, error=exc)


def _volume_inventory_item(volume: HawaiiVolume) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=volume.citation_path,
        source_url=volume.source_url,
        source_path=volume.source_path,
        source_format=volume.source_format,
        sha256=volume.sha256,
        metadata={
            "kind": "volume",
            "volume": volume.number,
            "chapter_range": volume.chapter_range,
        },
    )


def _volume_record(
    volume: HawaiiVolume,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    metadata = {
        "kind": "volume",
        "volume": volume.number,
        "chapter_range": volume.chapter_range,
    }
    return ProvisionRecord(
        id=deterministic_provision_id(volume.citation_path),
        jurisdiction="us-hi",
        document_class=DocumentClass.STATUTE.value,
        citation_path=volume.citation_path,
        body=None,
        heading=volume.heading,
        citation_label=volume.legal_identifier,
        version=version,
        source_url=volume.source_url,
        source_path=volume.source_path,
        source_id=volume.source_id,
        source_format=volume.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        level=0,
        ordinal=volume.ordinal,
        kind="volume",
        legal_identifier=volume.legal_identifier,
        identifiers={"hawaii:volume": volume.number},
        metadata=metadata,
    )


def _chapter_inventory_item(chapter: HawaiiChapter) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=chapter.citation_path,
        source_url=chapter.source_url,
        source_path=chapter.source_path,
        source_format=chapter.source_format,
        sha256=chapter.sha256,
        metadata=_chapter_metadata(chapter),
    )


def _chapter_record(
    chapter: HawaiiChapter,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
    directory_source: _RecordedSource,
) -> ProvisionRecord:
    metadata = _chapter_metadata(chapter)
    metadata["chapter_directory_source_path"] = directory_source.source_path
    metadata["chapter_directory_sha256"] = directory_source.sha256
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-hi",
        document_class=DocumentClass.STATUTE.value,
        citation_path=chapter.citation_path,
        body=None,
        heading=chapter.heading,
        citation_label=chapter.legal_identifier,
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
        ordinal=chapter.directory.ordinal,
        kind="chapter",
        legal_identifier=chapter.legal_identifier,
        identifiers={
            "hawaii:volume": chapter.directory.volume_number,
            "hawaii:chapter": chapter.chapter,
        },
        metadata=metadata,
    )


def _chapter_metadata(chapter: HawaiiChapter) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "chapter",
        "volume": chapter.directory.volume_number,
        "chapter": chapter.chapter,
        "padded_chapter": chapter.directory.padded_chapter,
    }
    if chapter.status:
        metadata["status"] = chapter.status
    return metadata


def _section_inventory_item(section: HawaiiSection) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=section.citation_path,
        source_url=section.source_url,
        source_path=section.source_path,
        source_format=section.source_format,
        sha256=section.sha256,
        metadata=_section_metadata(section),
    )


def _section_record(
    section: HawaiiSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-hi",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        body=section.body,
        heading=section.heading,
        citation_label=section.legal_identifier,
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
        ordinal=section.listing.ordinal,
        kind="section",
        legal_identifier=section.legal_identifier,
        identifiers={
            "hawaii:volume": section.listing.volume_number,
            "hawaii:chapter": section.listing.chapter,
            "hawaii:section": section.section,
        },
        metadata=_section_metadata(section),
    )


def _section_metadata(section: HawaiiSection) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "volume": section.listing.volume_number,
        "chapter": section.listing.chapter,
        "section": section.section,
        "source_filename": section.listing.filename,
    }
    if section.part_heading:
        metadata["part_heading"] = section.part_heading
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.source_notes:
        metadata["source_notes"] = list(section.source_notes)
    if section.status:
        metadata["status"] = section.status
    return metadata


def _append_unique(
    seen: set[str],
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    item: SourceInventoryItem,
    record: ProvisionRecord,
) -> bool:
    if item.citation_path in seen:
        return False
    seen.add(item.citation_path)
    items.append(item)
    records.append(record)
    return True


def _record_source(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    source: _HawaiiSource,
) -> _RecordedSource:
    path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        source.relative_path,
    )
    sha = store.write_bytes(path, source.data)
    return _RecordedSource(
        source_url=source.source_url,
        source_path=_store_relative_path(store, path),
        source_format=source.source_format,
        sha256=sha,
    )


def _download_hawaii_source(
    source_url: str,
    *,
    fetcher: _HawaiiFetcher,
    request_delay_seconds: float,
    timeout_seconds: float,
    request_attempts: int,
) -> bytes:
    last_error: BaseException | None = None
    for attempt in range(1, request_attempts + 1):
        try:
            fetcher.wait_for_request_slot()
            response = requests.get(
                source_url,
                timeout=timeout_seconds,
                headers={"User-Agent": HAWAII_USER_AGENT},
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds, 0.25) * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"Hawaii source request failed: {source_url}")


def _paragraphs(container: Any) -> list[tuple[Tag, str]]:
    paragraphs: list[tuple[Tag, str]] = []
    for tag in container.find_all("p") if hasattr(container, "find_all") else []:
        if not isinstance(tag, Tag):
            continue
        text = _clean_text(tag)
        if text:
            paragraphs.append((tag, text))
    return paragraphs


def _part_heading_before(paragraphs: list[tuple[Tag, str]]) -> str | None:
    for _, text in reversed(paragraphs):
        if re.match(r"^PART\s+[IVXLCDM0-9A-Z]+\.?\s+", text, re.I):
            return text
    return None


def _match_section_heading(text: str) -> re.Match[str] | None:
    candidate = _normalize_section_heading_candidate(text)
    return _SECTION_HEADING_PREFIX_RE.match(candidate) or _SECTION_HEADING_RE.match(candidate)


def _normalize_section_heading_candidate(text: str) -> str:
    normalized = text.replace("[", "").replace("]", "")
    normalized = normalized.replace(", and ", ", ")
    normalized = normalized.replace("\u2010", "-").replace("\u2011", "-")
    normalized = normalized.replace("\u2012", "-").replace("\u2013", "-")
    normalized = normalized.replace("\u2014", "-")
    normalized = normalized.replace("\u00a7\u00a7", "\u00a7")
    normalized = re.sub(r"\u00a7\s+\u00a7", "\u00a7", normalized)
    normalized = re.sub(r"\u00a7\s*", "\u00a7", normalized)
    normalized = re.sub(r"(\u00a7[0-9A-Z]+)\s+([0-9A-Z]+-)", r"\1\2", normalized)
    normalized = re.sub(r"(?<=[0-9A-Z])\s*-\s*(?=\d)", "-", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _repair_split_section_suffix(
    *,
    section: str,
    heading: str,
    fallback_section: str,
) -> tuple[str, str]:
    """Repair HRS pages where Word markup splits the final section suffix."""
    normalized_section = section.upper()
    normalized_fallback = fallback_section.upper()
    if not normalized_fallback.startswith(normalized_section):
        return normalized_section, heading
    suffix = normalized_fallback.removeprefix(normalized_section)
    if not suffix:
        return normalized_section, heading
    if not heading.upper().startswith(suffix):
        return normalized_section, heading
    repaired_heading = heading[len(suffix) :].strip()
    return normalized_fallback, repaired_heading or heading


def _remove_heading_prefix(text: str, *, section: str, heading: str) -> str:
    normalized = _normalize_section_heading_candidate(text)
    pattern = re.compile(
        rf"^\u00a7?\s*{re.escape(section)}"
        rf"{_SECTION_RANGE_TAIL_PATTERN}"
        rf"\s+{re.escape(heading)}\.?\s*",
        re.I | re.S,
    )
    match = pattern.match(normalized)
    if match is None:
        return text
    return normalized[match.end() :].strip()


def _pop_inline_history(body: str | None) -> tuple[str | None, list[str]]:
    if not body:
        return body, []
    match = _INLINE_HISTORY_RE.search(body)
    if match is None:
        return body, []
    history = match.group("history").strip()
    remaining = body[: match.start()].strip()
    return (remaining or None), [history]


def _section_from_filename(chapter: str, section_token: str) -> str:
    pieces = [piece for piece in section_token.split("_") if piece]
    if not pieces:
        return chapter
    base = _strip_leading_zeroes(pieces[0])
    if len(pieces) == 1:
        return f"{chapter}-{base}"
    decimal = "".join(_strip_leading_zeroes(piece) for piece in pieces[1:])
    return f"{chapter}-{base}.{decimal}"


def _strip_padded_chapter(value: str) -> str:
    match = re.match(r"(?P<number>\d+)(?P<suffix>[A-Z]?)$", value, re.I)
    if match is None:
        return value.upper()
    return f"{int(match.group('number'))}{match.group('suffix').upper()}"


def _strip_leading_zeroes(value: str) -> str:
    return str(int(value)) if value.isdigit() else value.lstrip("0") or "0"


def _section_status(
    heading: str,
    body: str | None,
    history: list[str],
    notes: list[str],
) -> str | None:
    text = "\n".join([heading, body or "", *history, *notes])
    if re.search(r"\bRepealed\b", text, re.I):
        return "repealed"
    if re.search(r"\bRenumbered\b", text, re.I):
        return "renumbered"
    return None


def _status_from_heading(heading: str) -> str | None:
    if re.search(r"\bRepealed\b", heading, re.I):
        return "repealed"
    return None


def _extract_references(text: str) -> list[str]:
    refs = [
        f"us-hi/statute/{match.group('section').upper()}"
        for match in _SECTION_REFERENCE_RE.finditer(text)
    ]
    return _dedupe_preserve_order(refs)


def _hawaii_run_id(
    version: str,
    *,
    volume_filter: str | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    if volume_filter is None and chapter_filter is None and limit is None:
        return version
    parts = [version, "us-hi"]
    if volume_filter is not None:
        parts.append(f"volume-{volume_filter}")
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter.lower()}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _volume_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:vol(?:ume)?)[-\s]*", "", text, flags=re.I)
    if not text:
        return None
    return f"{int(text):02d}" if text.isdigit() else text.upper()


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:chapter|ch\.?)[-\s]*", "", text, flags=re.I)
    return _strip_padded_chapter(text.upper()) if text else None


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _normalize_body(text: str) -> str | None:
    normalized = text.replace("\xa0", " ")
    normalized = re.sub(r"[ \t\r\f\v]+", " ", normalized)
    normalized = re.sub(r"\n[ \t]+", "\n", normalized)
    normalized = re.sub(r"[ \t]+\n", "\n", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    normalized = normalized.strip()
    return normalized or None


def _decode(value: str | bytes) -> str:
    if isinstance(value, str):
        return value
    try:
        return value.decode("utf-8-sig")
    except UnicodeDecodeError:
        return value.decode("iso-8859-1", errors="replace")


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


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
