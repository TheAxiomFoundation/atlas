"""Missouri Revised Statutes source-first corpus adapter."""

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
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

MISSOURI_REVISED_STATUTES_BASE_URL = "https://revisor.mo.gov/main/"
MISSOURI_ROOT_SOURCE_FORMAT = "missouri-rs-root-html"
MISSOURI_CHAPTER_SOURCE_FORMAT = "missouri-rs-chapter-html"
MISSOURI_VIEW_CHAPTER_SOURCE_FORMAT = "missouri-rs-view-chapter-html"
MISSOURI_SECTION_SOURCE_FORMAT = "missouri-rs-section-html"
MISSOURI_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_CHAPTER_HREF_RE = re.compile(r"/?main/OneChapter\.aspx\?chapter=(?P<chapter>\d+[A-Za-z]?)", re.I)
_SECTION_HREF_RE = re.compile(
    r"/?main/PageSelect\.aspx\?section=(?P<section>[^&]+)&bid=(?P<bid>\d+)",
    re.I,
)
_TITLE_SUMMARY_RE = re.compile(
    r"^(?P<roman>[IVXLCDM]+)\s+(?P<heading>.+)$",
    re.I,
)
_CHAPTER_LABEL_RE = re.compile(
    r"^(?P<chapter>\d+[A-Za-z]?)\s+(?P<heading>.+)$",
    re.I,
)
_SECTION_REFERENCE_RE = re.compile(
    r"(?:RSMo\s+|sections?\s+|section\s+|\u00a7\s*)"
    r"(?P<section>\d+[A-Za-z]?\.\d+[A-Za-z]?(?:\.\d+[A-Za-z]?)*)",
    re.I,
)
_EFFECTIVE_LONG_RE = re.compile(
    r"Effective\s*[-\u2010-\u2015]\s*(?P<day>\d{1,2})\s+"
    r"(?P<month>[A-Za-z]{3})\s+(?P<year>\d{4})",
    re.I,
)
_EFFECTIVE_SHORT_RE = re.compile(
    r"(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<year>\d{4})",
)


@dataclass(frozen=True)
class MissouriTitle:
    """One title listed in the official RSMo root index."""

    roman: str
    heading: str
    chapter_range: str | None
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"title-{_slug(self.roman)}"

    @property
    def citation_path(self) -> str:
        return f"us-mo/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"RSMo Title {self.roman}"


@dataclass(frozen=True)
class MissouriChapterListing:
    """One chapter link parsed from the official RSMo root index."""

    title_roman: str
    title_heading: str
    title_citation_path: str
    chapter: str
    heading: str
    source_url: str
    ordinal: int


@dataclass(frozen=True)
class MissouriChapter:
    """One Revised Statutes of Missouri chapter container."""

    title_roman: str
    title_heading: str
    title_citation_path: str
    chapter: str
    heading: str
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter.lower()}"

    @property
    def citation_path(self) -> str:
        return f"us-mo/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"RSMo Chapter {self.chapter}"


@dataclass(frozen=True)
class MissouriSectionListing:
    """One section link parsed from an official RSMo chapter page."""

    title_roman: str
    title_heading: str
    chapter: str
    chapter_heading: str
    chapter_citation_path: str
    section_label: str
    heading: str
    bid: str
    effective_date: str | None
    source_url: str
    ordinal: int

    @property
    def relative_source_name(self) -> str:
        return (
            f"{MISSOURI_SECTION_SOURCE_FORMAT}/chapter-{self.chapter.lower()}/"
            f"{_source_filename(self.section_label)}-{self.bid}.html"
        )


@dataclass(frozen=True)
class MissouriSection:
    """One current Revised Statutes of Missouri section."""

    listing: MissouriSectionListing
    section_label: str
    heading: str
    body: str | None
    source_history: tuple[str, ...]
    effective_text: str | None
    effective_date: str | None
    references_to: tuple[str, ...]
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    status: str | None = None

    @property
    def source_id(self) -> str:
        return _section_source_id(self.section_label)

    @property
    def citation_path(self) -> str:
        return f"us-mo/statute/{self.source_id}"

    @property
    def parent_citation_path(self) -> str:
        return self.listing.chapter_citation_path

    @property
    def legal_identifier(self) -> str:
        return f"Mo. Rev. Stat. \u00a7 {self.section_label}"


@dataclass(frozen=True)
class _MissouriSource:
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
class _MissouriChapterPage:
    listing: MissouriChapterListing
    source: _MissouriSource | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _MissouriSectionPage:
    listing: MissouriSectionListing
    source: _MissouriSource | None = None
    error: BaseException | None = None


class _MissouriFetcher:
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

    def fetch_root(self) -> _MissouriSource:
        relative_path = f"{MISSOURI_ROOT_SOURCE_FORMAT}/Home.aspx.html"
        source_url = urljoin(self.base_url, "Home.aspx")
        return _MissouriSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=MISSOURI_ROOT_SOURCE_FORMAT,
            data=self._fetch(relative_path, source_url),
        )

    def fetch_chapter(self, listing: MissouriChapterListing) -> _MissouriSource:
        relative_path = (
            f"{MISSOURI_CHAPTER_SOURCE_FORMAT}/chapter-{listing.chapter.lower()}.html"
        )
        return _MissouriSource(
            relative_path=relative_path,
            source_url=listing.source_url,
            source_format=MISSOURI_CHAPTER_SOURCE_FORMAT,
            data=self._fetch(relative_path, listing.source_url),
        )

    def fetch_view_chapter(self, listing: MissouriChapterListing) -> _MissouriSource:
        relative_path = (
            f"{MISSOURI_VIEW_CHAPTER_SOURCE_FORMAT}/chapter-{listing.chapter.lower()}.html"
        )
        source_url = urljoin(
            self.base_url,
            f"ViewChapter.aspx?chapter={quote(listing.chapter)}",
        )
        return _MissouriSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=MISSOURI_VIEW_CHAPTER_SOURCE_FORMAT,
            data=self._fetch(relative_path, source_url),
        )

    def fetch_section(self, listing: MissouriSectionListing) -> _MissouriSource:
        return _MissouriSource(
            relative_path=listing.relative_source_name,
            source_url=listing.source_url,
            source_format=MISSOURI_SECTION_SOURCE_FORMAT,
            data=self._fetch(listing.relative_source_name, listing.source_url),
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                cached_data = cached_path.read_bytes()
                if not _is_missouri_speeding_page(cached_data):
                    return cached_data
        data = _download_missouri_source(
            source_url,
            fetcher=self,
            request_delay_seconds=self.request_delay_seconds,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
        )
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return data

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            self._last_request_at = time.monotonic()


def extract_missouri_revised_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = MISSOURI_REVISED_STATUTES_BASE_URL,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
    workers: int = 8,
) -> StateStatuteExtractReport:
    """Snapshot official Revised Statutes of Missouri HTML and extract provisions."""
    jurisdiction = "us-mo"
    selection_filter = _selection_filter(only_title)
    run_id = _missouri_run_id(version, selection_filter=selection_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _MissouriFetcher(
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

    root_source = fetcher.fetch_root()
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
    titles, chapter_listings = parse_missouri_root(
        root_source.data,
        source=root_recorded,
        base_url=base_url,
    )
    titles, chapter_listings = _filter_root_entries(
        titles,
        chapter_listings,
        selection_filter=selection_filter,
    )
    if not chapter_listings:
        raise ValueError(f"no Missouri statute chapters selected for filter: {only_title!r}")

    for title in titles:
        if _append_unique(
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
        ):
            title_count += 1
            container_count += 1

    for chapter_page in _fetch_missouri_chapter_pages(
        fetcher,
        list(chapter_listings),
        workers=workers,
    ):
        if remaining_sections is not None and remaining_sections <= 0:
            break
        if chapter_page.error is not None:
            errors.append(f"chapter {chapter_page.listing.chapter}: {chapter_page.error}")
            continue
        assert chapter_page.source is not None
        chapter_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=chapter_page.source,
        )
        source_paths.append(
            store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                chapter_page.source.relative_path,
            )
        )
        chapter, listings = parse_missouri_chapter_page(
            chapter_page.source.data,
            listing=chapter_page.listing,
            source=chapter_recorded,
            base_url=base_url,
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
            ),
        ):
            container_count += 1

        selected_listings: list[MissouriSectionListing] = []
        for listing in listings:
            if (
                remaining_sections is not None
                and len(selected_listings) >= remaining_sections
            ):
                break
            selected_listings.append(listing)

        if not selected_listings:
            continue
        try:
            view_source = fetcher.fetch_view_chapter(chapter_page.listing)
        except BaseException as exc:  # pragma: no cover
            errors.append(f"chapter {chapter_page.listing.chapter} full text: {exc}")
            continue
        view_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=view_source,
        )
        source_paths.append(
            store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                view_source.relative_path,
            )
        )
        try:
            sections = parse_missouri_view_chapter_page(
                view_source.data,
                listings=tuple(selected_listings),
                source=view_recorded,
            )
        except ValueError as exc:
            errors.append(f"chapter {chapter.chapter} full text: {exc}")
            continue
        for section in sections:
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
        raise ValueError("no Missouri Revised Statutes provisions extracted")

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


def parse_missouri_root(
    html: str | bytes,
    *,
    source: _RecordedSource,
    base_url: str = MISSOURI_REVISED_STATUTES_BASE_URL,
) -> tuple[tuple[MissouriTitle, ...], tuple[MissouriChapterListing, ...]]:
    """Parse the official RSMo root page into title and chapter links."""
    soup = BeautifulSoup(_decode(html), "lxml")
    titles: list[MissouriTitle] = []
    chapters: list[MissouriChapterListing] = []
    seen_chapters: set[str] = set()
    for details in soup.find_all("details"):
        if not isinstance(details, Tag):
            continue
        summary = details.find("summary")
        if not isinstance(summary, Tag):
            continue
        summary_spans = [
            _clean_text(span) for span in summary.find_all("span", class_="lr-font-emph")
        ]
        title_text = summary_spans[-1] if summary_spans else _clean_text(summary)
        title_match = _TITLE_SUMMARY_RE.match(title_text)
        if title_match is None:
            continue
        roman = title_match.group("roman").upper()
        heading = _strip_terminal_period(title_match.group("heading").title())
        chapter_range = summary_spans[0] if len(summary_spans) > 1 else None
        title = MissouriTitle(
            roman=roman,
            heading=heading,
            chapter_range=chapter_range,
            source_url=source.source_url,
            source_path=source.source_path,
            source_format=source.source_format,
            sha256=source.sha256,
            ordinal=len(titles) + 1,
        )
        titles.append(title)
        for anchor in details.find_all("a", href=True):
            href = str(anchor["href"])
            match = _CHAPTER_HREF_RE.search(href)
            if match is None:
                continue
            chapter = match.group("chapter")
            if chapter in seen_chapters:
                continue
            seen_chapters.add(chapter)
            label = _clean_text(anchor)
            chapter_match = _CHAPTER_LABEL_RE.match(label)
            chapter_heading = (
                _strip_terminal_period(chapter_match.group("heading").title())
                if chapter_match is not None
                else _strip_terminal_period(label)
            )
            chapters.append(
                MissouriChapterListing(
                    title_roman=title.roman,
                    title_heading=title.heading,
                    title_citation_path=title.citation_path,
                    chapter=chapter,
                    heading=chapter_heading,
                    source_url=urljoin(base_url, f"OneChapter.aspx?chapter={quote(chapter)}"),
                    ordinal=len(chapters) + 1,
                )
            )
    return tuple(titles), tuple(chapters)


def parse_missouri_chapter_page(
    html: str | bytes,
    *,
    listing: MissouriChapterListing,
    source: _RecordedSource,
    base_url: str = MISSOURI_REVISED_STATUTES_BASE_URL,
) -> tuple[MissouriChapter, tuple[MissouriSectionListing, ...]]:
    """Parse one official RSMo chapter index page into section links."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapter_heading = listing.heading
    title_roman = listing.title_roman
    title_heading = listing.title_heading
    title_and_chapter = _chapter_header_text(soup)
    if title_and_chapter:
        title_match = re.search(
            r"Title\s+(?P<roman>[IVXLCDM]+)\s+(?P<title>.+?)\s+Chapter\s+"
            rf"{re.escape(listing.chapter)}\s+(?P<chapter>.+)$",
            title_and_chapter,
            re.I,
        )
        if title_match is not None:
            title_roman = title_match.group("roman").upper()
            title_heading = _strip_terminal_period(title_match.group("title").title())
            chapter_heading = _strip_terminal_period(
                re.split(r"[✹⚿]", title_match.group("chapter"), maxsplit=1)[0].title()
            )

    chapter = MissouriChapter(
        title_roman=title_roman,
        title_heading=title_heading,
        title_citation_path=listing.title_citation_path,
        chapter=listing.chapter,
        heading=chapter_heading,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        ordinal=listing.ordinal,
    )

    listings: list[MissouriSectionListing] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"])
        match = _SECTION_HREF_RE.search(href)
        if match is None:
            continue
        section_label = _normalize_section_label(unquote(match.group("section")))
        bid = match.group("bid")
        if section_label in seen:
            continue
        seen.add(section_label)
        row = anchor.find_parent("tr")
        heading, effective_date = _chapter_row_heading_and_date(row, anchor)
        listings.append(
            MissouriSectionListing(
                title_roman=title_roman,
                title_heading=title_heading,
                chapter=chapter.chapter,
                chapter_heading=chapter.heading,
                chapter_citation_path=chapter.citation_path,
                section_label=section_label,
                heading=heading or f"Section {section_label}",
                bid=bid,
                effective_date=effective_date,
                source_url=_section_source_url(base_url, section_label, bid),
                ordinal=len(listings) + 1,
            )
        )
    return chapter, tuple(listings)


def parse_missouri_section_page(
    html: str | bytes,
    *,
    listing: MissouriSectionListing,
    source: _RecordedSource,
) -> MissouriSection:
    """Parse one official current RSMo section page into normalized text."""
    soup = BeautifulSoup(_decode(html), "lxml")
    container = _section_container(soup, listing.section_label)
    if container is None:
        raise ValueError("missing current section container")
    heading = _section_heading(container, listing.section_label) or listing.heading
    effective_text = _clean_text(soup.find(id="effdt")) if soup.find(id="effdt") else None
    effective_date = _parse_effective_date(effective_text) or listing.effective_date
    source_history = tuple(_source_history(container))
    body = _section_body(container)
    references_to = tuple(
        _extract_references(
            "\n".join([heading, body or ""]),
            container=container,
            self_section=listing.section_label,
        )
    )
    status = _status(heading, body, source_history)
    return MissouriSection(
        listing=listing,
        section_label=listing.section_label,
        heading=heading,
        body=body,
        source_history=source_history,
        effective_text=effective_text,
        effective_date=effective_date,
        references_to=references_to,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        status=status,
    )


def parse_missouri_view_chapter_page(
    html: str | bytes,
    *,
    listings: tuple[MissouriSectionListing, ...],
    source: _RecordedSource,
) -> tuple[MissouriSection, ...]:
    """Parse an official full-chapter RSMo page into current section records."""
    soup = BeautifulSoup(_decode(html), "lxml")
    containers = _view_chapter_section_containers(soup)
    if not containers:
        raise ValueError("missing full-chapter section containers")
    if len(containers) < len(listings):
        raise ValueError(
            f"full-chapter section count mismatch: {len(containers)} < {len(listings)}"
        )
    sections: list[MissouriSection] = []
    for listing, container in zip(listings, containers, strict=False):
        heading = _section_heading(container, listing.section_label) or listing.heading
        source_history = tuple(_source_history(container))
        body = _section_body(container)
        effective_date = _view_chapter_effective_date(container) or listing.effective_date
        references_to = tuple(
            _extract_references(
                "\n".join([heading, body or ""]),
                container=container,
                self_section=listing.section_label,
            )
        )
        sections.append(
            MissouriSection(
                listing=listing,
                section_label=listing.section_label,
                heading=heading,
                body=body,
                source_history=source_history,
                effective_text=None,
                effective_date=effective_date,
                references_to=references_to,
                source_url=listing.source_url,
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                status=_status(heading, body, source_history),
            )
        )
    return tuple(sections)


def _fetch_missouri_chapter_pages(
    fetcher: _MissouriFetcher,
    listings: list[MissouriChapterListing],
    *,
    workers: int,
) -> list[_MissouriChapterPage]:
    if not listings:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_missouri_chapter_page(fetcher, listing) for listing in listings]
    results: list[_MissouriChapterPage] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_missouri_chapter_page, fetcher, listing): listing
            for listing in listings
        }
        for future in as_completed(future_map):
            listing = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_MissouriChapterPage(listing=listing, error=exc))
    order = {listing.chapter: index for index, listing in enumerate(listings)}
    return sorted(results, key=lambda page: order[page.listing.chapter])


def _fetch_missouri_chapter_page(
    fetcher: _MissouriFetcher,
    listing: MissouriChapterListing,
) -> _MissouriChapterPage:
    try:
        return _MissouriChapterPage(listing=listing, source=fetcher.fetch_chapter(listing))
    except BaseException as exc:  # pragma: no cover
        return _MissouriChapterPage(listing=listing, error=exc)


def _fetch_missouri_section_pages(
    fetcher: _MissouriFetcher,
    listings: list[MissouriSectionListing],
    *,
    workers: int,
) -> list[_MissouriSectionPage]:
    if not listings:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_missouri_section_page(fetcher, listing) for listing in listings]
    results: list[_MissouriSectionPage] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_missouri_section_page, fetcher, listing): listing
            for listing in listings
        }
        for future in as_completed(future_map):
            listing = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_MissouriSectionPage(listing=listing, error=exc))
    order = {listing.section_label: index for index, listing in enumerate(listings)}
    return sorted(results, key=lambda page: order[page.listing.section_label])


def _fetch_missouri_section_page(
    fetcher: _MissouriFetcher,
    listing: MissouriSectionListing,
) -> _MissouriSectionPage:
    try:
        return _MissouriSectionPage(listing=listing, source=fetcher.fetch_section(listing))
    except BaseException as exc:  # pragma: no cover
        return _MissouriSectionPage(listing=listing, error=exc)


def _title_inventory_item(title: MissouriTitle) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=title.citation_path,
        source_url=title.source_url,
        source_path=title.source_path,
        source_format=title.source_format,
        sha256=title.sha256,
        metadata={
            "kind": "title",
            "title": title.roman,
            "chapter_range": title.chapter_range,
        },
    )


def _title_record(
    title: MissouriTitle,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(title.citation_path),
        jurisdiction="us-mo",
        document_class=DocumentClass.STATUTE.value,
        citation_path=title.citation_path,
        body=None,
        heading=title.heading,
        citation_label=title.legal_identifier,
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
        identifiers={"missouri:title": title.roman},
        metadata={
            "kind": "title",
            "title": title.roman,
            "chapter_range": title.chapter_range,
        },
    )


def _chapter_inventory_item(chapter: MissouriChapter) -> SourceInventoryItem:
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
        },
    )


def _chapter_record(
    chapter: MissouriChapter,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-mo",
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
        parent_citation_path=chapter.title_citation_path,
        parent_id=deterministic_provision_id(chapter.title_citation_path),
        level=1,
        ordinal=chapter.ordinal,
        kind="chapter",
        legal_identifier=chapter.legal_identifier,
        identifiers={
            "missouri:title": chapter.title_roman,
            "missouri:chapter": chapter.chapter,
        },
        metadata={
            "kind": "chapter",
            "title": chapter.title_roman,
            "chapter": chapter.chapter,
        },
    )


def _section_inventory_item(section: MissouriSection) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=section.citation_path,
        source_url=section.source_url,
        source_path=section.source_path,
        source_format=section.source_format,
        sha256=section.sha256,
        metadata=_section_metadata(section),
    )


def _section_record(
    section: MissouriSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-mo",
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
            "missouri:title": section.listing.title_roman,
            "missouri:chapter": section.listing.chapter,
            "missouri:section": section.section_label,
            "missouri:bid": section.listing.bid,
        },
        metadata=_section_metadata(section),
    )


def _section_metadata(section: MissouriSection) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": section.listing.title_roman,
        "title_heading": section.listing.title_heading,
        "chapter": section.listing.chapter,
        "chapter_heading": section.listing.chapter_heading,
        "section": section.section_label,
        "bid": section.listing.bid,
    }
    if section.effective_date:
        metadata["effective_date"] = section.effective_date
    if section.effective_text:
        metadata["effective_text"] = section.effective_text
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.status:
        metadata["status"] = section.status
    return metadata


def _filter_root_entries(
    titles: tuple[MissouriTitle, ...],
    chapters: tuple[MissouriChapterListing, ...],
    *,
    selection_filter: str | None,
) -> tuple[tuple[MissouriTitle, ...], tuple[MissouriChapterListing, ...]]:
    if selection_filter is None:
        return titles, chapters
    selected_chapters = tuple(
        chapter
        for chapter in chapters
        if _matches_selection_filter(chapter.chapter, selection_filter)
        or _matches_selection_filter(chapter.title_roman, selection_filter)
        or _matches_selection_filter(_slug(chapter.title_heading), selection_filter)
    )
    selected_title_paths = {chapter.title_citation_path for chapter in selected_chapters}
    selected_titles = tuple(
        title for title in titles if title.citation_path in selected_title_paths
    )
    return selected_titles, selected_chapters


def _matches_selection_filter(value: str, selection_filter: str) -> bool:
    return _slug(value) == selection_filter


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
    source: _MissouriSource,
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


def _download_missouri_source(
    source_url: str,
    *,
    fetcher: _MissouriFetcher,
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
                headers={"User-Agent": MISSOURI_USER_AGENT},
            )
            response.raise_for_status()
            if _is_missouri_speeding_page(response.content):
                raise requests.RequestException("Missouri Revisor returned speeding page")
            return response.content
        except requests.RequestException as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds * 10, 3.0) * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"Missouri source request failed: {source_url}")


def _is_missouri_speeding_page(data: bytes) -> bool:
    return b"Speeding: 1" in data or b"Block.aspx" in data or b">Blocked<" in data


def _chapter_header_text(soup: BeautifulSoup) -> str | None:
    for tag in soup.find_all(["p", "div"]):
        text = _clean_text(tag)
        if len(text) > 300:
            continue
        if re.search(r"\bTitle\s+[IVXLCDM]+\b", text, re.I) and "Chapter" in text:
            return text
    return None


def _chapter_row_heading_and_date(row: Tag | None, anchor: Tag) -> tuple[str, str | None]:
    if not isinstance(row, Tag):
        return _strip_terminal_period(_clean_text(anchor)), None
    cells = row.find_all("td", recursive=False)
    heading_cell = cells[1] if len(cells) > 1 else row
    text = _clean_text(heading_cell)
    effective_date = _parse_effective_date(text)
    text = re.sub(r"\(\d{1,2}/\d{1,2}/\d{4}\)\s*$", "", text).strip()
    text = text.replace("...", "").strip()
    return _strip_terminal_period(text), effective_date


def _section_container(soup: BeautifulSoup, section_label: str) -> Tag | None:
    label = re.escape(section_label)
    for container in soup.find_all("div", class_="norm"):
        if not isinstance(container, Tag):
            continue
        bold = container.find("span", class_="bold")
        if not isinstance(bold, Tag):
            continue
        if re.match(rf"^\s*{label}\.", _clean_text(bold)):
            return container
    return None


def _view_chapter_section_containers(soup: BeautifulSoup) -> list[Tag]:
    containers: list[Tag] = []
    for container in soup.find_all("div", class_="norm"):
        if not isinstance(container, Tag):
            continue
        if isinstance(container.find("span", class_="bold"), Tag):
            containers.append(container)
    return containers


def _view_chapter_effective_date(container: Tag) -> str | None:
    sibling = container.find_next_sibling("p")
    if not isinstance(sibling, Tag):
        return None
    return _parse_effective_date(_clean_text(sibling))


def _section_heading(container: Tag, section_label: str) -> str | None:
    bold = container.find("span", class_="bold")
    if not isinstance(bold, Tag):
        return None
    heading = _clean_text(bold)
    heading = re.sub(rf"^\s*{re.escape(section_label)}\.\s*", "", heading)
    heading = re.sub(r"\s*(?:--+|[-\u2010-\u2015])\s*$", "", heading).strip()
    return _strip_terminal_period(heading) or None


def _source_history(container: Tag) -> list[str]:
    history: list[str] = []
    foot = container.find("div", class_="foot")
    if not isinstance(foot, Tag):
        return history
    for paragraph in foot.find_all("p"):
        text = _clean_text(paragraph)
        if not text or set(text) <= {"-", "\u00ad"}:
            continue
        history.append(text)
    return history


def _section_body(container: Tag) -> str | None:
    clone = BeautifulSoup(str(container), "lxml")
    for foot in clone.find_all("div", class_="foot"):
        foot.decompose()
    first_bold = clone.find("span", class_="bold")
    if isinstance(first_bold, Tag):
        first_bold.decompose()
    for tiny in clone.find_all(class_="tiny"):
        tiny.decompose()
    return _normalize_body(clone.get_text(" ", strip=True))


def _extract_references(
    text: str,
    *,
    container: Tag,
    self_section: str,
) -> list[str]:
    refs: list[str] = []
    for anchor in container.find_all("a", href=True):
        href = str(anchor["href"])
        parsed = urlparse(href)
        section_values = parse_qs(parsed.query).get("section", [])
        for section_value in section_values:
            section_label = _normalize_section_label(unquote(section_value))
            if section_label and section_label != self_section:
                refs.append(f"us-mo/statute/{_section_source_id(section_label)}")
    for match in _SECTION_REFERENCE_RE.finditer(text):
        section_label = _normalize_section_label(match.group("section"))
        if section_label and section_label != self_section:
            refs.append(f"us-mo/statute/{_section_source_id(section_label)}")
    return _dedupe_preserve_order(refs)


def _status(heading: str, body: str | None, history: tuple[str, ...]) -> str | None:
    text = "\n".join([heading, body or "", *history])
    if re.search(r"\bRepealed\b", text, re.I):
        return "repealed"
    if re.search(r"\bExpired\b", text, re.I):
        return "expired"
    if re.search(r"\bReserved\b", text, re.I):
        return "reserved"
    if re.search(r"\bTransferred\b", text, re.I):
        return "transferred"
    return None


def _section_source_url(base_url: str, section_label: str, bid: str) -> str:
    return urljoin(
        base_url.rstrip("/") + "/",
        "OneSection.aspx?" + urlencode({"section": section_label, "bid": bid}),
    )


def _source_filename(section_label: str) -> str:
    return re.sub(r"[^0-9A-Za-z.-]+", "-", section_label).strip("-").lower()


def _normalize_section_label(value: str) -> str:
    text = _clean_whitespace(value)
    text = text.replace("\u2010", "-").replace("\u2011", "-")
    text = text.replace("\u2012", "-").replace("\u2013", "-")
    text = text.replace("\u2014", "-")
    text = re.sub(r"\s+", "", text)
    return text.strip().removesuffix(".")


def _section_source_id(section_label: str) -> str:
    return _normalize_section_label(section_label).lower()


def _selection_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title|chapter|ch\.?)[-\s]*", "", text, flags=re.I)
    return _slug(text) if text else None


def _missouri_run_id(
    version: str,
    *,
    selection_filter: str | None,
    limit: int | None,
) -> str:
    if selection_filter is None and limit is None:
        return version
    parts = [version, "us-mo"]
    if selection_filter is not None:
        parts.append(selection_filter)
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _parse_effective_date(value: str | None) -> str | None:
    if not value:
        return None
    long_match = _EFFECTIVE_LONG_RE.search(value)
    if long_match is not None:
        month = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }.get(long_match.group("month").lower())
        if month is not None:
            try:
                return date(
                    int(long_match.group("year")),
                    month,
                    int(long_match.group("day")),
                ).isoformat()
            except ValueError:
                return None
    short_match = _EFFECTIVE_SHORT_RE.search(value)
    if short_match is None:
        return None
    try:
        return date(
            int(short_match.group("year")),
            int(short_match.group("month")),
            int(short_match.group("day")),
        ).isoformat()
    except ValueError:
        return None


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _normalize_body(text: str) -> str | None:
    lines = [_clean_whitespace(line) for line in text.splitlines()]
    normalized = "\n".join(line for line in lines if line)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    normalized = normalized.replace("\u00ad", "")
    normalized = normalized.strip()
    return normalized or None


def _decode(value: str | bytes) -> str:
    if isinstance(value, str):
        return value
    return value.decode("utf-8-sig", errors="replace")


def _clean_text(value: Any) -> str:
    text = value.get_text(" ", strip=True) if hasattr(value, "get_text") else str(value)
    return _clean_whitespace(text)


def _clean_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def _strip_terminal_period(value: str) -> str:
    return value.strip().removesuffix(".").strip()


def _slug(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"[^0-9a-z]+", "-", text)
    return text.strip("-")


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
