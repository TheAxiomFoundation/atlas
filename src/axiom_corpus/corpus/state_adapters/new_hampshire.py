"""New Hampshire Revised Statutes Annotated source-first corpus adapter."""

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
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

NEW_HAMPSHIRE_RSA_BASE_URL = "https://gc.nh.gov/rsa/html/"
NEW_HAMPSHIRE_ROOT_SOURCE_FORMAT = "new-hampshire-rsa-root-html"
NEW_HAMPSHIRE_TITLE_SOURCE_FORMAT = "new-hampshire-rsa-title-toc-html"
NEW_HAMPSHIRE_CHAPTER_SOURCE_FORMAT = "new-hampshire-rsa-chapter-toc-html"
NEW_HAMPSHIRE_MERGED_CHAPTER_SOURCE_FORMAT = "new-hampshire-rsa-merged-chapter-html"
NEW_HAMPSHIRE_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_TITLE_HREF_RE = re.compile(r"NHTOC/NHTOC-(?P<title>[IVXLCDM]+(?:-[A-Z])?)\.htm$", re.I)
_TITLE_LABEL_RE = re.compile(
    r"^TITLE\s+(?P<title>[IVXLCDM]+(?:-[A-Z])?):\s*(?P<heading>.+)$",
    re.I,
)
_CHAPTER_HREF_RE = re.compile(
    r"NHTOC-(?P<title>[IVXLCDM]+(?:-[A-Z])?)-(?P<chapter>\d+[A-Za-z]?(?:-[A-Za-z])?)\.htm$",
    re.I,
)
_CHAPTER_LABEL_RE = re.compile(
    r"^CHAPTER\s+(?P<chapter>\d+[A-Za-z]?(?:-[A-Za-z])?):\s*(?P<heading>.+)$",
    re.I,
)
_SECTION_HREF_RE = re.compile(
    r"(?P<chapter>\d+[A-Za-z]?(?:-[A-Za-z])?)-(?P<section>\d+[A-Za-z]?(?:-[A-Za-z])?)\.htm$",
    re.I,
)
_SECTION_LABEL_RE = re.compile(
    r"^Section:\s*(?P<label>\d+[A-Za-z]?(?:-[A-Za-z])?:\d+[A-Za-z]?(?:-[A-Za-z])?)\s+"
    r"(?P<heading>.+)$",
    re.I,
)
_RSA_REF_RE = re.compile(
    r"\bRSA\s+(?P<label>\d+[A-Za-z]?(?:-[A-Za-z])?:\d+[A-Za-z]?(?:-[A-Za-z])?)",
    re.I,
)


@dataclass(frozen=True)
class NewHampshireTitle:
    """One title listed in the official RSA table of contents."""

    title: str
    heading: str
    chapter_range: str | None
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"title-{_slug(self.title)}"

    @property
    def citation_path(self) -> str:
        return f"us-nh/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"RSA Title {self.title}"


@dataclass(frozen=True)
class NewHampshireChapterListing:
    """One chapter discovered from an official RSA title TOC."""

    title: str
    title_heading: str
    title_citation_path: str
    chapter: str
    heading: str
    source_url: str
    ordinal: int

    @property
    def merged_source_url(self) -> str:
        parsed = urlparse(self.source_url)
        title = quote(self.title, safe="-")
        chapter = quote(self.chapter, safe="-")
        return f"{parsed.scheme}://{parsed.netloc}/rsa/html/{title}/{chapter}/{chapter}-mrg.htm"


@dataclass(frozen=True)
class NewHampshireChapter:
    """One RSA chapter container."""

    listing: NewHampshireChapterListing
    heading: str
    source_url: str
    source_path: str
    source_format: str
    sha256: str

    @property
    def title(self) -> str:
        return self.listing.title

    @property
    def chapter(self) -> str:
        return self.listing.chapter

    @property
    def source_id(self) -> str:
        return f"chapter-{_slug(self.chapter)}"

    @property
    def citation_path(self) -> str:
        return f"us-nh/statute/{self.source_id}"

    @property
    def parent_citation_path(self) -> str:
        return self.listing.title_citation_path

    @property
    def legal_identifier(self) -> str:
        return f"RSA Chapter {self.chapter}"


@dataclass(frozen=True)
class NewHampshireSectionListing:
    """One section link parsed from an official RSA chapter TOC."""

    title: str
    title_heading: str
    chapter: str
    chapter_heading: str
    chapter_citation_path: str
    section_label: str
    heading: str
    source_url: str
    ordinal: int


@dataclass(frozen=True)
class NewHampshireSection:
    """One RSA section parsed from an official merged chapter page."""

    listing: NewHampshireSectionListing
    section_label: str
    heading: str
    body: str | None
    source_history: tuple[str, ...]
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
        return f"us-nh/statute/{self.source_id}"

    @property
    def parent_citation_path(self) -> str:
        return self.listing.chapter_citation_path

    @property
    def legal_identifier(self) -> str:
        return f"RSA {self.section_label}"


@dataclass(frozen=True)
class _NewHampshireSource:
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
class _NewHampshireTitlePage:
    title: NewHampshireTitle
    source: _NewHampshireSource | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _NewHampshireChapterPage:
    listing: NewHampshireChapterListing
    toc_source: _NewHampshireSource | None = None
    merged_source: _NewHampshireSource | None = None
    error: BaseException | None = None


class _NewHampshireFetcher:
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

    def fetch_root(self) -> _NewHampshireSource:
        relative_path = f"{NEW_HAMPSHIRE_ROOT_SOURCE_FORMAT}/nhtoc.htm"
        source_url = urljoin(self.base_url, "nhtoc.htm")
        return _NewHampshireSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=NEW_HAMPSHIRE_ROOT_SOURCE_FORMAT,
            data=self._fetch(relative_path, source_url),
        )

    def fetch_title(self, title: NewHampshireTitle) -> _NewHampshireSource:
        relative_path = f"{NEW_HAMPSHIRE_TITLE_SOURCE_FORMAT}/NHTOC-{title.title}.htm"
        return _NewHampshireSource(
            relative_path=relative_path,
            source_url=title.source_url,
            source_format=NEW_HAMPSHIRE_TITLE_SOURCE_FORMAT,
            data=self._fetch(relative_path, title.source_url),
        )

    def fetch_chapter_toc(self, listing: NewHampshireChapterListing) -> _NewHampshireSource:
        relative_path = (
            f"{NEW_HAMPSHIRE_CHAPTER_SOURCE_FORMAT}/"
            f"NHTOC-{listing.title}-{listing.chapter}.htm"
        )
        return _NewHampshireSource(
            relative_path=relative_path,
            source_url=listing.source_url,
            source_format=NEW_HAMPSHIRE_CHAPTER_SOURCE_FORMAT,
            data=self._fetch(relative_path, listing.source_url),
        )

    def fetch_merged_chapter(
        self,
        listing: NewHampshireChapterListing,
    ) -> _NewHampshireSource:
        relative_path = (
            f"{NEW_HAMPSHIRE_MERGED_CHAPTER_SOURCE_FORMAT}/"
            f"{listing.title}/{listing.chapter}/{listing.chapter}-mrg.htm"
        )
        return _NewHampshireSource(
            relative_path=relative_path,
            source_url=listing.merged_source_url,
            source_format=NEW_HAMPSHIRE_MERGED_CHAPTER_SOURCE_FORMAT,
            data=self._fetch(relative_path, listing.merged_source_url),
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_new_hampshire_source(
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


def extract_new_hampshire_rsa(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = NEW_HAMPSHIRE_RSA_BASE_URL,
    request_delay_seconds: float = 0.25,
    timeout_seconds: float = 30.0,
    request_attempts: int = 2,
    workers: int = 1,
) -> StateStatuteExtractReport:
    """Snapshot official New Hampshire RSA HTML and extract provisions."""
    jurisdiction = "us-nh"
    title_filter = _title_filter(only_title)
    run_id = _new_hampshire_run_id(version, title_filter=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _NewHampshireFetcher(
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
    titles = list(parse_new_hampshire_root(root_source.data, source=root_recorded, base_url=base_url))
    if title_filter is not None:
        titles = [title for title in titles if _slug(title.title) == title_filter]
    if not titles:
        raise ValueError(f"no New Hampshire RSA titles selected for filter: {only_title!r}")

    for title_page in _fetch_title_pages(fetcher, titles, workers=workers):
        if remaining_sections is not None and remaining_sections <= 0:
            break
        if title_page.error is not None:
            errors.append(f"title {title_page.title.title}: {title_page.error}")
            continue
        assert title_page.source is not None
        title_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=title_page.source,
        )
        source_paths.append(
            store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                title_page.source.relative_path,
            )
        )
        title = NewHampshireTitle(
            title=title_page.title.title,
            heading=title_page.title.heading,
            chapter_range=title_page.title.chapter_range,
            source_url=title_recorded.source_url,
            source_path=title_recorded.source_path,
            source_format=title_recorded.source_format,
            sha256=title_recorded.sha256,
            ordinal=title_page.title.ordinal,
        )
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
        chapter_listings = parse_new_hampshire_title_page(
            title_page.source.data,
            title=title,
            base_url=base_url,
        )
        for chapter_page in _fetch_chapter_pages(
            fetcher,
            list(chapter_listings),
            workers=workers,
        ):
            if remaining_sections is not None and remaining_sections <= 0:
                break
            if chapter_page.error is not None:
                errors.append(
                    f"chapter {chapter_page.listing.chapter}: {chapter_page.error}"
                )
                continue
            assert chapter_page.toc_source is not None
            assert chapter_page.merged_source is not None
            chapter_recorded = _record_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                source=chapter_page.toc_source,
            )
            merged_recorded = _record_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                source=chapter_page.merged_source,
            )
            source_paths.extend(
                [
                    store.source_path(
                        jurisdiction,
                        DocumentClass.STATUTE,
                        run_id,
                        chapter_page.toc_source.relative_path,
                    ),
                    store.source_path(
                        jurisdiction,
                        DocumentClass.STATUTE,
                        run_id,
                        chapter_page.merged_source.relative_path,
                    ),
                ]
            )
            chapter, listings = parse_new_hampshire_chapter_toc(
                chapter_page.toc_source.data,
                listing=chapter_page.listing,
                source=chapter_recorded,
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
            selected_listings: list[NewHampshireSectionListing] = []
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
                sections = parse_new_hampshire_merged_chapter(
                    chapter_page.merged_source.data,
                    listings=tuple(selected_listings),
                    source=merged_recorded,
                )
            except ValueError as exc:
                errors.append(f"chapter {chapter.chapter}: {exc}")
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
        raise ValueError("no New Hampshire RSA provisions extracted")

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


def parse_new_hampshire_root(
    html: str | bytes,
    *,
    source: _RecordedSource,
    base_url: str = NEW_HAMPSHIRE_RSA_BASE_URL,
) -> tuple[NewHampshireTitle, ...]:
    """Parse the official RSA root table of contents into titles."""
    soup = BeautifulSoup(_decode(html), "lxml")
    titles: list[NewHampshireTitle] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"])
        href_match = _TITLE_HREF_RE.search(href)
        label_match = _TITLE_LABEL_RE.match(_clean_text(anchor))
        if href_match is None or label_match is None:
            continue
        title = label_match.group("title").upper()
        chapter_range = None
        next_p = anchor.find_parent("li")
        if isinstance(next_p, Tag):
            chapter_range_node = next_p.find_next_sibling("p", class_="chapter_list")
            if isinstance(chapter_range_node, Tag):
                chapter_range = _clean_text(chapter_range_node)
        titles.append(
            NewHampshireTitle(
                title=title,
                heading=_strip_terminal_period(label_match.group("heading").title()),
                chapter_range=chapter_range,
                source_url=urljoin(base_url, href),
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                ordinal=len(titles) + 1,
            )
        )
    return tuple(titles)


def parse_new_hampshire_title_page(
    html: str | bytes,
    *,
    title: NewHampshireTitle,
    base_url: str = NEW_HAMPSHIRE_RSA_BASE_URL,
) -> tuple[NewHampshireChapterListing, ...]:
    """Parse one official RSA title TOC into chapter links."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapters: list[NewHampshireChapterListing] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"])
        href_match = _CHAPTER_HREF_RE.search(href)
        label_match = _CHAPTER_LABEL_RE.match(_clean_text(anchor))
        if href_match is None or label_match is None:
            continue
        chapter = label_match.group("chapter").upper()
        chapters.append(
            NewHampshireChapterListing(
                title=title.title,
                title_heading=title.heading,
                title_citation_path=title.citation_path,
                chapter=chapter,
                heading=_strip_terminal_period(label_match.group("heading").title()),
                source_url=urljoin(urljoin(base_url, "NHTOC/"), href),
                ordinal=len(chapters) + 1,
            )
        )
    return tuple(chapters)


def parse_new_hampshire_chapter_toc(
    html: str | bytes,
    *,
    listing: NewHampshireChapterListing,
    source: _RecordedSource,
) -> tuple[NewHampshireChapter, tuple[NewHampshireSectionListing, ...]]:
    """Parse one official RSA chapter TOC into section links."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapter = NewHampshireChapter(
        listing=listing,
        heading=listing.heading,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
    )
    sections: list[NewHampshireSectionListing] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"])
        href_match = _SECTION_HREF_RE.search(href)
        label_match = _SECTION_LABEL_RE.match(_clean_text(anchor))
        if href_match is None or label_match is None:
            continue
        section_label = _normalize_section_label(label_match.group("label"))
        sections.append(
            NewHampshireSectionListing(
                title=listing.title,
                title_heading=listing.title_heading,
                chapter=listing.chapter,
                chapter_heading=listing.heading,
                chapter_citation_path=chapter.citation_path,
                section_label=section_label,
                heading=_strip_terminal_period(label_match.group("heading")),
                source_url=urljoin(source.source_url, href),
                ordinal=len(sections) + 1,
            )
        )
    return chapter, tuple(sections)


def parse_new_hampshire_merged_chapter(
    html: str | bytes,
    *,
    listings: tuple[NewHampshireSectionListing, ...],
    source: _RecordedSource,
) -> tuple[NewHampshireSection, ...]:
    """Parse one official merged RSA chapter page into section records."""
    soup = BeautifulSoup(_decode(html), "lxml")
    codesects = [tag for tag in soup.find_all("codesect") if isinstance(tag, Tag)]
    sourcenotes = [tag for tag in soup.find_all("sourcenote") if isinstance(tag, Tag)]
    if len(codesects) < len(listings):
        raise ValueError(f"merged section count mismatch: {len(codesects)} < {len(listings)}")
    sections: list[NewHampshireSection] = []
    for index, (listing, codesect) in enumerate(zip(listings, codesects, strict=False)):
        body = _normalize_body(codesect.get_text(" ", strip=True))
        history = (
            tuple(_source_history(sourcenotes[index]))
            if index < len(sourcenotes)
            else ()
        )
        text_for_refs = "\n".join([listing.heading, body or ""])
        refs = tuple(_extract_references(text_for_refs, self_section=listing.section_label))
        sections.append(
            NewHampshireSection(
                listing=listing,
                section_label=listing.section_label,
                heading=listing.heading,
                body=body,
                source_history=history,
                references_to=refs,
                source_url=listing.source_url,
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                status=_status(listing.heading, body, history),
            )
        )
    return tuple(sections)


def _fetch_title_pages(
    fetcher: _NewHampshireFetcher,
    titles: list[NewHampshireTitle],
    *,
    workers: int,
) -> list[_NewHampshireTitlePage]:
    if not titles:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_title_page(fetcher, title) for title in titles]
    results: list[_NewHampshireTitlePage] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_title_page, fetcher, title): title for title in titles
        }
        for future in as_completed(future_map):
            title = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_NewHampshireTitlePage(title=title, error=exc))
    order = {title.title: index for index, title in enumerate(titles)}
    return sorted(results, key=lambda page: order[page.title.title])


def _fetch_title_page(
    fetcher: _NewHampshireFetcher,
    title: NewHampshireTitle,
) -> _NewHampshireTitlePage:
    try:
        return _NewHampshireTitlePage(title=title, source=fetcher.fetch_title(title))
    except BaseException as exc:  # pragma: no cover
        return _NewHampshireTitlePage(title=title, error=exc)


def _fetch_chapter_pages(
    fetcher: _NewHampshireFetcher,
    listings: list[NewHampshireChapterListing],
    *,
    workers: int,
) -> list[_NewHampshireChapterPage]:
    if not listings:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_chapter_page(fetcher, listing) for listing in listings]
    results: list[_NewHampshireChapterPage] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_chapter_page, fetcher, listing): listing
            for listing in listings
        }
        for future in as_completed(future_map):
            listing = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_NewHampshireChapterPage(listing=listing, error=exc))
    order = {listing.chapter: index for index, listing in enumerate(listings)}
    return sorted(results, key=lambda page: order[page.listing.chapter])


def _fetch_chapter_page(
    fetcher: _NewHampshireFetcher,
    listing: NewHampshireChapterListing,
) -> _NewHampshireChapterPage:
    try:
        return _NewHampshireChapterPage(
            listing=listing,
            toc_source=fetcher.fetch_chapter_toc(listing),
            merged_source=fetcher.fetch_merged_chapter(listing),
        )
    except BaseException as exc:  # pragma: no cover
        return _NewHampshireChapterPage(listing=listing, error=exc)


def _title_inventory_item(title: NewHampshireTitle) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=title.citation_path,
        source_url=title.source_url,
        source_path=title.source_path,
        source_format=title.source_format,
        sha256=title.sha256,
        metadata={
            "kind": "title",
            "title": title.title,
            "chapter_range": title.chapter_range,
        },
    )


def _title_record(
    title: NewHampshireTitle,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(title.citation_path),
        jurisdiction="us-nh",
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
        identifiers={"new_hampshire:title": title.title},
        metadata={
            "kind": "title",
            "title": title.title,
            "chapter_range": title.chapter_range,
        },
    )


def _chapter_inventory_item(chapter: NewHampshireChapter) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=chapter.citation_path,
        source_url=chapter.source_url,
        source_path=chapter.source_path,
        source_format=chapter.source_format,
        sha256=chapter.sha256,
        metadata={
            "kind": "chapter",
            "title": chapter.title,
            "chapter": chapter.chapter,
        },
    )


def _chapter_record(
    chapter: NewHampshireChapter,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-nh",
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
        ordinal=chapter.listing.ordinal,
        kind="chapter",
        legal_identifier=chapter.legal_identifier,
        identifiers={
            "new_hampshire:title": chapter.title,
            "new_hampshire:chapter": chapter.chapter,
        },
        metadata={
            "kind": "chapter",
            "title": chapter.title,
            "chapter": chapter.chapter,
        },
    )


def _section_inventory_item(section: NewHampshireSection) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=section.citation_path,
        source_url=section.source_url,
        source_path=section.source_path,
        source_format=section.source_format,
        sha256=section.sha256,
        metadata=_section_metadata(section),
    )


def _section_record(
    section: NewHampshireSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-nh",
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
            "new_hampshire:title": section.listing.title,
            "new_hampshire:chapter": section.listing.chapter,
            "new_hampshire:section": section.section_label,
        },
        metadata=_section_metadata(section),
    )


def _section_metadata(section: NewHampshireSection) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": section.listing.title,
        "title_heading": section.listing.title_heading,
        "chapter": section.listing.chapter,
        "chapter_heading": section.listing.chapter_heading,
        "section": section.section_label,
    }
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
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
    source: _NewHampshireSource,
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


def _download_new_hampshire_source(
    source_url: str,
    *,
    fetcher: _NewHampshireFetcher,
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
                headers={"User-Agent": NEW_HAMPSHIRE_USER_AGENT},
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds, 0.25) * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"New Hampshire source request failed: {source_url}")


def _source_history(source_note: Tag) -> list[str]:
    text = _clean_text(source_note)
    text = re.sub(r"^Source\.\s*", "", text, flags=re.I)
    return [text] if text else []


def _extract_references(text: str, *, self_section: str) -> list[str]:
    refs: list[str] = []
    for match in _RSA_REF_RE.finditer(text):
        label = _normalize_section_label(match.group("label"))
        if label != self_section:
            refs.append(f"us-nh/statute/{_section_source_id(label)}")
    return _dedupe_preserve_order(refs)


def _status(
    heading: str,
    body: str | None,
    history: tuple[str, ...],
) -> str | None:
    text = "\n".join([heading, body or "", *history])
    if re.search(r"\bRepealed\b", text, re.I):
        return "repealed"
    if re.search(r"\bExpired\b", text, re.I):
        return "expired"
    if re.search(r"\bOmitted\b", text, re.I):
        return "omitted"
    return None


def _normalize_section_label(value: str) -> str:
    text = _clean_whitespace(value)
    text = text.replace("\u2010", "-").replace("\u2011", "-")
    text = text.replace("\u2012", "-").replace("\u2013", "-")
    text = text.replace("\u2014", "-")
    text = re.sub(r"\s+", "", text)
    return text.upper().strip().removesuffix(".")


def _section_source_id(section_label: str) -> str:
    return _normalize_section_label(section_label).lower()


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title)[-\s]*", "", text, flags=re.I)
    return _slug(text) if text else None


def _new_hampshire_run_id(
    version: str,
    *,
    title_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and limit is None:
        return version
    parts = [version, "us-nh"]
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


def _normalize_body(text: str) -> str | None:
    normalized = _clean_whitespace(text)
    normalized = normalized.replace("\u0096", "-")
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
