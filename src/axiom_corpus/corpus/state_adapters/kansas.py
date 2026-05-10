"""Kansas Statutes source-first corpus adapter."""

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

KANSAS_STATUTES_BASE_URL = "https://ksrevisor.gov/"
KANSAS_ROOT_SOURCE_FORMAT = "kansas-statutes-root-html"
KANSAS_CHAPTER_SOURCE_FORMAT = "kansas-statutes-chapter-html"
KANSAS_SECTION_SOURCE_FORMAT = "kansas-statutes-section-html"
KANSAS_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_CHAPTER_HREF_RE = re.compile(r"/statutes/ksa_ch(?P<chapter>\d+[a-z]?)\.html$", re.I)
_CHAPTER_HEADING_RE = re.compile(
    r"^Chapter\s+(?P<chapter>\d+[a-z]?)\.\s*[-\u2010-\u2015]+\s*(?P<heading>.+)$",
    re.I,
)
_ARTICLE_HEADING_RE = re.compile(
    r"^Article\s+(?P<article>\d+[a-z]?)\.\s*[-\u2010-\u2015]+\s*(?P<heading>.+)$",
    re.I,
)
_SECTION_HREF_RE = re.compile(
    r"/statutes/chapters/ch(?P<chapter_dir>\d+[a-z]?)/(?P<filename>[^/]+\.html)$",
    re.I,
)
_SECTION_REFERENCE_RE = re.compile(
    r"(?:K\.S\.A\.|sections?|§+)\s*"
    r"(?P<section>\d+[A-Za-z]?(?:-[0-9A-Za-z,]+){1,2}[A-Za-z]?)",
    re.I,
)


@dataclass(frozen=True)
class KansasChapter:
    """One chapter discovered from the official Kansas statutes index."""

    chapter: str
    heading: str
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
        return f"us-ks/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"K.S.A. ch. {self.chapter}"


@dataclass(frozen=True)
class KansasArticle:
    """One article heading parsed from an official chapter page."""

    chapter: str
    article: str
    heading: str
    source_url: str
    source_path: str
    source_format: str
    sha256: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"chapter-{self.chapter}/article-{self.article}"

    @property
    def citation_path(self) -> str:
        return f"us-ks/statute/{self.source_id}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-ks/statute/chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"K.S.A. ch. {self.chapter}, art. {self.article}"


@dataclass(frozen=True)
class KansasSectionListing:
    """One section HTML link parsed from an official chapter page."""

    chapter: str
    article: str
    section_label: str
    heading: str
    source_url: str
    ordinal: int

    @property
    def relative_source_name(self) -> str:
        parsed = urlparse(self.source_url)
        match = _SECTION_HREF_RE.search(parsed.path)
        if match is None:
            return f"{KANSAS_SECTION_SOURCE_FORMAT}/{Path(parsed.path).name}"
        return (
            f"{KANSAS_SECTION_SOURCE_FORMAT}/ch{match.group('chapter_dir').lower()}/"
            f"{match.group('filename')}"
        )


@dataclass(frozen=True)
class KansasSection:
    """One Kansas statute section parsed from official section HTML."""

    listing: KansasSectionListing
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
        return f"us-ks/statute/{self.source_id}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-ks/statute/chapter-{self.listing.chapter}/article-{self.listing.article}"

    @property
    def legal_identifier(self) -> str:
        return f"K.S.A. {self.section_label}"


@dataclass(frozen=True)
class _KansasSource:
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
class _KansasChapterPage:
    chapter: str
    source: _KansasSource | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _KansasSectionPage:
    listing: KansasSectionListing
    source: _KansasSource | None = None
    error: BaseException | None = None


class _KansasFetcher:
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

    def fetch_root(self) -> _KansasSource:
        relative_path = f"{KANSAS_ROOT_SOURCE_FORMAT}/ksa.html"
        source_url = urljoin(self.base_url, "ksa.html")
        return _KansasSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=KANSAS_ROOT_SOURCE_FORMAT,
            data=self._fetch(relative_path, source_url),
        )

    def fetch_chapter(self, chapter: str) -> _KansasSource:
        relative_path = f"{KANSAS_CHAPTER_SOURCE_FORMAT}/ksa_ch{chapter.lower()}.html"
        source_url = urljoin(self.base_url, f"statutes/ksa_ch{chapter.lower()}.html")
        return _KansasSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=KANSAS_CHAPTER_SOURCE_FORMAT,
            data=self._fetch(relative_path, source_url),
        )

    def fetch_section(self, listing: KansasSectionListing) -> _KansasSource:
        return _KansasSource(
            relative_path=listing.relative_source_name,
            source_url=listing.source_url,
            source_format=KANSAS_SECTION_SOURCE_FORMAT,
            data=self._fetch(listing.relative_source_name, listing.source_url),
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_kansas_source(
            source_url,
            fetcher=self,
            request_delay_seconds=self.request_delay_seconds,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
        )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
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


def extract_kansas_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = KANSAS_STATUTES_BASE_URL,
    request_delay_seconds: float = 0.03,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
    workers: int = 8,
) -> StateStatuteExtractReport:
    """Snapshot official Kansas Statutes HTML and extract provisions."""
    jurisdiction = "us-ks"
    chapter_filter = _chapter_filter(only_title)
    run_id = _kansas_run_id(version, chapter_filter=chapter_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _KansasFetcher(
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
    root_chapters = parse_kansas_root(root_source.data, source=root_recorded, base_url=base_url)
    if chapter_filter is not None:
        root_chapters = tuple(
            chapter for chapter in root_chapters if chapter.chapter == chapter_filter
        )
    if not root_chapters:
        raise ValueError(f"no Kansas statute chapters selected for filter: {only_title!r}")

    for chapter_page in _fetch_kansas_chapter_pages(
        fetcher,
        [chapter.chapter for chapter in root_chapters],
        workers=workers,
    ):
        if remaining_sections is not None and remaining_sections <= 0:
            break
        root_chapter = next(
            chapter for chapter in root_chapters if chapter.chapter == chapter_page.chapter
        )
        if chapter_page.error is not None:
            errors.append(f"chapter {chapter_page.chapter}: {chapter_page.error}")
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
        chapter, articles, listings = parse_kansas_chapter_page(
            chapter_page.source.data,
            root_chapter=root_chapter,
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
            title_count += 1
            container_count += 1
        article_by_id: dict[str, KansasArticle] = {}
        for article in articles:
            if _append_unique(
                seen,
                items,
                records,
                _article_inventory_item(article),
                _article_record(
                    article,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                ),
            ):
                container_count += 1
            article_by_id[article.article] = article

        selected_listings: list[KansasSectionListing] = []
        for listing in listings:
            if (
                remaining_sections is not None
                and len(selected_listings) >= remaining_sections
            ):
                break
            if listing.article not in article_by_id:
                errors.append(
                    f"section {listing.section_label}: missing article {listing.article}"
                )
                continue
            selected_listings.append(listing)

        for section_page in _fetch_kansas_section_pages(
            fetcher,
            selected_listings,
            workers=workers,
        ):
            if section_page.error is not None:
                errors.append(
                    f"section {section_page.listing.section_label}: {section_page.error}"
                )
                continue
            assert section_page.source is not None
            section_recorded = _record_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                source=section_page.source,
            )
            source_paths.append(
                store.source_path(
                    jurisdiction,
                    DocumentClass.STATUTE,
                    run_id,
                    section_page.source.relative_path,
                )
            )
            try:
                section = parse_kansas_section_page(
                    section_page.source.data,
                    listing=section_page.listing,
                    source=section_recorded,
                )
            except ValueError as exc:
                errors.append(f"section {section_page.listing.section_label}: {exc}")
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
        raise ValueError("no Kansas Statutes provisions extracted")

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


def parse_kansas_root(
    html: str | bytes,
    *,
    source: _RecordedSource,
    base_url: str = KANSAS_STATUTES_BASE_URL,
) -> tuple[KansasChapter, ...]:
    """Parse the official Kansas statutes root page into chapter links."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapters: list[KansasChapter] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"])
        match = _CHAPTER_HREF_RE.search(href)
        if match is None:
            continue
        chapter = match.group("chapter").lower()
        if chapter in seen:
            continue
        seen.add(chapter)
        label = _clean_text(anchor)
        heading_match = _CHAPTER_HEADING_RE.match(label)
        heading = (
            _strip_terminal_period(heading_match.group("heading").title())
            if heading_match is not None
            else _strip_terminal_period(label)
        )
        chapters.append(
            KansasChapter(
                chapter=chapter,
                heading=heading,
                source_url=urljoin(base_url, href),
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                ordinal=len(chapters) + 1,
            )
        )
    return tuple(chapters)


def parse_kansas_chapter_page(
    html: str | bytes,
    *,
    root_chapter: KansasChapter,
    source: _RecordedSource,
    base_url: str = KANSAS_STATUTES_BASE_URL,
) -> tuple[KansasChapter, tuple[KansasArticle, ...], tuple[KansasSectionListing, ...]]:
    """Parse one official chapter page into article containers and section links."""
    soup = BeautifulSoup(_decode(html), "lxml")
    heading = root_chapter.heading
    for h2 in soup.find_all("h2"):
        match = _CHAPTER_HEADING_RE.match(_clean_text(h2))
        if match is not None and match.group("chapter").lower() == root_chapter.chapter:
            heading = _strip_terminal_period(match.group("heading").title())
            break
    chapter = KansasChapter(
        chapter=root_chapter.chapter,
        heading=heading,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        ordinal=root_chapter.ordinal,
    )

    articles: list[KansasArticle] = []
    listings: list[KansasSectionListing] = []
    listing_indexes_by_url: dict[str, int] = {}
    tree = soup.find("ul", id="tree")
    article_nodes = tree.find_all("li", recursive=False) if isinstance(tree, Tag) else []
    for article_node in article_nodes:
        article_anchor = article_node.find("a", recursive=False)
        if not isinstance(article_anchor, Tag):
            continue
        article_match = _ARTICLE_HEADING_RE.match(_clean_text(article_anchor))
        if article_match is None:
            continue
        article = article_match.group("article").lower()
        articles.append(
            KansasArticle(
                chapter=chapter.chapter,
                article=article,
                heading=_strip_terminal_period(article_match.group("heading").title()),
                source_url=source.source_url,
                source_path=source.source_path,
                source_format=source.source_format,
                sha256=source.sha256,
                ordinal=len(articles) + 1,
            )
        )
        section_list = article_node.find("ul", recursive=False)
        section_items = section_list.find_all("li", recursive=False) if isinstance(
            section_list,
            Tag,
        ) else []
        for section_item in section_items:
            section_anchor = section_item.find("a", href=True)
            if not isinstance(section_anchor, Tag):
                continue
            href = str(section_anchor["href"])
            if _SECTION_HREF_RE.search(href) is None:
                continue
            section_heading = _strip_terminal_period(_clean_text(section_anchor))
            section_label = _section_label_from_listing_item(
                section_item,
                section_anchor,
            )
            listing = KansasSectionListing(
                chapter=chapter.chapter,
                article=article,
                section_label=section_label,
                heading=section_heading,
                source_url=urljoin(base_url, href),
                ordinal=len(listings) + 1,
            )
            existing_index = listing_indexes_by_url.get(listing.source_url)
            if existing_index is not None:
                existing = listings[existing_index]
                if _section_label_score(listing.section_label) > _section_label_score(
                    existing.section_label
                ):
                    listings[existing_index] = KansasSectionListing(
                        chapter=listing.chapter,
                        article=listing.article,
                        section_label=listing.section_label,
                        heading=listing.heading,
                        source_url=listing.source_url,
                        ordinal=existing.ordinal,
                    )
                continue
            listing_indexes_by_url[listing.source_url] = len(listings)
            listings.append(listing)
    return chapter, tuple(articles), tuple(listings)


def parse_kansas_section_page(
    html: str | bytes,
    *,
    listing: KansasSectionListing,
    source: _RecordedSource,
) -> KansasSection:
    """Parse one official section page into normalized text."""
    soup = BeautifulSoup(_decode(html), "lxml")
    print_container = soup.find(id="print")
    if not isinstance(print_container, Tag):
        raise ValueError("missing printable statute container")
    statute_paragraphs = [
        tag
        for tag in print_container.find_all("p")
        if isinstance(tag, Tag)
        and (
            any(str(class_name).startswith("ksa_stat") for class_name in tag.get("class", []))
            or tag.find(class_="stat_number") is not None
        )
    ]
    if not statute_paragraphs:
        raise ValueError("missing statute paragraphs")

    first = statute_paragraphs[0]
    number_span = first.find(class_="stat_number")
    caption_span = first.find(class_="stat_caption")
    section_label = _normalize_section_label(
        _clean_text(number_span) if isinstance(number_span, Tag) else listing.section_label
    )
    heading = (
        _strip_terminal_period(_clean_text(caption_span))
        if isinstance(caption_span, Tag)
        else listing.heading
    )
    heading = heading or "Reserved"

    body_lines: list[str] = []
    source_history: list[str] = []
    for paragraph in statute_paragraphs:
        classes = {str(class_name) for class_name in paragraph.get("class", [])}
        if "ksa_stat_hist" in classes:
            history = _history_text(paragraph)
            if history:
                source_history.append(history)
            continue
        text = _statute_body_text(paragraph)
        if text:
            body_lines.append(text)
    body = _normalize_body("\n".join(body_lines))
    status = _status(heading, body, source_history)
    references_to = tuple(_extract_references("\n".join([heading, body or ""])))
    return KansasSection(
        listing=listing,
        section_label=section_label,
        heading=heading,
        body=body,
        source_history=tuple(source_history),
        references_to=references_to,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        status=status,
    )


def _fetch_kansas_chapter_pages(
    fetcher: _KansasFetcher,
    chapters: list[str],
    *,
    workers: int,
) -> list[_KansasChapterPage]:
    if not chapters:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_kansas_chapter_page(fetcher, chapter) for chapter in chapters]
    results: list[_KansasChapterPage] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_kansas_chapter_page, fetcher, chapter): chapter
            for chapter in chapters
        }
        for future in as_completed(future_map):
            chapter = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_KansasChapterPage(chapter=chapter, error=exc))
    order = {chapter: index for index, chapter in enumerate(chapters)}
    return sorted(results, key=lambda page: order[page.chapter])


def _fetch_kansas_chapter_page(
    fetcher: _KansasFetcher,
    chapter: str,
) -> _KansasChapterPage:
    try:
        return _KansasChapterPage(chapter=chapter, source=fetcher.fetch_chapter(chapter))
    except BaseException as exc:  # pragma: no cover
        return _KansasChapterPage(chapter=chapter, error=exc)


def _fetch_kansas_section_pages(
    fetcher: _KansasFetcher,
    listings: list[KansasSectionListing],
    *,
    workers: int,
) -> list[_KansasSectionPage]:
    if not listings:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_kansas_section_page(fetcher, listing) for listing in listings]
    results: list[_KansasSectionPage] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_kansas_section_page, fetcher, listing): listing
            for listing in listings
        }
        for future in as_completed(future_map):
            listing = future_map[future]
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(_KansasSectionPage(listing=listing, error=exc))
    order = {listing.source_url: index for index, listing in enumerate(listings)}
    return sorted(results, key=lambda page: order[page.listing.source_url])


def _fetch_kansas_section_page(
    fetcher: _KansasFetcher,
    listing: KansasSectionListing,
) -> _KansasSectionPage:
    try:
        return _KansasSectionPage(listing=listing, source=fetcher.fetch_section(listing))
    except BaseException as exc:  # pragma: no cover
        return _KansasSectionPage(listing=listing, error=exc)


def _chapter_inventory_item(chapter: KansasChapter) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=chapter.citation_path,
        source_url=chapter.source_url,
        source_path=chapter.source_path,
        source_format=chapter.source_format,
        sha256=chapter.sha256,
        metadata={
            "kind": "chapter",
            "chapter": chapter.chapter,
        },
    )


def _chapter_record(
    chapter: KansasChapter,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-ks",
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
        level=0,
        ordinal=chapter.ordinal,
        kind="chapter",
        legal_identifier=chapter.legal_identifier,
        identifiers={"kansas:chapter": chapter.chapter},
        metadata={"kind": "chapter", "chapter": chapter.chapter},
    )


def _article_inventory_item(article: KansasArticle) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=article.citation_path,
        source_url=article.source_url,
        source_path=article.source_path,
        source_format=article.source_format,
        sha256=article.sha256,
        metadata={
            "kind": "article",
            "chapter": article.chapter,
            "article": article.article,
        },
    )


def _article_record(
    article: KansasArticle,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(article.citation_path),
        jurisdiction="us-ks",
        document_class=DocumentClass.STATUTE.value,
        citation_path=article.citation_path,
        body=None,
        heading=article.heading,
        citation_label=article.legal_identifier,
        version=version,
        source_url=article.source_url,
        source_path=article.source_path,
        source_id=article.source_id,
        source_format=article.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=article.parent_citation_path,
        parent_id=deterministic_provision_id(article.parent_citation_path),
        level=1,
        ordinal=article.ordinal,
        kind="article",
        legal_identifier=article.legal_identifier,
        identifiers={
            "kansas:chapter": article.chapter,
            "kansas:article": article.article,
        },
        metadata={
            "kind": "article",
            "chapter": article.chapter,
            "article": article.article,
        },
    )


def _section_inventory_item(section: KansasSection) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=section.citation_path,
        source_url=section.source_url,
        source_path=section.source_path,
        source_format=section.source_format,
        sha256=section.sha256,
        metadata=_section_metadata(section),
    )


def _section_record(
    section: KansasSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-ks",
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
            "kansas:chapter": section.listing.chapter,
            "kansas:article": section.listing.article,
            "kansas:section": section.source_id,
            "kansas:section_label": section.section_label,
        },
        metadata=_section_metadata(section),
    )


def _section_metadata(section: KansasSection) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "chapter": section.listing.chapter,
        "article": section.listing.article,
        "section": section.source_id,
        "section_label": section.section_label,
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
    source: _KansasSource,
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


def _download_kansas_source(
    source_url: str,
    *,
    fetcher: _KansasFetcher,
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
                headers={"User-Agent": KANSAS_USER_AGENT},
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds, 0.25) * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"Kansas source request failed: {source_url}")


def _section_label_from_listing_item(item: Tag, anchor: Tag) -> str:
    full_text = _clean_text(item)
    heading = _clean_text(anchor)
    label = full_text
    if heading and label.endswith(heading):
        label = label[: -len(heading)].strip()
    return _normalize_section_label(label)


def _section_label_score(section_label: str) -> int:
    return 1 if re.match(r"^\d+[a-z]?\s*-", section_label, re.I) else 0


def _statute_body_text(paragraph: Tag) -> str:
    clone = BeautifulSoup(str(paragraph), "lxml")
    for span in clone.select(".stat_number, .stat_caption"):
        span.decompose()
    return _clean_text(clone)


def _history_text(paragraph: Tag) -> str:
    clone = BeautifulSoup(str(paragraph), "lxml")
    for span in clone.select(".history"):
        span.decompose()
    return _clean_text(clone)


def _normalize_section_label(value: str) -> str:
    text = _clean_whitespace(value)
    text = text.replace("\u2010", "-").replace("\u2011", "-")
    text = text.replace("\u2012", "-").replace("\u2013", "-")
    text = text.replace("\u2014", "-")
    text = re.sub(r"\s*-\s*", "-", text)
    text = re.sub(r"\s*,\s*", ",", text)
    text = re.sub(r",(?=\d+[a-z]?-)", ", ", text, flags=re.I)
    return text.removesuffix(".").strip()


def _section_source_id(section_label: str) -> str:
    source_id = section_label.strip()
    source_id = re.sub(r"\s+through\s+", "-through-", source_id, flags=re.I)
    source_id = re.sub(r"\s+to\s+", "-to-", source_id, flags=re.I)
    source_id = re.sub(r",\s+", "-and-", source_id)
    source_id = re.sub(r"\s+", "", source_id)
    return source_id


def _status(heading: str, body: str | None, history: list[str]) -> str | None:
    text = "\n".join([heading, body or "", *history])
    if re.search(r"\bRepealed\b", text, re.I):
        return "repealed"
    if re.search(r"\bExpired\b", text, re.I):
        return "expired"
    return None


def _extract_references(text: str) -> list[str]:
    refs = [
        f"us-ks/statute/{_section_source_id(_normalize_section_label(match.group('section')))}"
        for match in _SECTION_REFERENCE_RE.finditer(text)
    ]
    return _dedupe_preserve_order(refs)


def _kansas_run_id(
    version: str,
    *,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    if chapter_filter is None and limit is None:
        return version
    parts = [version, "us-ks"]
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter.lower()}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:chapter|ch\.?)[-\s]*", "", text, flags=re.I)
    return text.lower() if text else None


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _normalize_body(text: str) -> str | None:
    normalized = _clean_whitespace(text)
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
    return _clean_whitespace(text)


def _clean_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


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
