"""New Mexico Statutes Annotated source-first corpus adapter."""

from __future__ import annotations

import re
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
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

NEW_MEXICO_ONESOURCE_BASE_URL = "https://nmonesource.com"
NEW_MEXICO_NAV_SOURCE_FORMAT = "new-mexico-nmonesource-nav-html"
NEW_MEXICO_EPUB_SOURCE_FORMAT = "new-mexico-nmonesource-epub"
NEW_MEXICO_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_CHAPTER_TITLE_RE = re.compile(
    r"^\s*Chapter\s+(?P<chapter>\d+[A-Z]?)\s*-\s*(?P<heading>.+?)\s*$",
    re.I,
)
_ARTICLE_TITLE_RE = re.compile(
    r"^\s*ARTICLE\s+(?P<article>\d+[A-Z]?)\s+(?P<heading>.+?)\s*$",
    re.I,
)
_SECTION_TITLE_RE = re.compile(
    r"^\s*(?P<section>\d+[A-Z]?(?:-\d+[A-Z]?)*(?:\.\d+)?)\.\s*(?P<heading>.*?)\s*$",
    re.I,
)
_SECTION_CITATION_PATTERN = r"\d+[A-Z]?-\d+[A-Z]?(?:-\d+[A-Z]?)*(?:\.\d+[A-Z]?)?"
_SECTION_TITLE_PREFIX_RE = re.compile(
    rf"^\s*(?P<prefix>{_SECTION_CITATION_PATTERN}"
    rf"(?:\s*(?:,|and|to|through|thru)\s*{_SECTION_CITATION_PATTERN})*)"
    r"\.\s*(?P<heading>.*?)\s*$",
    re.I,
)
_SECTION_CITATION_RE = re.compile(_SECTION_CITATION_PATTERN, re.I)
_SECTION_REFERENCE_RE = re.compile(rf"^{_SECTION_CITATION_PATTERN}$", re.I)
_EFFECTIVE_NOTE_RE = re.compile(
    r"\((?P<note>(?:Effective|Contingent effective date|Repealed effective)[^)]+)\)",
    re.I,
)
_NAV_PAGE_RE = re.compile(r"nav_date\.do\?page=(?P<page>\d+)", re.I)


@dataclass(frozen=True)
class NewMexicoChapterLink:
    """One chapter link from the official NMOneSource current NMSA navigation."""

    chapter: str
    heading: str
    item_id: str
    source_url: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-nm/statute/chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"NMSA 1978 Chapter {self.chapter}"

    @property
    def epub_url(self) -> str:
        return urljoin(NEW_MEXICO_ONESOURCE_BASE_URL, f"/w/nmos/n-{self.item_id}-en.epub")


@dataclass(frozen=True)
class NewMexicoArticle:
    """Article parsed from one official NMOneSource chapter ePub."""

    chapter: str
    article: str
    heading: str
    source_file: str
    source_anchor: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-nm/statute/chapter-{self.chapter}/article-{self.article}"

    @property
    def legal_identifier(self) -> str:
        return f"NMSA 1978 Chapter {self.chapter}, Article {self.article}"


@dataclass(frozen=True)
class NewMexicoSection:
    """Section parsed from one official NMOneSource chapter ePub."""

    section: str
    heading: str | None
    body: str | None
    chapter: str
    article: str | None
    source_file: str
    source_anchor: str
    ordinal: int
    references_to: tuple[str, ...]
    source_history: tuple[str, ...]
    status: str | None = None
    effective_note: str | None = None
    variant: str | None = None
    section_group: tuple[str, ...] = ()

    @property
    def source_id(self) -> str:
        return _section_source_id(self.section, self.variant)

    @property
    def citation_path(self) -> str:
        return f"us-nm/statute/{self.source_id}"

    @property
    def canonical_citation_path(self) -> str:
        return f"us-nm/statute/{self.section}"

    @property
    def legal_identifier(self) -> str:
        return f"NMSA 1978 \u00a7 {self.section}"


@dataclass(frozen=True)
class NewMexicoParsedChapter:
    """Parsed contents of one official chapter ePub."""

    articles: tuple[NewMexicoArticle, ...]
    sections: tuple[NewMexicoSection, ...]


@dataclass(frozen=True)
class _NewMexicoSource:
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


class _NewMexicoFetcher:
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

    def fetch_nav_page(self, page: int) -> _NewMexicoSource:
        if page < 1:
            raise ValueError("navigation page must be positive")
        relative_path = f"{NEW_MEXICO_NAV_SOURCE_FORMAT}/nav_date-page-{page}.html"
        query = "iframe=true" if page == 1 else f"iframe=true&page={page}"
        source_url = f"{self.base_url}/nmos/nmsa/en/nav_date.do?{query}"
        data = self._fetch(relative_path, source_url)
        return _NewMexicoSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=NEW_MEXICO_NAV_SOURCE_FORMAT,
            data=data,
        )

    def fetch_epub(self, chapter: NewMexicoChapterLink) -> _NewMexicoSource:
        relative_path = (
            f"{NEW_MEXICO_EPUB_SOURCE_FORMAT}/chapter-{chapter.chapter}/"
            f"n-{chapter.item_id}-en.epub"
        )
        data = self._fetch(relative_path, chapter.epub_url)
        return _NewMexicoSource(
            relative_path=relative_path,
            source_url=chapter.epub_url,
            source_format=NEW_MEXICO_EPUB_SOURCE_FORMAT,
            data=data,
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_new_mexico_source(
            source_url,
            request_delay_seconds=self.request_delay_seconds,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
            fetcher=self,
        )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            _write_cache_bytes(cached_path, data)
        return data

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        wait_seconds = self.request_delay_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self._last_request_at = time.monotonic()


def extract_new_mexico_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    request_delay_seconds: float = 0.1,
    timeout_seconds: float = 90.0,
    request_attempts: int = 3,
    base_url: str = NEW_MEXICO_ONESOURCE_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official NMOneSource chapter ePubs and extract NMSA provisions."""
    jurisdiction = "us-nm"
    run_id = str(version)
    chapter_filter = _chapter_filter(only_title)
    source_as_of_text = source_as_of or run_id
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _NewMexicoFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        base_url=base_url,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    source_paths: list[Path] = []
    source_by_relative: dict[str, _RecordedSource] = {}
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    remaining_sections = limit

    nav_sources = _fetch_navigation_sources(fetcher)
    chapters = _dedupe_chapters(
        chapter
        for nav_source in nav_sources
        for chapter in parse_new_mexico_navigation_page(nav_source.data, base_url=base_url)
    )
    for nav_source in nav_sources:
        path, recorded = _record_source(store, jurisdiction, run_id, nav_source)
        source_paths.append(path)
        source_by_relative[nav_source.relative_path] = recorded
    if chapter_filter is not None:
        chapters = [chapter for chapter in chapters if chapter.chapter == chapter_filter]

    for chapter in chapters:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        epub_source = fetcher.fetch_epub(chapter)
        epub_path, epub_recorded = _record_source(store, jurisdiction, run_id, epub_source)
        source_paths.append(epub_path)
        source_by_relative[epub_source.relative_path] = epub_recorded
        try:
            parsed = parse_new_mexico_epub(epub_source.data, chapter=chapter)
        except (OSError, ValueError, zipfile.BadZipFile) as exc:
            errors.append(f"chapter {chapter.chapter}: {exc}")
            continue

        if chapter.citation_path in seen:
            errors.append(f"duplicate citation path: {chapter.citation_path}")
            continue
        seen.add(chapter.citation_path)
        title_count += 1
        _append_record(
            items,
            records,
            jurisdiction=jurisdiction,
            citation_path=chapter.citation_path,
            version=run_id,
            source_url=chapter.source_url,
            source_path=epub_recorded.source_path,
            source_format=epub_recorded.source_format,
            source_id=f"chapter-{chapter.chapter}",
            sha256=epub_recorded.sha256,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            kind="chapter",
            body=None,
            heading=chapter.heading,
            legal_identifier=chapter.legal_identifier,
            parent_citation_path=None,
            level=1,
            ordinal=chapter.ordinal,
            identifiers={"nmsa_chapter": chapter.chapter, "nmonesource_item_id": chapter.item_id},
            metadata={"source_type": "chapter_epub"},
        )

        for article in parsed.articles:
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
                source_url=_chapter_anchor_url(chapter, article.source_anchor),
                source_path=epub_recorded.source_path,
                source_format=epub_recorded.source_format,
                source_id=f"chapter-{chapter.chapter}/article-{article.article}",
                sha256=epub_recorded.sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="article",
                body=None,
                heading=article.heading,
                legal_identifier=article.legal_identifier,
                parent_citation_path=chapter.citation_path,
                level=2,
                ordinal=article.ordinal,
                identifiers={
                    "nmsa_chapter": chapter.chapter,
                    "nmsa_article": article.article,
                    "nmonesource_item_id": chapter.item_id,
                },
                metadata={
                    "source_type": "chapter_epub",
                    "source_file": article.source_file,
                    "source_anchor": article.source_anchor,
                },
            )

        for section in parsed.sections:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            if section.citation_path in seen:
                errors.append(f"duplicate citation path: {section.citation_path}")
                continue
            seen.add(section.citation_path)
            section_count += 1
            metadata: dict[str, Any] = {
                "source_type": "chapter_epub",
                "source_file": section.source_file,
                "source_anchor": section.source_anchor,
            }
            if section.references_to:
                metadata["references_to"] = list(section.references_to)
            if section.source_history:
                metadata["source_history"] = list(section.source_history)
            if section.status:
                metadata["status"] = section.status
            if section.effective_note:
                metadata["effective_note"] = section.effective_note
            if section.variant:
                metadata["variant"] = section.variant
                metadata["canonical_citation_path"] = section.canonical_citation_path
            if section.section_group:
                metadata["section_group"] = list(section.section_group)
            _append_record(
                items,
                records,
                jurisdiction=jurisdiction,
                citation_path=section.citation_path,
                version=run_id,
                source_url=_chapter_anchor_url(chapter, section.source_anchor),
                source_path=epub_recorded.source_path,
                source_format=epub_recorded.source_format,
                source_id=section.source_id,
                sha256=epub_recorded.sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="section",
                body=section.body,
                heading=section.heading,
                legal_identifier=section.legal_identifier,
                parent_citation_path=(
                    f"us-nm/statute/chapter-{chapter.chapter}/article-{section.article}"
                    if section.article
                    else chapter.citation_path
                ),
                level=3,
                ordinal=section.ordinal,
                identifiers={
                    "nmsa": section.section,
                    "nmsa_chapter": chapter.chapter,
                    "nmonesource_item_id": chapter.item_id,
                    **({"nmsa_variant": section.variant} if section.variant else {}),
                },
                metadata=metadata,
            )
            if remaining_sections is not None:
                remaining_sections -= 1

    if not records:
        raise ValueError("no New Mexico provisions extracted")

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


def parse_new_mexico_navigation_page(
    html: str | bytes,
    *,
    base_url: str = NEW_MEXICO_ONESOURCE_BASE_URL,
) -> tuple[NewMexicoChapterLink, ...]:
    """Parse one official NMOneSource current NMSA navigation page."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapters: list[NewMexicoChapterLink] = []
    ordinal = 0
    for link in soup.find_all("a", href=True):
        title = _clean_text(link)
        match = _CHAPTER_TITLE_RE.match(title)
        if not match:
            continue
        item_match = re.search(r"/nmos/nmsa/en/item/(?P<item_id>\d+)/index\.do", link["href"])
        if not item_match:
            continue
        ordinal += 1
        chapters.append(
            NewMexicoChapterLink(
                chapter=match.group("chapter").upper(),
                heading=match.group("heading").strip(),
                item_id=item_match.group("item_id"),
                source_url=urljoin(base_url, link["href"]),
                ordinal=ordinal,
            )
        )
    return tuple(chapters)


def parse_new_mexico_epub(
    data: bytes,
    *,
    chapter: NewMexicoChapterLink,
) -> NewMexicoParsedChapter:
    """Parse articles and sections from one official NMOneSource chapter ePub."""
    with zipfile.ZipFile(BytesIO(data)) as archive:
        toc = BeautifulSoup(archive.read("OEBPS/toc.ncx"), "xml")
        chunk_names = {
            content.get("src", "").split("#", 1)[0]
            for content in toc.find_all("content")
            if content.get("src")
        }
        chunks = {
            name: BeautifulSoup(archive.read(f"OEBPS/{name}"), "lxml")
            for name in chunk_names
            if name
        }
        articles: list[NewMexicoArticle] = []
        sections: list[NewMexicoSection] = []
        current_article: str | None = None
        article_ordinal = 0
        section_ordinal = 0
        occurrence_by_section: dict[str, int] = defaultdict(int)
        used_source_ids: set[str] = set()
        for entry in _iter_toc_entries(toc):
            source_file, source_anchor = _split_content_src(entry["src"])
            title = entry["title"]
            if entry["depth"] == 1:
                article_match = _ARTICLE_TITLE_RE.match(title)
                if not article_match:
                    continue
                current_article = article_match.group("article").upper()
                article_ordinal += 1
                articles.append(
                    NewMexicoArticle(
                        chapter=chapter.chapter,
                        article=current_article,
                        heading=article_match.group("heading").strip(),
                        source_file=source_file,
                        source_anchor=source_anchor,
                        ordinal=article_ordinal,
                    )
                )
                continue
            if entry["depth"] < 2:
                continue
            parsed_title = _parse_section_title(title)
            if parsed_title is None:
                continue
            parsed_sections, raw_heading = parsed_title
            for section in parsed_sections:
                section_ordinal += 1
                soup = chunks.get(source_file)
                if soup is None:
                    raise ValueError(f"missing ePub content file: {source_file}")
                body, history, references_to = _extract_section_body(
                    soup,
                    source_anchor=source_anchor,
                    section=section,
                )
                effective_note = _effective_note(raw_heading)
                heading = _heading_without_effective_note(raw_heading) or None
                occurrence_by_section[section] += 1
                occurrence = occurrence_by_section[section]
                variant = _variant_for_occurrence(effective_note, occurrence)
                source_id = _section_source_id(section, variant)
                if source_id in used_source_ids:
                    variant = _disambiguated_variant(
                        variant,
                        occurrence=occurrence,
                        section_number=section,
                        used_source_ids=used_source_ids,
                    )
                    source_id = _section_source_id(section, variant)
                used_source_ids.add(source_id)
                status = _section_status(heading, body, effective_note)
                sections.append(
                    NewMexicoSection(
                        section=section,
                        heading=heading,
                        body=body,
                        chapter=chapter.chapter,
                        article=current_article,
                        source_file=source_file,
                        source_anchor=source_anchor,
                        ordinal=section_ordinal,
                        references_to=references_to,
                        source_history=history,
                        status=status,
                        effective_note=effective_note,
                        variant=variant,
                        section_group=parsed_sections if len(parsed_sections) > 1 else (),
                    )
                )
    return NewMexicoParsedChapter(articles=tuple(articles), sections=tuple(sections))


def _fetch_navigation_sources(fetcher: _NewMexicoFetcher) -> tuple[_NewMexicoSource, ...]:
    first_page = fetcher.fetch_nav_page(1)
    pages = {1}
    for page in _parse_navigation_page_numbers(first_page.data):
        pages.add(page)
    sources = [first_page]
    for page in sorted(pages - {1}):
        sources.append(fetcher.fetch_nav_page(page))
    return tuple(sources)


def _parse_navigation_page_numbers(html: str | bytes) -> tuple[int, ...]:
    pages = {
        int(match.group("page"))
        for match in _NAV_PAGE_RE.finditer(_decode(html))
        if int(match.group("page")) >= 1
    }
    return tuple(sorted(pages))


def _dedupe_chapters(chapters: Any) -> list[NewMexicoChapterLink]:
    seen: set[str] = set()
    unique: list[NewMexicoChapterLink] = []
    ordinal = 0
    for chapter in chapters:
        if chapter.item_id in seen:
            continue
        seen.add(chapter.item_id)
        ordinal += 1
        unique.append(
            NewMexicoChapterLink(
                chapter=chapter.chapter,
                heading=chapter.heading,
                item_id=chapter.item_id,
                source_url=chapter.source_url,
                ordinal=ordinal,
            )
        )
    return unique


def _iter_toc_entries(toc: BeautifulSoup) -> list[dict[str, str | int]]:
    nav_map = toc.find("navMap")
    if nav_map is None:
        return []
    entries: list[dict[str, str | int]] = []

    def visit(point: Tag, depth: int) -> None:
        label = point.find("navLabel")
        text_node = label.find("text") if label is not None else None
        content = point.find("content", recursive=False)
        if text_node is not None and content is not None and content.get("src"):
            entries.append(
                {
                    "depth": depth,
                    "title": _clean_text(text_node),
                    "src": str(content["src"]),
                }
            )
        for child in point.find_all("navPoint", recursive=False):
            visit(child, depth + 1)

    for point in nav_map.find_all("navPoint", recursive=False):
        visit(point, 1)
    return entries


def _parse_section_title(title: str) -> tuple[tuple[str, ...], str] | None:
    """Parse NMOneSource TOC titles, including collapsed repealed section ranges."""
    normalized = re.sub(r"\s+", " ", title.replace("\xa0", " ")).strip()
    normalized = re.sub(
        r"(?<=[0-9A-Z])\.\s+(?=(?:to|through|thru)\s+\d)",
        " ",
        normalized,
        flags=re.I,
    )
    match = _SECTION_TITLE_PREFIX_RE.match(normalized)
    if not match:
        return None
    sections = tuple(_section_refs_from_prefix(match.group("prefix")))
    if not sections:
        return None
    return sections, match.group("heading").strip()


def _section_refs_from_prefix(prefix: str) -> list[str]:
    tokens = re.findall(
        rf"{_SECTION_CITATION_PATTERN}|,|and|to|through|thru",
        prefix,
        flags=re.I,
    )
    refs: list[str] = []
    last_ref: str | None = None
    range_pending = False
    for token in tokens:
        normalized_token = token.lower()
        if normalized_token in {",", "and"}:
            range_pending = False
            continue
        if normalized_token in {"to", "through", "thru"}:
            range_pending = True
            continue
        ref = _normalize_section_number(token)
        if range_pending and last_ref is not None:
            expanded = _expand_section_range(last_ref, ref)
            refs.extend(expanded[1:] if refs and expanded[:1] == [refs[-1]] else expanded)
        else:
            refs.append(ref)
        last_ref = ref
        range_pending = False
    return _dedupe_preserve_order(refs)


def _expand_section_range(start: str, end: str) -> list[str]:
    start_parts = _section_range_parts(start)
    end_parts = _section_range_parts(end)
    if start_parts is None or end_parts is None:
        return [start, end]
    start_prefix, start_number, start_decimal = start_parts
    end_prefix, end_number, end_decimal = end_parts
    if start_prefix != end_prefix:
        return [start, end]
    if (
        start_decimal is None
        and end_decimal is None
        and 0 <= end_number - start_number <= 500
    ):
        return [f"{start_prefix}-{number}" for number in range(start_number, end_number + 1)]
    if (
        start_number == end_number
        and start_decimal is not None
        and end_decimal is not None
        and 0 <= end_decimal - start_decimal <= 500
    ):
        return [
            f"{start_prefix}-{start_number}.{number}"
            for number in range(start_decimal, end_decimal + 1)
        ]
    return [start, end]


def _section_range_parts(value: str) -> tuple[str, int, int | None] | None:
    prefix, separator, tail = value.rpartition("-")
    if not separator or not prefix:
        return None
    number_text, dot, decimal_text = tail.partition(".")
    if not number_text.isdigit():
        return None
    decimal = None
    if dot:
        if not decimal_text.isdigit():
            return None
        decimal = int(decimal_text)
    return prefix, int(number_text), decimal


def _extract_section_body(
    soup: BeautifulSoup,
    *,
    source_anchor: str,
    section: str,
) -> tuple[str | None, tuple[str, ...], tuple[str, ...]]:
    anchor = soup.find(id=source_anchor) or soup.find(attrs={"name": source_anchor})
    if anchor is None:
        raise ValueError(f"missing section anchor: {source_anchor}")
    heading = anchor.find_parent(["h1", "h2", "h3", "h4", "h5"])
    if heading is None:
        raise ValueError(f"missing section heading for anchor: {source_anchor}")

    paragraphs: list[str] = []
    histories: list[str] = []
    references: list[str] = []
    in_annotations = False
    for element in heading.next_elements:
        if not isinstance(element, Tag):
            continue
        if element.name in {"h1", "h2", "h3", "h4", "h5"} and element is not heading:
            break
        if element.name == "h6":
            in_annotations = _clean_text(element).upper().startswith("ANNOTATIONS")
            continue
        if element.name != "p":
            continue
        classes = set(element.get("class") or [])
        if "annotations" in classes or in_annotations:
            continue
        text = _clean_text(element)
        if not text:
            continue
        references.extend(_references_from_tag(element, current_section=section))
        if "history" in classes:
            histories.append(text)
            continue
        paragraphs.append(text)
    body = "\n".join(paragraphs) if paragraphs else None
    return body, tuple(histories), tuple(_dedupe_preserve_order(references))


def _references_from_tag(tag: Tag, *, current_section: str) -> list[str]:
    references: list[str] = []
    for link in tag.find_all("a", href=True):
        parent = link.find_parent(attrs={"data-qweri-anchor": True})
        anchor = str(parent["data-qweri-anchor"]) if parent is not None else _clean_text(link)
        normalized = _normalize_section_number(anchor)
        if not _SECTION_REFERENCE_RE.fullmatch(normalized):
            continue
        citation_path = f"us-nm/statute/{normalized}"
        if normalized != current_section:
            references.append(citation_path)
    return references


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
    source: _NewMexicoSource,
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


def _download_new_mexico_source(
    source_url: str,
    *,
    request_delay_seconds: float,
    timeout_seconds: float,
    request_attempts: int,
    fetcher: _NewMexicoFetcher,
) -> bytes:
    last_error: requests.RequestException | None = None
    for attempt in range(1, request_attempts + 1):
        try:
            fetcher.wait_for_request_slot()
            response = requests.get(
                source_url,
                headers={"User-Agent": NEW_MEXICO_USER_AGENT},
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


def _split_content_src(src: str) -> tuple[str, str]:
    file_name, _, anchor = src.partition("#")
    if not file_name or not anchor:
        raise ValueError(f"invalid ePub content reference: {src}")
    return file_name, anchor


def _chapter_anchor_url(chapter: NewMexicoChapterLink, anchor: str) -> str:
    return f"{chapter.source_url}#!b/{anchor}"


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = text.removeprefix("chapter-").removeprefix("Chapter-")
    return text.upper() or None


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _store_relative_path(store: CorpusArtifactStore, path: Path) -> str:
    try:
        return path.relative_to(store.root).as_posix()
    except ValueError:
        return path.as_posix()


def _normalize_section_number(value: str) -> str:
    return value.strip().strip(".").upper()


def _heading_without_effective_note(value: str) -> str:
    return re.sub(r"\s+", " ", _EFFECTIVE_NOTE_RE.sub("", value)).strip()


def _effective_note(heading: str) -> str | None:
    match = _EFFECTIVE_NOTE_RE.search(heading)
    if not match:
        return None
    return re.sub(r"\s+", " ", match.group("note")).strip()


def _section_status(
    heading: str | None,
    body: str | None,
    effective_note: str | None,
) -> str | None:
    if _is_repealed(heading, body):
        return "repealed"
    if effective_note is None:
        return None
    normalized = effective_note.lower()
    if "through" in normalized or "until" in normalized:
        return "effective_until"
    return "future_or_conditional"


def _variant_for_occurrence(effective_note: str | None, occurrence: int) -> str | None:
    if occurrence == 1:
        return None
    if effective_note:
        date_match = re.search(r"([A-Z][a-z]+ \d{1,2}, \d{4})", effective_note)
        if date_match:
            try:
                value = datetime.strptime(date_match.group(1), "%B %d, %Y").date()
            except ValueError:
                pass
            else:
                return f"effective-{value.isoformat()}"
        return _slug(effective_note, fallback=f"variant-{occurrence}")
    return f"variant-{occurrence}"


def _disambiguated_variant(
    variant: str | None,
    *,
    occurrence: int,
    section_number: str,
    used_source_ids: set[str],
) -> str:
    root = variant or "variant"
    suffix = occurrence
    while True:
        candidate = f"{root}-{suffix}"
        if _section_source_id(section_number, candidate) not in used_source_ids:
            return candidate
        suffix += 1


def _section_source_id(section_number: str, variant: str | None) -> str:
    return section_number if variant is None else f"{section_number}@{variant}"


def _slug(value: str, *, fallback: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    if not slug:
        return fallback
    return slug[:80].rstrip("-")


def _is_repealed(heading: str | None, body: str | None) -> bool:
    text = f"{heading or ''} {body or ''}".lower()
    return "repealed" in text


def _clean_text(tag: Tag) -> str:
    return re.sub(r"\s+", " ", tag.get_text(" ", strip=True).replace("\xa0", " ")).strip()


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def _decode(data: str | bytes) -> str:
    if isinstance(data, str):
        return data
    return data.decode("utf-8", errors="replace")
