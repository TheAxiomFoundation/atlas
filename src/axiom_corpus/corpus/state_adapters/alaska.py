"""Alaska Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

ALASKA_STATUTES_DEFAULT_YEAR = 2024
ALASKA_STATUTES_BASE_URL = "https://www.akleg.gov/basis/statutes.asp"
ALASKA_TITLE_INDEX_SOURCE_FORMAT = "alaska-statutes-title-index-html"
ALASKA_TITLE_TOC_SOURCE_FORMAT = "alaska-statutes-title-toc-html"
ALASKA_CHAPTER_PRINT_SOURCE_FORMAT = "alaska-statutes-chapter-print-html"
ALASKA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_TITLE_RE = re.compile(
    r"Title\s+(?P<number>\d+)\.\s*(?P<heading>.+?)\.?\s*$",
    re.I | re.S,
)
_CHAPTER_RE = re.compile(
    r"Chapter\s+(?P<chapter>\d{2})\.\s*(?P<heading>.+?)\.?\s*$",
    re.I | re.S,
)
_ARTICLE_RE = re.compile(
    r"Article\s+(?P<article>[0-9A-Z]+)\.\s*(?P<heading>.+?)\.?\s*$",
    re.I | re.S,
)
_SECTION_HEADING_RE = re.compile(
    r"Sec\.\s+(?P<section>\d{2}\.\d{2}\.\d{3})\.\s*(?P<heading>.+?)\s*$",
    re.I | re.S,
)
_SECTION_REF_RE = re.compile(
    r"(?:AS\s+|Sec\.\s+|\u00a7+\s*)(?P<section>\d{2}\.\d{2}\.\d{3})",
    re.I,
)
_HREF_REF_RE = re.compile(r"statutes\.asp#(?P<section>\d{2}\.\d{2}\.\d{3})", re.I)


@dataclass(frozen=True)
class AlaskaTitle:
    """One Alaska Statutes title from the official title index."""

    number: str
    heading: str
    source_url: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-ak/statute/title-{self.number}"

    @property
    def source_id(self) -> str:
        return f"title-{self.number}"

    @property
    def legal_identifier(self) -> str:
        return f"Alaska Stat. Title {int(self.number)}"


@dataclass(frozen=True)
class AlaskaChapter:
    """One Alaska Statutes chapter from a title TOC."""

    title_number: str
    chapter: str
    heading: str
    source_url: str
    ordinal: int

    @property
    def number(self) -> str:
        return f"{self.title_number}.{self.chapter}"

    @property
    def citation_path(self) -> str:
        return f"us-ak/statute/{self.number}"

    @property
    def source_id(self) -> str:
        return f"chapter-{self.number}"

    @property
    def legal_identifier(self) -> str:
        return f"Alaska Stat. ch. {self.number}"


@dataclass(frozen=True)
class AlaskaArticle:
    """One article heading within an Alaska Statutes chapter."""

    chapter_number: str
    article: str
    heading: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-ak/statute/{self.chapter_number}/article-{self.article.lower()}"

    @property
    def source_id(self) -> str:
        return f"{self.chapter_number}-article-{self.article.lower()}"

    @property
    def legal_identifier(self) -> str:
        return f"Alaska Stat. ch. {self.chapter_number}, art. {self.article}"


@dataclass(frozen=True)
class AlaskaSection:
    """One Alaska Statutes section parsed from chapter print HTML."""

    chapter_number: str
    section: str
    heading: str
    body: str | None
    article: AlaskaArticle | None
    ordinal: int
    references_to: tuple[str, ...]
    status: str | None = None

    @property
    def citation_path(self) -> str:
        return f"us-ak/statute/{self.section}"

    @property
    def source_id(self) -> str:
        return self.section

    @property
    def legal_identifier(self) -> str:
        return f"Alaska Stat. \u00a7 {self.section}"


@dataclass(frozen=True)
class AlaskaChapterDocument:
    """Parsed official printable HTML for one Alaska Statutes chapter."""

    chapter: AlaskaChapter | None
    articles: tuple[AlaskaArticle, ...]
    sections: tuple[AlaskaSection, ...]


@dataclass(frozen=True)
class _AlaskaSource:
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
class _AlaskaChapterFetchResult:
    chapter: AlaskaChapter
    source: _AlaskaSource | None = None
    document: AlaskaChapterDocument | None = None
    error: BaseException | None = None


class _AlaskaFetcher:
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
        self.base_url = base_url
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0

    def fetch_title_index(self) -> _AlaskaSource:
        relative_path = f"{ALASKA_TITLE_INDEX_SOURCE_FORMAT}/index.html"
        data = self._fetch(relative_path, self.base_url)
        return _AlaskaSource(
            relative_path=relative_path,
            source_url=self.base_url,
            source_format=ALASKA_TITLE_INDEX_SOURCE_FORMAT,
            data=data,
        )

    def fetch_title_toc(self, title: AlaskaTitle) -> _AlaskaSource:
        relative_path = f"{ALASKA_TITLE_TOC_SOURCE_FORMAT}/title-{title.number}.html"
        source_url = f"{self.base_url}?{urlencode({'media': 'js', 'type': 'TOC', 'title': int(title.number)})}"
        data = self._fetch(relative_path, source_url)
        return _AlaskaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=ALASKA_TITLE_TOC_SOURCE_FORMAT,
            data=data,
        )

    def fetch_chapter_print(self, chapter: AlaskaChapter) -> _AlaskaSource:
        relative_path = (
            f"{ALASKA_CHAPTER_PRINT_SOURCE_FORMAT}/title-{chapter.title_number}/"
            f"chapter-{chapter.number}.html"
        )
        source_url = f"{self.base_url}?{urlencode({'media': 'print', 'secStart': chapter.number, 'secEnd': chapter.number})}"
        data = self._fetch(relative_path, source_url)
        return _AlaskaSource(
            relative_path=relative_path,
            source_url=source_url,
            source_format=ALASKA_CHAPTER_PRINT_SOURCE_FORMAT,
            data=data,
        )

    def _fetch(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            return (self.source_dir / relative_path).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_alaska_source(
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
        elapsed = time.monotonic() - self._last_request_at
        wait_seconds = self.request_delay_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self._last_request_at = time.monotonic()


def extract_alaska_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_year: int = ALASKA_STATUTES_DEFAULT_YEAR,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = ALASKA_STATUTES_BASE_URL,
    request_delay_seconds: float = 0.05,
    timeout_seconds: float = 30.0,
    request_attempts: int = 3,
    workers: int = 1,
) -> StateStatuteExtractReport:
    """Snapshot official Alaska Statutes HTML and extract provisions."""
    jurisdiction = "us-ak"
    title_filter = _title_filter(only_title)
    run_id = _alaska_run_id(version, title_filter=title_filter, limit=limit)
    source_as_of_text = source_as_of or str(version)
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _AlaskaFetcher(
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
    title_count = 0
    container_count = 0
    section_count = 0
    seen: set[str] = set()

    title_index = fetcher.fetch_title_index()
    title_index_path, title_index_recorded = _record_source(
        store,
        jurisdiction,
        run_id,
        title_index,
    )
    source_paths.append(title_index_path)
    titles = parse_alaska_title_index(title_index.data, base_url=base_url)
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
                identifiers={"alaska:title": title.number},
                metadata={
                    "kind": "title",
                    "source_year": source_year,
                    "title": title.number,
                },
            )

        toc_source = fetcher.fetch_title_toc(title)
        toc_path, toc_recorded = _record_source(store, jurisdiction, run_id, toc_source)
        source_paths.append(toc_path)
        chapters = parse_alaska_title_toc(toc_source.data, title=title, base_url=base_url)

        if limit is None:
            chapter_results = _fetch_alaska_chapter_results(
                fetcher,
                list(chapters),
                workers=workers,
            )
        else:
            chapter_results = [
                _fetch_alaska_chapter_result(fetcher, chapter) for chapter in chapters
            ]

        for result in chapter_results:
            chapter = result.chapter
            if limit is not None and section_count >= limit:
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
                    source_path=toc_recorded.source_path,
                    source_format=toc_recorded.source_format,
                    source_id=chapter.source_id,
                    sha256=toc_recorded.sha256,
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
                        "alaska:title": title.number,
                        "alaska:chapter": chapter.number,
                    },
                    metadata={
                        "kind": "chapter",
                        "source_year": source_year,
                        "title": title.number,
                        "chapter": chapter.number,
                    },
                )

            if result.error is not None:
                errors.append(f"chapter {chapter.number}: {result.error}")
                continue
            assert result.source is not None
            assert result.document is not None
            chapter_source = result.source
            chapter_path, chapter_recorded = _record_source(
                store,
                jurisdiction,
                run_id,
                chapter_source,
            )
            source_paths.append(chapter_path)
            document = result.document
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
                    source_url=chapter_source.source_url,
                    source_path=chapter_recorded.source_path,
                    source_format=chapter_recorded.source_format,
                    source_id=article.source_id,
                    sha256=chapter_recorded.sha256,
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
                        "alaska:title": title.number,
                        "alaska:chapter": chapter.number,
                        "alaska:article": article.article,
                    },
                    metadata={
                        "kind": "article",
                        "source_year": source_year,
                        "title": title.number,
                        "chapter": chapter.number,
                        "article": article.article,
                    },
                )

            for section in document.sections:
                if limit is not None and section_count >= limit:
                    break
                if section.citation_path in seen:
                    continue
                seen.add(section.citation_path)
                section_count += 1
                parent = section.article.citation_path if section.article else chapter.citation_path
                _append_record(
                    items,
                    records,
                    jurisdiction=jurisdiction,
                    citation_path=section.citation_path,
                    version=run_id,
                    source_url=chapter_source.source_url,
                    source_path=chapter_recorded.source_path,
                    source_format=chapter_recorded.source_format,
                    source_id=section.source_id,
                    sha256=chapter_recorded.sha256,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    kind="section",
                    body=section.body,
                    heading=section.heading,
                    legal_identifier=section.legal_identifier,
                    parent_citation_path=parent,
                    level=3 if section.article else 2,
                    ordinal=section.ordinal,
                    identifiers={
                        "alaska:title": title.number,
                        "alaska:chapter": chapter.number,
                        "alaska:section": section.section,
                    },
                    metadata={
                        "kind": "section",
                        "source_year": source_year,
                        "title": title.number,
                        "chapter": chapter.number,
                        "section": section.section,
                        **({"status": section.status} if section.status else {}),
                        **(
                            {"article": section.article.article}
                            if section.article is not None
                            else {}
                        ),
                        "references_to": list(section.references_to),
                    },
                )

    if not records:
        raise ValueError("no Alaska provisions extracted")

    inventory_path = store.inventory_path(jurisdiction, DocumentClass.STATUTE, run_id)
    provisions_path = store.provisions_path(jurisdiction, DocumentClass.STATUTE, run_id)
    coverage_path = store.coverage_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, items)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
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


def parse_alaska_title_index(
    html: str | bytes,
    *,
    base_url: str = ALASKA_STATUTES_BASE_URL,
) -> tuple[AlaskaTitle, ...]:
    """Parse the official Alaska Statutes title index."""
    soup = BeautifulSoup(_decode(html), "lxml")
    title_list = soup.find(id="TitleToc")
    links = title_list.find_all("a") if isinstance(title_list, Tag) else soup.find_all("a")
    titles: list[AlaskaTitle] = []
    ordinal = 0
    for link in links:
        text = _clean_text(link)
        match = _TITLE_RE.search(text)
        if match is None:
            continue
        ordinal += 1
        number = f"{int(match.group('number')):02d}"
        titles.append(
            AlaskaTitle(
                number=number,
                heading=_strip_terminal_period(match.group("heading")),
                source_url=urljoin(base_url, f"#{number}"),
                ordinal=ordinal,
            )
        )
    return tuple(titles)


def parse_alaska_title_toc(
    html: str | bytes,
    *,
    title: AlaskaTitle,
    base_url: str = ALASKA_STATUTES_BASE_URL,
) -> tuple[AlaskaChapter, ...]:
    """Parse official Alaska title TOC fragment into chapter targets."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapters: list[AlaskaChapter] = []
    ordinal = 0
    for link in soup.find_all("a"):
        text = _clean_text(link)
        match = _CHAPTER_RE.search(text)
        if match is None:
            continue
        ordinal += 1
        chapter = match.group("chapter")
        chapter_number = f"{title.number}.{chapter}"
        chapters.append(
            AlaskaChapter(
                title_number=title.number,
                chapter=chapter,
                heading=_strip_terminal_period(match.group("heading")),
                source_url=f"{base_url}#{chapter_number}",
                ordinal=ordinal,
            )
        )
    return tuple(chapters)


def parse_alaska_chapter_print(
    html: str | bytes,
    *,
    chapter: AlaskaChapter | None = None,
) -> AlaskaChapterDocument:
    """Parse official chapter printable HTML into chapter/article/section records."""
    soup = BeautifulSoup(_decode(html), "lxml")
    container = soup.find(class_="statute") or soup.body or soup
    current_chapter = chapter
    current_article: AlaskaArticle | None = None
    current_section_id: str | None = None
    current_heading: str | None = None
    current_body_parts: list[str] = []
    current_section_article: AlaskaArticle | None = None
    sections: list[AlaskaSection] = []
    articles: list[AlaskaArticle] = []
    article_ordinal = 0
    section_ordinal = 0

    def finish_section() -> None:
        nonlocal current_section_id, current_heading, current_body_parts
        nonlocal current_section_article, section_ordinal
        if current_section_id is None or current_heading is None:
            return
        section_ordinal += 1
        raw_body = "\n".join(current_body_parts)
        body = _normalize_body(raw_body)
        status = _section_status(current_heading, body)
        sections.append(
            AlaskaSection(
                chapter_number=current_chapter.number if current_chapter else "",
                section=current_section_id,
                heading=current_heading,
                body=body,
                article=current_section_article,
                ordinal=section_ordinal,
                references_to=tuple(_extract_references(f"{current_heading}\n{body or ''}")),
                status=status,
            )
        )
        current_section_id = None
        current_heading = None
        current_body_parts = []
        current_section_article = None

    for child in container.children:
        if isinstance(child, NavigableString):
            if current_section_id is not None:
                current_body_parts.append(str(child))
            continue
        if not isinstance(child, Tag):
            continue
        marker = _parse_marker(child)
        if marker is not None:
            marker_kind, marker_value, marker_heading = marker
            if marker_kind == "chapter":
                finish_section()
                if current_chapter is None:
                    title_number, chapter_number = marker_value.split(".", 1)
                    current_chapter = AlaskaChapter(
                        title_number=title_number,
                        chapter=chapter_number,
                        heading=marker_heading,
                        source_url=f"{ALASKA_STATUTES_BASE_URL}#{marker_value}",
                        ordinal=1,
                    )
                current_article = None
                continue
            if marker_kind == "article":
                finish_section()
                if current_chapter is not None:
                    article_ordinal += 1
                    article = AlaskaArticle(
                        chapter_number=current_chapter.number,
                        article=marker_value,
                        heading=marker_heading,
                        ordinal=article_ordinal,
                    )
                    articles.append(article)
                    current_article = article
                continue
            if marker_kind == "section":
                finish_section()
                current_section_id = marker_value
                current_heading = marker_heading
                current_body_parts = []
                current_section_article = current_article
                continue
        if current_section_id is not None:
            current_body_parts.append(_tag_text(child))
    finish_section()
    return AlaskaChapterDocument(
        chapter=current_chapter,
        articles=tuple(articles),
        sections=tuple(sections),
    )


def _fetch_alaska_chapter_results(
    fetcher: _AlaskaFetcher,
    chapters: list[AlaskaChapter],
    *,
    workers: int,
) -> list[_AlaskaChapterFetchResult]:
    if not chapters:
        return []
    max_workers = max(1, workers)
    if max_workers == 1:
        return [_fetch_alaska_chapter_result(fetcher, chapter) for chapter in chapters]
    results: list[_AlaskaChapterFetchResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_alaska_chapter_result, fetcher, chapter): chapter
            for chapter in chapters
        }
        for future in as_completed(future_map):
            try:
                results.append(future.result())
            except BaseException as exc:  # pragma: no cover
                results.append(
                    _AlaskaChapterFetchResult(chapter=future_map[future], error=exc)
                )
    order = {chapter.citation_path: index for index, chapter in enumerate(chapters)}
    return sorted(results, key=lambda result: order[result.chapter.citation_path])


def _fetch_alaska_chapter_result(
    fetcher: _AlaskaFetcher,
    chapter: AlaskaChapter,
) -> _AlaskaChapterFetchResult:
    try:
        source = fetcher.fetch_chapter_print(chapter)
        document = parse_alaska_chapter_print(source.data, chapter=chapter)
        return _AlaskaChapterFetchResult(
            chapter=chapter,
            source=source,
            document=document,
        )
    except BaseException as exc:  # pragma: no cover
        return _AlaskaChapterFetchResult(chapter=chapter, error=exc)


def _parse_marker(tag: Tag) -> tuple[str, str, str] | None:
    text = _clean_text(tag)
    anchor = tag.find("a", attrs={"name": True})
    anchor_name = str(anchor["name"]).strip() if isinstance(anchor, Tag) else None
    if anchor_name and re.fullmatch(r"\d{2}\.\d{2}", anchor_name):
        h6 = tag.find("h6")
        chapter_text = _clean_text(h6) if isinstance(h6, Tag) else text
        match = _CHAPTER_RE.search(chapter_text)
        if match is not None:
            return ("chapter", anchor_name, _strip_terminal_period(match.group("heading")))
    if anchor_name and re.fullmatch(r"\d{2}\.\d{2}\.\d{3}", anchor_name):
        match = _SECTION_HEADING_RE.search(text)
        if match is not None:
            return (
                "section",
                match.group("section"),
                _strip_terminal_period(match.group("heading")),
            )
    h7 = tag.find("h7")
    if isinstance(h7, Tag):
        match = _ARTICLE_RE.search(_clean_text(h7))
        if match is not None:
            return (
                "article",
                match.group("article").upper(),
                _strip_terminal_period(match.group("heading")),
            )
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
    source: _AlaskaSource,
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


def _download_alaska_source(
    source_url: str,
    *,
    fetcher: _AlaskaFetcher,
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
                    ALASKA_USER_AGENT,
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
    raise ValueError(f"Alaska source request failed: {source_url}")


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _alaska_run_id(version: str, *, title_filter: str | None, limit: int | None) -> str:
    if title_filter is None and limit is None:
        return version
    parts = [version, "us-ak"]
    if title_filter is not None:
        parts.append(f"title-{title_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title|Title)[-\s]*", "", text)
    if not text:
        return None
    return f"{int(text):02d}" if text.isdigit() else text.upper()


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _section_status(heading: str, body: str | None) -> str | None:
    text = f"{heading}\n{body or ''}"
    if re.search(r"\[\s*Repealed\b", text, re.I):
        return "repealed"
    if re.search(r"\[\s*Renumbered\b", text, re.I):
        return "renumbered"
    return None


def _extract_references(text: str) -> list[str]:
    refs = [
        f"us-ak/statute/{match.group('section').upper()}"
        for match in _SECTION_REF_RE.finditer(text)
    ]
    refs.extend(
        f"us-ak/statute/{match.group('section').upper()}"
        for match in _HREF_REF_RE.finditer(text)
    )
    return _dedupe_preserve_order(refs)


def _tag_text(tag: Tag) -> str:
    for br in tag.find_all("br"):
        br.replace_with("\n")
    return tag.get_text("\n", strip=False)


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
        return value.decode("utf-8")
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


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
