"""West Virginia Code source-first corpus adapter."""

from __future__ import annotations

import html as html_module
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

WEST_VIRGINIA_CODE_BASE_URL = "https://code.wvlegislature.gov"
WEST_VIRGINIA_INDEX_SOURCE_FORMAT = "west-virginia-code-html"
WEST_VIRGINIA_ARTICLE_SOURCE_FORMAT = "west-virginia-code-article-json"
WEST_VIRGINIA_SECTION_SOURCE_FORMAT = "west-virginia-code-section-html"
WEST_VIRGINIA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"
WEST_VIRGINIA_REQUEST_DELAY_SECONDS = 0.05
WEST_VIRGINIA_REQUEST_ATTEMPTS = 3
WEST_VIRGINIA_TIMEOUT_SECONDS = 90.0

_CHAPTER_RE = re.compile(r"^CHAPTER\s+(?P<chapter>\d+[A-Z]?)\.\s*(?P<heading>.+)$", re.I)
_ARTICLE_RE = re.compile(
    r"^CHAPTER\s+(?P<chapter>\d+[A-Z]?),\s*ARTICLE\s+(?P<article>\d+[A-Z]?)\.\s*(?P<heading>.+)$",
    re.I,
)
_SECTION_HREF_RE = re.compile(r"/(?P<section>\d+[A-Z]?-\d+[A-Z]?-[0-9A-Z]+[A-Z]?)/?$", re.I)
_SECTION_PART_PATTERN = r"\d+[A-Z]?\s*[-–]\s*\d+[A-Z]?\s*[-–]\s*[0-9A-Z]+[A-Z]?"
_SECTION_HEADING_RE = re.compile(
    rf"^[\x00-\x1f\s]*(?:(?:\d+\s*)?§\s*)?(?P<section>{_SECTION_PART_PATTERN})"
    rf"(?:\s+(?:to|through)\s+(?P<end>(?:\d+[A-Z]?\s*[-–]\s*\d+[A-Z]?\s*[-–]\s*)?[0-9A-Z]+[A-Z]?))?"
    r"\.?\s*(?P<heading>.*)$",
    re.I,
)
_REFERENCE_RE = re.compile(rf"§+\s*(?P<section>{_SECTION_PART_PATTERN})\b", re.I)


@dataclass(frozen=True)
class WestVirginiaChapter:
    """Chapter metadata from the official West Virginia Code master index."""

    chapter: str
    heading: str
    ordinal: int
    source_url: str

    @property
    def citation_path(self) -> str:
        return f"us-wv/statute/chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"W. Va. Code ch. {self.chapter}"


@dataclass(frozen=True)
class WestVirginiaArticle:
    """Article metadata from the official West Virginia Code master index."""

    chapter: str
    article: str
    heading: str
    ordinal: int
    source_url: str

    @property
    def citation_path(self) -> str:
        return f"us-wv/statute/chapter-{self.chapter}/article-{self.article}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-wv/statute/chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"W. Va. Code ch. {self.chapter}, art. {self.article}"


@dataclass(frozen=True)
class WestVirginiaSection:
    """Section metadata from the official West Virginia Code master index."""

    section: str
    heading: str | None
    chapter: str
    article: str
    ordinal: int
    source_url: str

    @property
    def citation_path(self) -> str:
        return f"us-wv/statute/{self.section}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-wv/statute/chapter-{self.chapter}/article-{self.article}"

    @property
    def legal_identifier(self) -> str:
        return f"W. Va. Code § {self.section}"


@dataclass(frozen=True)
class WestVirginiaCodeIndex:
    chapters: tuple[WestVirginiaChapter, ...]
    articles: tuple[WestVirginiaArticle, ...]
    sections: tuple[WestVirginiaSection, ...]


@dataclass(frozen=True)
class WestVirginiaSectionBody:
    section: str
    heading: str | None
    body: str | None
    references_to: tuple[str, ...] = ()
    status: str | None = None


@dataclass(frozen=True)
class _WestVirginiaSourcePage:
    relative_path: str
    source_url: str
    source_format: str
    data: bytes


class _WestVirginiaFetcher:
    def __init__(
        self,
        *,
        base_url: str,
        source_dir: Path | None,
        download_dir: Path | None,
        request_delay_seconds: float,
        request_attempts: int,
        timeout_seconds: float,
    ) -> None:
        self.base_url = _base_url(base_url)
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.request_delay_seconds = request_delay_seconds
        self.request_attempts = request_attempts
        self.timeout_seconds = timeout_seconds
        self._last_request_at = 0.0
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": WEST_VIRGINIA_USER_AGENT})

    def fetch_master(self) -> _WestVirginiaSourcePage:
        return self.fetch(
            f"{WEST_VIRGINIA_INDEX_SOURCE_FORMAT}/wvcodeentire.html",
            urljoin(self.base_url, "/wvcodeentire.htm"),
            source_format=WEST_VIRGINIA_INDEX_SOURCE_FORMAT,
        )

    def fetch_article(self, chapter: str, article: str) -> _WestVirginiaSourcePage:
        query = urlencode({"action": "get_all_sections", "chp": chapter, "art": article})
        return self.fetch(
            f"{WEST_VIRGINIA_ARTICLE_SOURCE_FORMAT}/chapter-{chapter}/article-{article}.json",
            urljoin(self.base_url, f"/wp-admin/admin-ajax.php?{query}"),
            source_format=WEST_VIRGINIA_ARTICLE_SOURCE_FORMAT,
            method="POST",
            data={"action": "get_all_sections", "chp": chapter, "art": article},
            headers={
                "Referer": urljoin(self.base_url, f"/{chapter}-{article}/"),
                "X-Requested-With": "XMLHttpRequest",
            },
        )

    def fetch_section(self, section: str) -> _WestVirginiaSourcePage:
        return self.fetch(
            f"{WEST_VIRGINIA_SECTION_SOURCE_FORMAT}/{section}.html",
            urljoin(self.base_url, f"/{section}/"),
            source_format=WEST_VIRGINIA_SECTION_SOURCE_FORMAT,
        )

    def fetch(
        self,
        relative_path: str,
        source_url: str,
        *,
        source_format: str,
        method: str = "GET",
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> _WestVirginiaSourcePage:
        normalized = _normalize_relative_path(relative_path)
        if self.source_dir is not None:
            source_path = _source_dir_file(self.source_dir, normalized)
            if source_path is None:
                raise ValueError(f"West Virginia source file does not exist: {self.source_dir / normalized}")
            return _WestVirginiaSourcePage(
                relative_path=normalized,
                source_url=source_url,
                source_format=source_format,
                data=source_path.read_bytes(),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            if cached_path.exists():
                return _WestVirginiaSourcePage(
                    relative_path=normalized,
                    source_url=source_url,
                    source_format=source_format,
                    data=cached_path.read_bytes(),
                )

        payload = self._download(source_url, method=method, data=data, headers=headers)
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            cached_path.write_bytes(payload)
        return _WestVirginiaSourcePage(
            relative_path=normalized,
            source_url=source_url,
            source_format=source_format,
            data=payload,
        )

    def _download(
        self,
        source_url: str,
        *,
        method: str,
        data: dict[str, str] | None,
        headers: dict[str, str] | None,
    ) -> bytes:
        last_error: Exception | None = None
        for attempt in range(1, max(1, self.request_attempts) + 1):
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            try:
                response = self._session.request(
                    method,
                    source_url,
                    data=data,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
                self._last_request_at = time.monotonic()
                response.raise_for_status()
                return response.content
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.request_attempts:
                    break
                time.sleep(min(2.0 * attempt, 8.0))
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"failed to fetch {source_url}")


def extract_west_virginia_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_chapter: str | int | None = None,
    only_article: str | int | None = None,
    limit: int | None = None,
    workers: int = 1,
    download_dir: str | Path | None = None,
    base_url: str = WEST_VIRGINIA_CODE_BASE_URL,
    request_delay_seconds: float = WEST_VIRGINIA_REQUEST_DELAY_SECONDS,
    request_attempts: int = WEST_VIRGINIA_REQUEST_ATTEMPTS,
    timeout_seconds: float = WEST_VIRGINIA_TIMEOUT_SECONDS,
) -> StateStatuteExtractReport:
    """Snapshot official West Virginia Code sources and extract provisions."""
    jurisdiction = "us-wv"
    chapter_filter = _code_filter(only_chapter)
    article_filter = _code_filter(only_article)
    run_id = _west_virginia_run_id(
        version,
        chapter_filter=chapter_filter,
        article_filter=article_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher_kwargs = {
        "base_url": base_url,
        "source_dir": Path(source_dir) if source_dir is not None else None,
        "download_dir": Path(download_dir) if download_dir is not None else None,
        "request_delay_seconds": request_delay_seconds,
        "request_attempts": request_attempts,
        "timeout_seconds": timeout_seconds,
    }
    fetcher = _WestVirginiaFetcher(
        **fetcher_kwargs,
    )

    thread_state = threading.local()

    def worker_fetch_article(
        article: WestVirginiaArticle,
    ) -> tuple[WestVirginiaArticle, _WestVirginiaSourcePage | Exception]:
        worker_fetcher = getattr(thread_state, "fetcher", None)
        if worker_fetcher is None:
            worker_fetcher = _WestVirginiaFetcher(**fetcher_kwargs)
            thread_state.fetcher = worker_fetcher
        try:
            return article, worker_fetcher.fetch_article(article.chapter, article.article)
        except Exception as exc:  # noqa: BLE001 - keep batch extraction moving and report per-article failures.
            return article, exc

    def article_page_results(
        selected_articles: tuple[WestVirginiaArticle, ...],
    ) -> list[tuple[WestVirginiaArticle, _WestVirginiaSourcePage | Exception]]:
        if workers <= 1 or limit is not None:
            return [worker_fetch_article(article) for article in selected_articles]
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            return list(executor.map(worker_fetch_article, selected_articles))

    source_paths: list[Path] = []
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    remaining_sections = limit

    master_page = fetcher.fetch_master()
    master_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, master_page.relative_path)
    master_sha = store.write_bytes(master_path, master_page.data)
    source_paths.append(master_path)
    master_source_key = _state_source_key(jurisdiction, run_id, master_page.relative_path)
    code_index = parse_west_virginia_code_index(master_page.data)
    chapters = tuple(
        chapter
        for chapter in code_index.chapters
        if chapter_filter is None or _canonical_code_part(chapter.chapter) == chapter_filter
    )
    if not chapters:
        raise ValueError(f"no West Virginia chapters selected for filter: {only_chapter!r}")
    chapter_map = {chapter.chapter: chapter for chapter in chapters}
    articles = tuple(
        article
        for article in code_index.articles
        if article.chapter in chapter_map
        and (article_filter is None or _canonical_code_part(article.article) == article_filter)
    )
    article_keys = {(article.chapter, article.article) for article in articles}
    sections_by_article: dict[tuple[str, str], list[WestVirginiaSection]] = {}
    for section in code_index.sections:
        key = (section.chapter, section.article)
        if section.chapter in chapter_map and key in article_keys:
            sections_by_article.setdefault(key, []).append(section)

    if not articles:
        raise ValueError(f"no West Virginia articles selected for filters: {only_chapter!r}, {only_article!r}")

    for chapter in chapters:
        if chapter.citation_path in seen:
            continue
        seen.add(chapter.citation_path)
        title_count += 1
        _append_record(
            items,
            records,
            jurisdiction=jurisdiction,
            citation_path=chapter.citation_path,
            version=run_id,
            source_url=master_page.source_url,
            source_path=master_source_key,
            source_format=master_page.source_format,
            source_id=f"chapter-{chapter.chapter}",
            sha256=master_sha,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            kind="chapter",
            body=None,
            heading=chapter.heading,
            legal_identifier=chapter.legal_identifier,
            parent_citation_path=None,
            level=0,
            ordinal=chapter.ordinal,
            identifiers={"west_virginia:chapter": chapter.chapter},
            metadata={"kind": "chapter", "chapter": chapter.chapter},
        )

    for article, article_result in article_page_results(articles):
        if remaining_sections is not None and remaining_sections <= 0:
            break
        if isinstance(article_result, Exception):
            skipped_source_count += 1
            errors.append(f"chapter {article.chapter} article {article.article}: {article_result}")
            continue
        article_page = article_result
        article_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, article_page.relative_path)
        article_sha = store.write_bytes(article_path, article_page.data)
        source_paths.append(article_path)
        article_source_key = _state_source_key(jurisdiction, run_id, article_page.relative_path)
        parsed_article_sections = parse_west_virginia_article_sections_json(article_page.data)
        parsed_sections = {section.section: section for section in parsed_article_sections}
        parsed_by_heading = _sections_by_heading(parsed_article_sections)
        article_sections = sections_by_article.get((article.chapter, article.article), [])

        if article.citation_path not in seen:
            seen.add(article.citation_path)
            container_count += 1
            _append_record(
                items,
                records,
                jurisdiction=jurisdiction,
                citation_path=article.citation_path,
                version=run_id,
                source_url=article_page.source_url,
                source_path=article_source_key,
                source_format=article_page.source_format,
                source_id=f"chapter-{article.chapter}-article-{article.article}",
                sha256=article_sha,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="article",
                body=None,
                heading=article.heading,
                legal_identifier=article.legal_identifier,
                parent_citation_path=article.parent_citation_path,
                level=1,
                ordinal=article.ordinal,
                identifiers={
                    "west_virginia:chapter": article.chapter,
                    "west_virginia:article": article.article,
                },
                metadata={"kind": "article", "chapter": article.chapter, "article": article.article},
            )

        for section in article_sections:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            if section.citation_path in seen:
                continue
            parsed = parsed_sections.get(section.section)
            if parsed is None and section.heading:
                parsed = parsed_by_heading.get(_heading_key(section.heading))
            section_source_url = article_page.source_url
            section_source_path = article_source_key
            section_source_format = article_page.source_format
            section_sha = article_sha
            if parsed is None and _status(section.heading, [section.heading or ""]) == "repealed":
                parsed = WestVirginiaSectionBody(
                    section=section.section,
                    heading=section.heading,
                    body=section.heading,
                    status="repealed",
                )
                section_source_url = master_page.source_url
                section_source_path = master_source_key
                section_source_format = master_page.source_format
                section_sha = master_sha
            if parsed is None:
                try:
                    section_page = fetcher.fetch_section(section.section)
                    section_path = store.source_path(
                        jurisdiction,
                        DocumentClass.STATUTE,
                        run_id,
                        section_page.relative_path,
                    )
                    section_sha = store.write_bytes(section_path, section_page.data)
                    source_paths.append(section_path)
                    section_source_path = _state_source_key(
                        jurisdiction,
                        run_id,
                        section_page.relative_path,
                    )
                    section_source_url = section_page.source_url
                    section_source_format = section_page.source_format
                    parsed = parse_west_virginia_section_html(
                        section_page.data,
                        fallback_section=section.section,
                    )
                except (requests.RequestException, ValueError) as exc:
                    errors.append(
                        f"chapter {article.chapter} article {article.article}: section fetch failed for "
                        f"{section.section}: {exc}"
                    )
            if parsed is None:
                errors.append(f"chapter {article.chapter} article {article.article}: missing body for {section.section}")
            seen.add(section.citation_path)
            section_count += 1
            _append_section_record(
                items,
                records,
                section,
                parsed,
                version=run_id,
                source_url=section_source_url,
                source_path=section_source_path,
                source_format=section_source_format,
                sha256=section_sha,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
            if remaining_sections is not None:
                remaining_sections -= 1

    if not records:
        raise ValueError("no West Virginia provisions extracted")

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
        skipped_source_count=skipped_source_count,
        errors=tuple(errors),
    )


def parse_west_virginia_code_index(html: str | bytes) -> WestVirginiaCodeIndex:
    """Parse the official all-code inventory page."""
    soup = BeautifulSoup(_decode(html), "lxml")
    chapters: list[WestVirginiaChapter] = []
    articles: list[WestVirginiaArticle] = []
    sections: list[WestVirginiaSection] = []
    current_chapter: str | None = None
    current_article: str | None = None
    chapter_ordinal = 0
    article_ordinal = 0
    section_ordinal = 0

    for tag in soup.select("#wrapper h1, #wrapper h2, #wrapper h3"):
        anchor = tag.find("a")
        if anchor is None:
            continue
        text = _clean_text(anchor.get_text(" ", strip=True))
        href = str(anchor.get("href") or "")
        if tag.name == "h1":
            match = _CHAPTER_RE.match(text)
            if match is None:
                continue
            current_chapter = _canonical_code_part(match.group("chapter"))
            current_article = None
            chapter_ordinal += 1
            chapters.append(
                WestVirginiaChapter(
                    chapter=current_chapter,
                    heading=_title_case(match.group("heading")),
                    ordinal=chapter_ordinal,
                    source_url=href,
                )
            )
            continue
        if tag.name == "h2":
            match = _ARTICLE_RE.match(text)
            if match is None:
                continue
            current_chapter = _canonical_code_part(match.group("chapter"))
            current_article = _canonical_code_part(match.group("article"))
            article_ordinal += 1
            articles.append(
                WestVirginiaArticle(
                    chapter=current_chapter,
                    article=current_article,
                    heading=_title_case(match.group("heading")),
                    ordinal=article_ordinal,
                    source_url=href,
                )
            )
            continue
        if tag.name == "h3" and current_chapter is not None and current_article is not None:
            section = _section_from_href(href) or _section_from_heading(text)
            if section is None:
                continue
            section_ordinal += 1
            sections.append(
                WestVirginiaSection(
                    section=section,
                    heading=_heading_from_section_text(text, section=section),
                    chapter=current_chapter,
                    article=current_article,
                    ordinal=section_ordinal,
                    source_url=href,
                )
            )

    return WestVirginiaCodeIndex(
        chapters=tuple(chapters),
        articles=tuple(articles),
        sections=tuple(sections),
    )


def parse_west_virginia_article_sections_json(data: str | bytes) -> tuple[WestVirginiaSectionBody, ...]:
    """Parse the official `get_all_sections` article JSON payload."""
    payload = json.loads(_decode(data))
    fragment = payload.get("html") if isinstance(payload, dict) else None
    if not isinstance(fragment, str):
        return ()
    soup = BeautifulSoup(fragment, "lxml")
    sections: list[WestVirginiaSectionBody] = []
    current_sections: tuple[str, ...] = ()
    current_heading: str | None = None
    current_lines: list[str] = []

    def finish_current() -> None:
        nonlocal current_sections, current_heading, current_lines
        if not current_sections:
            return
        body = "\n".join(line for line in current_lines if line).strip() or None
        for current_section in current_sections:
            sections.append(
                WestVirginiaSectionBody(
                    section=current_section,
                    heading=current_heading,
                    body=body,
                    references_to=_references_to(
                        [current_heading or "", *(current_lines or [])],
                        self_section=current_section,
                    ),
                    status=_status(current_heading, current_lines),
                )
            )
        current_sections = ()
        current_heading = None
        current_lines = []

    for node in _fragment_nodes(soup):
        text = _node_text(node)
        node_sections = _sections_from_heading(text)
        if node_sections and (isinstance(node, Tag) and node.name == "h4" or not current_sections):
            finish_current()
            current_sections = node_sections
            current_heading = _heading_from_section_text(text, section=node_sections[0])
            current_lines = []
            continue
        if current_sections:
            node_heading_sections = _sections_from_heading(text)
            if node_heading_sections and text.startswith("§"):
                finish_current()
                current_sections = node_heading_sections
                current_heading = _heading_from_section_text(text, section=node_heading_sections[0])
                current_lines = []
                continue
        if not current_sections:
            continue
        text = _body_node_text(node)
        if text:
            current_lines.extend(text)

    finish_current()
    return tuple(sections)


def parse_west_virginia_section_html(
    html: str | bytes,
    *,
    fallback_section: str | None = None,
) -> WestVirginiaSectionBody | None:
    """Parse a single official section page."""
    soup = BeautifulSoup(_decode(html), "lxml")
    section_text = soup.select_one(".sectiontext")
    if section_text is None:
        return None
    payload = json.dumps({"html": str(section_text)})
    sections = parse_west_virginia_article_sections_json(payload)
    if not sections:
        return None
    if fallback_section is None:
        return sections[0]
    for section in sections:
        if section.section == fallback_section:
            return section
    return _retarget_section_body(sections[0], fallback_section)


def _append_section_record(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    section: WestVirginiaSection,
    parsed: WestVirginiaSectionBody | None,
    *,
    version: str,
    source_url: str,
    source_path: str,
    source_format: str,
    sha256: str,
    source_as_of: str,
    expression_date: str,
) -> None:
    heading = parsed.heading if parsed and parsed.heading else section.heading
    metadata: dict[str, Any] = {
        "kind": "section",
        "chapter": section.chapter,
        "article": section.article,
        "section": section.section,
    }
    if parsed and parsed.references_to:
        metadata["references_to"] = list(parsed.references_to)
    if parsed and parsed.status:
        metadata["status"] = parsed.status
    _append_record(
        items,
        records,
        jurisdiction="us-wv",
        citation_path=section.citation_path,
        version=version,
        source_url=source_url,
        source_path=source_path,
        source_format=source_format,
        source_id=f"section-{section.section}",
        sha256=sha256,
        source_as_of=source_as_of,
        expression_date=expression_date,
        kind="section",
        body=parsed.body if parsed else None,
        heading=heading,
        legal_identifier=section.legal_identifier,
        parent_citation_path=section.parent_citation_path,
        level=2,
        ordinal=section.ordinal,
        identifiers={
            "west_virginia:chapter": section.chapter,
            "west_virginia:article": section.article,
            "west_virginia:section": section.section,
        },
        metadata=metadata,
    )


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
            parent_id=deterministic_provision_id(parent_citation_path) if parent_citation_path else None,
            level=level,
            ordinal=ordinal,
            kind=kind,
            legal_identifier=legal_identifier,
            identifiers=identifiers,
            metadata=metadata,
        )
    )


def _fragment_nodes(soup: BeautifulSoup) -> list[Tag | NavigableString]:
    body = soup.body
    if body is not None:
        return list(body.children)
    return list(soup.children)


def _node_text(node: Tag | NavigableString) -> str:
    if isinstance(node, NavigableString):
        return _clean_text(str(node))
    return _clean_text(node.get_text(" ", strip=True))


def _body_node_text(node: Tag | NavigableString) -> list[str]:
    if isinstance(node, NavigableString):
        text = _clean_text(str(node))
        return [text] if text else []
    if node.name == "table":
        rows: list[str] = []
        for tr in node.find_all("tr"):
            cells = [
                _clean_text(cell.get_text(" ", strip=True))
                for cell in tr.find_all(["th", "td"])
                if _clean_text(cell.get_text(" ", strip=True))
            ]
            if cells:
                rows.append(" | ".join(cells))
        return rows
    text = _clean_text(node.get_text(" ", strip=True))
    return [text] if text else []


def _references_to(body_lines: list[str], *, self_section: str) -> tuple[str, ...]:
    refs: list[str] = []
    for line in body_lines:
        for match in _REFERENCE_RE.finditer(line):
            section = _normalize_section(match.group("section"))
            if section == self_section:
                continue
            refs.append(f"us-wv/statute/{section}")
    return tuple(dict.fromkeys(refs))


def _sections_by_heading(
    sections: tuple[WestVirginiaSectionBody, ...],
) -> dict[str, WestVirginiaSectionBody]:
    grouped: dict[str, WestVirginiaSectionBody | None] = {}
    for section in sections:
        if not section.heading:
            continue
        key = _heading_key(section.heading)
        if key in grouped:
            grouped[key] = None
        else:
            grouped[key] = section
    return {key: value for key, value in grouped.items() if value is not None}


def _heading_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _clean_text(value).lower()).strip()


def _retarget_section_body(
    section: WestVirginiaSectionBody,
    fallback_section: str,
) -> WestVirginiaSectionBody:
    body_lines = section.body.splitlines() if section.body else []
    return WestVirginiaSectionBody(
        section=fallback_section,
        heading=section.heading,
        body=section.body,
        references_to=_references_to([section.heading or "", *body_lines], self_section=fallback_section),
        status=section.status,
    )


def _status(heading: str | None, body_lines: list[str]) -> str | None:
    joined = " ".join(part for part in [heading or "", *body_lines[:3]] if part).lower()
    if "repealed" in joined:
        return "repealed"
    if "reserved" in joined:
        return "reserved"
    return None


def _section_from_href(href: str) -> str | None:
    match = _SECTION_HREF_RE.search(href)
    if match is None:
        return None
    return _normalize_section(match.group("section"))


def _section_from_heading(text: str) -> str | None:
    sections = _sections_from_heading(text)
    return sections[0] if sections else None


def _sections_from_heading(text: str) -> tuple[str, ...]:
    match = _SECTION_HEADING_RE.match(_clean_section_heading_text(text))
    if match is None:
        return ()
    start = _normalize_section(match.group("section"))
    end = match.group("end")
    if not end:
        return (start,)
    expanded = _expand_section_range(start, end)
    return expanded or (start,)


def _heading_from_section_text(text: str, *, section: str | None = None) -> str | None:
    cleaned = _clean_section_heading_text(text)
    match = _SECTION_HEADING_RE.match(cleaned)
    if match is not None:
        return _clean_heading(match.group("heading"))
    if section is not None:
        pattern = re.compile(rf"^§+\s*{re.escape(section)}\.?\s*", re.I)
        stripped = pattern.sub("", cleaned, count=1)
        if stripped != cleaned:
            return _clean_heading(stripped)
    return _clean_heading(text)


def _normalize_section(value: str) -> str:
    parts = [_canonical_code_part(part) for part in re.split(r"[-–]", _clean_text(value))]
    return "-".join(part for part in parts if part)


def _clean_section_heading_text(value: str | None) -> str:
    text = _clean_text(value)
    text = re.sub(r"§(?P<prefix>\d+[A-Z]?)-§(?P=prefix)-", r"§\g<prefix>-", text, flags=re.I)
    return text


def _expand_section_range(start: str, end: str) -> tuple[str, ...]:
    start_parts = start.split("-")
    if len(start_parts) != 3:
        return ()
    end_text = _normalize_section(end)
    end_parts = end_text.split("-")
    if len(end_parts) == 1:
        end_parts = [start_parts[0], start_parts[1], end_parts[0]]
    if len(end_parts) != 3 or start_parts[:2] != end_parts[:2]:
        return ()
    start_num = start_parts[2]
    end_num = end_parts[2]
    if not start_num.isdigit() or not end_num.isdigit():
        return ()
    first = int(start_num)
    last = int(end_num)
    if last < first or last - first > 500:
        return ()
    return tuple(f"{start_parts[0]}-{start_parts[1]}-{number}" for number in range(first, last + 1))


def _code_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    return _canonical_code_part(str(value))


def _canonical_code_part(value: str) -> str:
    return _clean_text(value).upper()


def _clean_heading(value: str | None) -> str | None:
    if value is None:
        return None
    text = _clean_text(value).rstrip(".")
    return text or None


def _clean_text(value: str | None) -> str:
    if value is None:
        return ""
    text = html_module.unescape(value).replace("\xa0", " ").replace("\ufeff", "")
    return re.sub(r"\s+", " ", text).strip()


def _title_case(value: str | None) -> str:
    text = _clean_text(value).removesuffix(".")
    if not text:
        return ""
    if any(char.islower() for char in text):
        return text
    small = {"A", "An", "And", "As", "At", "But", "By", "For", "In", "Nor", "Of", "On", "Or", "The", "To"}
    words = text.title().split()
    return " ".join(word.lower() if index and word in small else word for index, word in enumerate(words))


def _decode(data: str | bytes) -> str:
    if isinstance(data, bytes):
        return data.decode("utf-8-sig", errors="replace")
    return data


def _west_virginia_run_id(
    version: str,
    *,
    chapter_filter: str | None,
    article_filter: str | None,
    limit: int | None,
) -> str:
    if chapter_filter is None and article_filter is None and limit is None:
        return version
    parts = [version, "us-wv"]
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter}")
    if article_filter is not None:
        parts.append(f"article-{article_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _base_url(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _normalize_relative_path(value: str) -> str:
    return "/".join(part for part in value.strip().split("/") if part)


def _source_dir_file(source_dir: Path, relative_path: str) -> Path | None:
    candidates = [source_dir / relative_path]
    if relative_path.startswith(f"{WEST_VIRGINIA_INDEX_SOURCE_FORMAT}/"):
        candidates.append(source_dir / relative_path.removeprefix(f"{WEST_VIRGINIA_INDEX_SOURCE_FORMAT}/"))
    if relative_path.startswith(f"{WEST_VIRGINIA_ARTICLE_SOURCE_FORMAT}/"):
        candidates.append(source_dir / relative_path.removeprefix(f"{WEST_VIRGINIA_ARTICLE_SOURCE_FORMAT}/"))
    if relative_path.startswith(f"{WEST_VIRGINIA_SECTION_SOURCE_FORMAT}/"):
        candidates.append(source_dir / relative_path.removeprefix(f"{WEST_VIRGINIA_SECTION_SOURCE_FORMAT}/"))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None
