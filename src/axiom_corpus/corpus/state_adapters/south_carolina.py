"""South Carolina Code of Laws source-first corpus adapter."""

from __future__ import annotations

import html as html_module
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

SOUTH_CAROLINA_BASE_URL = "https://www.scstatehouse.gov"
SOUTH_CAROLINA_SOURCE_FORMAT = "south-carolina-code-html"
SOUTH_CAROLINA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"
SOUTH_CAROLINA_REQUEST_DELAY_SECONDS = 0.15
SOUTH_CAROLINA_REQUEST_ATTEMPTS = 3
SOUTH_CAROLINA_TIMEOUT_SECONDS = 90.0

_TITLE_LINK_RE = re.compile(
    r'<a\s+href="(?P<href>/code/title(?P<title>\d+)\.php)">\s*Title\s+\d+\s*</a>'
    r"\s*-\s*(?P<heading>[^<]+)",
    re.I,
)
_CHAPTER_HREF_RE = re.compile(r"/code/t(?P<title>\d{1,2})c(?P<chapter>\d{3})\.php$", re.I)
_CHAPTER_ROW_RE = re.compile(
    r"\b(?:CHAPTER|ARTICLE)\s+(?P<chapter>\d+[A-Z]?)\s*-\s*(?P<heading>.+?)(?:\s+HTML\b|$)",
    re.I,
)
_SECTION_RE = re.compile(
    r"^SECTION\s+(?P<section>\d{1,2}-\d+[A-Z]?-\d+(?:\.\d+)?[A-Z]?)\.\s*(?P<heading>.*)$",
    re.I,
)
_ARTICLE_RE = re.compile(r"^ARTICLE\s+(?P<article>[0-9A-Z]+)$", re.I)
_REFERENCE_RE = re.compile(r"\b(?P<section>\d{1,2}-\d+[A-Z]?-\d+(?:\.\d+)?[A-Z]?)\b")


@dataclass(frozen=True)
class SouthCarolinaTitle:
    """Title metadata from the official Code of Laws index."""

    number: int
    heading: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-sc/statute/title-{self.number}"

    @property
    def legal_identifier(self) -> str:
        return f"S.C. Code Title {self.number}"


@dataclass(frozen=True)
class SouthCarolinaChapter:
    """Chapter metadata from an official title page."""

    title: int
    number: str
    heading: str | None
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-sc/statute/title-{self.title}/chapter-{self.number}"

    @property
    def legal_identifier(self) -> str:
        return f"S.C. Code Title {self.title}, Chapter {self.number}"


@dataclass(frozen=True)
class SouthCarolinaSection:
    """Parsed section from an official chapter page."""

    section: str
    heading: str | None
    body: str | None
    title: int
    chapter: str
    ordinal: int
    article: str | None = None
    article_heading: str | None = None
    references_to: tuple[str, ...] = ()
    source_history: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    status: str | None = None

    @property
    def citation_path(self) -> str:
        return f"us-sc/statute/{self.section}"

    @property
    def parent_citation_path(self) -> str:
        return f"us-sc/statute/title-{self.title}/chapter-{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        return f"S.C. Code Section {self.section}"


@dataclass(frozen=True)
class _SouthCarolinaSourcePage:
    relative_path: str
    source_url: str
    data: bytes


class _SouthCarolinaFetcher:
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
        self._session.headers.update({"User-Agent": SOUTH_CAROLINA_USER_AGENT})

    def fetch_master(self) -> _SouthCarolinaSourcePage:
        return self.fetch(
            f"{SOUTH_CAROLINA_SOURCE_FORMAT}/statmast.html",
            urljoin(self.base_url, "/code/statmast.php"),
        )

    def fetch_title(self, title: int) -> _SouthCarolinaSourcePage:
        return self.fetch(
            f"{SOUTH_CAROLINA_SOURCE_FORMAT}/title-{title}.html",
            urljoin(self.base_url, f"/code/title{title}.php"),
        )

    def fetch_chapter(self, title: int, chapter: str | int) -> _SouthCarolinaSourcePage:
        chapter_number = int(str(chapter))
        return self.fetch(
            f"{SOUTH_CAROLINA_SOURCE_FORMAT}/title-{title}/chapter-{chapter_number}.html",
            urljoin(self.base_url, f"/code/t{title:02d}c{chapter_number:03d}.php"),
        )

    def fetch(self, relative_path: str, source_url: str) -> _SouthCarolinaSourcePage:
        normalized = _normalize_relative_path(relative_path)
        if self.source_dir is not None:
            source_path = _source_dir_file(self.source_dir, normalized)
            if source_path is None:
                raise ValueError(f"South Carolina source file does not exist: {self.source_dir / normalized}")
            return _SouthCarolinaSourcePage(
                relative_path=normalized,
                source_url=source_url,
                data=source_path.read_bytes(),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            if cached_path.exists():
                return _SouthCarolinaSourcePage(
                    relative_path=normalized,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )

        data = self._download(source_url)
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            cached_path.write_bytes(data)
        return _SouthCarolinaSourcePage(relative_path=normalized, source_url=source_url, data=data)

    def _download(self, source_url: str) -> bytes:
        last_error: Exception | None = None
        for attempt in range(1, max(1, self.request_attempts) + 1):
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            try:
                response = self._session.get(source_url, timeout=self.timeout_seconds)
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


def extract_south_carolina_code(
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
    base_url: str = SOUTH_CAROLINA_BASE_URL,
    request_delay_seconds: float = SOUTH_CAROLINA_REQUEST_DELAY_SECONDS,
    request_attempts: int = SOUTH_CAROLINA_REQUEST_ATTEMPTS,
    timeout_seconds: float = SOUTH_CAROLINA_TIMEOUT_SECONDS,
) -> StateStatuteExtractReport:
    """Snapshot official South Carolina Code HTML and extract provisions."""
    jurisdiction = "us-sc"
    title_filter = _title_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter)
    run_id = _south_carolina_run_id(
        version,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _SouthCarolinaFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        request_delay_seconds=request_delay_seconds,
        request_attempts=request_attempts,
        timeout_seconds=timeout_seconds,
    )

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
    master_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        master_page.relative_path,
    )
    master_sha = store.write_bytes(master_path, master_page.data)
    source_paths.append(master_path)
    master_source_key = _state_source_key(jurisdiction, run_id, master_page.relative_path)
    titles = tuple(
        title
        for title in parse_south_carolina_master_index_html(master_page.data)
        if title_filter is None or title.number == title_filter
    )
    if not titles:
        raise ValueError(f"no South Carolina titles selected for filter: {only_title!r}")

    for title in titles:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        try:
            title_page = fetcher.fetch_title(title.number)
        except ValueError as exc:
            skipped_source_count += 1
            errors.append(f"title {title.number}: {exc}")
            continue

        title_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            title_page.relative_path,
        )
        title_sha = store.write_bytes(title_path, title_page.data)
        source_paths.append(title_path)
        title_source_key = _state_source_key(jurisdiction, run_id, title_page.relative_path)
        chapters = tuple(
            chapter
            for chapter in parse_south_carolina_title_html(title_page.data, title=title.number)
            if chapter_filter is None or chapter.number == chapter_filter
        )
        if not chapters:
            skipped_source_count += 1
            errors.append(f"title {title.number}: no chapters selected")
            continue

        if title.citation_path not in seen:
            seen.add(title.citation_path)
            title_count += 1
            _append_record(
                items,
                records,
                citation_path=title.citation_path,
                version=run_id,
                source_url=title_page.source_url,
                source_path=title_source_key,
                source_id=f"title-{title.number}",
                sha256=title_sha,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="title",
                body=None,
                heading=title.heading,
                legal_identifier=title.legal_identifier,
                parent_citation_path=None,
                level=0,
                ordinal=title.ordinal,
                identifiers={"south_carolina:title": str(title.number)},
                metadata={
                    "kind": "title",
                    "title": str(title.number),
                    "index_source_path": master_source_key,
                    "index_sha256": master_sha,
                },
            )

        for chapter in chapters:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            try:
                chapter_page = fetcher.fetch_chapter(title.number, chapter.number)
            except (requests.RequestException, ValueError) as exc:
                skipped_source_count += 1
                errors.append(f"title {title.number} chapter {chapter.number}: {exc}")
                continue

            chapter_path = store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                chapter_page.relative_path,
            )
            chapter_sha = store.write_bytes(chapter_path, chapter_page.data)
            source_paths.append(chapter_path)
            chapter_source_key = _state_source_key(jurisdiction, run_id, chapter_page.relative_path)
            sections = parse_south_carolina_chapter_html(
                chapter_page.data,
                title=title.number,
                chapter=chapter.number,
            )
            chapter_body_lines = _chapter_note_lines(chapter_page.data) if not sections else []
            chapter_body = "\n".join(chapter_body_lines).strip() or None
            chapter_status = _status(chapter.heading, chapter_body_lines)

            if chapter.citation_path not in seen:
                seen.add(chapter.citation_path)
                container_count += 1
                _append_record(
                    items,
                    records,
                    citation_path=chapter.citation_path,
                    version=run_id,
                    source_url=chapter_page.source_url,
                    source_path=chapter_source_key,
                    source_id=f"title-{title.number}-chapter-{chapter.number}",
                    sha256=chapter_sha,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    kind="chapter",
                    body=chapter_body,
                    heading=chapter.heading,
                    legal_identifier=chapter.legal_identifier,
                    parent_citation_path=title.citation_path,
                    level=1,
                    ordinal=chapter.ordinal,
                    identifiers={
                        "south_carolina:title": str(title.number),
                        "south_carolina:chapter": chapter.number,
                    },
                    metadata={
                        "kind": "chapter",
                        "title": str(title.number),
                        "chapter": chapter.number,
                        **({"status": chapter_status} if chapter_status else {}),
                    },
                )

            if not sections:
                if not chapter_body:
                    errors.append(f"title {title.number} chapter {chapter.number}: no sections parsed")
                continue
            for section in sections:
                if remaining_sections is not None and remaining_sections <= 0:
                    break
                if section.citation_path in seen:
                    continue
                seen.add(section.citation_path)
                section_count += 1
                _append_section_record(
                    items,
                    records,
                    section,
                    version=run_id,
                    source_url=chapter_page.source_url,
                    source_path=chapter_source_key,
                    sha256=chapter_sha,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
                if remaining_sections is not None:
                    remaining_sections -= 1

    if not records:
        raise ValueError("no South Carolina provisions extracted")

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


def parse_south_carolina_master_index_html(html: str | bytes) -> tuple[SouthCarolinaTitle, ...]:
    """Parse the official Code of Laws master title index."""
    text = _decode(html)
    titles: list[SouthCarolinaTitle] = []
    seen: set[int] = set()
    for match in _TITLE_LINK_RE.finditer(text):
        number = int(match.group("title"))
        if number in seen:
            continue
        seen.add(number)
        titles.append(
            SouthCarolinaTitle(
                number=number,
                heading=_title_case(_clean_text(html_module.unescape(match.group("heading")))),
                ordinal=len(titles) + 1,
            )
        )
    return tuple(titles)


def parse_south_carolina_title_html(
    html: str | bytes,
    *,
    title: int,
) -> tuple[SouthCarolinaChapter, ...]:
    """Parse one official title table of contents page."""
    soup = BeautifulSoup(html, "lxml")
    chapters: list[SouthCarolinaChapter] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        match = _CHAPTER_HREF_RE.search(href)
        if not match or int(match.group("title")) != int(title):
            continue
        chapter = str(int(match.group("chapter")))
        if chapter in seen:
            continue
        seen.add(chapter)
        row = anchor.find_parent("tr")
        text = _clean_text(row.get_text(" ", strip=True) if row is not None else anchor.parent.get_text(" ", strip=True))
        row_match = _CHAPTER_ROW_RE.search(text)
        heading = _title_case(row_match.group("heading")) if row_match else None
        chapters.append(
            SouthCarolinaChapter(
                title=int(title),
                number=chapter,
                heading=heading,
                ordinal=len(chapters) + 1,
            )
        )
    return tuple(chapters)


def parse_south_carolina_chapter_html(
    html: str | bytes,
    *,
    title: int,
    chapter: str | int,
) -> tuple[SouthCarolinaSection, ...]:
    """Parse sections from one official chapter page."""
    chapter_number = str(int(str(chapter)))
    soup = BeautifulSoup(html, "lxml")
    _replace_tables_with_text(soup)
    lines = [_clean_text(line) for line in soup.get_text("\n", strip=True).splitlines()]
    lines = [line for line in lines if line]

    sections: list[SouthCarolinaSection] = []
    current_section: str | None = None
    current_heading: str | None = None
    current_body: list[str] = []
    current_article: str | None = None
    current_article_heading: str | None = None
    pending_article: str | None = None
    pending_article_heading: str | None = None

    def finish_current() -> None:
        nonlocal current_section, current_heading, current_body, current_article, current_article_heading
        if current_section is None:
            return
        body_lines = [line for line in current_body if line]
        body = "\n".join(body_lines).strip() or None
        source_history = tuple(
            dict.fromkeys(line for line in body_lines if line.upper().startswith("HISTORY:"))
        )
        notes = tuple(
            dict.fromkeys(
                line
                for line in body_lines
                if line.upper().startswith(("EDITOR'S NOTE", "CODE COMMISSIONER", "EFFECT OF AMENDMENT"))
            )
        )
        references = _references_to(body_lines, self_section=current_section)
        sections.append(
            SouthCarolinaSection(
                section=current_section,
                heading=current_heading,
                body=body,
                title=int(title),
                chapter=chapter_number,
                ordinal=len(sections) + 1,
                article=current_article,
                article_heading=current_article_heading,
                references_to=references,
                source_history=source_history,
                notes=notes,
                status=_status(current_heading, body_lines),
            )
        )
        current_section = None
        current_heading = None
        current_body = []
        current_article = None
        current_article_heading = None

    i = 0
    while i < len(lines):
        line = lines[i]
        article_match = _ARTICLE_RE.match(line)
        if article_match:
            pending_article = article_match.group("article").upper()
            pending_article_heading = None
            if i + 1 < len(lines) and not _SECTION_RE.match(lines[i + 1]):
                pending_article_heading = _title_case(lines[i + 1])
                i += 2
            else:
                i += 1
            continue

        section_match = _SECTION_RE.match(line)
        if section_match:
            finish_current()
            current_section = section_match.group("section")
            inline_heading = _clean_heading(section_match.group("heading"))
            if inline_heading:
                current_heading = inline_heading
                i += 1
            else:
                current_heading = None
                j = i + 1
                while j < len(lines):
                    candidate = lines[j]
                    if candidate and not _SECTION_RE.match(candidate) and not _ARTICLE_RE.match(candidate):
                        current_heading = _clean_heading(candidate)
                        break
                    j += 1
                i = j + 1
            current_article = pending_article
            current_article_heading = pending_article_heading
            pending_article = None
            pending_article_heading = None
            continue

        if current_section is not None and not _chapter_noise(line):
            current_body.append(line)
        i += 1

    finish_current()
    return tuple(sections)


def _append_section_record(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    section: SouthCarolinaSection,
    *,
    version: str,
    source_url: str,
    source_path: str,
    sha256: str,
    source_as_of: str,
    expression_date: str,
) -> None:
    metadata = _section_metadata(section)
    _append_record(
        items,
        records,
        citation_path=section.citation_path,
        version=version,
        source_url=source_url,
        source_path=source_path,
        source_id=f"section-{section.section}",
        sha256=sha256,
        source_as_of=source_as_of,
        expression_date=expression_date,
        kind="section",
        body=section.body,
        heading=section.heading,
        legal_identifier=section.legal_identifier,
        parent_citation_path=section.parent_citation_path,
        level=2,
        ordinal=section.ordinal,
        identifiers={
            "south_carolina:title": str(section.title),
            "south_carolina:chapter": section.chapter,
            "south_carolina:section": section.section,
        },
        metadata=metadata,
    )


def _append_record(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    *,
    citation_path: str,
    version: str,
    source_url: str,
    source_path: str,
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
            source_format=SOUTH_CAROLINA_SOURCE_FORMAT,
            sha256=sha256,
            metadata=metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(citation_path),
            jurisdiction="us-sc",
            document_class=DocumentClass.STATUTE.value,
            citation_path=citation_path,
            body=body,
            heading=heading,
            citation_label=legal_identifier,
            version=version,
            source_url=source_url,
            source_path=source_path,
            source_id=source_id,
            source_format=SOUTH_CAROLINA_SOURCE_FORMAT,
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


def _section_metadata(section: SouthCarolinaSection) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": str(section.title),
        "chapter": section.chapter,
        "section": section.section,
    }
    if section.article:
        metadata["article"] = section.article
    if section.article_heading:
        metadata["article_heading"] = section.article_heading
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.notes:
        metadata["notes"] = list(section.notes)
    if section.status:
        metadata["status"] = section.status
    return metadata


def _references_to(body_lines: list[str], *, self_section: str) -> tuple[str, ...]:
    refs: list[str] = []
    for line in body_lines:
        for match in _REFERENCE_RE.finditer(line):
            section = match.group("section")
            if section == self_section:
                continue
            refs.append(f"us-sc/statute/{section}")
    return tuple(dict.fromkeys(refs))


def _replace_tables_with_text(soup: BeautifulSoup) -> None:
    for table in soup.find_all("table"):
        rows: list[str] = []
        for tr in table.find_all("tr"):
            cells = [
                _clean_text(cell.get_text(" ", strip=True))
                for cell in tr.find_all(["th", "td"])
                if _clean_text(cell.get_text(" ", strip=True))
            ]
            if cells:
                rows.append(" | ".join(cells))
        table.replace_with("\n" + "\n".join(rows) + "\n")


def _chapter_note_lines(html: str | bytes) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    _replace_tables_with_text(soup)
    lines = [_clean_text(line) for line in soup.get_text("\n", strip=True).splitlines()]
    lines = [line for line in lines if line]
    body: list[str] = []
    started = False
    for line in lines:
        if not started:
            if line.startswith("CHAPTER "):
                started = True
            continue
        if _footer_noise(line):
            break
        if _chapter_noise(line):
            continue
        body.append(line)
    return body


def _chapter_noise(line: str) -> bool:
    return bool(
        line.startswith("South Carolina Law")
        or line == "South Carolina Code of Laws"
        or line == "Code of Laws"
        or line.startswith("Title ")
        or line.startswith("CHAPTER ")
    )


def _footer_noise(line: str) -> bool:
    return bool(
        line.startswith("South Carolina Legislative Services Agency")
        or line in {"Disclaimer", "Policies", "Photo Credits", "Contact Us"}
        or line.startswith("Legislative Services Agency")
        or line.startswith("h t t p")
    )


def _status(heading: str | None, body_lines: list[str]) -> str | None:
    joined = " ".join(part for part in [heading or "", *body_lines[:3]] if part).lower()
    if "repealed" in joined:
        return "repealed"
    if "reserved" in joined:
        return "reserved"
    return None


def _clean_heading(value: str | None) -> str | None:
    if value is None:
        return None
    text = _clean_text(value).rstrip(".")
    return text or None


def _clean_text(value: str | None) -> str:
    if value is None:
        return ""
    text = html_module.unescape(value).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _title_case(value: str | None) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    if any(char.islower() for char in text):
        return text.removesuffix(".")
    small = {"A", "An", "And", "As", "At", "But", "By", "For", "In", "Nor", "Of", "On", "Or", "The", "To"}
    words = text.title().split()
    return " ".join(word.lower() if index and word in small else word for index, word in enumerate(words))


def _decode(html: str | bytes) -> str:
    if isinstance(html, bytes):
        return html.decode("utf-8-sig", errors="replace")
    return html


def _title_filter(value: str | int | None) -> int | None:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    if not match:
        raise ValueError(f"invalid South Carolina title filter: {value!r}")
    return int(match.group(0))


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    if not match:
        raise ValueError(f"invalid South Carolina chapter filter: {value!r}")
    return str(int(match.group(0)))


def _south_carolina_run_id(
    version: str,
    *,
    title_filter: int | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and chapter_filter is None and limit is None:
        return version
    parts = [version, "us-sc"]
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


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _base_url(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _normalize_relative_path(value: str) -> str:
    return "/".join(part for part in value.strip().split("/") if part)


def _source_dir_file(source_dir: Path, relative_path: str) -> Path | None:
    candidates = [source_dir / relative_path]
    if relative_path.startswith(f"{SOUTH_CAROLINA_SOURCE_FORMAT}/"):
        candidates.append(source_dir / relative_path.removeprefix(f"{SOUTH_CAROLINA_SOURCE_FORMAT}/"))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None
