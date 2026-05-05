"""Nevada Revised Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import time
from collections import defaultdict
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
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

NEVADA_NRS_BASE_URL = "https://www.leg.state.nv.us/NRS/"
NEVADA_NRS_SOURCE_FORMAT = "nevada-nrs-html"
NEVADA_NRS_DEFAULT_YEAR = 2025
NEVADA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_NRS_CHAPTER_HREF_RE = re.compile(r"^NRS-(?P<chapter>[0-9A-Z]+)\.html$", re.I)
_NRS_TITLE_RE = re.compile(r"^TITLE\s+(?P<number>\d+)\s*[\u2014-]\s*(?P<heading>.+)$", re.I)
_NRS_CHAPTER_LINE_RE = re.compile(
    r"^Chapter\s+(?P<chapter>\d+[A-Z]?)\s*(?:[\u2014-]\s*(?P<heading>.+))?$",
    re.I,
)
_NRS_ANCHOR_RE = re.compile(r"NRS(?P<chapter>\d+[A-Z]?)Sec(?P<section>[0-9A-Z.]+)", re.I)
_NRS_TEXT_RE = re.compile(r"\bNRS\s+(?P<cite>\d+[A-Z]?\.\d+[A-Z]?(?:\.\d+)?)\b", re.I)
_REVISION_RE = re.compile(r"\[Rev\.\s*(?P<revision>.+?)--(?P<year>\d{4})\]", re.S)
_EFFECTIVE_NOTE_RE = re.compile(r"\[(?P<note>Effective[^\]]+)\]\s*$", re.I)


@dataclass(frozen=True)
class NevadaNrsTitle:
    """Title entry parsed from the official NRS table of titles and chapters."""

    number: str
    heading: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"title-{self.number}"

    @property
    def citation_path(self) -> str:
        return f"us-nv/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"NRS Title {self.number}"


@dataclass(frozen=True)
class NevadaNrsChapter:
    """Chapter entry parsed from the official NRS table of titles and chapters."""

    chapter: str
    href: str
    heading: str | None
    ordinal: int
    title_number: str | None = None
    title_heading: str | None = None
    title_ordinal: int | None = None

    @property
    def citation_path(self) -> str:
        return f"us-nv/statute/{self.chapter}"

    @property
    def legal_identifier(self) -> str:
        if self.chapter == "0":
            return "NRS Preliminary Chapter"
        return f"NRS Chapter {self.chapter}"

    @property
    def parent_citation_path(self) -> str | None:
        if self.title_number is None:
            return None
        return f"us-nv/statute/title-{self.title_number}"


@dataclass(frozen=True)
class NevadaNrsIndex:
    """Parsed official NRS table of titles and chapters."""

    titles: tuple[NevadaNrsTitle, ...]
    chapters: tuple[NevadaNrsChapter, ...]


@dataclass(frozen=True)
class NevadaNrsSection:
    """Section text parsed from one official NRS chapter page."""

    section: str
    source_id: str
    heading: str | None
    body: str | None
    ordinal: int
    references_to: tuple[str, ...]
    source_history: tuple[str, ...]
    effective_note: str | None = None
    variant: str | None = None

    @property
    def chapter(self) -> str:
        return self.section.split(".", 1)[0]

    @property
    def citation_path(self) -> str:
        return f"us-nv/statute/{self.source_id}"

    @property
    def canonical_citation_path(self) -> str:
        return f"us-nv/statute/{self.section}"

    @property
    def legal_identifier(self) -> str:
        return f"NRS {self.section}"


@dataclass(frozen=True)
class NevadaNrsChapterParse:
    """Parsed content from one official NRS chapter page."""

    chapter: str | None
    heading: str | None
    revision: str | None
    source_year: int | None
    sections: tuple[NevadaNrsSection, ...]


def extract_nevada_nrs(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    source_year: int = NEVADA_NRS_DEFAULT_YEAR,
    only_title: str | int | None = None,
    only_chapter: str | int | None = None,
    limit: int | None = None,
    workers: int = 8,
    download_dir: str | Path | None = None,
    base_url: str = NEVADA_NRS_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official NRS HTML and extract normalized provision records."""
    jurisdiction = "us-nv"
    title_filter = _title_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter)
    run_id = _nevada_run_id(
        version,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
        limit=limit,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    source_root = Path(source_dir) if source_dir is not None else None
    cache_root = Path(download_dir) if download_dir is not None else None

    index_bytes = _nevada_index_bytes(source_root, cache_root, base_url)
    index_relative = f"{NEVADA_NRS_SOURCE_FORMAT}/index.html"
    index_artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        index_relative,
    )
    index_sha256 = store.write_bytes(index_artifact_path, index_bytes)
    index_source_key = _state_source_key(jurisdiction, run_id, index_relative)

    parsed_index = parse_nevada_nrs_index(index_bytes)
    chapters = _selected_chapters(
        parsed_index.chapters,
        title_filter=title_filter,
        chapter_filter=chapter_filter,
    )
    if not chapters:
        raise ValueError(f"no Nevada NRS chapters selected for filters: {only_title!r}, {only_chapter!r}")

    title_by_number = {title.number: title for title in parsed_index.titles}
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = [index_artifact_path]
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    errors: list[str] = []
    remaining_sections = limit

    for chapter, source_url, data, error in _iter_nevada_chapter_sources(
        chapters,
        source_dir=source_root,
        download_dir=cache_root,
        base_url=base_url,
        workers=workers,
        sequential=limit is not None,
    ):
        if remaining_sections is not None and remaining_sections <= 0:
            break
        if error is not None:
            skipped_source_count += 1
            errors.append(error)
            continue

        if chapter.title_number is not None and chapter.title_number not in title_by_number:
            errors.append(f"{chapter.href}: missing title metadata for title {chapter.title_number}")
        title = title_by_number.get(chapter.title_number or "")
        if title is not None and title.citation_path not in seen:
            seen.add(title.citation_path)
            title_count += 1
            container_count += 1
            _append_inventory_and_record(
                items,
                records,
                citation_path=title.citation_path,
                version=run_id,
                source_url=base_url,
                source_path=index_source_key,
                source_id=title.source_id,
                sha256=index_sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="title",
                heading=title.heading,
                legal_identifier=title.legal_identifier,
                level=0,
                ordinal=title.ordinal,
                identifiers={"nevada:title": title.number},
                metadata={
                    "kind": "title",
                    "title": title.number,
                    "source_year": source_year,
                },
            )

        artifact_relative = f"{NEVADA_NRS_SOURCE_FORMAT}/{chapter.href}"
        artifact_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            artifact_relative,
        )
        sha256 = store.write_bytes(artifact_path, data)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, artifact_relative)
        parsed_chapter = parse_nevada_chapter_html(data, chapter=chapter.chapter)
        chapter_heading = parsed_chapter.heading or chapter.heading
        chapter_source_year = parsed_chapter.source_year or source_year

        if chapter.citation_path not in seen:
            seen.add(chapter.citation_path)
            container_count += 1
            _append_inventory_and_record(
                items,
                records,
                citation_path=chapter.citation_path,
                version=run_id,
                source_url=source_url,
                source_path=source_key,
                source_id=chapter.chapter,
                sha256=sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="chapter",
                heading=chapter_heading,
                legal_identifier=chapter.legal_identifier,
                parent_citation_path=chapter.parent_citation_path,
                level=1 if chapter.parent_citation_path else 0,
                ordinal=chapter.ordinal,
                identifiers=_chapter_identifiers(chapter),
                metadata={
                    "kind": "chapter",
                    "chapter": chapter.chapter,
                    "title": chapter.title_number,
                    "revision": parsed_chapter.revision,
                    "source_year": chapter_source_year,
                    "parent_citation_path": chapter.parent_citation_path,
                },
            )

        for section in parsed_chapter.sections:
            if remaining_sections is not None and remaining_sections <= 0:
                break
            if section.citation_path in seen:
                continue
            seen.add(section.citation_path)
            section_count += 1
            _append_inventory_and_record(
                items,
                records,
                citation_path=section.citation_path,
                version=run_id,
                source_url=source_url,
                source_path=source_key,
                source_id=section.source_id,
                sha256=sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="section",
                heading=section.heading,
                body=section.body,
                legal_identifier=section.legal_identifier,
                parent_citation_path=chapter.citation_path,
                level=2 if chapter.parent_citation_path else 1,
                ordinal=section.ordinal,
                identifiers=_section_identifiers(section),
                metadata=_section_metadata(
                    section,
                    chapter=chapter,
                    source_year=chapter_source_year,
                    revision=parsed_chapter.revision,
                ),
            )
            if remaining_sections is not None:
                remaining_sections -= 1

    if not items:
        raise ValueError("no Nevada NRS provisions extracted")

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


def parse_nevada_nrs_index(html: str | bytes) -> NevadaNrsIndex:
    """Parse the official NRS table of titles and chapters."""
    soup = BeautifulSoup(html, "lxml")
    titles: list[NevadaNrsTitle] = []
    chapters: list[NevadaNrsChapter] = []
    title_ordinal = 0
    chapter_ordinal = 0

    for heading in soup.find_all("h3"):
        if not isinstance(heading, Tag):
            continue
        text = _clean_text(heading.get_text(" ", strip=True))
        link = heading.find("a", href=True)
        if isinstance(link, Tag):
            href = str(link.get("href") or "")
            if href == "NRS-000.html":
                chapters.append(
                    NevadaNrsChapter(
                        chapter="0",
                        href=href,
                        heading="PRELIMINARY CHAPTER",
                        ordinal=chapter_ordinal,
                    )
                )
                chapter_ordinal += 1
            continue

        title_match = _NRS_TITLE_RE.match(text)
        if not title_match:
            continue
        title_number = str(int(title_match.group("number")))
        title_heading = _clean_text(title_match.group("heading")).title()
        title = NevadaNrsTitle(
            number=title_number,
            heading=title_heading,
            ordinal=title_ordinal,
        )
        titles.append(title)
        title_ordinal += 1

        card = heading.find_parent("div", class_="card")
        if not isinstance(card, Tag):
            continue
        for list_item in card.find_all("li"):
            if not isinstance(list_item, Tag):
                continue
            chapter_link = list_item.find("a", href=True)
            if not isinstance(chapter_link, Tag):
                continue
            href = str(chapter_link.get("href") or "")
            chapter = _chapter_from_href(href)
            if chapter is None:
                continue
            chapter_line = _clean_text(list_item.get_text(" ", strip=True))
            chapter_match = _NRS_CHAPTER_LINE_RE.match(chapter_line)
            heading_text = (
                _clean_text(chapter_match.group("heading")).title()
                if chapter_match and chapter_match.group("heading")
                else None
            )
            chapters.append(
                NevadaNrsChapter(
                    chapter=chapter,
                    href=href,
                    heading=heading_text,
                    ordinal=chapter_ordinal,
                    title_number=title.number,
                    title_heading=title.heading,
                    title_ordinal=title.ordinal,
                )
            )
            chapter_ordinal += 1

    return NevadaNrsIndex(titles=tuple(titles), chapters=tuple(chapters))


def parse_nevada_chapter_html(
    html: str | bytes,
    *,
    chapter: str | None = None,
) -> NevadaNrsChapterParse:
    """Parse one official NRS chapter HTML page."""
    soup = BeautifulSoup(html, "lxml")
    revision, source_year = _revision_metadata(soup)
    chapter_heading = _chapter_heading(soup)
    parsed_chapter = chapter or _chapter_from_heading(chapter_heading)
    sections: list[NevadaNrsSection] = []
    occurrence_by_section: dict[str, int] = defaultdict(int)
    used_source_ids: set[str] = set()

    for paragraph in soup.find_all("p"):
        if not isinstance(paragraph, Tag):
            continue
        section_span = paragraph.find("span", class_="Section")
        if not isinstance(section_span, Tag):
            continue
        section_number = _section_number(paragraph)
        if not section_number:
            continue
        heading, effective_note = _section_heading(paragraph)
        body_lines, history, references = _section_content(paragraph, section_number)
        occurrence_by_section[section_number] += 1
        occurrence = occurrence_by_section[section_number]
        variant = _variant_for_occurrence(effective_note, occurrence)
        source_id = _section_source_id(section_number, variant)
        if source_id in used_source_ids:
            variant = _disambiguated_variant(
                variant,
                occurrence=occurrence,
                section_number=section_number,
                used_source_ids=used_source_ids,
            )
            source_id = _section_source_id(section_number, variant)
        used_source_ids.add(source_id)
        self_path = f"us-nv/statute/{section_number}"
        references_to = tuple(ref for ref in references if ref != self_path)
        sections.append(
            NevadaNrsSection(
                section=section_number,
                source_id=source_id,
                heading=heading,
                body="\n".join(body_lines).strip() or None,
                ordinal=len(sections),
                references_to=tuple(dict.fromkeys(references_to)),
                source_history=history,
                effective_note=effective_note,
                variant=variant,
            )
        )

    return NevadaNrsChapterParse(
        chapter=parsed_chapter,
        heading=chapter_heading,
        revision=revision,
        source_year=source_year,
        sections=tuple(sections),
    )


def _iter_nevada_chapter_sources(
    chapters: Sequence[NevadaNrsChapter],
    *,
    source_dir: Path | None,
    download_dir: Path | None,
    base_url: str,
    workers: int,
    sequential: bool,
) -> Iterator[tuple[NevadaNrsChapter, str, bytes, str | None]]:
    if source_dir is not None:
        for chapter in chapters:
            try:
                yield chapter, urljoin(base_url, chapter.href), _read_nevada_source_file(
                    source_dir, chapter.href
                ), None
            except OSError as exc:
                yield chapter, urljoin(base_url, chapter.href), b"", f"{chapter.href}: {exc}"
        return

    refs = tuple((chapter, urljoin(base_url, chapter.href), download_dir) for chapter in chapters)
    if sequential or workers <= 1:
        for ref in refs:
            yield _fetch_remote_chapter(ref)
        return
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        yield from executor.map(_fetch_remote_chapter, refs)


def _fetch_remote_chapter(
    ref: tuple[NevadaNrsChapter, str, Path | None],
) -> tuple[NevadaNrsChapter, str, bytes, str | None]:  # pragma: no cover
    chapter, source_url, download_dir = ref
    try:
        return chapter, source_url, _download_nevada_source_bytes(
            source_url,
            cache_name=chapter.href,
            download_dir=download_dir,
        ), None
    except requests.RequestException as exc:
        return chapter, source_url, b"", f"{chapter.href}: {exc}"


def _nevada_index_bytes(
    source_dir: Path | None,
    download_dir: Path | None,
    base_url: str,
) -> bytes:
    if source_dir is not None:
        return _read_nevada_source_file(source_dir, "index.html")
    return _download_nevada_source_bytes(
        base_url,
        cache_name="index.html",
        download_dir=download_dir,
    )


def _read_nevada_source_file(source_dir: Path, relative_name: str) -> bytes:
    candidates = (
        source_dir / relative_name,
        source_dir / NEVADA_NRS_SOURCE_FORMAT / relative_name,
        source_dir / "NRS.html" if relative_name == "index.html" else source_dir / relative_name,
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate.read_bytes()
    raise FileNotFoundError(relative_name)


def _download_nevada_source_bytes(
    url: str,
    *,
    cache_name: str,
    download_dir: Path | None,
) -> bytes:  # pragma: no cover
    if download_dir is not None:
        download_dir.mkdir(parents=True, exist_ok=True)
        cached_path = download_dir / cache_name
        if cached_path.exists():
            return cached_path.read_bytes()
    response = _get_with_retries(url)
    data = response.content
    if download_dir is not None:
        _write_cache_bytes(cached_path, data)
    return data


def _get_with_retries(url: str, *, attempts: int = 3) -> requests.Response:  # pragma: no cover
    last_error: requests.RequestException | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": NEVADA_USER_AGENT},
                timeout=60,
            )
            if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
    assert last_error is not None
    raise last_error


def _write_cache_bytes(path: Path, data: bytes) -> None:  # pragma: no cover
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _selected_chapters(
    chapters: Sequence[NevadaNrsChapter],
    *,
    title_filter: str | None,
    chapter_filter: str | None,
) -> tuple[NevadaNrsChapter, ...]:
    selected: list[NevadaNrsChapter] = []
    for chapter in chapters:
        if title_filter is not None and chapter.title_number != title_filter:
            continue
        if chapter_filter is not None and chapter.chapter != chapter_filter:
            continue
        selected.append(chapter)
    return tuple(selected)


def _section_content(
    section_paragraph: Tag,
    section_number: str,
) -> tuple[list[str], tuple[str, ...], tuple[str, ...]]:
    body_lines: list[str] = []
    source_history: list[str] = []
    references: list[str] = []
    first_line = _section_start_body_text(section_paragraph)
    if first_line:
        body_lines.append(first_line)
    references.extend(_nevada_references(section_paragraph, self_section=section_number))
    references.extend(_nevada_text_references(first_line, self_section=section_number))

    for sibling in section_paragraph.find_next_siblings("p"):
        if not isinstance(sibling, Tag):
            continue
        if sibling.find("span", class_="Section"):
            break
        text = _clean_text(sibling.get_text(" ", strip=True))
        if not text:
            continue
        references.extend(_nevada_references(sibling, self_section=section_number))
        references.extend(_nevada_text_references(text, self_section=section_number))
        classes = {str(item) for item in sibling.get("class") or ()}
        if "SourceNote" in classes:
            source_history.append(text)
        elif "SectBody" in classes:
            body_lines.append(text)

    return (
        body_lines,
        tuple(dict.fromkeys(source_history)),
        tuple(dict.fromkeys(references)),
    )


def _section_number(paragraph: Tag) -> str:
    parts: list[str] = []
    for span in paragraph.find_all("span", class_="Section"):
        if isinstance(span, Tag):
            parts.append(_clean_text(span.get_text(" ", strip=True)))
    return "".join(parts).replace(" ", "")


def _section_start_body_text(paragraph: Tag) -> str:
    clone_soup = BeautifulSoup(str(paragraph), "lxml")
    clone = clone_soup.find("p")
    if not isinstance(clone, Tag):
        return ""
    for span in list(clone.find_all("span")):
        if not isinstance(span, Tag):
            continue
        if span.attrs is None:
            continue
        classes = {str(item) for item in span.get("class") or ()}
        if classes.intersection({"Empty", "Section", "Leadline"}):
            span.decompose()
    return _clean_text(clone.get_text(" ", strip=True))


def _section_heading(paragraph: Tag) -> tuple[str | None, str | None]:
    leadline = paragraph.find("span", class_="Leadline")
    if not isinstance(leadline, Tag):
        return None, None
    text = _clean_text(leadline.get_text(" ", strip=True))
    if not text:
        return None, None
    effective_note: str | None = None
    note_match = _EFFECTIVE_NOTE_RE.search(text)
    if note_match:
        effective_note = _clean_text(note_match.group("note"))
        text = _clean_text(text[: note_match.start()])
    return text.rstrip(".") or None, effective_note


def _nevada_references(root: Tag, *, self_section: str) -> tuple[str, ...]:
    refs: list[str] = []
    self_path = f"us-nv/statute/{self_section}"
    for link in root.find_all("a", href=True):
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "")
        ref = _reference_from_href(href)
        if ref is not None and ref != self_path:
            refs.append(ref)
    return tuple(dict.fromkeys(refs))


def _reference_from_href(href: str) -> str | None:
    match = _NRS_ANCHOR_RE.search(href)
    if not match:
        return None
    chapter = _normalize_chapter_token(match.group("chapter"))
    section = _clean_text(match.group("section"))
    return f"us-nv/statute/{chapter}.{section}"


def _nevada_text_references(text: str, *, self_section: str) -> tuple[str, ...]:
    self_path = f"us-nv/statute/{self_section}"
    refs: list[str] = []
    for match in _NRS_TEXT_RE.finditer(text):
        ref = f"us-nv/statute/{match.group('cite')}"
        if ref != self_path:
            refs.append(ref)
    return tuple(dict.fromkeys(refs))


def _append_inventory_and_record(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    *,
    citation_path: str,
    version: str,
    source_url: str | None,
    source_path: str,
    source_id: str,
    sha256: str,
    source_as_of: str,
    expression_date: str,
    kind: str,
    heading: str | None,
    legal_identifier: str,
    level: int,
    ordinal: int | None,
    identifiers: dict[str, str],
    metadata: dict[str, Any],
    body: str | None = None,
    parent_citation_path: str | None = None,
) -> None:
    clean_metadata = {key: value for key, value in metadata.items() if value is not None}
    if parent_citation_path is not None:
        clean_metadata["parent_citation_path"] = parent_citation_path
    items.append(
        SourceInventoryItem(
            citation_path=citation_path,
            source_url=source_url,
            source_path=source_path,
            source_format=NEVADA_NRS_SOURCE_FORMAT,
            sha256=sha256,
            metadata=clean_metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(citation_path),
            jurisdiction="us-nv",
            document_class=DocumentClass.STATUTE.value,
            citation_path=citation_path,
            body=body,
            heading=heading,
            citation_label=legal_identifier,
            version=version,
            source_url=source_url,
            source_path=source_path,
            source_id=source_id,
            source_format=NEVADA_NRS_SOURCE_FORMAT,
            source_as_of=source_as_of,
            expression_date=expression_date,
            parent_citation_path=parent_citation_path,
            parent_id=(
                deterministic_provision_id(parent_citation_path)
                if parent_citation_path is not None
                else None
            ),
            level=level,
            ordinal=ordinal,
            kind=kind,
            legal_identifier=legal_identifier,
            identifiers=identifiers,
            metadata=clean_metadata,
        )
    )


def _chapter_identifiers(chapter: NevadaNrsChapter) -> dict[str, str]:
    identifiers = {"nevada:chapter": chapter.chapter}
    if chapter.title_number is not None:
        identifiers["nevada:title"] = chapter.title_number
    return identifiers


def _section_identifiers(section: NevadaNrsSection) -> dict[str, str]:
    identifiers = {
        "nevada:chapter": section.chapter,
        "nevada:section": section.section,
    }
    if section.variant is not None:
        identifiers["nevada:variant"] = section.variant
    return identifiers


def _section_metadata(
    section: NevadaNrsSection,
    *,
    chapter: NevadaNrsChapter,
    source_year: int,
    revision: str | None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "chapter": chapter.chapter,
        "title": chapter.title_number,
        "section": section.section,
        "source_year": source_year,
        "revision": revision,
    }
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.effective_note:
        metadata["effective_note"] = section.effective_note
        metadata["status"] = _effective_status(section.effective_note)
    if section.variant:
        metadata["variant"] = section.variant
        metadata["canonical_citation_path"] = section.canonical_citation_path
    return metadata


def _effective_status(note: str) -> str:
    normalized = note.lower()
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


def _revision_metadata(soup: BeautifulSoup) -> tuple[str | None, int | None]:
    text = _clean_text(soup.get_text(" ", strip=True))
    match = _REVISION_RE.search(text)
    if not match:
        return None, None
    return _clean_text(f"Rev. {match.group('revision')}--{match.group('year')}"), int(
        match.group("year")
    )


def _chapter_heading(soup: BeautifulSoup) -> str | None:
    for paragraph in soup.find_all("p"):
        if not isinstance(paragraph, Tag):
            continue
        text = _clean_text(paragraph.get_text(" ", strip=True))
        if text.startswith("CHAPTER ") or text == "PRELIMINARY CHAPTER":
            return text.title()
    title = soup.find("title")
    if isinstance(title, Tag):
        text = _clean_text(title.get_text(" ", strip=True))
        if text.startswith("NRS: "):
            return text.removeprefix("NRS: ").title()
    return None


def _chapter_from_heading(heading: str | None) -> str | None:
    if heading is None:
        return None
    if heading.upper() == "PRELIMINARY CHAPTER":
        return "0"
    match = re.match(r"Chapter\s+(?P<chapter>\d+[A-Z]?)\b", heading, re.I)
    if not match:
        return None
    return _normalize_chapter_token(match.group("chapter"))


def _chapter_from_href(href: str) -> str | None:
    match = _NRS_CHAPTER_HREF_RE.match(Path(href).name)
    if not match:
        return None
    return _normalize_chapter_token(match.group("chapter"))


def _normalize_chapter_token(value: str) -> str:
    match = re.fullmatch(r"0*(?P<number>\d+)(?P<suffix>[A-Z]*)", value.upper())
    if not match:
        return value.upper()
    return f"{int(match.group('number'))}{match.group('suffix')}"


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    if not match:
        raise ValueError(f"invalid Nevada title filter: {value!r}")
    return str(int(match.group(0)))


def _chapter_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).upper().replace("CHAPTER", "").strip()
    match = re.search(r"\d+[A-Z]?", cleaned)
    if not match:
        raise ValueError(f"invalid Nevada chapter filter: {value!r}")
    return _normalize_chapter_token(match.group(0))


def _nevada_run_id(
    version: str,
    *,
    title_filter: str | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and chapter_filter is None and limit is None:
        return version
    parts = [version, "us-nv"]
    if title_filter is not None:
        parts.append(f"title-{title_filter}")
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _clean_text(value: str | None) -> str:
    text = (value or "").replace("\xa0", " ").replace("\u2002", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)
