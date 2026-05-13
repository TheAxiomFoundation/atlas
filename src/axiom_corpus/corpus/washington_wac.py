"""Washington Administrative Code source adapter."""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TextIO
from urllib.parse import parse_qs, quote, unquote_plus, urlparse

import requests
from bs4 import BeautifulSoup, Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

WASHINGTON_WAC_BASE_URL = "https://app.leg.wa.gov/WAC/default.aspx"
WASHINGTON_WAC_SOURCE_FORMAT = "washington-wac-html"
WASHINGTON_WAC_USER_AGENT = "axiom-corpus/0.1 (max@axiom-foundation.org)"


@dataclass(frozen=True)
class WashingtonWacExtractReport:
    jurisdiction: str
    document_class: str
    version: str
    title_count: int
    chapter_count: int
    section_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]
    skipped_source_count: int = 0
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class _WacTitle:
    num: str
    heading: str | None
    href: str
    ordinal: int

    @property
    def source_url(self) -> str:
        return _wac_cite_url(self.num)

    @property
    def citation_path(self) -> str:
        return f"us-wa/regulation/{_path_token(self.num)}"


@dataclass(frozen=True)
class _WacChapter:
    title: _WacTitle
    num: str
    heading: str | None
    href: str
    ordinal: int

    @property
    def source_url(self) -> str:
        return _wac_cite_url(self.num)

    @property
    def full_source_url(self) -> str:
        return f"{self.source_url}&full=true"

    @property
    def citation_path(self) -> str:
        return f"{self.title.citation_path}/{_path_token(self.num)}"


@dataclass(frozen=True)
class _WacSection:
    chapter: _WacChapter
    section: str
    heading: str | None
    body: str | None
    status: str
    source_history: tuple[str, ...]
    notes: tuple[str, ...]
    references_to: tuple[str, ...]
    ordinal: int

    @property
    def source_url(self) -> str:
        return _wac_cite_url(self.section)

    @property
    def source_id(self) -> str:
        return f"section-{self.section}"

    @property
    def citation_path(self) -> str:
        return f"{self.chapter.citation_path}/{_path_token(self.section)}"


def washington_wac_run_id(
    version: str,
    *,
    only_title: str | None = None,
    only_chapter: str | None = None,
    limit: int | None = None,
) -> str:
    parts = [version]
    if only_chapter:
        parts.append(_path_token(only_chapter))
    elif only_title:
        parts.append(_path_token(only_title))
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def extract_washington_wac(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    only_chapter: str | None = None,
    limit: int | None = None,
    workers: int = 4,
    download_dir: str | Path | None = None,
    progress_stream: TextIO | None = None,
) -> WashingtonWacExtractReport:
    """Snapshot official Washington Administrative Code HTML and extract provisions."""

    jurisdiction = "us-wa"
    document_class = DocumentClass.REGULATION.value
    only_title_cite = _wac_cite_filter(only_title, strict=False) if only_title else None
    only_chapter_cite = (
        _wac_cite_filter(only_chapter, strict=False) if only_chapter else None
    )
    if only_title_cite and _wac_cite_depth(only_title_cite) != 1:
        only_chapter_cite = only_title_cite
        only_title_cite = _wac_title_from_cite(only_chapter_cite)
    if only_chapter_cite and _wac_cite_depth(only_chapter_cite) != 2:
        raise ValueError(f"only_chapter must be a WAC chapter citation: {only_chapter!r}")
    only_title_num = (
        only_title_cite
        or (_wac_title_from_cite(only_chapter_cite) if only_chapter_cite else None)
    )
    run_id = washington_wac_run_id(
        version,
        only_title=only_title_num,
        only_chapter=only_chapter_cite,
        limit=limit,
    )
    source_root = Path(source_dir) if source_dir is not None else None
    download_root = Path(download_dir) if download_dir is not None and source_root is None else None
    session = _wac_session()

    index_relative = "washington-wac-html/index.html"
    index_bytes = _load_wac_html(
        session,
        source_root,
        download_root,
        relative_name=index_relative,
        url=WASHINGTON_WAC_BASE_URL,
    )
    index_path = store.source_path(jurisdiction, document_class, run_id, index_relative)
    index_sha = store.write_bytes(index_path, index_bytes)
    source_paths: list[Path] = [index_path]

    detected_source_as_of = _wac_source_as_of(index_bytes)
    source_as_of_text = source_as_of or detected_source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    titles = _parse_wac_titles(index_bytes)
    if only_title_num is not None:
        titles = tuple(title for title in titles if title.num == only_title_num)
    if not titles:
        raise ValueError(f"no Washington WAC titles selected for filter: {only_title!r}")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    chapters: list[_WacChapter] = []
    remaining = limit
    title_count = 0
    chapter_count = 0
    section_count = 0
    skipped_source_count = 0
    seen_citation_paths: set[str] = set()

    root_path = "us-wa/regulation"
    root_source_key = _wac_source_key(jurisdiction, run_id, index_relative)
    items.append(
        SourceInventoryItem(
            citation_path=root_path,
            source_url=WASHINGTON_WAC_BASE_URL,
            source_path=root_source_key,
            source_format=WASHINGTON_WAC_SOURCE_FORMAT,
            sha256=index_sha,
            metadata={
                "kind": "collection",
                "source_as_of": source_as_of_text,
                "title_count": len(titles),
            },
        )
    )
    records.append(
        _wac_root_provision(
            version=run_id,
            source_path=root_source_key,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            title_count=len(titles),
        )
    )
    seen_citation_paths.add(root_path)

    for title in titles:
        if remaining is not None and remaining <= 0:
            break
        title_relative = _wac_title_relative(title)
        try:
            title_bytes = _load_wac_html(
                session,
                source_root,
                download_root,
                relative_name=title_relative,
                url=title.source_url,
            )
        except (OSError, requests.RequestException) as exc:
            errors.append(f"title {title.num}: {exc}")
            continue
        title_path = store.source_path(jurisdiction, document_class, run_id, title_relative)
        title_sha = store.write_bytes(title_path, title_bytes)
        source_paths.append(title_path)
        title_source_key = _wac_source_key(jurisdiction, run_id, title_relative)
        title_with_heading = _WacTitle(
            num=title.num,
            heading=_wac_title_heading(title_bytes) or title.heading,
            href=title.href,
            ordinal=title.ordinal,
        )
        raw_title_chapters = _parse_wac_chapters(title_bytes, title=title_with_heading)
        title_body = None if raw_title_chapters else _wac_container_body(title_bytes)
        title_metadata: dict[str, object] = {
            "kind": "title",
            "title": title_with_heading.num,
            "heading": title_with_heading.heading,
        }
        if title_body:
            title_metadata["status"] = _wac_container_status(title_body)
            title_metadata["has_active_chapters"] = False
        if title_with_heading.citation_path not in seen_citation_paths:
            seen_citation_paths.add(title_with_heading.citation_path)
            items.append(
                _wac_container_inventory_item(
                    title_with_heading.citation_path,
                    source_url=title_with_heading.source_url,
                    source_path=title_source_key,
                    sha256=title_sha,
                    metadata=title_metadata,
                )
            )
            records.append(
                _wac_title_provision(
                    title_with_heading,
                    version=run_id,
                    source_path=title_source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    body=title_body,
                    metadata=title_metadata,
                )
            )
            title_count += 1
            if remaining is not None:
                remaining -= 1

        title_chapters = raw_title_chapters
        if only_chapter_cite is not None:
            title_chapters = tuple(
                chapter for chapter in title_chapters if chapter.num == only_chapter_cite
            )
        if not title_chapters:
            if only_chapter_cite is not None:
                errors.append(f"title {title.num}: chapter {only_chapter_cite} not found")
            elif not title_body:
                errors.append(f"title {title.num}: no chapters parsed")
        chapters.extend(title_chapters)

    chapters_to_fetch = tuple(chapters)
    if remaining is not None:
        chapters_to_fetch = chapters_to_fetch[: max(remaining, 0)]
    for chapter, chapter_bytes, error in _iter_wac_chapter_sources(
        source_root,
        download_root,
        chapters_to_fetch,
        workers=workers,
    ):
        if remaining is not None and remaining <= 0:
            break
        if error is not None or chapter_bytes is None:
            errors.append(f"chapter {chapter.num}: {error or 'unknown source error'}")
            continue
        chapter_relative = _wac_chapter_relative(chapter)
        chapter_path = store.source_path(jurisdiction, document_class, run_id, chapter_relative)
        chapter_sha = store.write_bytes(chapter_path, chapter_bytes)
        source_paths.append(chapter_path)
        chapter_source_key = _wac_source_key(jurisdiction, run_id, chapter_relative)
        chapter_with_heading = _WacChapter(
            title=chapter.title,
            num=chapter.num,
            heading=_wac_chapter_heading(chapter_bytes) or chapter.heading,
            href=chapter.href,
            ordinal=chapter.ordinal,
        )
        sections = _parse_wac_chapter_sections(chapter_bytes, chapter=chapter_with_heading)
        chapter_body = None if sections else _wac_container_body(chapter_bytes)
        chapter_metadata: dict[str, object] = {
            "kind": "chapter",
            "title": chapter_with_heading.title.num,
            "chapter": chapter_with_heading.num,
            "heading": chapter_with_heading.heading,
        }
        if chapter_body:
            chapter_metadata["status"] = _wac_container_status(chapter_body)
            chapter_metadata["has_active_sections"] = False
        if chapter_with_heading.citation_path not in seen_citation_paths:
            seen_citation_paths.add(chapter_with_heading.citation_path)
            items.append(
                _wac_container_inventory_item(
                    chapter_with_heading.citation_path,
                    source_url=chapter_with_heading.full_source_url,
                    source_path=chapter_source_key,
                    sha256=chapter_sha,
                    metadata=chapter_metadata,
                )
            )
            records.append(
                _wac_chapter_provision(
                    chapter_with_heading,
                    version=run_id,
                    source_path=chapter_source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    body=chapter_body,
                    metadata=chapter_metadata,
                )
            )
            chapter_count += 1
            if remaining is not None:
                remaining -= 1

        if not sections and not chapter_body:
            skipped_source_count += 1
            errors.append(f"chapter {chapter.num}: no sections parsed")
        for section in sections:
            if remaining is not None and remaining <= 0:
                break
            if section.citation_path in seen_citation_paths:
                continue
            seen_citation_paths.add(section.citation_path)
            items.append(
                SourceInventoryItem(
                    citation_path=section.citation_path,
                    source_url=section.source_url,
                    source_path=chapter_source_key,
                    source_format=WASHINGTON_WAC_SOURCE_FORMAT,
                    sha256=chapter_sha,
                    metadata={
                        "kind": "section",
                        "title": section.chapter.title.num,
                        "chapter": section.chapter.num,
                        "section": section.section,
                        "heading": section.heading,
                        "status": section.status,
                        "source_history": list(section.source_history),
                        "notes": list(section.notes),
                        "parent_citation_path": section.chapter.citation_path,
                        "references_to": list(section.references_to),
                        "source_id": section.source_id,
                    },
                )
            )
            records.append(
                _wac_section_provision(
                    section,
                    version=run_id,
                    source_path=chapter_source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
            )
            section_count += 1
            if remaining is not None:
                remaining -= 1
        if progress_stream is not None and chapter_count and chapter_count % 100 == 0:
            print(
                f"processed Washington WAC chapter {chapter_count} "
                f"({section_count} sections)",
                file=progress_stream,
                flush=True,
            )

    if not items:
        raise ValueError("no Washington WAC provisions extracted")

    inventory_path = store.inventory_path(jurisdiction, document_class, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path(jurisdiction, document_class, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=document_class,
        version=run_id,
    )
    coverage_path = store.coverage_path(jurisdiction, document_class, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return WashingtonWacExtractReport(
        jurisdiction=jurisdiction,
        document_class=document_class,
        version=run_id,
        title_count=title_count,
        chapter_count=chapter_count,
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


def _parse_wac_titles(data: bytes) -> tuple[_WacTitle, ...]:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    wrapper = soup.find(id="contentWrapper")
    titles: list[_WacTitle] = []
    seen: set[str] = set()
    for row in (wrapper or soup).find_all("tr"):
        if not isinstance(row, Tag):
            continue
        link = _first_wac_cite_link(row, depth=1)
        if link is None:
            continue
        cite = _wac_cite_from_href(str(link.get("href") or ""))
        if cite is None or cite in seen:
            continue
        link_text = _clean_text(link.get_text(" ", strip=True))
        if not link_text.lower().startswith("title"):
            continue
        titles.append(
            _WacTitle(
                num=cite,
                heading=_listing_heading(row, link_text),
                href=str(link.get("href") or ""),
                ordinal=_ordinal(cite) or len(titles),
            )
        )
        seen.add(cite)
    return tuple(sorted(titles, key=lambda title: title.ordinal))


def _parse_wac_chapters(data: bytes, *, title: _WacTitle) -> tuple[_WacChapter, ...]:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    wrapper = soup.find(id="contentWrapper")
    chapters: list[_WacChapter] = []
    seen: set[str] = set()
    for row in (wrapper or soup).find_all("tr"):
        if not isinstance(row, Tag):
            continue
        link = _first_wac_cite_link(row, depth=2)
        if link is None:
            continue
        cite = _wac_cite_from_href(str(link.get("href") or ""))
        if (
            cite is None
            or cite in seen
            or _wac_title_from_cite(cite) != title.num
        ):
            continue
        link_text = _clean_text(link.get_text(" ", strip=True))
        chapters.append(
            _WacChapter(
                title=title,
                num=cite,
                heading=_listing_heading(row, link_text),
                href=str(link.get("href") or ""),
                ordinal=_ordinal(cite) or len(chapters),
            )
        )
        seen.add(cite)
    return tuple(sorted(chapters, key=lambda chapter: chapter.ordinal))


def _first_wac_cite_link(root: Tag, *, depth: int) -> Tag | None:
    for link in root.find_all("a", href=True):
        if not isinstance(link, Tag):
            continue
        cite = _wac_cite_from_href(str(link.get("href") or ""))
        if cite is not None and _wac_cite_depth(cite) == depth:
            return link
    return None


def _parse_wac_chapter_sections(data: bytes, *, chapter: _WacChapter) -> tuple[_WacSection, ...]:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    sections: list[_WacSection] = []
    seen: set[str] = set()
    for ordinal, anchor in enumerate(
        soup.select("#ContentPlaceHolder1_pnlExpanded a[name]"),
    ):
        if not isinstance(anchor, Tag):
            continue
        section = _wac_cite_filter(str(anchor.get("name") or ""), strict=False)
        if (
            section is None
            or _wac_cite_depth(section) != 3
            or not _wac_section_in_chapter(section, chapter.num)
            or section in seen
        ):
            continue
        parsed = _parse_wac_section(anchor, chapter=chapter, ordinal=ordinal)
        if parsed is not None:
            sections.append(parsed)
            seen.add(section)
    return tuple(sections)


def _parse_wac_section(
    anchor: Tag,
    *,
    chapter: _WacChapter,
    ordinal: int,
) -> _WacSection | None:
    section = _wac_cite_filter(str(anchor.get("name") or ""), strict=False)
    root = anchor.find_parent("span")
    if section is None or not isinstance(root, Tag):
        return None
    divs = tuple(
        child
        for child in root.children
        if isinstance(child, Tag) and child.name.lower() == "div"
    )
    if len(divs) < 2:
        return None
    heading = _heading_text(divs[1])
    status = "repealed" if _is_repealed_heading(heading) else "active"
    return _WacSection(
        chapter=chapter,
        section=section,
        heading=heading,
        body=_section_body(divs),
        status=status,
        source_history=_source_history(divs),
        notes=_notes(divs),
        references_to=_references(root, self_path=_wac_citation_path(section)),
        ordinal=ordinal,
    )


def _iter_wac_chapter_sources(
    source_root: Path | None,
    download_root: Path | None,
    chapters: tuple[_WacChapter, ...],
    *,
    workers: int,
) -> Iterator[tuple[_WacChapter, bytes | None, str | None]]:
    if source_root is not None:
        for chapter in chapters:
            try:
                yield chapter, (source_root / _wac_chapter_relative(chapter)).read_bytes(), None
            except OSError as exc:
                yield chapter, None, str(exc)
        return

    worker_count = max(1, workers)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        yield from executor.map(
            lambda chapter: _load_wac_chapter_source(download_root, chapter),
            chapters,
        )


def _load_wac_chapter_source(
    download_root: Path | None,
    chapter: _WacChapter,
) -> tuple[_WacChapter, bytes | None, str | None]:
    try:
        session = _wac_session()
        data = _load_wac_html(
            session,
            None,
            download_root,
            relative_name=_wac_chapter_relative(chapter),
            url=chapter.full_source_url,
        )
        return chapter, data, None
    except (OSError, requests.RequestException) as exc:
        return chapter, None, str(exc)


def _load_wac_html(
    session: requests.Session,
    source_root: Path | None,
    download_root: Path | None,
    *,
    relative_name: str,
    url: str,
) -> bytes:
    if source_root is not None:
        return (source_root / relative_name).read_bytes()
    response = session.get(url, timeout=120)
    response.raise_for_status()
    data = bytes(response.content)
    if download_root is not None:
        path = download_root / relative_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return data


def _wac_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": WASHINGTON_WAC_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def _wac_source_as_of(data: bytes) -> str | None:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    updated = soup.find(id="ContentPlaceHolder1_pnlDefaultUpdated")
    text = _clean_text(updated.get_text(" ", strip=True)) if isinstance(updated, Tag) else ""
    try:
        return datetime.strptime(text, "%B %d, %Y").date().isoformat()
    except ValueError:
        return None


def _wac_title_heading(data: bytes) -> str | None:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    h1 = soup.find("h1")
    text = _clean_text(h1.get_text(" ", strip=True)) if isinstance(h1, Tag) else ""
    match = re.match(r"^Title\s+\S+\s+WAC\s*(?P<heading>.*)$", text, re.I)
    return _clean_text(match.group("heading")) if match else None


def _wac_chapter_heading(data: bytes) -> str | None:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    h1 = soup.find("h1")
    text = _clean_text(h1.get_text(" ", strip=True)) if isinstance(h1, Tag) else ""
    match = re.match(r"^Chapter\s+\S+\s+WAC\s*(?P<heading>.*)$", text, re.I)
    if match and _clean_text(match.group("heading")):
        return _clean_text(match.group("heading"))
    wrapper = soup.find(id="contentWrapper")
    h2 = wrapper.find("h2") if isinstance(wrapper, Tag) else None
    return _clean_text(h2.get_text(" ", strip=True)) if isinstance(h2, Tag) else None


def _listing_heading(row: Tag, link_text: str) -> str | None:
    cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
    text = " ".join(cells) if cells else row.get_text(" ", strip=True)
    text = _clean_text(text)
    if text.startswith(link_text):
        text = text[len(link_text) :]
    return _clean_text(text.lstrip("-:")).rstrip(".") or None


def _wac_container_body(data: bytes) -> str | None:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    wrapper = soup.find(id="contentWrapper") or soup.find(id="ContentPlaceHolder1_pnlExpanded")
    if not isinstance(wrapper, Tag):
        return None
    lines = [
        _clean_text(line)
        for line in wrapper.get_text("\n", strip=True).splitlines()
        if _clean_text(line)
    ]
    body = "\n".join(lines).strip()
    return body or None


def _wac_container_status(body: str) -> str:
    text = body.lower()
    if "omitted from this code" in text:
        return "omitted"
    if "no active sections" in text or "no longer in effect" in text:
        return "no_active_sections"
    if "disposition of sections formerly codified" in text:
        return "repealed"
    return "non_section_content"


def _wac_container_inventory_item(
    citation_path: str,
    *,
    source_url: str,
    source_path: str,
    sha256: str,
    metadata: dict[str, object],
) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=citation_path,
        source_url=source_url,
        source_path=source_path,
        source_format=WASHINGTON_WAC_SOURCE_FORMAT,
        sha256=sha256,
        metadata=metadata,
    )


def _wac_root_provision(
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    title_count: int,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id("us-wa/regulation"),
        jurisdiction="us-wa",
        document_class=DocumentClass.REGULATION.value,
        citation_path="us-wa/regulation",
        citation_label="Washington Administrative Code",
        heading="Washington Administrative Code",
        body=None,
        version=version,
        source_url=WASHINGTON_WAC_BASE_URL,
        source_path=source_path,
        source_format=WASHINGTON_WAC_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=None,
        parent_id=None,
        level=0,
        ordinal=0,
        kind="collection",
        legal_identifier="Washington Administrative Code",
        identifiers={"state:code": "WAC"},
        metadata={"title_count": title_count},
    )


def _wac_title_provision(
    title: _WacTitle,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    body: str | None = None,
    metadata: dict[str, object] | None = None,
) -> ProvisionRecord:
    legal_identifier = f"Title {title.num} WAC"
    provision_metadata: dict[str, object] = {"title": title.num}
    if metadata:
        provision_metadata.update(metadata)
    return ProvisionRecord(
        id=deterministic_provision_id(title.citation_path),
        jurisdiction="us-wa",
        document_class=DocumentClass.REGULATION.value,
        citation_path=title.citation_path,
        citation_label=legal_identifier,
        heading=title.heading,
        body=body,
        version=version,
        source_url=title.source_url,
        source_path=source_path,
        source_format=WASHINGTON_WAC_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path="us-wa/regulation",
        parent_id=deterministic_provision_id("us-wa/regulation"),
        level=1,
        ordinal=title.ordinal,
        kind="title",
        legal_identifier=legal_identifier,
        identifiers={"washington:wac_title": title.num},
        metadata=provision_metadata,
    )


def _wac_chapter_provision(
    chapter: _WacChapter,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    body: str | None = None,
    metadata: dict[str, object] | None = None,
) -> ProvisionRecord:
    legal_identifier = f"Chapter {chapter.num} WAC"
    provision_metadata: dict[str, object] = {
        "title": chapter.title.num,
        "chapter": chapter.num,
    }
    if metadata:
        provision_metadata.update(metadata)
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-wa",
        document_class=DocumentClass.REGULATION.value,
        citation_path=chapter.citation_path,
        citation_label=legal_identifier,
        heading=chapter.heading,
        body=body,
        version=version,
        source_url=chapter.full_source_url,
        source_path=source_path,
        source_format=WASHINGTON_WAC_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=chapter.title.citation_path,
        parent_id=deterministic_provision_id(chapter.title.citation_path),
        level=2,
        ordinal=chapter.ordinal,
        kind="chapter",
        legal_identifier=legal_identifier,
        identifiers={
            "washington:wac_title": chapter.title.num,
            "washington:wac_chapter": chapter.num,
        },
        metadata=provision_metadata,
    )


def _wac_section_provision(
    section: _WacSection,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-wa",
        document_class=DocumentClass.REGULATION.value,
        citation_path=section.citation_path,
        citation_label=f"WAC {section.section}",
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=section.source_url,
        source_path=source_path,
        source_id=section.source_id,
        source_format=WASHINGTON_WAC_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.chapter.citation_path,
        parent_id=deterministic_provision_id(section.chapter.citation_path),
        level=3,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=f"WAC {section.section}",
        identifiers={
            "washington:wac_title": section.chapter.title.num,
            "washington:wac_chapter": section.chapter.num,
            "washington:wac_section": section.section,
        },
        metadata={
            "title": section.chapter.title.num,
            "chapter": section.chapter.num,
            "section": section.section,
            "status": section.status,
            "source_history": list(section.source_history),
            "notes": list(section.notes),
            "references_to": list(section.references_to),
        },
    )


def _heading_text(tag: Tag) -> str | None:
    return _clean_text(tag.get_text(" ", strip=True)).rstrip(".") or None


def _section_body(divs: tuple[Tag, ...]) -> str | None:
    lines: list[str] = []
    for div in divs[2:]:
        if _is_source_history_div(div) or _is_notes_heading_div(div):
            break
        child_lines = [
            _clean_text(child.get_text(" ", strip=True))
            for child in div.find_all("div", recursive=False)
            if isinstance(child, Tag)
        ]
        if child_lines:
            lines.extend(line for line in child_lines if line)
            continue
        text = _clean_text(div.get_text(" ", strip=True))
        if text:
            lines.append(text)
    body = "\n".join(lines).strip()
    return body or None


def _source_history(divs: tuple[Tag, ...]) -> tuple[str, ...]:
    for div in divs:
        if not _is_source_history_div(div):
            continue
        text = _clean_text(div.get_text(" ", strip=True))
        return (text,) if text else ()
    return ()


def _notes(divs: tuple[Tag, ...]) -> tuple[str, ...]:
    notes: list[str] = []
    in_notes = False
    for div in divs:
        if _is_notes_heading_div(div):
            in_notes = True
            continue
        if not in_notes:
            continue
        text = _clean_text(div.get_text(" ", strip=True))
        if text:
            notes.append(text)
    return tuple(notes)


def _is_source_history_div(tag: Tag) -> bool:
    style = str(tag.get("style") or "").replace(" ", "").lower()
    return "margin-top:15pt" in style


def _is_notes_heading_div(tag: Tag) -> bool:
    text = _clean_text(tag.get_text(" ", strip=True)).strip(":").lower()
    return text == "notes"


def _is_repealed_heading(heading: str | None) -> bool:
    text = (heading or "").lower()
    return text.startswith("repealed") or text.startswith("[repealed")


def _references(root: Tag, *, self_path: str) -> tuple[str, ...]:
    refs: set[str] = set()
    for link in root.find_all("a"):
        if not isinstance(link, Tag):
            continue
        ref = _href_to_citation_path(str(link.get("href") or ""))
        if ref and ref != self_path:
            refs.add(ref)
    return tuple(sorted(refs))


def _href_to_citation_path(href: str) -> str | None:
    parsed = urlparse(href)
    cite = _wac_cite_from_href(href)
    if cite is not None and "/wac/" in parsed.path.lower():
        return _wac_citation_path(cite)
    if "/rcw/" in parsed.path.lower():
        query = {key.lower(): value for key, value in parse_qs(parsed.query).items()}
        raw = query.get("cite", [None])[0]
        if raw:
            return _rcw_citation_path(raw)
    return None


def _wac_citation_path(cite: str) -> str:
    title = _wac_title_from_cite(cite)
    if _wac_cite_depth(cite) == 1:
        return f"us-wa/regulation/{_path_token(title)}"
    chapter = _wac_chapter_from_cite(cite)
    if _wac_cite_depth(cite) == 2:
        return f"us-wa/regulation/{_path_token(title)}/{_path_token(chapter)}"
    return (
        f"us-wa/regulation/{_path_token(title)}/{_path_token(chapter)}/"
        f"{_path_token(cite)}"
    )


def _rcw_citation_path(cite: str) -> str | None:
    text = unquote_plus(cite).strip()
    if not re.fullmatch(r"\d+[A-Za-z]?(?:\.\d+[A-Za-z]?){0,2}", text):
        return None
    parts = text.split(".")
    title = _path_token(parts[0])
    if len(parts) == 1:
        return f"us-wa/statute/{title}"
    chapter = _path_token(".".join(parts[:2]))
    if len(parts) == 2:
        return f"us-wa/statute/{title}/{chapter}"
    return f"us-wa/statute/{title}/{chapter}/{_path_token(text)}"


def _wac_cite_from_href(href: str) -> str | None:
    parsed = urlparse(href)
    query = {key.lower(): value for key, value in parse_qs(parsed.query).items()}
    raw = query.get("cite", [None])[0] or (parsed.fragment if parsed.fragment else None)
    return _wac_cite_filter(raw, strict=False)


def _wac_cite_filter(value: str | None, *, strict: bool = True) -> str | None:
    if value is None:
        return None
    text = unquote_plus(value).strip()
    text = re.sub(r"^(?:title|chapter|wac)\s+", "", text, flags=re.I)
    text = re.sub(r"\s+WAC$", "", text, flags=re.I).strip()
    title_pattern = r"\d+[A-Za-z]?"
    chapter_pattern = rf"{title_pattern}-\d+[A-Za-z]?"
    section_pattern = rf"{chapter_pattern}-\d+[A-Za-z]?"
    if not re.fullmatch(rf"(?:{title_pattern}|{chapter_pattern}|{section_pattern})", text):
        if strict and value.strip():
            raise ValueError(f"invalid Washington Administrative Code citation: {value!r}")
        return None
    return _normalize_wac_cite(text)


def _normalize_wac_cite(cite: str) -> str:
    parts = cite.split("-")
    normalized: list[str] = []
    for index, part in enumerate(parts):
        match = re.match(r"(\d+)([A-Za-z]?)$", part)
        if match and index == 0:
            normalized.append(f"{int(match.group(1))}{match.group(2).upper()}")
        elif match:
            normalized.append(f"{match.group(1)}{match.group(2).upper()}")
        else:
            normalized.append(part)
    return "-".join(normalized)


def _wac_cite_depth(cite: str) -> int:
    return cite.count("-") + 1


def _wac_title_from_cite(cite: str) -> str:
    return cite.split("-", 1)[0]


def _wac_chapter_from_cite(cite: str) -> str:
    parts = cite.split("-")
    return "-".join(parts[:2]) if len(parts) >= 2 else cite


def _wac_section_in_chapter(section: str, chapter: str) -> bool:
    return section.startswith(f"{chapter}-")


def _wac_title_relative(title: _WacTitle) -> str:
    return f"washington-wac-html/titles/title-{_path_token(title.num)}.html"


def _wac_chapter_relative(chapter: _WacChapter) -> str:
    return f"washington-wac-html/chapters/chapter-{_path_token(chapter.num)}-full.html"


def _wac_cite_url(cite: str) -> str:
    return f"{WASHINGTON_WAC_BASE_URL}?cite={quote(cite, safe='-')}"


def _wac_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.REGULATION.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _path_token(value: str) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9.-]+", "-", text).strip("-")
    return text or "0"


def _ordinal(value: str | None) -> int | None:
    if not value:
        return None
    numbers = [int(part) for part in re.findall(r"\d+", value)]
    if not numbers:
        return None
    ordinal = 0
    for number in numbers[:3]:
        ordinal = ordinal * 1_000 + min(number, 999)
    suffix_match = re.search(r"[A-Za-z]+$", value)
    if suffix_match:
        ordinal = ordinal * 100 + sum(ord(char.upper()) for char in suffix_match.group(0))
    return ordinal


def _main(argv: list[str] | None = None) -> int:
    raise SystemExit("Use `axiom-corpus extract-washington-wac` to run this adapter.")


if __name__ == "__main__":
    sys.exit(_main())
