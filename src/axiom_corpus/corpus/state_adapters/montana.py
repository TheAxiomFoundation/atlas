"""Montana Code Annotated source-first corpus adapter."""

from __future__ import annotations

import re
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
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

MONTANA_CODE_BASE_URL = "https://mca.legmt.gov/bills/mca/"
MONTANA_CODE_SOURCE_FORMAT = "montana-code-html"
MONTANA_CODE_DEFAULT_YEAR = 2025
MONTANA_USER_AGENT = "Axiom/1.0 (Statute Research; contact@axiom-foundation.org)"

_MONTANA_SECTION_CITE_PATTERN = r"\d{1,2}-[0-9A-Za-z]{1,3}-\d{3,4}(?:\.\d+|[A-Za-z])?"
_MONTANA_TEXT_CITE_RE = re.compile(rf"\b(?P<cite>{_MONTANA_SECTION_CITE_PATTERN})\b")
_ENCODED_SECTION_RE = re.compile(
    r"(?P<title>\d{4})-(?P<chapter>[0-9A-Za-z]{4})-(?P<part>\d{4})-(?P<section>[0-9A-Za-z]{4})\.html$"
)


@dataclass(frozen=True)
class MontanaCodeProvision:
    """Parsed MCA title/chapter/article/part/section node."""

    kind: str
    source_id: str
    display_number: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int
    references_to: tuple[str, ...] = ()
    source_history: tuple[str, ...] = ()
    status: str | None = None
    source_year: int | None = None

    @property
    def citation_path(self) -> str:
        return f"us-mt/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        if self.kind == "section" and not self.source_id.startswith("0-"):
            return f"MCA {self.source_id}"
        if self.source_id.startswith("0-"):
            return f"Mont. Const. {self.display_number}"
        return f"MCA {self.display_number}"


@dataclass(frozen=True)
class _MontanaSourcePage:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _MontanaLink:
    source_id: str
    display_number: str
    heading: str
    relative_path: str
    kind: str


class _MontanaFetcher:
    def __init__(
        self,
        *,
        base_url: str,
        source_dir: Path | None,
        download_dir: Path | None,
    ) -> None:
        self.base_url = _base_url(base_url)
        self.source_dir = source_dir
        self.download_dir = download_dir

    def fetch(self, relative_path: str) -> _MontanaSourcePage:
        normalized = _normalize_relative_path(relative_path)
        source_url = urljoin(self.base_url, normalized)
        if self.source_dir is not None:
            path = self.source_dir / normalized
            if not path.exists():
                raise ValueError(f"Montana source file does not exist: {path}")
            return _MontanaSourcePage(
                relative_path=normalized,
                source_url=source_url,
                data=path.read_bytes(),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            if cached_path.exists():
                return _MontanaSourcePage(
                    relative_path=normalized,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )

        data = _download_montana_page(source_url)
        if self.download_dir is not None:
            cached_path = self.download_dir / normalized
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            cached_path.write_bytes(data)
        return _MontanaSourcePage(relative_path=normalized, source_url=source_url, data=data)


def extract_montana_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_year: int = MONTANA_CODE_DEFAULT_YEAR,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    workers: int = 8,
    base_url: str = MONTANA_CODE_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official MCA HTML and extract normalized provisions."""
    jurisdiction = "us-mt"
    title_filter = _montana_title_filter(only_title)
    run_id = _montana_run_id(version, only_title=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _MontanaFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
    )

    source_paths: list[Path] = []
    source_pages: dict[str, tuple[str, str]] = {}
    provisions: list[MontanaCodeProvision] = []
    errors: list[str] = []
    ordinal = 0

    root_page = fetcher.fetch("index.html")
    _record_source_page(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        page=root_page,
        source_paths=source_paths,
        source_pages=source_pages,
    )
    root_links = _parse_montana_title_links(root_page.data)
    if title_filter is not None:
        root_links = tuple(link for link in root_links if link.source_id == title_filter)
    if not root_links:
        raise ValueError(f"no Montana Code title sources selected for filter: {only_title!r}")

    title_pages = _fetch_pages(fetcher, [link.relative_path for link in root_links], workers=workers)
    for fetched_title_page in title_pages:
        _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=fetched_title_page,
            source_paths=source_paths,
            source_pages=source_pages,
        )
    title_page_by_relative = {
        fetched_title_page.relative_path: fetched_title_page
        for fetched_title_page in title_pages
    }

    chapter_links: list[_MontanaLink] = []
    for link in root_links:
        selected_title_page = title_page_by_relative.get(link.relative_path)
        if selected_title_page is None:  # pragma: no cover
            errors.append(f"title {link.source_id}: source page fetch failed")
            continue
        provision = _montana_container_provision(
            kind="title",
            source_id=link.source_id,
            display_number=link.display_number,
            heading=_title_heading(selected_title_page.data) or link.heading,
            parent_citation_path=None,
            level=0,
            ordinal=ordinal,
            source_year=source_year,
        )
        provisions.append(provision)
        ordinal += 1
        chapter_links.extend(
            _parse_montana_chapter_links(
                selected_title_page.data,
                selected_title_page.relative_path,
                parent_title=link.source_id,
            )
        )

    chapter_pages = _fetch_pages(fetcher, [link.relative_path for link in chapter_links], workers=workers)
    for fetched_chapter_page in chapter_pages:
        _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=fetched_chapter_page,
            source_paths=source_paths,
            source_pages=source_pages,
        )
    chapter_page_by_relative = {
        fetched_chapter_page.relative_path: fetched_chapter_page
        for fetched_chapter_page in chapter_pages
    }

    part_links: list[_MontanaLink] = []
    for link in chapter_links:
        selected_chapter_page = chapter_page_by_relative.get(link.relative_path)
        if selected_chapter_page is None:  # pragma: no cover
            errors.append(f"{link.kind} {link.source_id}: source page fetch failed")
            continue
        provisions.append(
            _montana_container_provision(
                kind=link.kind,
                source_id=link.source_id,
                display_number=link.display_number,
                heading=_chapter_heading(selected_chapter_page.data, link.kind) or link.heading,
                parent_citation_path=f"us-mt/statute/{_parent_source_id(link.source_id, 1)}",
                level=1,
                ordinal=ordinal,
                source_year=source_year,
            )
        )
        ordinal += 1
        part_links.extend(
            _parse_montana_part_links(
                selected_chapter_page.data,
                selected_chapter_page.relative_path,
                parent_source_id=link.source_id,
            )
        )

    part_pages = _fetch_pages(fetcher, [link.relative_path for link in part_links], workers=workers)
    for fetched_part_page in part_pages:
        _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=fetched_part_page,
            source_paths=source_paths,
            source_pages=source_pages,
        )
    part_page_by_relative = {
        fetched_part_page.relative_path: fetched_part_page for fetched_part_page in part_pages
    }

    section_links: list[_MontanaLink] = []
    for link in part_links:
        selected_part_page = part_page_by_relative.get(link.relative_path)
        if selected_part_page is None:  # pragma: no cover
            errors.append(f"part {link.source_id}: source page fetch failed")
            continue
        provisions.append(
            _montana_container_provision(
                kind="part",
                source_id=link.source_id,
                display_number=link.display_number,
                heading=_part_heading(selected_part_page.data) or link.heading,
                parent_citation_path=f"us-mt/statute/{_parent_source_id(link.source_id, 2)}",
                level=2,
                ordinal=ordinal,
                source_year=source_year,
            )
        )
        ordinal += 1
        section_links.extend(
            _parse_montana_section_links(
                selected_part_page.data,
                selected_part_page.relative_path,
                parent_source_id=link.source_id,
            )
        )

    if limit is not None:
        section_limit = max(0, limit - len(provisions))
        section_links = section_links[:section_limit]

    section_pages = _fetch_pages(fetcher, [link.relative_path for link in section_links], workers=workers)
    for fetched_section_page in section_pages:
        _record_source_page(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            page=fetched_section_page,
            source_paths=source_paths,
            source_pages=source_pages,
        )
    section_page_by_relative = {
        fetched_section_page.relative_path: fetched_section_page
        for fetched_section_page in section_pages
    }

    for link in section_links:
        selected_section_page = section_page_by_relative.get(link.relative_path)
        if selected_section_page is None:  # pragma: no cover
            errors.append(f"section {link.source_id}: source page fetch failed")
            continue
        try:
            section = parse_montana_section_html(
                selected_section_page.data,
                fallback_source_id=link.source_id,
                parent_source_id=_section_parent_source_id(link.source_id),
                source_year=source_year,
                ordinal=ordinal,
            )
        except ValueError as exc:
            errors.append(f"section {link.source_id}: {exc}")
            continue
        provisions.append(section)
        ordinal += 1

    if limit is not None:
        provisions = provisions[:limit]
    if not provisions:  # pragma: no cover
        raise ValueError("no Montana Code provisions extracted")

    seen: set[str] = set()
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    title_count = 0
    container_count = 0
    section_count = 0
    source_relative_by_key = {
        (link.kind, link.source_id): link.relative_path
        for link in (*root_links, *chapter_links, *part_links, *section_links)
    }
    for provision in provisions:
        if provision.citation_path in seen:
            continue
        seen.add(provision.citation_path)
        relative_path = source_relative_by_key[(provision.kind, provision.source_id)]
        source_path, sha256 = source_pages[relative_path]
        source_url = urljoin(fetcher.base_url, relative_path)
        items.append(
            _inventory_item(
                provision,
                source_url=source_url,
                source_path=source_path,
                sha256=sha256,
            )
        )
        records.append(
            _provision_record(
                provision,
                version=run_id,
                source_url=source_url,
                source_path=source_path,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
        )
        if provision.kind == "title":
            title_count += 1
        if provision.kind == "section":
            section_count += 1
        else:
            container_count += 1

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


def parse_montana_section_html(
    html: str | bytes,
    *,
    fallback_source_id: str,
    parent_source_id: str | None,
    source_year: int = MONTANA_CODE_DEFAULT_YEAR,
    ordinal: int = 0,
) -> MontanaCodeProvision:
    """Parse one official MCA section HTML page."""
    soup = BeautifulSoup(html, "lxml")
    content = soup.find("div", class_="section-content")
    if not isinstance(content, Tag):
        raise ValueError("section content not found")
    heading = _span_text(soup, "section-section-title") or _title_text(soup)
    body_lines = [
        _clean_text(paragraph.get_text(" ", strip=True))
        for paragraph in content.find_all("p")
        if isinstance(paragraph, Tag) and _clean_text(paragraph.get_text(" ", strip=True))
    ]
    if not body_lines:
        text = _clean_text(content.get_text(" ", strip=True))
        if text:
            body_lines.append(text)
    body = "\n".join(body_lines).strip() or None

    citation = _first_citation(content) or fallback_source_id
    source_id = (
        fallback_source_id
        if fallback_source_id.startswith("0-")
        else _normalize_montana_source_id(citation)
    )
    display_number = citation
    if fallback_source_id.startswith("0-") and citation == fallback_source_id:
        display_number = fallback_source_id.rsplit("-", 1)[-1]
    self_path = f"us-mt/statute/{source_id}"
    references = tuple(
        ref
        for ref in _section_references(content, self_path=self_path)
        if ref != self_path
    )
    history = _section_history(soup)
    return MontanaCodeProvision(
        kind="section",
        source_id=source_id,
        display_number=display_number,
        heading=heading,
        body=body,
        parent_citation_path=(
            f"us-mt/statute/{parent_source_id}" if parent_source_id is not None else None
        ),
        level=3,
        ordinal=ordinal,
        references_to=tuple(dict.fromkeys(references)),
        source_history=history,
        status=_status(heading, body),
        source_year=source_year,
    )


def _montana_container_provision(
    *,
    kind: str,
    source_id: str,
    display_number: str,
    heading: str | None,
    parent_citation_path: str | None,
    level: int,
    ordinal: int,
    source_year: int,
) -> MontanaCodeProvision:
    return MontanaCodeProvision(
        kind=kind,
        source_id=source_id,
        display_number=display_number,
        heading=heading,
        body=None,
        parent_citation_path=parent_citation_path,
        level=level,
        ordinal=ordinal,
        status=_status(heading, None),
        source_year=source_year,
    )


def _fetch_pages(
    fetcher: _MontanaFetcher,
    relative_paths: Iterable[str],
    *,
    workers: int,
) -> tuple[_MontanaSourcePage, ...]:
    unique = tuple(dict.fromkeys(_normalize_relative_path(path) for path in relative_paths))
    if not unique:
        return ()
    if workers <= 1:
        return tuple(fetcher.fetch(path) for path in unique)
    pages: dict[str, _MontanaSourcePage] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_path = {executor.submit(fetcher.fetch, path): path for path in unique}
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            pages[path] = future.result()
    return tuple(pages[path] for path in unique)


def _record_source_page(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    page: _MontanaSourcePage,
    source_paths: list[Path],
    source_pages: dict[str, tuple[str, str]],
) -> None:
    artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        f"{MONTANA_CODE_SOURCE_FORMAT}/{page.relative_path}",
    )
    sha256 = store.write_bytes(artifact_path, page.data)
    source_paths.append(artifact_path)
    source_pages[page.relative_path] = (
        _state_source_key(
            jurisdiction,
            run_id,
            f"{MONTANA_CODE_SOURCE_FORMAT}/{page.relative_path}",
        ),
        sha256,
    )


def _parse_montana_title_links(html: str | bytes) -> tuple[_MontanaLink, ...]:
    soup = BeautifulSoup(html, "lxml")
    links: list[_MontanaLink] = []
    for anchor in soup.select(".title-toc-content a[href]"):
        if not isinstance(anchor, Tag):  # pragma: no cover
            continue
        source_id = str(anchor.get("data-titlenumber") or "")
        if not source_id.isdigit():
            source_id = _id_from_title_href(str(anchor.get("href") or ""))
        source_id = str(int(source_id))
        text = _clean_text(anchor.get_text(" ", strip=True))
        links.append(
            _MontanaLink(
                source_id=source_id,
                display_number=_display_from_title_text(text, source_id),
                heading=text,
                relative_path=_resolve_relative("index.html", str(anchor.get("href") or "")),
                kind="title",
            )
        )
    return tuple(links)


def _parse_montana_chapter_links(
    html: str | bytes,
    current_relative: str,
    *,
    parent_title: str,
) -> tuple[_MontanaLink, ...]:
    soup = BeautifulSoup(html, "lxml")
    links: list[_MontanaLink] = []
    for anchor in soup.select(".chapter-toc-content a[href]"):
        if not isinstance(anchor, Tag):  # pragma: no cover
            continue
        href = str(anchor.get("href") or "")
        kind = "article" if "article_" in href else "chapter"
        number = _encoded_path_token(href, "article" if kind == "article" else "chapter")
        if number is None:
            continue
        source_id = f"{parent_title}-{number}"
        text = _clean_text(anchor.get_text(" ", strip=True))
        links.append(
            _MontanaLink(
                source_id=source_id,
                display_number=_chapter_display(kind, text, parent_title, number),
                heading=text,
                relative_path=_resolve_relative(current_relative, href),
                kind=kind,
            )
        )
    return tuple(links)


def _parse_montana_part_links(
    html: str | bytes,
    current_relative: str,
    *,
    parent_source_id: str,
) -> tuple[_MontanaLink, ...]:
    soup = BeautifulSoup(html, "lxml")
    links: list[_MontanaLink] = []
    for anchor in soup.select(".part-toc-content a[href]"):
        if not isinstance(anchor, Tag):  # pragma: no cover
            continue
        number = _encoded_path_number(str(anchor.get("href") or ""), "part")
        if number is None:
            continue
        source_id = f"{parent_source_id}-{number}"
        text = _clean_text(anchor.get_text(" ", strip=True))
        links.append(
            _MontanaLink(
                source_id=source_id,
                display_number=_part_display(text, source_id, number),
                heading=text,
                relative_path=_resolve_relative(current_relative, str(anchor.get("href") or "")),
                kind="part",
            )
        )
    return tuple(links)


def _parse_montana_section_links(
    html: str | bytes,
    current_relative: str,
    *,
    parent_source_id: str,
) -> tuple[_MontanaLink, ...]:
    soup = BeautifulSoup(html, "lxml")
    links: list[_MontanaLink] = []
    for anchor in soup.select(".section-toc-content a[href]"):
        if not isinstance(anchor, Tag):  # pragma: no cover
            continue
        href = str(anchor.get("href") or "")
        relative_path = _resolve_relative(current_relative, href)
        path_source_id = _source_id_from_section_relative(relative_path)
        citation = _first_citation(anchor) or path_source_id
        source_id = (
            path_source_id
            if path_source_id.startswith("0-")
            else _normalize_montana_source_id(citation)
        )
        text = _clean_text(anchor.get_text(" ", strip=True))
        heading = text
        if citation and text.startswith(citation):
            heading = text.removeprefix(citation).strip(" .\u2002\u2003\u2009\u00a0")
        links.append(
            _MontanaLink(
                source_id=source_id,
                display_number=citation,
                heading=heading or text,
                relative_path=relative_path,
                kind="section",
            )
        )
    return tuple(links)


def _inventory_item(
    provision: MontanaCodeProvision,
    *,
    source_url: str,
    source_path: str,
    sha256: str,
) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=provision.citation_path,
        source_url=source_url,
        source_path=source_path,
        source_format=MONTANA_CODE_SOURCE_FORMAT,
        sha256=sha256,
        metadata=_metadata(provision),
    )


def _provision_record(
    provision: MontanaCodeProvision,
    *,
    version: str,
    source_url: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(provision.citation_path),
        jurisdiction="us-mt",
        document_class=DocumentClass.STATUTE.value,
        citation_path=provision.citation_path,
        body=provision.body,
        heading=provision.heading,
        citation_label=provision.legal_identifier,
        version=version,
        source_url=source_url,
        source_path=source_path,
        source_id=provision.source_id,
        source_format=MONTANA_CODE_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=provision.parent_citation_path,
        parent_id=(
            deterministic_provision_id(provision.parent_citation_path)
            if provision.parent_citation_path
            else None
        ),
        level=provision.level,
        ordinal=provision.ordinal,
        kind=provision.kind,
        legal_identifier=provision.legal_identifier,
        identifiers={
            "montana:kind": provision.kind,
            "montana:source_id": provision.source_id,
            "montana:display_number": provision.display_number,
        },
        metadata=_metadata(provision),
    )


def _metadata(provision: MontanaCodeProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "display_number": provision.display_number,
    }
    if provision.source_year is not None:
        metadata["source_year"] = provision.source_year
    if provision.parent_citation_path:
        metadata["parent_citation_path"] = provision.parent_citation_path
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.source_history:
        metadata["source_history"] = list(provision.source_history)
    if provision.status:
        metadata["status"] = provision.status
    return metadata


def _section_references(root: Tag, *, self_path: str) -> tuple[str, ...]:
    refs: list[str] = []
    for anchor in root.find_all("a", href=True):
        if not isinstance(anchor, Tag):  # pragma: no cover
            continue
        ref_id = _first_citation(anchor)
        if ref_id:
            ref_id = _normalize_montana_source_id(ref_id)
        else:
            ref_id = _source_id_from_section_relative(str(anchor.get("href") or ""))
        if ref_id:
            refs.append(f"us-mt/statute/{ref_id}")
    text = _clean_text(root.get_text(" ", strip=True))
    for match in _MONTANA_TEXT_CITE_RE.finditer(text):
        refs.append(f"us-mt/statute/{_normalize_montana_source_id(match.group('cite'))}")
    return tuple(dict.fromkeys(ref for ref in refs if ref != self_path))


def _section_history(soup: BeautifulSoup) -> tuple[str, ...]:
    history = soup.find("div", class_="history-content")
    if not isinstance(history, Tag):
        return ()
    text = _clean_text(history.get_text(" ", strip=True))
    text = re.sub(r"^History:\s*", "", text, flags=re.I).strip()
    return (text,) if text else ()


def _status(heading: str | None, body: str | None) -> str | None:
    text = " ".join(part for part in (heading, body) if part).lower()
    if "repealed" in text:
        return "repealed"
    if "reserved" in text:
        return "reserved"
    if "terminated" in text:
        return "terminated"
    return None


def _download_montana_page(source_url: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            response = requests.get(
                source_url,
                headers={"User-Agent": MONTANA_USER_AGENT},
                timeout=60,
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 3:
                break
            time.sleep(0.5 * 2**attempt)
    raise ValueError(f"failed to fetch Montana source page {source_url}: {last_error}")


def _title_heading(html: str | bytes) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    return _span_text(soup, "chapter-title-title")


def _chapter_heading(html: str | bytes, kind: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    if kind == "article":
        return _span_text(soup, "part-chapter-title")
    return _span_text(soup, "part-chapter-title")


def _part_heading(html: str | bytes) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    return _span_text(soup, "section-part-title")


def _span_text(soup: BeautifulSoup | Tag, class_name: str) -> str | None:
    elem = soup.find(class_=class_name)
    if not isinstance(elem, Tag):
        return None
    text = _clean_text(elem.get_text(" ", strip=True))
    return text or None


def _title_text(soup: BeautifulSoup) -> str | None:
    elem = soup.find("title")
    if not isinstance(elem, Tag):
        return None
    text = _clean_text(elem.get_text(" ", strip=True))
    if "," in text:
        text = text.split(",", 1)[0].strip()
    return text or None


def _first_citation(root: Tag) -> str | None:
    citation = root.find("span", class_="citation")
    if isinstance(citation, Tag):
        text = _clean_text(citation.get_text(" ", strip=True))
        return text or None
    text = _clean_text(root.get_text(" ", strip=True))
    match = re.search(rf"(?P<cite>{_MONTANA_SECTION_CITE_PATTERN})", text)
    if match:
        return match.group("cite")
    match = re.match(r"(?P<cite>\d+)\.", text)
    if match:
        return match.group("cite")
    return None


def _source_id_from_section_relative(value: str) -> str:
    parsed = urlparse(value)
    path = parsed.path
    match = _ENCODED_SECTION_RE.search(path)
    if not match:
        return ""
    title = _decode_montana_number(match.group("title"))
    chapter = _decode_montana_token(match.group("chapter"))
    part = _decode_montana_number(match.group("part"))
    section = _decode_montana_section_token(match.group("section"))
    if title == 0:
        return f"0-{chapter}-{part}-{str(section).lower()}"
    if not isinstance(section, int):
        match = re.fullmatch(r"(?P<number>\d+)(?P<suffix>[A-Z]+)", section)
        if match:
            full_section = part * 100 + int(match.group("number"))
            return f"{title}-{chapter}-{full_section}{match.group('suffix')}"
        return f"{title}-{chapter}-{part}-{section}"
    return f"{title}-{chapter}-{part * 100 + section}"


def _normalize_montana_source_id(value: str) -> str:
    cleaned = _clean_text(value).strip(".")
    if re.fullmatch(_MONTANA_SECTION_CITE_PATTERN, cleaned):
        title, chapter, section = cleaned.split("-")
        return f"{int(title)}-{chapter.upper()}-{_normalize_montana_section_number(section)}"
    if re.fullmatch(r"\d+(?:-\d+){3}", cleaned):
        return "-".join(str(int(part)) for part in cleaned.split("-"))
    return cleaned


def _parent_source_id(source_id: str, depth: int) -> str | None:
    parts = source_id.split("-")
    if len(parts) <= depth:
        return None
    return "-".join(parts[:depth])


def _section_parent_source_id(source_id: str) -> str | None:
    parts = source_id.split("-")
    if source_id.startswith("0-") and len(parts) >= 4:
        return "-".join(parts[:3])
    if len(parts) == 3:
        title, chapter, full_section = parts
        return f"{title}-{chapter}-{_montana_part_number_from_section_token(full_section)}"
    return _parent_source_id(source_id, 3)


def _id_from_title_href(href: str) -> str:
    number = _encoded_path_number(href, "title")
    if number is None:
        raise ValueError(f"invalid Montana title href: {href}")
    return str(number)


def _encoded_path_number(href: str, segment: str) -> int | None:
    token = _encoded_path_token(href, segment)
    if token is None or not token.isdigit():
        return None
    return int(token)


def _encoded_path_token(href: str, segment: str) -> str | None:
    match = re.search(rf"{segment}_([0-9A-Za-z]+)", href)
    if not match:
        return None
    return _decode_montana_token(match.group(1))


def _decode_montana_number(value: str) -> int:
    return int(value) // 10


def _decode_montana_token(value: str) -> str:
    if value.isdigit():
        return str(_decode_montana_number(value))
    match = re.fullmatch(r"(?P<number>\d+)(?P<suffix>[A-Za-z]+)", value)
    if match:
        return f"{int(match.group('number'))}{match.group('suffix').upper()}"
    return value.upper().lstrip("0") or value.upper()


def _decode_montana_section_token(value: str) -> int | str:
    if value.isdigit():
        return _decode_montana_number(value)
    return value.upper().lstrip("0") or value.upper()


def _normalize_montana_section_number(value: str) -> str:
    match = re.fullmatch(r"(?P<number>\d+)(?P<suffix>\.\d+|[A-Za-z]+)?", value)
    if not match:
        return value.upper()
    section = str(int(match.group("number")))
    suffix = match.group("suffix") or ""
    return f"{section}{suffix.upper()}"


def _montana_part_number_from_section_token(value: str) -> int:
    match = re.match(r"\d+", value)
    if not match:
        raise ValueError(f"invalid Montana section number: {value}")
    return int(match.group(0)) // 100


def _display_from_title_text(text: str, source_id: str) -> str:
    if source_id == "0":
        return "Constitution"
    match = re.match(r"TITLE\s+\d+", text, flags=re.I)
    return match.group(0).title() if match else f"Title {source_id}"


def _chapter_display(kind: str, text: str, parent_title: str, number: str) -> str:
    if kind == "article":
        match = re.match(r"ARTICLE\s+([IVXLCDM]+|\d+)", text, flags=re.I)
        return f"Article {match.group(1)}" if match else f"Article {number}"
    return f"{parent_title}-{number}"


def _part_display(text: str, source_id: str, number: int) -> str:
    match = re.match(r"Part\s+([IVXLCDM]+|\d+)", text, flags=re.I)
    if match:
        return f"Part {match.group(1)}"
    if source_id.startswith("0-"):
        return f"Part {number}"
    return source_id


def _resolve_relative(current_relative: str, href: str) -> str:
    current_url = f"https://example.test/{current_relative}"
    return _normalize_relative_path(urlparse(urljoin(current_url, href)).path.lstrip("/"))


def _normalize_relative_path(value: str) -> str:
    parsed = urlparse(value)
    path = parsed.path or value
    return path.lstrip("./")


def _base_url(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _montana_title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    if not match:
        raise ValueError(f"invalid Montana title filter: {value!r}")
    return str(int(match.group(0)))


def _montana_run_id(version: str, *, only_title: str | None, limit: int | None) -> str:
    if only_title is None and limit is None:
        return version
    parts = [version, "us-mt"]
    if only_title is not None:
        parts.append(f"title-{only_title}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _clean_text(value: str | None) -> str:
    text = re.sub(r"\s+", " ", value or "").strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)
