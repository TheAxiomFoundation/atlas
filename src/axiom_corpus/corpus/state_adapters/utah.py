"""Utah Code source-first corpus adapter."""

from __future__ import annotations

import posixpath
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, unquote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

UTAH_CODE_SOURCE_URL = "https://le.utah.gov/xcode/C_1800010118000101.html"
UTAH_CODE_BASELINE_XML_URL = "https://le.utah.gov/xcode/C_1800010118000101.xml"
UTAH_CODE_BASELINE_VERSION = "1800010118000101"
UTAH_CODE_HTML_SOURCE_FORMAT = "utah-code-html"
UTAH_CODE_XML_SOURCE_FORMAT = "utah-code-xml"
UTAH_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_SECTION_REF_RE = re.compile(r"\b(?P<label>\d+[A-Z]?-\d+[A-Z]?-\d+[A-Z0-9.]*)(?:\([^)]+\))*\b", re.I)
_DATE_RE = re.compile(r"^(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<year>\d{4})$")
_STATUS_RE = re.compile(
    r"\(?\b(?P<kind>Effective|Superseded)\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})\b\)?",
    re.I,
)


@dataclass(frozen=True)
class UtahSource:
    """One recorded official Utah Code source document."""

    source_url: str
    source_path: str
    source_format: str
    sha256: str
    source_document_id: str


@dataclass(frozen=True)
class UtahLink:
    """One active child link discovered from an official Utah index table."""

    kind: str
    label: str
    heading: str | None
    source_url: str
    relative_path: str
    ordinal: int
    status: str | None = None
    status_date: str | None = None


@dataclass(frozen=True)
class UtahProvision:
    """One normalized Utah Code title/chapter/part/section provision."""

    kind: str
    label: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int
    title: str
    chapter: str | None
    part: str | None
    effective_date: str | None
    end_date: str | None
    end_date_type: str | None
    source_history: tuple[str, ...]
    references_to: tuple[str, ...]
    source: UtahSource

    @property
    def citation_path(self) -> str:
        if self.kind == "title":
            return f"us-ut/statute/title-{_path_segment(self.label)}"
        if self.kind == "chapter":
            return f"us-ut/statute/title-{_path_segment(self.title)}/chapter-{_path_segment(self.label)}"
        if self.kind == "part":
            return (
                f"us-ut/statute/title-{_path_segment(self.title)}"
                f"/chapter-{_path_segment(self.chapter or '')}/part-{_path_segment(self.label)}"
            )
        return f"us-ut/statute/{self.label}"

    @property
    def source_id(self) -> str:
        if self.kind == "title":
            return f"title-{_path_segment(self.label)}"
        if self.kind == "chapter":
            return f"chapter-{_path_segment(self.label)}"
        if self.kind == "part":
            return f"part-{_path_segment(self.label)}"
        return self.label

    @property
    def legal_identifier(self) -> str:
        if self.kind == "title":
            return f"Utah Code title {self.label}"
        if self.kind == "chapter":
            return f"Utah Code ch. {self.label}"
        if self.kind == "part":
            return f"Utah Code part {self.label}"
        return f"Utah Code \u00a7 {self.label}"


@dataclass(frozen=True)
class _SourceDocument:
    source_url: str
    relative_path: str
    data: bytes


@dataclass(frozen=True)
class _FetchResult:
    key: str
    source: _SourceDocument | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _SectionTarget:
    link: UtahLink
    title: str
    chapter: str
    part: str | None
    parent_citation_path: str
    level: int


class _UtahFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch(self, source_url: str, relative_path: str) -> _SourceDocument:
        data = self._fetch_bytes(source_url, relative_path)
        return _SourceDocument(source_url=source_url, relative_path=relative_path, data=data)

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            self._last_request_at = time.monotonic()

    def _fetch_bytes(self, source_url: str, relative_path: str) -> bytes:
        if self.source_dir is not None:
            path = self.source_dir / relative_path
            if path.exists():
                return path.read_bytes()
        if self.download_dir is not None:
            path = self.download_dir / relative_path
            if path.exists():
                return path.read_bytes()
        data = _download_utah_source(source_url, fetcher=self)
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return data


def extract_utah_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_url: str = UTAH_CODE_SOURCE_URL,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
    workers: int = 8,
) -> StateStatuteExtractReport:
    """Snapshot official Utah Code HTML/XML and extract normalized provisions."""
    jurisdiction = "us-ut"
    title_filter = _normalize_title_filter(only_title)
    run_id = _utah_run_id(version, only_title=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    expression_date_value = _parse_iso_date(expression_date_text)
    fetcher = _UtahFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    root_relative = _relative_from_url(source_url, UTAH_CODE_HTML_SOURCE_FORMAT)
    root_doc = fetcher.fetch(source_url, root_relative)
    baseline_relative = _relative_from_url(UTAH_CODE_BASELINE_XML_URL, UTAH_CODE_XML_SOURCE_FORMAT)
    baseline_doc = fetcher.fetch(UTAH_CODE_BASELINE_XML_URL, baseline_relative)
    source_paths: list[Path] = []
    records: list[ProvisionRecord] = []
    items: list[SourceInventoryItem] = []
    seen: set[str] = set()
    section_targets: list[_SectionTarget] = []

    root_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, root_doc.relative_path)
    store.write_bytes(root_path, root_doc.data)
    source_paths.append(root_path)
    baseline_source = _write_source(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        doc=baseline_doc,
        source_paths=source_paths,
    )
    baseline_sections = _baseline_section_elements(
        baseline_doc.data,
        expression_date=expression_date_value,
    )

    title_links = [
        link
        for link in parse_utah_child_links(
            root_doc.data,
            source_url=root_doc.source_url,
            expression_date=expression_date_value,
        )
        if link.kind == "title"
        and (title_filter is None or _normalize_title_filter(link.label) == title_filter)
    ]
    if not title_links:
        raise ValueError(f"no Utah Code titles selected for filter: {only_title!r}")

    title_docs = _fetch_documents(fetcher, title_links, workers=workers)
    chapter_links: list[tuple[UtahLink, UtahProvision]] = []
    for title_link, title_doc in zip(title_links, title_docs, strict=True):
        title_source = _write_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            doc=title_doc,
            source_paths=source_paths,
        )
        title = UtahProvision(
            kind="title",
            label=title_link.label,
            heading=title_link.heading,
            body=None,
            parent_citation_path=None,
            level=0,
            ordinal=title_link.ordinal,
            title=title_link.label,
            chapter=None,
            part=None,
            effective_date=_status_effective_date(title_link),
            end_date=_status_end_date(title_link),
            end_date_type=_status_end_date_type(title_link),
            source_history=(),
            references_to=(),
            source=title_source,
        )
        _append_provision(
            title,
            seen=seen,
            records=records,
            items=items,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
        )
        for chapter_link in parse_utah_child_links(
            title_doc.data,
            source_url=title_doc.source_url,
            expression_date=expression_date_value,
        ):
            if chapter_link.kind == "chapter":
                chapter_links.append((chapter_link, title))

    chapter_docs = _fetch_documents(fetcher, [link for link, _title in chapter_links], workers=workers)
    part_links: list[tuple[UtahLink, UtahProvision]] = []
    for (chapter_link, title), chapter_doc in zip(chapter_links, chapter_docs, strict=True):
        chapter_source = _write_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            doc=chapter_doc,
            source_paths=source_paths,
        )
        chapter = UtahProvision(
            kind="chapter",
            label=chapter_link.label,
            heading=chapter_link.heading,
            body=None,
            parent_citation_path=title.citation_path,
            level=1,
            ordinal=chapter_link.ordinal,
            title=title.label,
            chapter=chapter_link.label,
            part=None,
            effective_date=_status_effective_date(chapter_link),
            end_date=_status_end_date(chapter_link),
            end_date_type=_status_end_date_type(chapter_link),
            source_history=(),
            references_to=(),
            source=chapter_source,
        )
        _append_provision(
            chapter,
            seen=seen,
            records=records,
            items=items,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
        )
        for child_link in parse_utah_child_links(
            chapter_doc.data,
            source_url=chapter_doc.source_url,
            expression_date=expression_date_value,
        ):
            if child_link.kind == "part":
                part_links.append((child_link, chapter))
            elif child_link.kind == "section":
                section_targets.append(
                    _SectionTarget(
                        link=child_link,
                        title=title.label,
                        chapter=chapter.label,
                        part=None,
                        parent_citation_path=chapter.citation_path,
                        level=2,
                    )
                )

    part_docs = _fetch_documents(fetcher, [link for link, _chapter in part_links], workers=workers)
    for (part_link, chapter), part_doc in zip(part_links, part_docs, strict=True):
        part_source = _write_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            doc=part_doc,
            source_paths=source_paths,
        )
        part = UtahProvision(
            kind="part",
            label=part_link.label,
            heading=part_link.heading,
            body=None,
            parent_citation_path=chapter.citation_path,
            level=2,
            ordinal=part_link.ordinal,
            title=chapter.title,
            chapter=chapter.label,
            part=part_link.label,
            effective_date=_status_effective_date(part_link),
            end_date=_status_end_date(part_link),
            end_date_type=_status_end_date_type(part_link),
            source_history=(),
            references_to=(),
            source=part_source,
        )
        _append_provision(
            part,
            seen=seen,
            records=records,
            items=items,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
        )
        for section_link in parse_utah_child_links(
            part_doc.data,
            source_url=part_doc.source_url,
            expression_date=expression_date_value,
        ):
            if section_link.kind != "section":
                continue
            section_targets.append(
                _SectionTarget(
                    link=section_link,
                    title=chapter.title,
                    chapter=chapter.label,
                    part=part.label,
                    parent_citation_path=part.citation_path,
                    level=3,
                )
            )

    if limit is not None:
        section_targets = section_targets[: max(0, limit - len(records))]

    per_section_targets = [
        target
        for target in section_targets
        if not _can_use_baseline_section(target, baseline_sections)
    ]
    section_docs = _fetch_documents(
        fetcher,
        [target.link for target in per_section_targets],
        workers=workers,
    )
    per_section_docs = {
        target.link.source_url: doc for target, doc in zip(per_section_targets, section_docs, strict=True)
    }
    for target in section_targets:
        if _can_use_baseline_section(target, baseline_sections):
            section = parse_utah_section_element(
                baseline_sections[target.link.label],
                target=target,
                source=baseline_source,
                expression_date=expression_date_value,
            )
        else:
            section_doc = per_section_docs[target.link.source_url]
            section_source = _write_source(
                store,
                jurisdiction=jurisdiction,
                run_id=run_id,
                doc=section_doc,
                source_paths=source_paths,
            )
            section = parse_utah_section_xml(
                section_doc.data,
                target=target,
                source=section_source,
                expression_date=expression_date_value,
            )
        if section is None:
            continue
        _append_provision(
            section,
            seen=seen,
            records=records,
            items=items,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
        )

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
        title_count=sum(1 for record in records if record.kind == "title"),
        container_count=sum(1 for record in records if record.kind in {"chapter", "part"}),
        section_count=sum(1 for record in records if record.kind == "section"),
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def parse_utah_child_links(
    html_data: str | bytes,
    *,
    source_url: str,
    expression_date: date,
) -> tuple[UtahLink, ...]:
    """Parse active child links from one official Utah Code index HTML page."""
    soup = BeautifulSoup(_html_text(html_data), "lxml")
    table = soup.find("table", id="childtbl")
    if not isinstance(table, Tag):
        return ()
    links: list[UtahLink] = []
    for row in table.find_all("tr"):
        if not isinstance(row, Tag):
            continue
        anchor = row.find("a", href=True)
        if not isinstance(anchor, Tag):
            continue
        parsed = _kind_label_from_href(str(anchor.get("href") or ""))
        if parsed is None:
            continue
        kind, label = parsed
        cells = [cell for cell in row.find_all("td", recursive=False) if isinstance(cell, Tag)]
        heading_cell = cells[1] if len(cells) > 1 else None
        status, status_date = _row_status(heading_cell)
        if not _status_is_active(status, status_date, expression_date=expression_date):
            continue
        extension = "xml" if kind == "section" else "html"
        child_url = _content_url_from_href(source_url, str(anchor["href"]), extension=extension)
        source_format = UTAH_CODE_XML_SOURCE_FORMAT if kind == "section" else UTAH_CODE_HTML_SOURCE_FORMAT
        links.append(
            UtahLink(
                kind=kind,
                label=label,
                heading=_heading_from_cell(heading_cell),
                source_url=child_url,
                relative_path=_relative_from_url(child_url, source_format),
                ordinal=len(links) + 1,
                status=status,
                status_date=status_date,
            )
        )
    return tuple(links)


def parse_utah_section_xml(
    xml_data: str | bytes,
    *,
    target: _SectionTarget,
    source: UtahSource,
    expression_date: date,
) -> UtahProvision | None:
    """Parse one official Utah section XML document."""
    root = _section_xml_root(xml_data)
    return parse_utah_section_element(
        root,
        target=target,
        source=source,
        expression_date=expression_date,
    )


def _section_xml_root(xml_data: str | bytes) -> ET.Element:
    try:
        return ET.fromstring(xml_data)
    except ET.ParseError:
        text = xml_data.decode("utf-8", errors="replace") if isinstance(xml_data, bytes) else xml_data
        text = re.sub(r"<\?xml[^>]*\?>", "", text).strip()
        wrapper = ET.fromstring(f"<utah-sections>{text}</utah-sections>")
        sections = [child for child in list(wrapper) if _local_name(child.tag) == "section"]
        if not sections:
            raise
        return sections[-1]


def parse_utah_section_element(
    root: ET.Element,
    *,
    target: _SectionTarget,
    source: UtahSource,
    expression_date: date,
) -> UtahProvision | None:
    """Parse one official Utah section XML element."""
    if _local_name(root.tag) != "section":
        raise ValueError(f"expected Utah section XML root <section>, got <{root.tag}>")
    if not _is_active_node(root, expression_date=expression_date):
        return None
    label = _node_number(root) or target.link.label
    return UtahProvision(
        kind="section",
        label=label,
        heading=_child_text(root, "catchline") or target.link.heading,
        body=_section_body(root, label),
        parent_citation_path=target.parent_citation_path,
        level=target.level,
        ordinal=target.link.ordinal,
        title=target.title,
        chapter=target.chapter,
        part=target.part,
        effective_date=_child_text(root, "effdate") or _status_effective_date(target.link),
        end_date=_child_text(root, "enddate") or _status_end_date(target.link),
        end_date_type=_child_attr(root, "enddate", "type") or _status_end_date_type(target.link),
        source_history=tuple(
            text
            for history in root.findall("histories/history")
            if (text := _clean_text("".join(history.itertext())))
        ),
        references_to=tuple(_references_to(root, self_label=label)),
        source=source,
    )


def _section_body(section_el: ET.Element, section_label: str) -> str | None:
    lines: list[str] = []
    direct = _inline_text(section_el)
    if direct:
        lines.append(direct)
    for subsection in _child_elements(section_el, "subsection"):
        _append_subsection_lines(subsection, lines, section_label=section_label)
    return "\n".join(lines) if lines else None


def _append_subsection_lines(
    subsection: ET.Element,
    lines: list[str],
    *,
    section_label: str,
) -> None:
    number = _node_number(subsection)
    prefix = _subsection_suffix(number, section_label=section_label)
    text = _inline_text(subsection)
    if text:
        lines.append(f"{prefix} {text}".strip() if prefix else text)
    elif prefix:
        lines.append(prefix)
    for child in _child_elements(subsection, "subsection"):
        _append_subsection_lines(child, lines, section_label=section_label)


def _inline_text(element: ET.Element) -> str:
    parts: list[str] = []
    if element.text:
        parts.append(element.text)
    for child in list(element):
        tag = _local_name(child.tag)
        if tag in {"catchline", "effdate", "enddate", "histories", "subsection", "sinfo"}:
            if child.tail:
                parts.append(child.tail)
            continue
        if tag == "tab":
            parts.append(" ")
        else:
            parts.append("".join(child.itertext()))
        if child.tail:
            parts.append(child.tail)
    return _clean_text(" ".join(parts))


def _references_to(section_el: ET.Element, *, self_label: str) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()
    self_path = f"us-ut/statute/{self_label}"
    for xref in section_el.iter("xref"):
        for text in (xref.get("refnumber"), "".join(xref.itertext())):
            for match in _SECTION_REF_RE.finditer(text or ""):
                label = _strip_subsection(match.group("label"))
                path = f"us-ut/statute/{label}"
                if path != self_path and path not in seen:
                    seen.add(path)
                    refs.append(path)
    body_text = " ".join(section_el.itertext())
    for match in _SECTION_REF_RE.finditer(body_text):
        label = _strip_subsection(match.group("label"))
        path = f"us-ut/statute/{label}"
        if path != self_path and path not in seen:
            seen.add(path)
            refs.append(path)
    return refs


def _fetch_documents(
    fetcher: _UtahFetcher,
    links: list[UtahLink],
    *,
    workers: int,
) -> tuple[_SourceDocument, ...]:
    if workers <= 1 or len(links) <= 1:
        results = [
            _FetchResult(link.source_url, source=fetcher.fetch(link.source_url, link.relative_path))
            for link in links
        ]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(fetcher.fetch, link.source_url, link.relative_path): link
                for link in links
            }
            for future in as_completed(futures):
                link = futures[future]
                try:
                    results.append(_FetchResult(link.source_url, source=future.result()))
                except BaseException as exc:
                    results.append(_FetchResult(link.source_url, error=exc))
    errors = [f"{result.key}: {result.error}" for result in results if result.error]
    if errors:
        raise RuntimeError("; ".join(errors[:5]))
    by_url = {result.key: result.source for result in results if result.source is not None}
    return tuple(by_url[link.source_url] for link in links if link.source_url in by_url)


def _download_utah_source(source_url: str, *, fetcher: _UtahFetcher) -> bytes:
    last_error: BaseException | None = None
    for attempt in range(1, fetcher.request_attempts + 1):
        try:
            fetcher.wait_for_request_slot()
            response = requests.get(
                source_url,
                timeout=fetcher.timeout_seconds,
                headers={"Accept": "application/xml,text/html,*/*", "User-Agent": UTAH_USER_AGENT},
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt < fetcher.request_attempts:
                time.sleep(_retry_delay(exc, attempt=attempt))
    if last_error is not None:
        raise last_error
    raise ValueError(f"Utah source request failed: {source_url}")


def _write_source(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    doc: _SourceDocument,
    source_paths: list[Path],
) -> UtahSource:
    path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, doc.relative_path)
    sha256 = store.write_bytes(path, doc.data)
    source_paths.append(path)
    return UtahSource(
        source_url=doc.source_url,
        source_path=_store_relative_path(store, path),
        source_format=doc.relative_path.split("/", 1)[0],
        sha256=sha256,
        source_document_id=Path(urlparse(doc.source_url).path).stem,
    )


def _append_provision(
    provision: UtahProvision,
    *,
    seen: set[str],
    records: list[ProvisionRecord],
    items: list[SourceInventoryItem],
    version: str,
    source_as_of: str,
    expression_date: str,
) -> None:
    if provision.citation_path in seen:
        return
    seen.add(provision.citation_path)
    records.append(
        _provision_record(
            provision,
            version=version,
            source_as_of=source_as_of,
            expression_date=expression_date,
        )
    )
    items.append(
        SourceInventoryItem(
            citation_path=provision.citation_path,
            source_url=provision.source.source_url,
            source_path=provision.source.source_path,
            source_format=provision.source.source_format,
            sha256=provision.source.sha256,
            metadata=_metadata(provision),
        )
    )


def _provision_record(
    provision: UtahProvision,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    parent_id = (
        deterministic_provision_id(provision.parent_citation_path)
        if provision.parent_citation_path
        else None
    )
    return ProvisionRecord(
        id=deterministic_provision_id(provision.citation_path),
        jurisdiction="us-ut",
        document_class=DocumentClass.STATUTE.value,
        citation_path=provision.citation_path,
        body=provision.body,
        heading=provision.heading,
        citation_label=provision.legal_identifier,
        version=version,
        source_url=provision.source.source_url,
        source_path=provision.source.source_path,
        source_id=provision.source_id,
        source_format=provision.source.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=provision.parent_citation_path,
        parent_id=parent_id,
        level=provision.level,
        ordinal=provision.ordinal,
        kind=provision.kind,
        legal_identifier=provision.legal_identifier,
        identifiers=_identifiers(provision),
        metadata=_metadata(provision),
    )


def _identifiers(provision: UtahProvision) -> dict[str, str]:
    identifiers = {"utah:title": provision.title}
    if provision.chapter:
        identifiers["utah:chapter"] = provision.chapter
    if provision.part:
        identifiers["utah:part"] = provision.part
    if provision.kind == "section":
        identifiers["utah:section"] = provision.label
    return identifiers


def _metadata(provision: UtahProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source_authority": "Utah State Legislature",
        "source_document_id": provision.source.source_document_id,
    }
    if provision.effective_date:
        metadata["effective_date"] = provision.effective_date
    if provision.end_date:
        metadata["end_date"] = provision.end_date
    if provision.end_date_type:
        metadata["end_date_type"] = provision.end_date_type
    if provision.source_history:
        metadata["source_history"] = list(provision.source_history)
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    return metadata


def _kind_label_from_href(href: str) -> tuple[str, str] | None:
    parsed = urlparse(href)
    content_id = parse_qs(parsed.query).get("v", [None])[0]
    if not content_id:
        return None
    code = content_id.split("_", 1)[0].removeprefix("C")
    if "-S" in code:
        prefix, suffix = code.split("-S", 1)
        return "section", f"{prefix}-{suffix}"
    if "-P" in code:
        prefix, suffix = code.split("-P", 1)
        return "part", f"{prefix}-{suffix}"
    if "-" in code:
        return "chapter", code
    return "title", code


def _content_url_from_href(source_url: str, href: str, *, extension: str) -> str:
    absolute = urljoin(source_url, href)
    parsed = urlparse(absolute)
    content_id = parse_qs(parsed.query).get("v", [None])[0]
    if not content_id:
        content_id = Path(parsed.path).stem
    content_path = posixpath.join(posixpath.dirname(parsed.path), f"{content_id}.{extension}")
    return urlunparse((parsed.scheme, parsed.netloc, content_path, "", "", ""))


def _relative_from_url(source_url: str, source_format: str) -> str:
    path = unquote(urlparse(source_url).path).lstrip("/")
    path = path.split("xcode/", 1)[1] if "xcode/" in path else Path(path).name
    return f"{source_format}/{path}"


def _heading_from_cell(cell: Tag | None) -> str | None:
    if cell is None:
        return None
    text = _clean_text(cell.get_text(" "))
    text = _STATUS_RE.sub("", text)
    text = text.replace("()", "")
    return _clean_text(text) or None


def _row_status(cell: Tag | None) -> tuple[str | None, str | None]:
    if cell is None:
        return None, None
    match = _STATUS_RE.search(_clean_text(cell.get_text(" ")))
    if match is None:
        return None, None
    return match.group("kind").lower(), match.group("date")


def _status_is_active(status: str | None, status_date: str | None, *, expression_date: date) -> bool:
    parsed_date = _parse_utah_date(status_date)
    if status == "effective" and parsed_date is not None:
        return parsed_date <= expression_date
    if status == "superseded" and parsed_date is not None:
        return parsed_date > expression_date
    return True


def _status_effective_date(link: UtahLink) -> str | None:
    return link.status_date if link.status == "effective" else None


def _status_end_date(link: UtahLink) -> str | None:
    return link.status_date if link.status == "superseded" else None


def _status_end_date_type(link: UtahLink) -> str | None:
    return "SC" if link.status == "superseded" else None


def _is_active_node(element: ET.Element, *, expression_date: date) -> bool:
    effective_date = _parse_utah_date(_child_text(element, "effdate"))
    if effective_date is not None and effective_date > expression_date:
        return False
    end_date = _parse_utah_date(_child_text(element, "enddate"))
    return end_date is None or end_date > expression_date


def _retry_delay(exc: BaseException, *, attempt: int) -> float:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except ValueError:
                pass
        if exc.response.status_code == 429:
            return min(60.0, 5.0 * attempt)
        if exc.response.status_code >= 500:
            return min(90.0, 5.0 * attempt)
    return 0.5 * attempt


def _baseline_section_elements(
    xml_data: bytes,
    *,
    expression_date: date,
) -> dict[str, ET.Element]:
    root = ET.fromstring(xml_data)
    if _local_name(root.tag) != "code":
        raise ValueError(f"expected Utah Code baseline XML root <code>, got <{root.tag}>")
    sections: dict[str, ET.Element] = {}
    for section in root.iter("section"):
        label = _node_number(section)
        if label and _is_active_node(section, expression_date=expression_date):
            sections[label] = section
    return sections


def _can_use_baseline_section(
    target: _SectionTarget,
    baseline_sections: dict[str, ET.Element],
) -> bool:
    if target.link.label not in baseline_sections:
        return False
    return Path(urlparse(target.link.source_url).path).stem.endswith(
        f"_{UTAH_CODE_BASELINE_VERSION}"
    )


def _parse_utah_date(value: str | None) -> date | None:
    if not value:
        return None
    match = _DATE_RE.match(value.strip())
    if match is None:
        return None
    return date(
        int(match.group("year")),
        int(match.group("month")),
        int(match.group("day")),
    )


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _child_elements(element: ET.Element, tag: str) -> tuple[ET.Element, ...]:
    return tuple(child for child in list(element) if _local_name(child.tag) == tag)


def _child_text(element: ET.Element, tag: str) -> str | None:
    child = next((item for item in list(element) if _local_name(item.tag) == tag), None)
    if child is None:
        return None
    text = _clean_text(" ".join(child.itertext()))
    return text or None


def _child_attr(element: ET.Element, tag: str, attr: str) -> str | None:
    child = next((item for item in list(element) if _local_name(item.tag) == tag), None)
    if child is None:
        return None
    value = child.get(attr)
    return _clean_text(value) or None


def _node_number(element: ET.Element) -> str | None:
    value = element.get("number")
    return _clean_text(value) or None


def _subsection_suffix(number: str | None, *, section_label: str) -> str:
    if not number:
        return ""
    text = number.removeprefix(section_label)
    return text if text.startswith("(") else number


def _strip_subsection(label: str) -> str:
    return label.split("(", 1)[0]


def _normalize_title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower().startswith("title "):
        text = text.split(None, 1)[1]
    return text.upper()


def _utah_run_id(version: str, *, only_title: str | None, limit: int | None) -> str:
    parts = [version]
    if only_title:
        parts.append(f"title-{_path_segment(only_title)}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _path_segment(value: str) -> str:
    return re.sub(r"[^a-z0-9.]+", "-", value.lower()).strip("-")


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _html_text(html_data: str | bytes) -> str:
    if isinstance(html_data, bytes):
        return html_data.decode("utf-8", errors="replace")
    return html_data


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)


def _store_relative_path(store: CorpusArtifactStore, path: Path) -> str:
    try:
        return str(path.relative_to(store.root))
    except ValueError:
        return str(path)


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)
