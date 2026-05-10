"""Alabama Code source-first corpus adapter."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

ALABAMA_BASE_URL = "https://alison.legislature.state.al.us"
ALABAMA_GRAPHQL_URL = f"{ALABAMA_BASE_URL}/graphql"
ALABAMA_GRAPHQL_SOURCE_FORMAT = "alabama-code-graphql-json"
ALABAMA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"
ALABAMA_SECTION_PAGE_SIZE = 1000

_CODE_REFERENCE_RE = re.compile(
    r"(?:Section|Sections|\u00a7+)\s+(?P<section>\d+[A-Z]?-\d+[A-Z]?-[0-9A-Z.]+)",
    re.I,
)
_PREFIX_RE = re.compile(
    r"^(?P<kind>Title|Chapter|Article|Part|Division|Section)\s+"
    r"(?P<display_id>\S+)\s*(?P<heading>.*)$",
    re.I,
)

_SCAFFOLD_QUERY = """
query codeOfAlabamaScaffold {
  scaffold: codeOfAlabamaScaffold
}
"""

_TITLES_QUERY = """
query codeOfAlabamaTitles {
  titles: codeOfAlabamaTitles
}
"""

_SECTIONS_QUERY = """
query codeOfAlabamaSections($limit: Int, $offset: Int) {
  codesOfAlabama(
    where: { type: { eq: Section }, isContentNode: { eq: true } }
    limit: $limit
    offset: $offset
  ) {
    count
    data {
      id
      codeId
      displayId
      title
      content
      history
      parentId
      type
      isContentNode
      sectionRange
      effectiveDate
      supersessionDate
    }
  }
}
"""


@dataclass(frozen=True)
class AlabamaNode:
    """One official Code of Alabama hierarchy/content node."""

    code_id: str
    parent_id: str | None = None
    display_id: str | None = None
    title: str | None = None
    section_range: str | None = None
    effective_date: str | None = None
    supersession_date: str | None = None
    graph_id: str | None = None
    node_type: str | None = None
    is_content_node: bool | None = None
    content_html: str | None = None
    history: str | None = None
    source_path: str | None = None
    source_format: str | None = None
    source_sha256: str | None = None
    ordinal: int = 0
    level: int = 0
    citation_path: str | None = None
    parent_citation_path: str | None = None

    @property
    def kind(self) -> str:
        if self.node_type:
            return self.node_type.lower()
        if self.content_html is not None:
            return "section"
        parsed = _parse_title_parts(self.title or "", self.display_id)
        if parsed is not None:
            return parsed[0].lower()
        return "container"

    @property
    def heading(self) -> str | None:
        parsed = _parse_title_parts(self.title or "", self.display_id)
        heading = parsed[2] if parsed is not None else self.title or self.display_id
        heading = _strip_terminal_period(heading)
        return heading or None

    @property
    def legal_identifier(self) -> str:
        if self.kind == "section" and self.display_id:
            return f"Code of Ala. \u00a7 {self.display_id}"
        if self.kind == "title" and self.display_id:
            return f"Code of Ala. Title {self.display_id}"
        if self.kind == "chapter" and self.display_id:
            title = _ancestor_display_id(self, "title")
            if title:
                return f"Code of Ala. Title {title}, Chapter {self.display_id}"
        display = self.display_id or self.code_id
        return f"Code of Ala. {self.kind.title()} {display}"


@dataclass(frozen=True)
class _AlabamaSource:
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


class _AlabamaFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        graphql_url: str,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.graphql_url = graphql_url
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0

    def fetch_scaffold(self) -> _AlabamaSource:
        return self._fetch_graphql(
            "scaffold.json",
            query=_SCAFFOLD_QUERY,
            variables={},
        )

    def fetch_titles(self) -> _AlabamaSource:
        return self._fetch_graphql(
            "titles.json",
            query=_TITLES_QUERY,
            variables={},
        )

    def fetch_sections_page(self, *, offset: int, limit: int) -> _AlabamaSource:
        return self._fetch_graphql(
            f"sections-current-offset-{offset}-limit-{limit}.json",
            query=_SECTIONS_QUERY,
            variables={"offset": offset, "limit": limit},
        )

    def _fetch_graphql(
        self,
        name: str,
        *,
        query: str,
        variables: dict[str, Any],
    ) -> _AlabamaSource:
        relative_path = f"{ALABAMA_GRAPHQL_SOURCE_FORMAT}/{name}"
        if self.source_dir is not None:
            data = (self.source_dir / relative_path).read_bytes()
        elif self.download_dir is not None and (self.download_dir / relative_path).exists():
            data = (self.download_dir / relative_path).read_bytes()
        else:
            data = _download_alabama_graphql(
                self.graphql_url,
                query=query,
                variables=variables,
                fetcher=self,
                request_delay_seconds=self.request_delay_seconds,
                timeout_seconds=self.timeout_seconds,
                request_attempts=self.request_attempts,
            )
            if self.download_dir is not None:
                cached_path = self.download_dir / relative_path
                cached_path.parent.mkdir(parents=True, exist_ok=True)
                _write_cache_bytes(cached_path, data)
        return _AlabamaSource(
            relative_path=relative_path,
            source_url=self.graphql_url,
            source_format=ALABAMA_GRAPHQL_SOURCE_FORMAT,
            data=data,
        )

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        wait_seconds = self.request_delay_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self._last_request_at = time.monotonic()


def extract_alabama_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    graphql_url: str = ALABAMA_GRAPHQL_URL,
    request_delay_seconds: float = 0.05,
    timeout_seconds: float = 90.0,
    request_attempts: int = 3,
    page_size: int = ALABAMA_SECTION_PAGE_SIZE,
) -> StateStatuteExtractReport:
    """Snapshot the official ALISON GraphQL Code of Alabama and extract provisions."""
    jurisdiction = "us-al"
    title_filter = _title_filter(only_title)
    run_id = _alabama_run_id(version, title_filter=title_filter, limit=limit)
    source_as_of_text = source_as_of or str(version)
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _AlabamaFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        graphql_url=graphql_url,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    source_paths: list[Path] = []
    source_records: dict[str, _RecordedSource] = {}

    scaffold_source = fetcher.fetch_scaffold()
    scaffold_path, scaffold_record = _record_source(store, jurisdiction, run_id, scaffold_source)
    source_paths.append(scaffold_path)
    source_records[scaffold_source.relative_path] = scaffold_record

    titles_source = fetcher.fetch_titles()
    titles_path, titles_record = _record_source(store, jurisdiction, run_id, titles_source)
    source_paths.append(titles_path)
    source_records[titles_source.relative_path] = titles_record

    nodes_by_code_id = _nodes_from_scaffold_and_titles(
        _graphql_data(scaffold_source.data, "scaffold"),
        _graphql_data(titles_source.data, "titles"),
        titles_record=titles_record,
    )
    selected_section_ids: set[str] = set()
    total_sections: int | None = None
    fetched_sections = 0
    offset = 0
    effective_page_size = max(1, page_size)
    while total_sections is None or offset < total_sections:
        if limit is not None and fetched_sections >= limit:
            break
        current_limit = (
            min(effective_page_size, limit - fetched_sections)
            if limit is not None
            else effective_page_size
        )
        page_source = fetcher.fetch_sections_page(offset=offset, limit=current_limit)
        page_path, page_record = _record_source(store, jurisdiction, run_id, page_source)
        source_paths.append(page_path)
        source_records[page_source.relative_path] = page_record
        payload = _graphql_data(page_source.data, "codesOfAlabama")
        total_sections = int(payload.get("count") or 0)
        rows = payload.get("data") or []
        if not rows:
            break
        for row in rows:
            node = _node_from_section_row(row, page_record=page_record, ordinal=offset + 1)
            nodes_by_code_id[node.code_id] = _merge_alabama_node(
                nodes_by_code_id.get(node.code_id),
                node,
            )
            selected_section_ids.add(node.code_id)
            fetched_sections += 1
            offset += 1
        if len(rows) < current_limit:
            break

    nodes = _prepare_alabama_nodes(nodes_by_code_id)
    full_corpus = (
        limit is None
        and title_filter is None
        and total_sections is not None
        and len(selected_section_ids) >= total_sections
    )
    nodes = _select_alabama_nodes(
        nodes,
        selected_section_ids,
        title_filter=title_filter,
        full_corpus=full_corpus,
    )

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    errors: list[str] = []

    for node in nodes:
        if node.citation_path is None:
            errors.append(f"node {node.code_id}: missing citation path")
            continue
        citation_path = node.citation_path
        metadata = _alabama_metadata(node)
        if citation_path in seen:
            if node.kind != "section":
                continue
            metadata["canonical_citation_path"] = citation_path
            citation_path = f"{citation_path}@code-{node.code_id}"
            if citation_path in seen:
                continue
        seen.add(citation_path)
        source_path = node.source_path or titles_record.source_path
        source_format = node.source_format or titles_record.source_format
        source_sha256 = node.source_sha256 or titles_record.sha256
        if node.kind == "section":
            section_count += 1
        elif node.kind == "title":
            title_count += 1
        else:
            container_count += 1
        _append_record(
            items,
            records,
            jurisdiction=jurisdiction,
            citation_path=citation_path,
            version=run_id,
            source_url=_alabama_source_url(node),
            source_path=source_path,
            source_format=source_format,
            source_id=node.code_id,
            sha256=source_sha256,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            kind=node.kind,
            body=_content_text(node.content_html),
            heading=node.heading,
            legal_identifier=node.legal_identifier,
            parent_citation_path=node.parent_citation_path,
            level=node.level,
            ordinal=node.ordinal,
            identifiers=_alabama_identifiers(node),
            metadata=metadata,
        )

    if not records:
        raise ValueError("no Alabama provisions extracted")

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


def parse_alabama_deflated_table(value: str) -> tuple[dict[str, str | None], ...]:
    """Parse ALISON's compact table encoding used by scaffold/title queries."""
    if len(value) < 2:
        return ()
    column_separator = value[0]
    row_separator = value[1]
    rows = [row.split(column_separator) for row in value[2:].split(row_separator) if row]
    if not rows:
        return ()
    headers = rows[0]
    parsed: list[dict[str, str | None]] = []
    for row in rows[1:]:
        parsed.append(
            {
                header: (row[index] if index < len(row) and row[index] != "" else None)
                for index, header in enumerate(headers)
            }
        )
    return tuple(parsed)


def _nodes_from_scaffold_and_titles(
    scaffold: str,
    titles: str,
    *,
    titles_record: _RecordedSource,
) -> dict[str, AlabamaNode]:
    nodes: dict[str, AlabamaNode] = {}
    for ordinal, row in enumerate(parse_alabama_deflated_table(scaffold), start=1):
        code_id = row.get("codeId")
        if not code_id:
            continue
        nodes[code_id] = AlabamaNode(
            code_id=code_id,
            parent_id=row.get("parentId"),
            display_id=row.get("displayId"),
            ordinal=ordinal,
            source_path=titles_record.source_path,
            source_format=titles_record.source_format,
            source_sha256=titles_record.sha256,
        )
    for row in parse_alabama_deflated_table(titles):
        code_id = row.get("codeId")
        if not code_id:
            continue
        existing = nodes.get(code_id) or AlabamaNode(code_id=code_id)
        title_text = row.get("title") or existing.title
        parsed_title = _parse_title_parts(title_text or "", existing.display_id)
        nodes[code_id] = replace(
            existing,
            display_id=existing.display_id or (parsed_title[1] if parsed_title else None),
            title=title_text,
            section_range=row.get("sectionRange") or existing.section_range,
            effective_date=row.get("effectiveDate") or existing.effective_date,
        )
    return nodes


def _node_from_section_row(
    row: dict[str, Any],
    *,
    page_record: _RecordedSource,
    ordinal: int,
) -> AlabamaNode:
    return AlabamaNode(
        code_id=str(row["codeId"]),
        graph_id=_optional_text(row.get("id")),
        parent_id=_optional_text(row.get("parentId")),
        display_id=_optional_text(row.get("displayId")),
        title=_optional_text(row.get("title")),
        content_html=_optional_text(row.get("content")),
        history=_optional_text(row.get("history")),
        node_type=_optional_text(row.get("type")),
        is_content_node=bool(row.get("isContentNode")),
        section_range=_optional_text(row.get("sectionRange")),
        effective_date=_optional_text(row.get("effectiveDate")),
        supersession_date=_optional_text(row.get("supersessionDate")),
        source_path=page_record.source_path,
        source_format=page_record.source_format,
        source_sha256=page_record.sha256,
        ordinal=ordinal,
    )


def _merge_alabama_node(
    existing: AlabamaNode | None,
    incoming: AlabamaNode,
) -> AlabamaNode:
    if existing is None:
        return incoming
    return replace(
        existing,
        graph_id=incoming.graph_id or existing.graph_id,
        parent_id=incoming.parent_id or existing.parent_id,
        display_id=incoming.display_id or existing.display_id,
        title=incoming.title or existing.title,
        section_range=incoming.section_range or existing.section_range,
        effective_date=incoming.effective_date or existing.effective_date,
        supersession_date=incoming.supersession_date or existing.supersession_date,
        node_type=incoming.node_type or existing.node_type,
        is_content_node=incoming.is_content_node
        if incoming.is_content_node is not None
        else existing.is_content_node,
        content_html=incoming.content_html or existing.content_html,
        history=incoming.history or existing.history,
        source_path=incoming.source_path or existing.source_path,
        source_format=incoming.source_format or existing.source_format,
        source_sha256=incoming.source_sha256 or existing.source_sha256,
        ordinal=existing.ordinal or incoming.ordinal,
    )


def _prepare_alabama_nodes(nodes_by_code_id: dict[str, AlabamaNode]) -> list[AlabamaNode]:
    prepared: dict[str, AlabamaNode] = {}

    def prepare(node: AlabamaNode) -> AlabamaNode:
        if node.code_id in prepared:
            return prepared[node.code_id]
        parent: AlabamaNode | None = None
        if node.parent_id and node.parent_id in nodes_by_code_id:
            parent = prepare(nodes_by_code_id[node.parent_id])
        level = (parent.level + 1) if parent else 0
        citation_path = _alabama_citation_path(node, parent)
        updated = replace(
            node,
            level=level,
            citation_path=citation_path,
            parent_citation_path=parent.citation_path if parent else None,
        )
        prepared[node.code_id] = updated
        return updated

    for node in nodes_by_code_id.values():
        prepare(node)
    return sorted(prepared.values(), key=lambda node: node.ordinal)


def _select_alabama_nodes(
    nodes: list[AlabamaNode],
    section_ids: set[str],
    *,
    title_filter: str | None,
    full_corpus: bool,
) -> list[AlabamaNode]:
    by_code_id = {node.code_id: node for node in nodes}
    if full_corpus:
        return [node for node in nodes if node.title or node.content_html]
    selected: set[str] = set()

    def include_ancestors(node: AlabamaNode) -> None:
        selected.add(node.code_id)
        if node.parent_id and node.parent_id in by_code_id:
            include_ancestors(by_code_id[node.parent_id])

    for node in nodes:
        if node.kind == "section" and node.code_id in section_ids:
            include_ancestors(node)
    if title_filter is not None:
        selected = {
            code_id
            for code_id in selected
            if _node_title_display(by_code_id[code_id]) == title_filter
            or _has_title_ancestor(by_code_id[code_id], by_code_id, title_filter)
        }
    return [node for node in nodes if node.code_id in selected and (node.title or node.content_html)]


def _section_node_count(nodes: list[AlabamaNode]) -> int:
    return sum(1 for node in nodes if node.kind == "section" and node.content_html is not None)


def _alabama_citation_path(node: AlabamaNode, parent: AlabamaNode | None) -> str:
    if node.kind == "section" and node.display_id:
        return f"us-al/statute/{node.display_id}"
    token = _alabama_path_token(node)
    if parent is None or node.kind == "title":
        return f"us-al/statute/{token}"
    return f"{parent.citation_path}/{token}"


def _alabama_path_token(node: AlabamaNode) -> str:
    display = node.display_id or node.code_id
    if node.kind == "title":
        return f"title-{_slug(display)}"
    return f"{node.kind}-{_slug(display)}"


def _alabama_metadata(node: AlabamaNode) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": node.kind,
        "code_id": node.code_id,
    }
    optional: dict[str, Any] = {
        "graph_id": node.graph_id,
        "parent_code_id": node.parent_id,
        "display_id": node.display_id,
        "section_range": node.section_range,
        "effective_date": node.effective_date,
        "supersession_date": node.supersession_date,
        "status": _alabama_status(node),
    }
    metadata.update({key: value for key, value in optional.items() if value})
    references = _alabama_references_to(node)
    if references:
        metadata["references_to"] = list(references)
    if node.history:
        metadata["source_history"] = [node.history]
    return metadata


def _alabama_identifiers(node: AlabamaNode) -> dict[str, str]:
    identifiers = {"alabama:code_id": node.code_id}
    if node.display_id:
        identifiers[f"alabama:{node.kind}"] = node.display_id
    title = _ancestor_display_id(node, "title")
    if title:
        identifiers["alabama:title"] = title
    return identifiers


def _alabama_references_to(node: AlabamaNode) -> tuple[str, ...]:
    if node.display_id is None:
        return ()
    text = "\n".join(part for part in [node.content_html, node.history] if part)
    refs: list[str] = []
    for match in _CODE_REFERENCE_RE.finditer(text):
        section = match.group("section")
        if section != node.display_id:
            refs.append(f"us-al/statute/{section}")
    return tuple(_dedupe_preserve_order(refs))


def _alabama_status(node: AlabamaNode) -> str | None:
    text = " ".join(part for part in [node.title, node.content_html, node.history] if part)
    return "repealed" if "repealed" in text.lower() else None


def _alabama_source_url(node: AlabamaNode) -> str:
    if node.kind == "section" and node.display_id:
        url = f"{ALABAMA_BASE_URL}/code-of-alabama?section={node.display_id}"
        if node.effective_date:
            return f"{url}&version={node.effective_date}"
        return url
    return f"{ALABAMA_BASE_URL}/code-of-alabama"


def _content_text(content_html: str | None) -> str | None:
    if not content_html:
        return None
    soup = BeautifulSoup(content_html, "lxml")
    paragraphs = [
        _clean_text(paragraph)
        for paragraph in soup.find_all("p")
        if _clean_text(paragraph)
    ]
    if paragraphs:
        return "\n".join(paragraphs)
    text = _clean_text(soup)
    return text or None


def _parse_title_parts(
    title: str,
    display_id: str | None,
) -> tuple[str, str | None, str] | None:
    match = _PREFIX_RE.match(title.strip())
    if match is None:
        return None
    heading = match.group("heading").strip()
    return match.group("kind").title(), match.group("display_id") or display_id, heading


def _node_title_display(node: AlabamaNode) -> str | None:
    if node.kind == "title":
        return node.display_id
    return _ancestor_display_id(node, "title")


def _has_title_ancestor(
    node: AlabamaNode,
    nodes_by_code_id: dict[str, AlabamaNode],
    title_filter: str,
) -> bool:
    if node.kind == "title":
        return node.display_id == title_filter
    if node.parent_id and node.parent_id in nodes_by_code_id:
        return _has_title_ancestor(nodes_by_code_id[node.parent_id], nodes_by_code_id, title_filter)
    return False


def _ancestor_display_id(node: AlabamaNode, kind: str) -> str | None:
    if node.kind == kind:
        return node.display_id
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
    source: _AlabamaSource,
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


def _download_alabama_graphql(
    source_url: str,
    *,
    query: str,
    variables: dict[str, Any],
    fetcher: _AlabamaFetcher,
    request_delay_seconds: float,
    timeout_seconds: float,
    request_attempts: int,
) -> bytes:
    last_error: requests.RequestException | None = None
    for attempt in range(1, request_attempts + 1):
        try:
            fetcher.wait_for_request_slot()
            response = requests.post(
                source_url,
                json={"query": query, "variables": variables},
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": ALABAMA_USER_AGENT,
                },
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("errors"):
                raise ValueError(payload["errors"])
            return json.dumps(payload, sort_keys=True).encode("utf-8")
        except requests.RequestException as exc:  # pragma: no cover
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(request_delay_seconds, 0.25) * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError("Alabama GraphQL request failed")


def _graphql_data(data: bytes, key: str) -> Any:
    payload = json.loads(data)
    if payload.get("errors"):
        raise ValueError(payload["errors"])
    data_payload = payload.get("data")
    if not isinstance(data_payload, dict) or key not in data_payload:
        raise ValueError(f"Alabama GraphQL response missing data.{key}")
    return data_payload[key]


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _alabama_run_id(version: str, *, title_filter: str | None, limit: int | None) -> str:
    if title_filter is None and limit is None:
        return version
    parts = [version, "us-al"]
    if title_filter is not None:
        parts.append(f"title-{title_filter.lower()}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title|Title)[-\s]*", "", text)
    return text.upper() or None


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _strip_terminal_period(value: str) -> str:
    return value.strip().removesuffix(".").strip()


def _clean_text(value: Any) -> str:
    text = value.get_text(" ", strip=True) if hasattr(value, "get_text") else str(value)
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "node"


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
