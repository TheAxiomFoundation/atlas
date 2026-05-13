"""Build the precomputed navigation index from `corpus.provisions` rows.

The app browses the legal corpus as a tree. Until now it has assembled tree
nodes live by issuing prefix `LIKE` queries against `corpus.provisions`. As
state corpora grow those queries occasionally hit Supabase's statement
timeout. The navigation index is a derived parent/child serving index that
moves the hierarchy work offline so app navigation can be a simple indexed
`parent_path` lookup.

`corpus.provisions` remains the source of truth for legal text. Rows here are
fully derivable from a snapshot of `corpus.provisions` plus the same
hierarchy rules the app would otherwise reconstruct at request time:

* If a record has `parent_citation_path` and that parent exists in the input
  set, that wins.
* Otherwise we walk path-segment prefixes upward and link to the nearest
  ancestor that exists in the input set.
* Otherwise the row is a top-level navigation root in its
  (jurisdiction, doc_type) scope.

This avoids "pulling apart" provisions that are not real corpus child nodes:
a synthetic intermediate is never invented. A node's segment, label, and
sort_key are all deterministic given the input, so repeated builds produce
identical rows.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from axiom_corpus.corpus.models import ProvisionRecord
from axiom_corpus.corpus.supabase import deterministic_provision_id

NAVIGATION_NODES_COLUMNS: tuple[str, ...] = (
    "id",
    "jurisdiction",
    "doc_type",
    "path",
    "parent_path",
    "segment",
    "label",
    "sort_key",
    "depth",
    "provision_id",
    "citation_path",
    "version",
    "has_children",
    "child_count",
    "has_rulespec",
    "encoded_descendant_count",
    "status",
)


@dataclass(frozen=True)
class NavigationNode:
    """Row to be upserted into `corpus.navigation_nodes`."""

    id: str
    jurisdiction: str
    doc_type: str
    path: str
    parent_path: str | None
    segment: str
    label: str
    sort_key: str
    depth: int
    provision_id: str | None
    citation_path: str | None
    version: str | None = None
    has_children: bool = False
    child_count: int = 0
    has_rulespec: bool = False
    encoded_descendant_count: int = 0
    status: str | None = None

    def to_supabase_row(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "jurisdiction": self.jurisdiction,
            "doc_type": self.doc_type,
            "path": self.path,
            "parent_path": self.parent_path,
            "segment": self.segment,
            "label": self.label,
            "sort_key": self.sort_key,
            "depth": self.depth,
            "provision_id": self.provision_id,
            "citation_path": self.citation_path,
            "version": self.version,
            "has_children": self.has_children,
            "child_count": self.child_count,
            "has_rulespec": self.has_rulespec,
            "encoded_descendant_count": self.encoded_descendant_count,
            "status": self.status,
        }


def deterministic_navigation_id(path: str, version: str | None = None) -> str:
    """Return the stable UUID for a navigation node keyed by path and version."""
    normalized_version = str(version or "").strip()
    if not normalized_version:
        return str(uuid5(NAMESPACE_URL, f"axiom-navigation:{path}"))
    identity = json.dumps(
        ["axiom-navigation", normalized_version, path],
        separators=(",", ":"),
    )
    return str(uuid5(NAMESPACE_URL, identity))


def build_navigation_nodes(
    records: Iterable[ProvisionRecord],
    *,
    jurisdiction: str | None = None,
    document_class: str | None = None,
    encoded_paths: Iterable[str] | None = None,
) -> tuple[NavigationNode, ...]:
    """Project provision records into `corpus.navigation_nodes` rows.

    The returned tuple is sorted by `(parent_path, sort_key, path)` so repeated
    runs on identical input produce byte-identical output, regardless of the
    order in which the source provisions were emitted.

    Optional `jurisdiction` / `document_class` filters mirror the scope flags
    on the CLI: callers that want the full corpus pass them as ``None``.

    ``encoded_paths`` augments ``record.has_rulespec`` from an external source
    (typically the jurisdiction's `rulespec-*` repo, see
    ``axiom_corpus.corpus.rulespec_paths``). A node whose path is in the set
    is treated as encoded even when the corresponding provision row has
    ``has_rulespec=False``. Ancestor ``encoded_descendant_count`` then
    propagates from those augmented values, so encoded-only browsing is
    discoverable from the top of the tree.
    """
    filtered: list[ProvisionRecord] = []
    seen_paths: set[str] = set()
    for record in records:
        if jurisdiction is not None and record.jurisdiction != jurisdiction:
            continue
        if document_class is not None and record.document_class != document_class:
            continue
        if record.citation_path in seen_paths:
            # Provisions JSONL should be unique per citation_path, but be
            # defensive: collapse duplicates rather than emitting two nodes.
            continue
        seen_paths.add(record.citation_path)
        filtered.append(record)

    encoded_set: set[str] = set(encoded_paths) if encoded_paths is not None else set()

    by_path: dict[str, ProvisionRecord] = {r.citation_path: r for r in filtered}

    parent_paths: dict[str, str | None] = {}
    for record in filtered:
        parent_paths[record.citation_path] = _resolve_parent_path(record, by_path)

    _break_parent_cycles(parent_paths)

    depths = _resolve_depths(parent_paths)

    nodes: dict[str, NavigationNode] = {}
    for record in filtered:
        path = record.citation_path
        parent_path = parent_paths[path]
        segment = _segment(path, parent_path)
        nodes[path] = NavigationNode(
            id=deterministic_navigation_id(path, record.version),
            jurisdiction=record.jurisdiction,
            doc_type=record.document_class,
            path=path,
            parent_path=parent_path,
            segment=segment,
            label=_label_for(record, segment),
            sort_key=_sort_key(record, segment),
            depth=depths[path],
            provision_id=_provision_id_for_navigation(record),
            citation_path=path,
            version=record.version,
            has_rulespec=bool(record.has_rulespec) or path in encoded_set,
            status=_status_for(record),
        )

    children_by_parent: dict[str, list[NavigationNode]] = defaultdict(list)
    for node in nodes.values():
        if node.parent_path is not None and node.parent_path in nodes:
            children_by_parent[node.parent_path].append(node)

    encoded_descendants: dict[str, int] = defaultdict(int)
    for node in sorted(nodes.values(), key=lambda n: -n.depth):
        own = (1 if node.has_rulespec else 0) + encoded_descendants[node.path]
        if node.parent_path is not None and node.parent_path in nodes:
            encoded_descendants[node.parent_path] += own

    finalized = [
        replace(
            node,
            has_children=bool(children_by_parent.get(node.path)),
            child_count=len(children_by_parent.get(node.path, ())),
            encoded_descendant_count=encoded_descendants[node.path],
        )
        for node in nodes.values()
    ]
    return tuple(
        sorted(
            finalized,
            key=lambda n: (n.parent_path or "", n.sort_key, n.path),
        )
    )


def _provision_id_for_navigation(record: ProvisionRecord) -> str:
    legacy_id = deterministic_provision_id(record.citation_path)
    if record.version and (record.id is None or record.id == legacy_id):
        return deterministic_provision_id(record.citation_path, record.version)
    return record.id or legacy_id


def group_nodes_by_scope(
    nodes: Iterable[NavigationNode],
) -> dict[tuple[str, str, str | None], tuple[NavigationNode, ...]]:
    """Group navigation rows by ``(jurisdiction, doc_type, version)``.

    Used by the writer to scope deletes during rebuilds without disturbing
    unrelated jurisdictions, document classes, or source versions.
    """
    grouped: dict[tuple[str, str, str | None], list[NavigationNode]] = defaultdict(list)
    for node in nodes:
        grouped[(node.jurisdiction, node.doc_type, node.version)].append(node)
    return {key: tuple(values) for key, values in grouped.items()}


def _resolve_parent_path(
    record: ProvisionRecord,
    by_path: dict[str, ProvisionRecord],
) -> str | None:
    explicit = record.parent_citation_path
    if explicit and explicit != record.citation_path and explicit in by_path:
        return explicit
    parts = record.citation_path.split("/")
    for size in range(len(parts) - 1, 0, -1):
        candidate = "/".join(parts[:size])
        if candidate in by_path and candidate != record.citation_path:
            return candidate
    return None


def _break_parent_cycles(parent_paths: dict[str, str | None]) -> None:
    """Promote any node that participates in a parent cycle to a root.

    `_resolve_parent_path` rejects self-edges, but a record A whose parent is B
    while B's parent is A would still produce a two-cycle. Walking the chain
    here catches that and any longer cycle. One stable member of each actual
    cycle has its `parent_path` cleared so descendants outside the cycle remain
    attached and repeated builds produce the same rows regardless of input
    order.
    """
    for start in sorted(parent_paths):
        chain: list[str] = []
        seen_at: dict[str, int] = {}
        cursor: str | None = start
        while cursor is not None and cursor in parent_paths:
            if cursor in seen_at:
                cycle_nodes = chain[seen_at[cursor] :]
                parent_paths[min(cycle_nodes)] = None
                break
            seen_at[cursor] = len(chain)
            chain.append(cursor)
            cursor = parent_paths.get(cursor)


def _resolve_depths(parent_paths: dict[str, str | None]) -> dict[str, int]:
    depths: dict[str, int] = {}

    def depth_of(path: str, stack: tuple[str, ...] = ()) -> int:
        if path in depths:
            return depths[path]
        if path in stack:
            # Defensive: parent cycles in the dataset would otherwise recurse
            # forever. Treat the cycle entry as a root.
            depths[path] = 0
            return 0
        parent = parent_paths.get(path)
        if parent is None or parent not in parent_paths:
            depths[path] = 0
            return 0
        value = depth_of(parent, stack + (path,)) + 1
        depths[path] = value
        return value

    for path in parent_paths:
        depth_of(path)
    return depths


def _segment(path: str, parent_path: str | None) -> str:
    if parent_path and path.startswith(parent_path + "/"):
        return path[len(parent_path) + 1 :]
    if "/" in path:
        return path.rsplit("/", 1)[-1]
    return path


def _label_for(record: ProvisionRecord, segment: str) -> str:
    for candidate in (record.heading, record.citation_label):
        if candidate:
            text = candidate.strip()
            if text:
                return text
    return segment


def _status_for(record: ProvisionRecord) -> str | None:
    if record.metadata:
        status = record.metadata.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip()
    return None


_SORT_NUMERIC_RUN = re.compile(r"(\d+)")
_SORT_PAD_WIDTH = 12


def _sort_key(record: ProvisionRecord, segment: str) -> str:
    """Return a natural-order sort key.

    Falling back to a derivation that already exists in `corpus.provisions`:
    `level` orders peer groups, and within a group the segment's numeric runs
    are zero-padded so 2 < 10 even when compared lexicographically. The
    leading ordinal slot keeps explicit `ordinal` values authoritative when
    set.
    """
    ordinal_slot = (
        f"{record.ordinal:08d}"
        if isinstance(record.ordinal, int) and record.ordinal >= 0
        else "z" * 8
    )
    normalized = _normalize_sort_segment(segment)
    return f"{ordinal_slot}|{normalized}"


def _normalize_sort_segment(segment: str) -> str:
    lowered = segment.lower()
    return _SORT_NUMERIC_RUN.sub(_pad_match, lowered)


def _pad_match(match: re.Match[str]) -> str:
    digits = match.group(1)
    return digits.rjust(_SORT_PAD_WIDTH, "0")
