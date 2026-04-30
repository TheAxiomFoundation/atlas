"""Analytics over source inventories, normalized provisions, and Supabase counts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.models import (
    DocumentClass,
    ProvisionRecord,
    SourceInventoryItem,
)

DEFAULT_LEGACY_COUNT_DOCUMENT_CLASS = DocumentClass.STATUTE.value


@dataclass(frozen=True)
class AnalyticsRow:
    jurisdiction: str
    document_class: str
    version: str
    source_count: int = 0
    provision_count: int = 0
    matched_count: int = 0
    missing_count: int = 0
    extra_count: int = 0
    duplicate_source_count: int = 0
    duplicate_provision_count: int = 0
    coverage_complete: bool = False
    supabase_count: int = 0

    def to_mapping(self) -> dict[str, Any]:
        return {
            "jurisdiction": self.jurisdiction,
            "document_class": self.document_class,
            "version": self.version,
            "source_count": self.source_count,
            "provision_count": self.provision_count,
            "matched_count": self.matched_count,
            "missing_count": self.missing_count,
            "extra_count": self.extra_count,
            "duplicate_source_count": self.duplicate_source_count,
            "duplicate_provision_count": self.duplicate_provision_count,
            "coverage_complete": self.coverage_complete,
            "supabase_count": self.supabase_count,
        }


@dataclass(frozen=True)
class AnalyticsReport:
    version: str
    rows: tuple[AnalyticsRow, ...]

    def totals_by_document_class(self) -> dict[str, dict[str, int]]:
        totals: dict[str, dict[str, int]] = {}
        for row in self.rows:
            bucket = totals.setdefault(
                row.document_class,
                {
                    "source_count": 0,
                    "provision_count": 0,
                    "matched_count": 0,
                    "missing_count": 0,
                    "extra_count": 0,
                    "duplicate_source_count": 0,
                    "duplicate_provision_count": 0,
                    "supabase_count": 0,
                    "complete_count": 0,
                    "incomplete_count": 0,
                },
            )
            bucket["source_count"] += row.source_count
            bucket["provision_count"] += row.provision_count
            bucket["matched_count"] += row.matched_count
            bucket["missing_count"] += row.missing_count
            bucket["extra_count"] += row.extra_count
            bucket["duplicate_source_count"] += row.duplicate_source_count
            bucket["duplicate_provision_count"] += row.duplicate_provision_count
            bucket["supabase_count"] += row.supabase_count
            if row.coverage_complete:
                bucket["complete_count"] += 1
            else:
                bucket["incomplete_count"] += 1
        return dict(sorted(totals.items()))

    def totals(self) -> dict[str, int]:
        return {
            "source_count": sum(row.source_count for row in self.rows),
            "provision_count": sum(row.provision_count for row in self.rows),
            "matched_count": sum(row.matched_count for row in self.rows),
            "missing_count": sum(row.missing_count for row in self.rows),
            "extra_count": sum(row.extra_count for row in self.rows),
            "duplicate_source_count": sum(row.duplicate_source_count for row in self.rows),
            "duplicate_provision_count": sum(row.duplicate_provision_count for row in self.rows),
            "supabase_count": sum(row.supabase_count for row in self.rows),
            "complete_count": sum(1 for row in self.rows if row.coverage_complete),
            "incomplete_count": sum(1 for row in self.rows if not row.coverage_complete),
        }

    def to_mapping(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "totals": self.totals(),
            "totals_by_document_class": self.totals_by_document_class(),
            "rows": [row.to_mapping() for row in self.rows],
        }


def load_provision_count_snapshot(
    path: str | Path | None,
    *,
    legacy_document_class: str = DEFAULT_LEGACY_COUNT_DOCUMENT_CLASS,
) -> dict[tuple[str, str], int]:
    if path is None:
        return {}
    data = json.loads(Path(path).read_text())
    if isinstance(data, dict):
        rows = data.get("rows") or data.get("items")
        if isinstance(rows, list):
            return _counts_from_rows(rows, legacy_document_class=legacy_document_class)
        return {
            (str(jurisdiction), legacy_document_class): int(count)
            for jurisdiction, count in data.items()
        }
    if isinstance(data, list):
        return _counts_from_rows(data, legacy_document_class=legacy_document_class)
    raise ValueError("count snapshot must be a JSON object or list")


def build_analytics_report(
    store: CorpusArtifactStore,
    *,
    version: str,
    provision_counts: dict[tuple[str, str], int] | None = None,
    jurisdictions: tuple[str, ...] = (),
    document_classes: tuple[str, ...] = (),
) -> AnalyticsReport:
    inventories = _load_inventory_items(store, version)
    provisions = _load_provision_records(store, version)
    count_rows = provision_counts or {}
    keys = set(inventories)
    keys.update(provisions)
    keys.update(count_rows)

    if jurisdictions:
        jurisdiction_set = set(jurisdictions)
        keys = {key for key in keys if key[0] in jurisdiction_set}
    if document_classes:
        class_set = set(document_classes)
        keys = {key for key in keys if key[1] in class_set}

    rows = []
    for key in sorted(keys):
        jurisdiction, document_class = key
        source_inventory = inventories.get(key, ())
        provision_records = provisions.get(key, ())
        coverage = compare_provision_coverage(
            source_inventory,
            provision_records,
            jurisdiction=jurisdiction,
            document_class=document_class,
            version=version,
        )
        rows.append(
            AnalyticsRow(
                jurisdiction=jurisdiction,
                document_class=document_class,
                version=version,
                source_count=coverage.source_count,
                provision_count=coverage.provision_count,
                matched_count=coverage.matched_count,
                missing_count=len(coverage.missing_from_provisions),
                extra_count=len(coverage.extra_provisions),
                duplicate_source_count=len(coverage.duplicate_source_citations),
                duplicate_provision_count=len(coverage.duplicate_provision_citations),
                coverage_complete=coverage.complete,
                supabase_count=count_rows.get(key, 0),
            )
        )
    return AnalyticsReport(version=version, rows=tuple(rows))


def _counts_from_rows(
    rows: list[dict[str, Any]],
    *,
    legacy_document_class: str,
) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        jurisdiction = row.get("jurisdiction")
        if not jurisdiction:
            continue
        document_class = (
            row.get("document_class")
            or row.get("doc_type")
            or row.get("document_type")
            or legacy_document_class
        )
        count = row.get("count", row.get("provision_count", row.get("section_count", 0)))
        key = (str(jurisdiction), str(document_class))
        counts[key] = counts.get(key, 0) + int(count)
    return counts


def _load_inventory_items(
    store: CorpusArtifactStore,
    version: str,
) -> dict[tuple[str, str], tuple[SourceInventoryItem, ...]]:
    root = store.root / "inventory"
    inventories: dict[tuple[str, str], tuple[SourceInventoryItem, ...]] = {}
    if not root.exists():
        return inventories
    exact_keys: set[tuple[str, str]] = set()
    for path in sorted(root.glob(f"*/*/{version}.json")):
        document_class = path.parent.name
        jurisdiction = path.parent.parent.name
        key = (jurisdiction, document_class)
        inventories[key] = load_source_inventory(path)
        exact_keys.add(key)

    scoped_items: dict[tuple[str, str], dict[str, SourceInventoryItem]] = {}
    for path in sorted(root.glob(f"*/*/{version}-*.json")):
        document_class = path.parent.name
        jurisdiction = path.parent.parent.name
        key = (jurisdiction, document_class)
        if key in exact_keys:
            continue
        by_citation = scoped_items.setdefault(key, {})
        for item in load_source_inventory(path):
            by_citation.setdefault(item.citation_path, item)

    for key, by_citation in scoped_items.items():
        inventories[key] = tuple(
            by_citation[citation_path] for citation_path in sorted(by_citation)
        )
    return inventories


def _load_provision_records(
    store: CorpusArtifactStore,
    version: str,
) -> dict[tuple[str, str], tuple[ProvisionRecord, ...]]:
    root = store.root / "provisions"
    records: dict[tuple[str, str], tuple[ProvisionRecord, ...]] = {}
    if not root.exists():
        return records
    exact_keys: set[tuple[str, str]] = set()
    for path in sorted(root.glob(f"*/*/{version}.jsonl")):
        document_class = path.parent.name
        jurisdiction = path.parent.parent.name
        key = (jurisdiction, document_class)
        records[key] = load_provisions(path)
        exact_keys.add(key)

    scoped_records: dict[tuple[str, str], dict[str, ProvisionRecord]] = {}
    for path in sorted(root.glob(f"*/*/{version}-*.jsonl")):
        document_class = path.parent.name
        jurisdiction = path.parent.parent.name
        key = (jurisdiction, document_class)
        if key in exact_keys:
            continue
        by_citation = scoped_records.setdefault(key, {})
        for record in load_provisions(path):
            by_citation[record.citation_path] = record

    for key, by_citation in scoped_records.items():
        records[key] = tuple(by_citation[citation_path] for citation_path in sorted(by_citation))
    return records
