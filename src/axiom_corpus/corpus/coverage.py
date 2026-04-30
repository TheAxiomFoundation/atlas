"""Coverage accounting from source inventory to normalized provisions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem


def _duplicates(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    dupes: set[str] = set()
    for value in values:
        if value in seen:
            dupes.add(value)
        seen.add(value)
    return tuple(sorted(dupes))


@dataclass(frozen=True)
class ProvisionCoverageReport:
    jurisdiction: str
    document_class: str
    version: str
    source_count: int
    provision_count: int
    matched_count: int
    missing_from_provisions: tuple[str, ...]
    extra_provisions: tuple[str, ...]
    duplicate_source_citations: tuple[str, ...] = ()
    duplicate_provision_citations: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return (
            self.source_count > 0
            and not self.missing_from_provisions
            and not self.extra_provisions
            and not self.duplicate_source_citations
            and not self.duplicate_provision_citations
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "jurisdiction": self.jurisdiction,
            "document_class": self.document_class,
            "version": self.version,
            "complete": self.complete,
            "source_count": self.source_count,
            "provision_count": self.provision_count,
            "matched_count": self.matched_count,
            "missing_from_provisions": list(self.missing_from_provisions),
            "extra_provisions": list(self.extra_provisions),
            "duplicate_source_citations": list(self.duplicate_source_citations),
            "duplicate_provision_citations": list(self.duplicate_provision_citations),
        }


def compare_provision_coverage(
    source_inventory: tuple[SourceInventoryItem, ...],
    provisions: tuple[ProvisionRecord, ...],
    jurisdiction: str,
    document_class: str,
    version: str,
) -> ProvisionCoverageReport:
    source_paths = [item.citation_path for item in source_inventory]
    provision_paths = [record.citation_path for record in provisions]
    source_set = set(source_paths)
    provision_set = set(provision_paths)
    return ProvisionCoverageReport(
        jurisdiction=jurisdiction,
        document_class=document_class,
        version=version,
        source_count=len(source_inventory),
        provision_count=len(provisions),
        matched_count=len(source_set & provision_set),
        missing_from_provisions=tuple(sorted(source_set - provision_set)),
        extra_provisions=tuple(sorted(provision_set - source_set)),
        duplicate_source_citations=_duplicates(source_paths),
        duplicate_provision_citations=_duplicates(provision_paths),
    )
