"""Completion reporting for production regulation ingestion."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from axiom_corpus.corpus.analytics import load_provision_count_snapshot
from axiom_corpus.corpus.r2 import ArtifactReport, ArtifactScopeRow
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope
from axiom_corpus.corpus.state_statute_completion import (
    US_STATE_STATUTE_JURISDICTIONS,
    StateStatuteCompletionRow,
    StateStatuteCompletionStatus,
    StateStatuteJurisdiction,
    _build_completion_row,
    _load_validation_report_state,
    _supabase_count,
)

REGULATION_DOCUMENT_CLASS = "regulation"

RegulationCompletionStatus = StateStatuteCompletionStatus
RegulationJurisdiction = StateStatuteJurisdiction
RegulationCompletionRow = StateStatuteCompletionRow

US_REGULATION_JURISDICTIONS: tuple[RegulationJurisdiction, ...] = (
    RegulationJurisdiction("us", "Federal"),
    *US_STATE_STATUTE_JURISDICTIONS,
)


@dataclass(frozen=True)
class RegulationCompletionReport:
    release_name: str
    local_root: Path
    expected_jurisdiction_count: int
    release_regulation_scope_count: int
    validation_report_path: Path | None
    validation_report_present: bool
    validation_report_ok: bool | None
    validation_report_truncated: bool
    supabase_counts_path: Path | None
    rows: tuple[RegulationCompletionRow, ...]

    @property
    def complete(self) -> bool:
        return all(
            row.status is RegulationCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
            for row in self.rows
        )

    def status_counts(self) -> dict[str, int]:
        counts = Counter(row.status.value for row in self.rows)
        return {
            status.value: counts.get(status.value, 0)
            for status in RegulationCompletionStatus
        }

    def to_mapping(self) -> dict[str, Any]:
        productionized = self.status_counts()[
            RegulationCompletionStatus.PRODUCTIONIZED_AND_VALIDATED.value
        ]
        return {
            "release": self.release_name,
            "local_root": str(self.local_root),
            "complete": self.complete,
            "document_class": REGULATION_DOCUMENT_CLASS,
            "expected_jurisdiction_count": self.expected_jurisdiction_count,
            "release_regulation_scope_count": self.release_regulation_scope_count,
            "productionized_and_validated_count": productionized,
            "unfinished_count": self.expected_jurisdiction_count - productionized,
            "status_counts": self.status_counts(),
            "validation_report_path": (
                str(self.validation_report_path) if self.validation_report_path else None
            ),
            "validation_report_present": self.validation_report_present,
            "validation_report_ok": self.validation_report_ok,
            "validation_report_truncated": self.validation_report_truncated,
            "supabase_counts_path": str(self.supabase_counts_path)
            if self.supabase_counts_path
            else None,
            "unfinished_jurisdictions": [
                row.jurisdiction
                for row in self.rows
                if row.status is not RegulationCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
            ],
            "rows": [row.to_mapping() for row in self.rows],
        }


def build_regulation_completion_report(
    root: str | Path,
    *,
    release: ReleaseManifest,
    artifact_report: ArtifactReport,
    supabase_counts_path: str | Path | None = None,
    validation_report_path: str | Path | None = None,
    expected_jurisdictions: tuple[
        RegulationJurisdiction, ...
    ] = US_REGULATION_JURISDICTIONS,
) -> RegulationCompletionReport:
    """Classify each federal/state regulation corpus against production state."""

    expected = {scope.jurisdiction: scope for scope in expected_jurisdictions}
    release_scope_by_jurisdiction = _release_regulation_scopes_by_jurisdiction(
        release,
        expected_jurisdictions=frozenset(expected),
    )
    rows_by_jurisdiction = _artifact_rows_by_jurisdiction(artifact_report, expected)
    supabase_counts = (
        load_provision_count_snapshot(supabase_counts_path)
        if supabase_counts_path is not None
        else None
    )
    validation = _load_validation_report_state(validation_report_path)

    rows = tuple(
        _build_completion_row(
            jurisdiction,
            release_scope=release_scope_by_jurisdiction.get(jurisdiction.jurisdiction),
            artifact_rows=rows_by_jurisdiction.get(jurisdiction.jurisdiction, ()),
            supabase_count=_supabase_count(
                supabase_counts,
                jurisdiction.jurisdiction,
                REGULATION_DOCUMENT_CLASS,
            ),
            validation=validation,
        )
        for jurisdiction in expected_jurisdictions
    )
    return RegulationCompletionReport(
        release_name=release.name,
        local_root=Path(root),
        expected_jurisdiction_count=len(expected_jurisdictions),
        release_regulation_scope_count=len(release_scope_by_jurisdiction),
        validation_report_path=validation.path,
        validation_report_present=validation.present,
        validation_report_ok=validation.ok,
        validation_report_truncated=validation.truncated,
        supabase_counts_path=Path(supabase_counts_path) if supabase_counts_path else None,
        rows=rows,
    )


def _release_regulation_scopes_by_jurisdiction(
    release: ReleaseManifest,
    *,
    expected_jurisdictions: frozenset[str],
) -> dict[str, ReleaseScope]:
    scopes: dict[str, ReleaseScope] = {}
    for scope in release.scopes:
        if scope.document_class != REGULATION_DOCUMENT_CLASS:
            continue
        if scope.jurisdiction not in expected_jurisdictions:
            continue
        scopes[scope.jurisdiction] = scope
    return scopes


def _artifact_rows_by_jurisdiction(
    artifact_report: ArtifactReport,
    expected: dict[str, RegulationJurisdiction],
) -> dict[str, tuple[ArtifactScopeRow, ...]]:
    grouped: dict[str, list[ArtifactScopeRow]] = defaultdict(list)
    for row in artifact_report.rows:
        if row.document_class != REGULATION_DOCUMENT_CLASS:
            continue
        if row.jurisdiction not in expected:
            continue
        grouped[row.jurisdiction].append(row)
    return {
        jurisdiction: tuple(sorted(rows, key=lambda row: row.version))
        for jurisdiction, rows in grouped.items()
    }
