"""Completion reporting for production regulation ingestion."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from axiom_corpus.corpus.analytics import load_provision_count_snapshot
from axiom_corpus.corpus.r2 import ArtifactReport, ArtifactScopeRow
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope
from axiom_corpus.corpus.state_statute_completion import (
    US_STATE_STATUTE_JURISDICTIONS,
    StateStatuteCompletionRow,
    StateStatuteJurisdiction,
    _build_completion_row,
    _load_validation_report_state,
    _supabase_count,
)

REGULATION_DOCUMENT_CLASS = "regulation"

RegulationJurisdiction = StateStatuteJurisdiction


class RegulationCompletionStatus(StrEnum):
    """High-level production state for one expected regulation corpus."""

    PRODUCTIONIZED_AND_VALIDATED = "productionized_and_validated"
    TARGETED_SCOPE_PROMOTED = "targeted_scope_promoted"
    PRODUCTION_BLOCKED_OR_INCOMPLETE = "production_blocked_or_incomplete"
    LOCAL_ARTIFACTS_PRESENT_NOT_PROMOTED = "local_artifacts_present_not_promoted"
    LOCAL_ARTIFACTS_INCOMPLETE = "local_artifacts_incomplete"
    SUPABASE_ONLY_LEGACY = "supabase_only_legacy"
    SOURCE_ACCESS_BLOCKED = "source_access_blocked"
    MISSING_SOURCE_FIRST_EXTRACTION = "missing_source_first_extraction"


@dataclass(frozen=True)
class RegulationCompletionRow:
    jurisdiction: str
    name: str
    status: RegulationCompletionStatus
    release_scope_present: bool
    release_version: str | None
    best_local_version: str | None
    local_scope_count: int
    local_complete: bool
    coverage_complete: bool | None
    r2_complete: bool | None
    release_provision_count: int | None
    best_local_provision_count: int | None
    supabase_count: int | None
    supabase_matches_release: bool | None
    validation_error_count: int
    validation_warning_count: int
    validation_codes: tuple[str, ...]
    mismatch_reasons: tuple[str, ...]
    source_access_status: str | None
    source_access_note: str | None
    next_action: str
    statewide_scope: bool | None
    targeted_scope: bool

    def to_mapping(self) -> dict[str, Any]:
        return {
            "jurisdiction": self.jurisdiction,
            "name": self.name,
            "status": self.status.value,
            "release_scope_present": self.release_scope_present,
            "release_version": self.release_version,
            "best_local_version": self.best_local_version,
            "local_scope_count": self.local_scope_count,
            "local_complete": self.local_complete,
            "coverage_complete": self.coverage_complete,
            "r2_complete": self.r2_complete,
            "release_provision_count": self.release_provision_count,
            "best_local_provision_count": self.best_local_provision_count,
            "supabase_count": self.supabase_count,
            "supabase_matches_release": self.supabase_matches_release,
            "validation_error_count": self.validation_error_count,
            "validation_warning_count": self.validation_warning_count,
            "validation_codes": list(self.validation_codes),
            "mismatch_reasons": list(self.mismatch_reasons),
            "source_access_status": self.source_access_status,
            "source_access_note": self.source_access_note,
            "next_action": self.next_action,
            "statewide_scope": self.statewide_scope,
            "targeted_scope": self.targeted_scope,
        }

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
        targeted = self.status_counts()[RegulationCompletionStatus.TARGETED_SCOPE_PROMOTED.value]
        return {
            "release": self.release_name,
            "local_root": str(self.local_root),
            "complete": self.complete,
            "document_class": REGULATION_DOCUMENT_CLASS,
            "expected_jurisdiction_count": self.expected_jurisdiction_count,
            "release_regulation_scope_count": self.release_regulation_scope_count,
            "productionized_and_validated_count": productionized,
            "targeted_scope_promoted_count": targeted,
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
        _build_regulation_completion_row(
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
            ),
            release_scope=release_scope_by_jurisdiction.get(jurisdiction.jurisdiction),
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


def _build_regulation_completion_row(
    base_row: StateStatuteCompletionRow,
    *,
    release_scope: ReleaseScope | None,
) -> RegulationCompletionRow:
    targeted_scope = (
        release_scope is not None and not _looks_statewide_regulation_version(release_scope.version)
    )
    status = RegulationCompletionStatus(base_row.status.value)
    mismatch_reasons = base_row.mismatch_reasons
    next_action = base_row.next_action
    if (
        targeted_scope
        and status is RegulationCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
    ):
        status = RegulationCompletionStatus.TARGETED_SCOPE_PROMOTED
        mismatch_reasons = (*mismatch_reasons, "targeted_regulation_scope")
        next_action = (
            "expand to the complete official regulation corpus for this jurisdiction "
            "or keep this as a targeted policy-document scope"
        )
    return RegulationCompletionRow(
        jurisdiction=base_row.jurisdiction,
        name=base_row.name,
        status=status,
        release_scope_present=base_row.release_scope_present,
        release_version=base_row.release_version,
        best_local_version=base_row.best_local_version,
        local_scope_count=base_row.local_scope_count,
        local_complete=base_row.local_complete,
        coverage_complete=base_row.coverage_complete,
        r2_complete=base_row.r2_complete,
        release_provision_count=base_row.release_provision_count,
        best_local_provision_count=base_row.best_local_provision_count,
        supabase_count=base_row.supabase_count,
        supabase_matches_release=base_row.supabase_matches_release,
        validation_error_count=base_row.validation_error_count,
        validation_warning_count=base_row.validation_warning_count,
        validation_codes=base_row.validation_codes,
        mismatch_reasons=mismatch_reasons,
        source_access_status=base_row.source_access_status,
        source_access_note=base_row.source_access_note,
        next_action=next_action,
        statewide_scope=(
            _looks_statewide_regulation_version(release_scope.version)
            if release_scope is not None
            else None
        ),
        targeted_scope=targeted_scope,
    )


def _looks_statewide_regulation_version(version: str) -> bool:
    """Date-only release versions denote complete jurisdiction-wide regulation corpora."""

    parts = version.split("-")
    return (
        len(parts) == 3
        and len(parts[0]) == 4
        and len(parts[1]) == 2
        and len(parts[2]) == 2
        and all(part.isdigit() for part in parts)
    )
