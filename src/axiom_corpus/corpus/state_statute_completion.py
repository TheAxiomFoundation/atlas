"""Completion reporting for production state statute ingestion."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from axiom_corpus.corpus.analytics import load_provision_count_snapshot
from axiom_corpus.corpus.r2 import ArtifactReport, ArtifactScopeRow
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope

STATE_STATUTE_DOCUMENT_CLASS = "statute"


class StateStatuteCompletionStatus(StrEnum):
    """High-level production state for one expected state statute corpus."""

    PRODUCTIONIZED_AND_VALIDATED = "productionized_and_validated"
    PRODUCTION_BLOCKED_OR_INCOMPLETE = "production_blocked_or_incomplete"
    LOCAL_ARTIFACTS_PRESENT_NOT_PROMOTED = "local_artifacts_present_not_promoted"
    LOCAL_ARTIFACTS_INCOMPLETE = "local_artifacts_incomplete"
    SUPABASE_ONLY_LEGACY = "supabase_only_legacy"
    MISSING_SOURCE_FIRST_EXTRACTION = "missing_source_first_extraction"


@dataclass(frozen=True)
class StateStatuteJurisdiction:
    jurisdiction: str
    name: str


US_STATE_STATUTE_JURISDICTIONS: tuple[StateStatuteJurisdiction, ...] = (
    StateStatuteJurisdiction("us-al", "Alabama"),
    StateStatuteJurisdiction("us-ak", "Alaska"),
    StateStatuteJurisdiction("us-az", "Arizona"),
    StateStatuteJurisdiction("us-ar", "Arkansas"),
    StateStatuteJurisdiction("us-ca", "California"),
    StateStatuteJurisdiction("us-co", "Colorado"),
    StateStatuteJurisdiction("us-ct", "Connecticut"),
    StateStatuteJurisdiction("us-de", "Delaware"),
    StateStatuteJurisdiction("us-dc", "District of Columbia"),
    StateStatuteJurisdiction("us-fl", "Florida"),
    StateStatuteJurisdiction("us-ga", "Georgia"),
    StateStatuteJurisdiction("us-hi", "Hawaii"),
    StateStatuteJurisdiction("us-id", "Idaho"),
    StateStatuteJurisdiction("us-il", "Illinois"),
    StateStatuteJurisdiction("us-in", "Indiana"),
    StateStatuteJurisdiction("us-ia", "Iowa"),
    StateStatuteJurisdiction("us-ks", "Kansas"),
    StateStatuteJurisdiction("us-ky", "Kentucky"),
    StateStatuteJurisdiction("us-la", "Louisiana"),
    StateStatuteJurisdiction("us-me", "Maine"),
    StateStatuteJurisdiction("us-md", "Maryland"),
    StateStatuteJurisdiction("us-ma", "Massachusetts"),
    StateStatuteJurisdiction("us-mi", "Michigan"),
    StateStatuteJurisdiction("us-mn", "Minnesota"),
    StateStatuteJurisdiction("us-ms", "Mississippi"),
    StateStatuteJurisdiction("us-mo", "Missouri"),
    StateStatuteJurisdiction("us-mt", "Montana"),
    StateStatuteJurisdiction("us-ne", "Nebraska"),
    StateStatuteJurisdiction("us-nv", "Nevada"),
    StateStatuteJurisdiction("us-nh", "New Hampshire"),
    StateStatuteJurisdiction("us-nj", "New Jersey"),
    StateStatuteJurisdiction("us-nm", "New Mexico"),
    StateStatuteJurisdiction("us-ny", "New York"),
    StateStatuteJurisdiction("us-nc", "North Carolina"),
    StateStatuteJurisdiction("us-nd", "North Dakota"),
    StateStatuteJurisdiction("us-oh", "Ohio"),
    StateStatuteJurisdiction("us-ok", "Oklahoma"),
    StateStatuteJurisdiction("us-or", "Oregon"),
    StateStatuteJurisdiction("us-pa", "Pennsylvania"),
    StateStatuteJurisdiction("us-ri", "Rhode Island"),
    StateStatuteJurisdiction("us-sc", "South Carolina"),
    StateStatuteJurisdiction("us-sd", "South Dakota"),
    StateStatuteJurisdiction("us-tn", "Tennessee"),
    StateStatuteJurisdiction("us-tx", "Texas"),
    StateStatuteJurisdiction("us-ut", "Utah"),
    StateStatuteJurisdiction("us-vt", "Vermont"),
    StateStatuteJurisdiction("us-va", "Virginia"),
    StateStatuteJurisdiction("us-wa", "Washington"),
    StateStatuteJurisdiction("us-wv", "West Virginia"),
    StateStatuteJurisdiction("us-wi", "Wisconsin"),
    StateStatuteJurisdiction("us-wy", "Wyoming"),
)


@dataclass(frozen=True)
class ValidationScopeSummary:
    error_count: int = 0
    warning_count: int = 0
    codes: tuple[str, ...] = ()

    def problem_count(self, *, strict_warnings: bool) -> int:
        if strict_warnings:
            return self.error_count + self.warning_count
        return self.error_count


@dataclass(frozen=True)
class ValidationReportState:
    path: Path | None
    present: bool
    ok: bool | None
    truncated: bool
    strict_warnings: bool
    error_count: int
    warning_count: int
    issues_by_scope: dict[tuple[str, str, str], ValidationScopeSummary]


@dataclass(frozen=True)
class StateStatuteCompletionRow:
    jurisdiction: str
    name: str
    status: StateStatuteCompletionStatus
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
    next_action: str

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
            "next_action": self.next_action,
        }


@dataclass(frozen=True)
class StateStatuteCompletionReport:
    release_name: str
    local_root: Path
    expected_jurisdiction_count: int
    release_statute_scope_count: int
    validation_report_path: Path | None
    validation_report_present: bool
    validation_report_ok: bool | None
    validation_report_truncated: bool
    supabase_counts_path: Path | None
    rows: tuple[StateStatuteCompletionRow, ...]

    @property
    def complete(self) -> bool:
        return all(
            row.status is StateStatuteCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
            for row in self.rows
        )

    def status_counts(self) -> dict[str, int]:
        counts = Counter(row.status.value for row in self.rows)
        return {
            status.value: counts.get(status.value, 0) for status in StateStatuteCompletionStatus
        }

    def to_mapping(self) -> dict[str, Any]:
        productionized = self.status_counts()[
            StateStatuteCompletionStatus.PRODUCTIONIZED_AND_VALIDATED.value
        ]
        return {
            "release": self.release_name,
            "local_root": str(self.local_root),
            "complete": self.complete,
            "expected_jurisdiction_count": self.expected_jurisdiction_count,
            "release_statute_scope_count": self.release_statute_scope_count,
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
                if row.status is not StateStatuteCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
            ],
            "rows": [row.to_mapping() for row in self.rows],
        }


def build_state_statute_completion_report(
    root: str | Path,
    *,
    release: ReleaseManifest,
    artifact_report: ArtifactReport,
    supabase_counts_path: str | Path | None = None,
    validation_report_path: str | Path | None = None,
    expected_jurisdictions: tuple[StateStatuteJurisdiction, ...] = US_STATE_STATUTE_JURISDICTIONS,
) -> StateStatuteCompletionReport:
    """Classify each state statute corpus against source-first production state."""

    expected = {state.jurisdiction: state for state in expected_jurisdictions}
    release_scope_by_jurisdiction = _release_statute_scopes_by_jurisdiction(
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
            state,
            release_scope=release_scope_by_jurisdiction.get(state.jurisdiction),
            artifact_rows=rows_by_jurisdiction.get(state.jurisdiction, ()),
            supabase_count=_supabase_count(
                supabase_counts,
                state.jurisdiction,
                STATE_STATUTE_DOCUMENT_CLASS,
            ),
            validation=validation,
        )
        for state in expected_jurisdictions
    )
    return StateStatuteCompletionReport(
        release_name=release.name,
        local_root=Path(root),
        expected_jurisdiction_count=len(expected_jurisdictions),
        release_statute_scope_count=len(release_scope_by_jurisdiction),
        validation_report_path=validation.path,
        validation_report_present=validation.present,
        validation_report_ok=validation.ok,
        validation_report_truncated=validation.truncated,
        supabase_counts_path=Path(supabase_counts_path) if supabase_counts_path else None,
        rows=rows,
    )


def _build_completion_row(
    state: StateStatuteJurisdiction,
    *,
    release_scope: ReleaseScope | None,
    artifact_rows: tuple[ArtifactScopeRow, ...],
    supabase_count: int | None,
    validation: ValidationReportState,
) -> StateStatuteCompletionRow:
    best_row = _best_artifact_row(artifact_rows)
    complete_unpromoted_row = _best_complete_unpromoted_row(artifact_rows)
    release_row = (
        _find_release_row(artifact_rows, release_scope.version)
        if release_scope is not None
        else None
    )
    primary_row = release_row or complete_unpromoted_row or best_row
    validation_summary = _validation_summary(validation, release_scope)
    supabase_matches_release = _supabase_matches_release(supabase_count, release_row)
    mismatch_reasons = _mismatch_reasons(
        release_scope=release_scope,
        release_row=release_row,
        primary_row=primary_row,
        supabase_matches_release=supabase_matches_release,
        validation=validation,
        validation_summary=validation_summary,
    )
    status = _classify_status(
        release_scope=release_scope,
        release_row=release_row,
        complete_unpromoted_row=complete_unpromoted_row,
        artifact_rows=artifact_rows,
        supabase_count=supabase_count,
        validation=validation,
        validation_summary=validation_summary,
        mismatch_reasons=mismatch_reasons,
    )
    return StateStatuteCompletionRow(
        jurisdiction=state.jurisdiction,
        name=state.name,
        status=status,
        release_scope_present=release_scope is not None,
        release_version=release_scope.version if release_scope is not None else None,
        best_local_version=best_row.version if best_row is not None else None,
        local_scope_count=len(artifact_rows),
        local_complete=_source_first_complete(primary_row),
        coverage_complete=primary_row.coverage_complete if primary_row is not None else None,
        r2_complete=_r2_complete(primary_row) if primary_row is not None else None,
        release_provision_count=release_row.provision_count if release_row is not None else None,
        best_local_provision_count=best_row.provision_count if best_row is not None else None,
        supabase_count=supabase_count,
        supabase_matches_release=supabase_matches_release,
        validation_error_count=validation_summary.error_count,
        validation_warning_count=validation_summary.warning_count,
        validation_codes=validation_summary.codes,
        mismatch_reasons=mismatch_reasons,
        next_action=_next_action(status),
    )


def _classify_status(
    *,
    release_scope: ReleaseScope | None,
    release_row: ArtifactScopeRow | None,
    complete_unpromoted_row: ArtifactScopeRow | None,
    artifact_rows: tuple[ArtifactScopeRow, ...],
    supabase_count: int | None,
    validation: ValidationReportState,
    validation_summary: ValidationScopeSummary,
    mismatch_reasons: tuple[str, ...],
) -> StateStatuteCompletionStatus:
    if release_scope is not None:
        if (
            release_row is not None
            and _source_first_complete(release_row)
            and _r2_complete(release_row) is not False
            and (supabase_count is None or supabase_count == (release_row.provision_count or 0))
            and validation.present
            and validation_summary.problem_count(strict_warnings=validation.strict_warnings) == 0
            and not mismatch_reasons
        ):
            return StateStatuteCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
        return StateStatuteCompletionStatus.PRODUCTION_BLOCKED_OR_INCOMPLETE

    if complete_unpromoted_row is not None:
        return StateStatuteCompletionStatus.LOCAL_ARTIFACTS_PRESENT_NOT_PROMOTED

    if artifact_rows:
        return StateStatuteCompletionStatus.LOCAL_ARTIFACTS_INCOMPLETE

    if supabase_count is not None and supabase_count > 0:
        return StateStatuteCompletionStatus.SUPABASE_ONLY_LEGACY

    return StateStatuteCompletionStatus.MISSING_SOURCE_FIRST_EXTRACTION


def _mismatch_reasons(
    *,
    release_scope: ReleaseScope | None,
    release_row: ArtifactScopeRow | None,
    primary_row: ArtifactScopeRow | None,
    supabase_matches_release: bool | None,
    validation: ValidationReportState,
    validation_summary: ValidationScopeSummary,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if release_scope is not None:
        if release_row is None:
            reasons.append("missing_release_artifacts")
        elif not _source_first_complete(release_row):
            reasons.extend(release_row.mismatch_reasons())
        if release_row is not None and _r2_complete(release_row) is False:
            reasons.append("r2_incomplete")
        if supabase_matches_release is False:
            reasons.append("supabase_count_mismatch")
        if not validation.present:
            reasons.append("validation_report_missing")
        elif validation_summary.error_count:
            reasons.append("validation_errors")
        elif validation.strict_warnings and validation_summary.warning_count:
            reasons.append("validation_warnings")
    elif primary_row is not None and not _source_first_complete(primary_row):
        reasons.extend(primary_row.mismatch_reasons())
    return tuple(dict.fromkeys(reasons))


def _next_action(status: StateStatuteCompletionStatus) -> str:
    if status is StateStatuteCompletionStatus.PRODUCTIONIZED_AND_VALIDATED:
        return "none"
    if status is StateStatuteCompletionStatus.PRODUCTION_BLOCKED_OR_INCOMPLETE:
        return "repair release artifacts, R2 sync, Supabase counts, or validation errors"
    if status is StateStatuteCompletionStatus.LOCAL_ARTIFACTS_PRESENT_NOT_PROMOTED:
        return (
            "review artifacts, add scope to current release, sync R2, load Supabase, and validate"
        )
    if status is StateStatuteCompletionStatus.LOCAL_ARTIFACTS_INCOMPLETE:
        return "rerun or repair source-first extraction until inventory, provisions, and coverage are complete"
    if status is StateStatuteCompletionStatus.SUPABASE_ONLY_LEGACY:
        return "rerun from primary official sources into source-first artifacts"
    return "build a source-first extractor from primary official sources"


def _release_statute_scopes_by_jurisdiction(
    release: ReleaseManifest,
    *,
    expected_jurisdictions: frozenset[str],
) -> dict[str, ReleaseScope]:
    scopes: dict[str, ReleaseScope] = {}
    for scope in release.scopes:
        if scope.document_class != STATE_STATUTE_DOCUMENT_CLASS:
            continue
        if scope.jurisdiction not in expected_jurisdictions:
            continue
        scopes[scope.jurisdiction] = scope
    return scopes


def _artifact_rows_by_jurisdiction(
    artifact_report: ArtifactReport,
    expected: dict[str, StateStatuteJurisdiction],
) -> dict[str, tuple[ArtifactScopeRow, ...]]:
    grouped: dict[str, list[ArtifactScopeRow]] = defaultdict(list)
    for row in artifact_report.rows:
        if row.document_class != STATE_STATUTE_DOCUMENT_CLASS:
            continue
        if row.jurisdiction not in expected:
            continue
        grouped[row.jurisdiction].append(row)
    return {
        jurisdiction: tuple(sorted(rows, key=lambda row: row.version))
        for jurisdiction, rows in grouped.items()
    }


def _supabase_count(
    counts: dict[tuple[str, str], int] | None,
    jurisdiction: str,
    document_class: str,
) -> int | None:
    if counts is None:
        return None
    return counts.get((jurisdiction, document_class), 0)


def _validation_summary(
    validation: ValidationReportState,
    release_scope: ReleaseScope | None,
) -> ValidationScopeSummary:
    if release_scope is None:
        return ValidationScopeSummary()
    return validation.issues_by_scope.get(release_scope.key, ValidationScopeSummary())


def _supabase_matches_release(
    supabase_count: int | None,
    release_row: ArtifactScopeRow | None,
) -> bool | None:
    if supabase_count is None or release_row is None or release_row.provision_count is None:
        return None
    return supabase_count == release_row.provision_count


def _find_release_row(
    rows: tuple[ArtifactScopeRow, ...],
    version: str,
) -> ArtifactScopeRow | None:
    for row in rows:
        if row.version == version:
            return row
    return None


def _best_complete_unpromoted_row(
    rows: tuple[ArtifactScopeRow, ...],
) -> ArtifactScopeRow | None:
    candidates = tuple(
        row
        for row in rows
        if _source_first_complete(row) and not _looks_partial_version(row.version)
    )
    return _best_artifact_row(candidates)


def _best_artifact_row(rows: tuple[ArtifactScopeRow, ...]) -> ArtifactScopeRow | None:
    if not rows:
        return None
    return max(rows, key=_artifact_row_rank)


def _artifact_row_rank(row: ArtifactScopeRow) -> tuple[int, int, int, int, str]:
    return (
        int(_source_first_complete(row)),
        int(not _looks_partial_version(row.version)),
        int(_local_complete(row)),
        row.provision_count or 0,
        row.version,
    )


def _source_first_complete(row: ArtifactScopeRow | None) -> bool:
    return (
        row is not None
        and (_local_complete(row) or _r2_complete(row) is True)
        and row.coverage_complete is True
    )


def _local_complete(row: ArtifactScopeRow) -> bool:
    return row.local_inventory and row.local_provisions and row.local_coverage


def _r2_complete(row: ArtifactScopeRow | None) -> bool | None:
    if row is None or row.remote_inventory is None:
        return None
    return (
        row.remote_inventory
        and bool(row.remote_provisions)
        and bool(row.remote_coverage)
        and (row.local_source_files == 0 or bool(row.remote_source_files))
    )


def _looks_partial_version(version: str) -> bool:
    lowered = version.lower()
    markers = ("probe", "limit", "sample", "smoke", "test")
    return any(marker in lowered for marker in markers)


def _load_validation_report_state(
    path: str | Path | None,
) -> ValidationReportState:
    if path is None:
        return ValidationReportState(
            path=None,
            present=False,
            ok=None,
            truncated=False,
            strict_warnings=False,
            error_count=0,
            warning_count=0,
            issues_by_scope={},
        )
    report_path = Path(path)
    data = json.loads(report_path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"validation report must be a JSON object: {report_path}")
    return ValidationReportState(
        path=report_path,
        present=True,
        ok=bool(data.get("ok")),
        truncated=bool(data.get("issues_truncated")),
        strict_warnings=bool(data.get("strict_warnings")),
        error_count=int(data.get("error_count", 0)),
        warning_count=int(data.get("warning_count", 0)),
        issues_by_scope=_validation_issues_by_scope(data.get("issues", [])),
    )


def _validation_issues_by_scope(
    raw_issues: Any,
) -> dict[tuple[str, str, str], ValidationScopeSummary]:
    summaries: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
        lambda: {"error_count": 0, "warning_count": 0, "codes": set()}
    )
    if not isinstance(raw_issues, list):
        return {}
    for issue in raw_issues:
        if not isinstance(issue, dict):
            continue
        jurisdiction = issue.get("jurisdiction")
        document_class = issue.get("document_class")
        version = issue.get("version")
        if not jurisdiction or not document_class or not version:
            continue
        key = (str(jurisdiction), str(document_class), str(version))
        summary = summaries[key]
        severity = issue.get("severity")
        if severity == "error":
            summary["error_count"] += 1
        elif severity == "warning":
            summary["warning_count"] += 1
        code = issue.get("code")
        if code:
            summary["codes"].add(str(code))
    return {
        key: ValidationScopeSummary(
            error_count=int(value["error_count"]),
            warning_count=int(value["warning_count"]),
            codes=tuple(sorted(value["codes"])),
        )
        for key, value in summaries.items()
    }
