import json

import pytest

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.r2 import (
    ArtifactReport,
    ArtifactScopeRow,
    RemoteArtifact,
    build_artifact_report,
)
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope
from axiom_corpus.corpus.state_statute_completion import (
    StateStatuteCompletionStatus,
    StateStatuteJurisdiction,
    build_state_statute_completion_report,
)


def test_state_statute_completion_classifies_release_local_legacy_and_missing(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    _write_complete_statute_scope(store, "us-co", "2026-04-29", count=1)
    _write_complete_statute_scope(store, "us-ny", "2026-04-29", count=2)
    counts_path = tmp_path / "counts.json"
    counts_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "statute",
                        "provision_count": 1,
                    },
                    {
                        "jurisdiction": "us-ny",
                        "document_class": "statute",
                        "provision_count": 2,
                    },
                    {
                        "jurisdiction": "us-al",
                        "document_class": "statute",
                        "provision_count": 7,
                    },
                ]
            }
        )
    )
    validation_path = tmp_path / "validate-release-current.json"
    validation_path.write_text(
        json.dumps(
            {
                "ok": True,
                "error_count": 0,
                "warning_count": 0,
                "strict_warnings": False,
                "issues_truncated": False,
                "issues": [],
            }
        )
    )
    release = ReleaseManifest(
        name="current",
        scopes=(ReleaseScope("us-co", "statute", "2026-04-29"),),
    )
    artifact_report = build_artifact_report(
        store.root,
        prefixes=("sources", "inventory", "provisions", "coverage"),
        document_class="statute",
        supabase_counts_path=counts_path,
    )

    report = build_state_statute_completion_report(
        store.root,
        release=release,
        artifact_report=artifact_report,
        supabase_counts_path=counts_path,
        validation_report_path=validation_path,
    )
    rows = {row.jurisdiction: row for row in report.rows}

    assert rows["us-co"].status is StateStatuteCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
    assert rows["us-ny"].status is StateStatuteCompletionStatus.LOCAL_ARTIFACTS_PRESENT_NOT_PROMOTED
    assert rows["us-al"].status is StateStatuteCompletionStatus.SUPABASE_ONLY_LEGACY
    assert rows["us-ak"].status is StateStatuteCompletionStatus.MISSING_SOURCE_FIRST_EXTRACTION
    assert report.status_counts()["productionized_and_validated"] == 1
    assert report.complete is False


def test_state_statute_completion_cli_writes_report(tmp_path, capsys):
    store = CorpusArtifactStore(tmp_path / "corpus")
    _write_complete_statute_scope(store, "us-co", "2026-04-29", count=1)
    (store.root / "releases").mkdir(parents=True)
    (store.root / "releases" / "current.json").write_text(
        json.dumps(
            {
                "name": "current",
                "scopes": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "statute",
                        "version": "2026-04-29",
                    }
                ],
            }
        )
    )
    (store.root / "analytics").mkdir(parents=True)
    (store.root / "analytics" / "validate-release-current.json").write_text(
        json.dumps(
            {
                "ok": True,
                "error_count": 0,
                "warning_count": 0,
                "strict_warnings": False,
                "issues_truncated": False,
                "issues": [],
            }
        )
    )
    counts_path = tmp_path / "counts.json"
    counts_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "statute",
                        "provision_count": 1,
                    }
                ]
            }
        )
    )
    output = store.root / "analytics" / "state-statute-completion-current.json"

    exit_code = main(
        [
            "state-statute-completion",
            "--base",
            str(store.root),
            "--supabase-counts",
            str(counts_path),
            "--output",
            str(output),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    written = json.loads(output.read_text())

    assert exit_code == 0
    assert payload["written_to"] == str(output)
    assert written["release"] == "current"
    assert written["rows"][5]["jurisdiction"] == "us-co"
    assert written["rows"][5]["status"] == "productionized_and_validated"


def test_state_statute_completion_blocks_release_on_mismatches(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    _write_complete_statute_scope(store, "us-co", "2026-04-29", count=1)
    counts_path = tmp_path / "counts.json"
    counts_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "statute",
                        "provision_count": 2,
                    }
                ]
            }
        )
    )
    validation_path = tmp_path / "validate-release-current.json"
    validation_path.write_text(
        json.dumps(
            {
                "ok": False,
                "error_count": 0,
                "warning_count": 1,
                "strict_warnings": True,
                "issues_truncated": False,
                "issues": [
                    {
                        "severity": "warning",
                        "code": "empty_provision_text",
                        "jurisdiction": "us-co",
                        "document_class": "statute",
                        "version": "2026-04-29",
                    }
                ],
            }
        )
    )
    release = ReleaseManifest(
        name="current",
        scopes=(ReleaseScope("us-co", "statute", "2026-04-29"),),
    )
    artifact_report = build_artifact_report(
        store.root,
        prefixes=("sources", "inventory", "provisions", "coverage"),
        document_class="statute",
        supabase_counts_path=counts_path,
        remote={
            "inventory/us-co/statute/2026-04-29.json": RemoteArtifact(
                key="inventory/us-co/statute/2026-04-29.json",
                size=1,
            )
        },
    )

    report = build_state_statute_completion_report(
        store.root,
        release=release,
        artifact_report=artifact_report,
        supabase_counts_path=counts_path,
        validation_report_path=validation_path,
    )
    row = next(row for row in report.rows if row.jurisdiction == "us-co")

    assert row.status is StateStatuteCompletionStatus.PRODUCTION_BLOCKED_OR_INCOMPLETE
    assert row.supabase_matches_release is False
    assert row.r2_complete is False
    assert set(row.mismatch_reasons) == {
        "r2_incomplete",
        "supabase_count_mismatch",
        "validation_warnings",
    }


def test_state_statute_completion_accepts_r2_only_release_artifacts(tmp_path):
    validation_path = tmp_path / "validate-release-current.json"
    validation_path.write_text(
        json.dumps(
            {
                "ok": True,
                "error_count": 0,
                "warning_count": 1,
                "strict_warnings": False,
                "issues_truncated": False,
                "issues": [
                    {
                        "severity": "warning",
                        "code": "remote_only_scope_not_deep_validated",
                        "jurisdiction": "us-co",
                        "document_class": "statute",
                        "version": "v1",
                    }
                ],
            }
        )
    )
    release = ReleaseManifest(
        name="current",
        scopes=(ReleaseScope("us-co", "statute", "v1"),),
    )
    artifact_report = ArtifactReport(
        local_root=tmp_path,
        prefixes=(),
        local_count=0,
        local_bytes=0,
        local_by_prefix={},
        remote_count=3,
        remote_bytes=3,
        remote_by_prefix=None,
        rows=(
            ArtifactScopeRow(
                jurisdiction="us-co",
                document_class="statute",
                version="v1",
                remote_inventory=True,
                remote_provisions=True,
                remote_coverage=True,
                coverage_complete=True,
                provision_count=4,
            ),
        ),
    )

    report = build_state_statute_completion_report(
        tmp_path,
        release=release,
        artifact_report=artifact_report,
        validation_report_path=validation_path,
        expected_jurisdictions=(StateStatuteJurisdiction("us-co", "Colorado"),),
    )
    row = report.rows[0]

    assert row.status is StateStatuteCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
    assert row.local_complete is True
    assert row.r2_complete is True
    assert row.release_provision_count == 4
    assert row.mismatch_reasons == ()


def test_state_statute_completion_reports_incomplete_and_unvalidated_scopes(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    _write_complete_statute_scope(store, "us-co", "2026-04-29", count=1)
    store.write_inventory(
        store.inventory_path("us-az", "statute", "2026-04-29-limit-1"),
        [SourceInventoryItem(citation_path="us-az/statute/1")],
    )
    release = ReleaseManifest(
        name="current",
        scopes=(ReleaseScope("us-co", "statute", "2026-04-29"),),
    )
    artifact_report = build_artifact_report(
        store.root,
        prefixes=("sources", "inventory", "provisions", "coverage"),
        document_class="statute",
    )

    report = build_state_statute_completion_report(
        store.root,
        release=release,
        artifact_report=artifact_report,
    )
    rows = {row.jurisdiction: row for row in report.rows}

    assert rows["us-co"].status is StateStatuteCompletionStatus.PRODUCTION_BLOCKED_OR_INCOMPLETE
    assert rows["us-co"].mismatch_reasons == ("validation_report_missing",)
    assert rows["us-az"].status is StateStatuteCompletionStatus.LOCAL_ARTIFACTS_INCOMPLETE
    assert rows["us-az"].mismatch_reasons == (
        "missing_local_provisions",
        "missing_local_coverage",
    )


def test_state_statute_completion_handles_defensive_branches(tmp_path):
    validation_path = tmp_path / "validation.json"
    validation_path.write_text(
        json.dumps(
            {
                "ok": False,
                "error_count": 1,
                "warning_count": 1,
                "strict_warnings": True,
                "issues_truncated": False,
                "issues": [
                    None,
                    {"jurisdiction": "us-co"},
                    {
                        "severity": "error",
                        "code": "broken_scope",
                        "jurisdiction": "us-co",
                        "document_class": "statute",
                        "version": "v1",
                    },
                    {
                        "severity": "warning",
                        "code": "empty_provision_text",
                        "jurisdiction": "us-ca",
                        "document_class": "statute",
                        "version": "v1",
                    },
                ],
            }
        )
    )
    release = ReleaseManifest(
        name="current",
        scopes=(
            ReleaseScope("us-co", "statute", "v1"),
            ReleaseScope("us-ny", "statute", "v1"),
            ReleaseScope("us-ca", "statute", "v1"),
            ReleaseScope("us-co", "regulation", "v1"),
            ReleaseScope("us-zz", "statute", "v1"),
        ),
    )
    artifact_report = ArtifactReport(
        local_root=tmp_path,
        prefixes=(),
        local_count=0,
        local_bytes=0,
        local_by_prefix={},
        remote_count=None,
        remote_bytes=None,
        remote_by_prefix=None,
        rows=(
            ArtifactScopeRow(
                jurisdiction="us-co",
                document_class="statute",
                version="v1",
                local_inventory=True,
            ),
            ArtifactScopeRow(
                jurisdiction="us-ca",
                document_class="statute",
                version="v1",
                local_inventory=True,
                local_provisions=True,
                local_coverage=True,
                coverage_complete=True,
                provision_count=1,
            ),
            ArtifactScopeRow(jurisdiction="us-co", document_class="regulation", version="v1"),
            ArtifactScopeRow(jurisdiction="us-zz", document_class="statute", version="v1"),
        ),
    )

    report = build_state_statute_completion_report(
        tmp_path,
        release=release,
        artifact_report=artifact_report,
        validation_report_path=validation_path,
        expected_jurisdictions=(
            StateStatuteJurisdiction("us-ca", "California"),
            StateStatuteJurisdiction("us-co", "Colorado"),
            StateStatuteJurisdiction("us-ny", "New York"),
        ),
    )
    rows = {row.jurisdiction: row for row in report.rows}

    assert rows["us-ca"].mismatch_reasons == ("validation_warnings",)
    assert rows["us-co"].mismatch_reasons == (
        "missing_local_provisions",
        "missing_local_coverage",
        "validation_errors",
    )
    assert rows["us-ny"].mismatch_reasons == ("missing_release_artifacts",)


def test_state_statute_completion_rejects_invalid_validation_report(tmp_path):
    validation_path = tmp_path / "validation.json"
    validation_path.write_text("[]")

    with pytest.raises(ValueError, match="validation report"):
        build_state_statute_completion_report(
            tmp_path,
            release=ReleaseManifest(name="current", scopes=()),
            artifact_report=ArtifactReport(
                local_root=tmp_path,
                prefixes=(),
                local_count=0,
                local_bytes=0,
                local_by_prefix={},
                remote_count=None,
                remote_bytes=None,
                remote_by_prefix=None,
                rows=(),
            ),
            validation_report_path=validation_path,
        )


def _write_complete_statute_scope(
    store: CorpusArtifactStore,
    jurisdiction: str,
    version: str,
    *,
    count: int,
) -> None:
    source_path = store.source_path(jurisdiction, "statute", version, "source.html")
    source_sha = store.write_text(source_path, "<html>Official source.</html>")
    records = [
        ProvisionRecord(
            jurisdiction=jurisdiction,
            document_class="statute",
            citation_path=f"{jurisdiction}/statute/{index + 1}",
            body=f"Provision {index + 1}.",
            version=version,
            source_path=source_path.relative_to(store.root).as_posix(),
            source_as_of=version,
            expression_date=version,
        )
        for index in range(count)
    ]
    store.write_inventory(
        store.inventory_path(jurisdiction, "statute", version),
        [
            SourceInventoryItem(
                citation_path=record.citation_path,
                source_path=source_path.relative_to(store.root).as_posix(),
                source_format="html",
                sha256=source_sha,
            )
            for record in records
        ],
    )
    store.write_provisions(store.provisions_path(jurisdiction, "statute", version), records)
    store.write_json(
        store.coverage_path(jurisdiction, "statute", version),
        {
            "complete": True,
            "source_count": count,
            "provision_count": count,
            "matched_count": count,
            "missing_from_provisions": [],
            "extra_provisions": [],
        },
    )
