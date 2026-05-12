import json

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.r2 import ArtifactReport, ArtifactScopeRow, build_artifact_report
from axiom_corpus.corpus.regulation_completion import (
    RegulationCompletionStatus,
    RegulationJurisdiction,
    build_regulation_completion_report,
)
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope


def test_regulation_completion_classifies_federal_state_legacy_and_missing(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    _write_complete_regulation_scope(store, "us", "2026-05-01", count=2)
    _write_complete_regulation_scope(store, "us-co", "2026-04-29", count=1)
    _write_complete_regulation_scope(
        store,
        "us-ca",
        "2026-05-12-capi-cvcb-regulations",
        count=4,
    )
    _write_complete_regulation_scope(store, "us-ny", "2026-05-10", count=3)
    counts_path = tmp_path / "counts.json"
    counts_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "jurisdiction": "us",
                        "document_class": "regulation",
                        "provision_count": 2,
                    },
                    {
                        "jurisdiction": "us-co",
                        "document_class": "regulation",
                        "provision_count": 1,
                    },
                    {
                        "jurisdiction": "us-ca",
                        "document_class": "regulation",
                        "provision_count": 4,
                    },
                    {
                        "jurisdiction": "us-az",
                        "document_class": "regulation",
                        "provision_count": 9,
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
        scopes=(
            ReleaseScope("us", "regulation", "2026-05-01"),
            ReleaseScope("us-co", "regulation", "2026-04-29"),
            ReleaseScope("us-ca", "regulation", "2026-05-12-capi-cvcb-regulations"),
            ReleaseScope("us-co", "statute", "2026-04-29"),
        ),
    )
    artifact_report = build_artifact_report(
        store.root,
        prefixes=("sources", "inventory", "provisions", "coverage"),
        document_class="regulation",
        supabase_counts_path=counts_path,
    )

    report = build_regulation_completion_report(
        store.root,
        release=release,
        artifact_report=artifact_report,
        supabase_counts_path=counts_path,
        validation_report_path=validation_path,
        expected_jurisdictions=(
            RegulationJurisdiction("us", "Federal"),
            RegulationJurisdiction("us-co", "Colorado"),
            RegulationJurisdiction("us-ca", "California"),
            RegulationJurisdiction("us-ny", "New York"),
            RegulationJurisdiction("us-az", "Arizona"),
            RegulationJurisdiction("us-ak", "Alaska"),
        ),
    )
    rows = {row.jurisdiction: row for row in report.rows}

    assert rows["us"].status is RegulationCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
    assert rows["us-co"].status is RegulationCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
    assert rows["us-ca"].status is RegulationCompletionStatus.TARGETED_SCOPE_PROMOTED
    assert rows["us-ca"].targeted_scope is True
    assert rows["us-ca"].statewide_scope is False
    assert rows["us-ny"].status is RegulationCompletionStatus.LOCAL_ARTIFACTS_PRESENT_NOT_PROMOTED
    assert rows["us-az"].status is RegulationCompletionStatus.SUPABASE_ONLY_LEGACY
    assert rows["us-ak"].status is RegulationCompletionStatus.MISSING_SOURCE_FIRST_EXTRACTION
    assert report.release_regulation_scope_count == 3
    assert report.status_counts()["productionized_and_validated"] == 2
    assert report.status_counts()["targeted_scope_promoted"] == 1
    assert report.to_mapping()["document_class"] == "regulation"
    assert report.to_mapping()["targeted_scope_promoted_count"] == 1


def test_regulation_completion_cli_writes_report(tmp_path, capsys):
    store = CorpusArtifactStore(tmp_path / "corpus")
    _write_complete_regulation_scope(store, "us", "2026-05-01", count=2)
    (store.root / "releases").mkdir(parents=True)
    (store.root / "releases" / "current.json").write_text(
        json.dumps(
            {
                "name": "current",
                "scopes": [
                    {
                        "jurisdiction": "us",
                        "document_class": "regulation",
                        "version": "2026-05-01",
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
                        "jurisdiction": "us",
                        "document_class": "regulation",
                        "provision_count": 2,
                    }
                ]
            }
        )
    )
    output = store.root / "analytics" / "regulation-completion-current.json"

    exit_code = main(
        [
            "regulation-completion",
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
    federal = next(row for row in written["rows"] if row["jurisdiction"] == "us")

    assert exit_code == 0
    assert payload["written_to"] == str(output)
    assert written["release"] == "current"
    assert written["release_regulation_scope_count"] == 1
    assert federal["status"] == "productionized_and_validated"


def test_regulation_completion_accepts_r2_only_release_artifacts(tmp_path):
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
                        "jurisdiction": "us",
                        "document_class": "regulation",
                        "version": "2026-05-01",
                    }
                ],
            }
        )
    )
    release = ReleaseManifest(
        name="current",
        scopes=(ReleaseScope("us", "regulation", "2026-05-01"),),
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
                jurisdiction="us",
                document_class="regulation",
                version="2026-05-01",
                remote_inventory=True,
                remote_provisions=True,
                remote_coverage=True,
                coverage_complete=True,
                provision_count=4,
            ),
        ),
    )

    report = build_regulation_completion_report(
        tmp_path,
        release=release,
        artifact_report=artifact_report,
        validation_report_path=validation_path,
        expected_jurisdictions=(RegulationJurisdiction("us", "Federal"),),
    )
    row = report.rows[0]

    assert row.status is RegulationCompletionStatus.PRODUCTIONIZED_AND_VALIDATED
    assert row.r2_complete is True
    assert row.validation_codes == ("remote_only_scope_not_deep_validated",)


def _write_complete_regulation_scope(
    store: CorpusArtifactStore,
    jurisdiction: str,
    version: str,
    *,
    count: int,
) -> None:
    source_path = store.source_path(jurisdiction, "regulation", version, "source.html")
    source_sha = store.write_text(source_path, "<html>Official regulation source.</html>")
    records = [
        ProvisionRecord(
            jurisdiction=jurisdiction,
            document_class="regulation",
            citation_path=f"{jurisdiction}/regulation/{index + 1}",
            body=f"Regulation provision {index + 1}.",
            version=version,
            source_path=source_path.relative_to(store.root).as_posix(),
            source_as_of=version,
            expression_date=version,
        )
        for index in range(count)
    ]
    store.write_inventory(
        store.inventory_path(jurisdiction, "regulation", version),
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
    store.write_provisions(
        store.provisions_path(jurisdiction, "regulation", version),
        records,
    )
    store.write_json(
        store.coverage_path(jurisdiction, "regulation", version),
        {
            "complete": True,
            "source_count": count,
            "provision_count": count,
            "matched_count": count,
            "missing_from_provisions": [],
            "extra_provisions": [],
        },
    )
