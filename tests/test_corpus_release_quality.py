import pytest

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.r2 import ArtifactReport, ArtifactScopeRow, ArtifactSupabaseGroup
from axiom_corpus.corpus.release_quality import validate_release
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope


def test_validate_release_reports_artifact_report_mismatches(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    artifact_report = ArtifactReport(
        local_root=store.root,
        prefixes=(),
        local_count=0,
        local_bytes=0,
        local_by_prefix={},
        remote_count=None,
        remote_bytes=None,
        remote_by_prefix=None,
        rows=(ArtifactScopeRow(jurisdiction="us-co", document_class="statute", version="v1"),),
        supabase_groups=(
            ArtifactSupabaseGroup(
                jurisdiction="us-co",
                document_class="statute",
                scope_count=1,
                versions=("v1",),
                provision_count=1,
                supabase_count=2,
            ),
        ),
    )

    report = validate_release(
        store.root,
        ReleaseManifest(name="current", scopes=()),
        artifact_report=artifact_report,
        max_issues=1,
    )
    payload = report.to_mapping()

    assert report.error_count == 2
    assert report.truncated is True
    assert payload["issues_truncated"] is True
    assert payload["issues"][0]["code"] == "artifact_report_mismatch"
    with pytest.raises(ValueError, match="max_issues"):
        validate_release(store.root, ReleaseManifest(name="current", scopes=()), max_issues=0)


def test_validate_release_accepts_r2_complete_remote_only_scope(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    artifact_report = ArtifactReport(
        local_root=store.root,
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
                provision_count=10,
                supabase_count=10,
            ),
        ),
    )

    report = validate_release(
        store.root,
        ReleaseManifest(
            name="current",
            scopes=(ReleaseScope("us-co", "statute", "v1"),),
        ),
        artifact_report=artifact_report,
    )

    assert report.ok is True
    assert report.error_count == 0
    assert report.warning_count == 1
    assert report.issues[0].code == "remote_only_scope_not_deep_validated"


def test_validate_release_reports_scope_invariant_errors(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    version = "2026-04-29"
    source = store.source_path("us-co", "statute", version, "source.html")
    store.write_text(source, "<p>Official source.</p>")
    store.write_inventory(
        store.inventory_path("us-co", "statute", version),
        [
            SourceInventoryItem(
                citation_path="",
                source_path=source.relative_to(store.root).as_posix(),
                sha256="wrong-sha",
            ),
            SourceInventoryItem(citation_path="us-co/statute/1"),
        ],
    )
    store.write_provisions(
        store.provisions_path("us-co", "statute", version),
        [
            ProvisionRecord(
                jurisdiction="us-ny",
                document_class="policy",
                citation_path="us-co/statute/1",
                id="duplicate-id",
                version="old",
                parent_citation_path="missing-parent",
                expression_date="not-a-date",
            ),
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="statute",
                citation_path="us-co/statute/1",
                id="duplicate-id",
                body="Duplicate.",
                version=version,
                parent_citation_path="us-co/statute/1",
                parent_id="wrong-parent",
                source_as_of=version,
                expression_date=version,
            ),
        ],
    )
    store.write_json(
        store.coverage_path("us-co", "statute", version),
        {
            "complete": False,
            "source_count": 99,
            "provision_count": 99,
            "matched_count": 99,
            "missing_from_provisions": [""],
            "extra_provisions": [],
        },
    )
    release = ReleaseManifest(
        name="current",
        scopes=(ReleaseScope("us-co", "statute", version),),
    )

    report = validate_release(store.root, release, strict_warnings=True)
    codes = {issue.code for issue in report.issues}

    assert report.ok is False
    assert {
        "coverage_count_mismatch",
        "coverage_incomplete",
        "duplicate_provision_citation",
        "duplicate_provision_id",
        "empty_inventory_citation",
        "empty_provision_text",
        "invalid_expression_date",
        "missing_inventory_source_path",
        "missing_parent_citation",
        "missing_parent_id",
        "parent_id_mismatch",
        "persisted_coverage_incomplete",
        "provision_document_class_mismatch",
        "provision_jurisdiction_mismatch",
        "provision_version_mismatch",
        "source_sha256_mismatch",
    }.issubset(codes)


def test_validate_release_reports_missing_and_invalid_artifacts(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    version = "2026-04-29"
    store.inventory_path("us-bad-inventory", "statute", version).parent.mkdir(parents=True)
    store.inventory_path("us-bad-inventory", "statute", version).write_text("{")

    store.write_inventory(
        store.inventory_path("us-bad-provisions", "statute", version),
        [SourceInventoryItem(citation_path="us-bad-provisions/statute/1")],
    )
    store.provisions_path("us-bad-provisions", "statute", version).parent.mkdir(parents=True)
    store.provisions_path("us-bad-provisions", "statute", version).write_text("{")

    store.write_inventory(
        store.inventory_path("us-bad-coverage", "statute", version),
        [SourceInventoryItem(citation_path="us-bad-coverage/statute/1")],
    )
    store.write_provisions(
        store.provisions_path("us-bad-coverage", "statute", version),
        [
            ProvisionRecord(
                jurisdiction="us-bad-coverage",
                document_class="statute",
                citation_path="us-bad-coverage/statute/1",
                body="Text.",
                version=version,
                source_as_of=version,
                expression_date=version,
            )
        ],
    )
    store.coverage_path("us-bad-coverage", "statute", version).parent.mkdir(parents=True)
    store.coverage_path("us-bad-coverage", "statute", version).write_text("[]")

    store.write_inventory(
        store.inventory_path("us-json-coverage", "statute", version),
        [SourceInventoryItem(citation_path="us-json-coverage/statute/1")],
    )
    store.write_provisions(
        store.provisions_path("us-json-coverage", "statute", version),
        [
            ProvisionRecord(
                jurisdiction="us-json-coverage",
                document_class="statute",
                citation_path="us-json-coverage/statute/1",
                body="Text.",
                version=version,
                source_as_of=version,
                expression_date=version,
            )
        ],
    )
    store.coverage_path("us-json-coverage", "statute", version).parent.mkdir(parents=True)
    store.coverage_path("us-json-coverage", "statute", version).write_text("{")

    store.write_inventory(
        store.inventory_path("us-bogus-class", "bogus", version),
        [SourceInventoryItem(citation_path="us-bogus-class/bogus/1")],
    )
    store.write_provisions(
        store.provisions_path("us-bogus-class", "bogus", version),
        [
            ProvisionRecord(
                jurisdiction="us-bogus-class",
                document_class="bogus",
                citation_path="us-bogus-class/bogus/1",
                body="Text.",
                version=version,
                source_as_of=version,
                expression_date=version,
            )
        ],
    )
    store.write_json(
        store.coverage_path("us-bogus-class", "bogus", version),
        {
            "complete": True,
            "source_count": 1,
            "provision_count": 1,
            "matched_count": 1,
            "missing_from_provisions": [],
            "extra_provisions": [],
        },
    )

    release = ReleaseManifest(
        name="current",
        scopes=(
            ReleaseScope("us-missing", "statute", version),
            ReleaseScope("us-bad-inventory", "statute", version),
            ReleaseScope("us-bad-provisions", "statute", version),
            ReleaseScope("us-bad-coverage", "statute", version),
            ReleaseScope("us-json-coverage", "statute", version),
            ReleaseScope("us-bogus-class", "bogus", version),
        ),
    )

    report = validate_release(store.root, release)
    codes = {issue.code for issue in report.issues}

    assert {
        "invalid_coverage",
        "invalid_document_class",
        "invalid_inventory",
        "invalid_provisions",
        "missing_coverage",
        "missing_inventory",
        "missing_provisions",
    }.issubset(codes)
