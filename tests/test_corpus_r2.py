import json
from io import BytesIO
from pathlib import Path

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.r2 import (
    R2Config,
    RemoteArtifact,
    build_artifact_report,
    build_artifact_report_with_r2,
    build_release_artifact_manifest,
    load_r2_config,
    sync_artifacts_to_r2,
)


class FakeR2Client:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.uploads = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        assert kwargs["Bucket"] == "axiom-corpus"
        contents = [
            {"Key": key, "Size": _object_size(payload), "ETag": f'"{key}"'}
            for key, payload in sorted(self.objects.items())
            if key.startswith(kwargs["Prefix"])
        ]
        return [{"Contents": contents}]

    def upload_file(self, filename, bucket, key, **kwargs):
        assert bucket == "axiom-corpus"
        self.uploads.append((Path(filename), key, kwargs["ExtraArgs"]))
        self.objects[key] = Path(filename).stat().st_size

    def get_object(self, **kwargs):
        assert kwargs["Bucket"] == "axiom-corpus"
        payload = self.objects[kwargs["Key"]]
        if isinstance(payload, int):
            payload = b""
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        return {"Body": BytesIO(payload)}


def _object_size(payload):
    if isinstance(payload, int):
        return payload
    if isinstance(payload, str):
        return len(payload.encode("utf-8"))
    return len(payload)


def test_load_r2_config_uses_env_without_exposing_secret():
    config = load_r2_config(
        environ={
            "R2_ACCOUNT_ID": "acct",
            "R2_ACCESS_KEY_ID": "key",
            "R2_SECRET_ACCESS_KEY": "secret",
        },
        credential_path=Path("/missing/credentials.json"),
    )

    assert config.bucket == "axiom-corpus"
    assert config.endpoint_url == "https://acct.r2.cloudflarestorage.com"
    assert config.access_key_id == "key"
    assert config.secret_access_key == "secret"


def test_sync_artifacts_to_r2_uploads_missing_and_size_changed_files(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-co/policy/doc")],
    )
    store.write_provisions(
        store.provisions_path("us-co", "policy", "2026-04-30"),
        [
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="policy",
                citation_path="us-co/policy/doc",
                body="Text.",
            )
        ],
    )
    unchanged = store.inventory_path("us-co", "policy", "2026-04-30")
    client = FakeR2Client({"inventory/us-co/policy/2026-04-30.json": unchanged.stat().st_size})
    config = R2Config(
        bucket="axiom-corpus",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
    )

    dry = sync_artifacts_to_r2(
        store.root,
        config=config,
        client=client,
        prefixes=("inventory", "provisions"),
        dry_run=True,
    )
    assert dry.candidate_upload_count == 1
    assert dry.planned_upload_count == 1
    assert dry.limited_upload_count == 0
    assert dry.uploaded_count == 0
    assert client.uploads == []

    live = sync_artifacts_to_r2(
        store.root,
        config=config,
        client=client,
        prefixes=("inventory", "provisions"),
        dry_run=False,
    )

    assert live.planned_upload_count == 1
    assert live.uploaded_keys == ("provisions/us-co/policy/2026-04-30.jsonl",)
    assert len(client.uploads) == 1
    assert client.uploads[0][2]["Metadata"]["sha256"]


def test_sync_artifacts_to_r2_force_uploads_same_size_files(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_json(store.root / "snapshots" / "counts.json", {"rows": []})
    snapshot = store.root / "snapshots" / "counts.json"
    client = FakeR2Client({"snapshots/counts.json": snapshot.stat().st_size})
    config = R2Config(
        bucket="axiom-corpus",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
    )

    report = sync_artifacts_to_r2(
        store.root,
        config=config,
        client=client,
        prefixes=("snapshots",),
        dry_run=False,
        force=True,
    )

    assert report.planned_upload_count == 1
    assert report.skipped_count == 0
    assert report.uploaded_keys == ("snapshots/counts.json",)


def test_sync_artifacts_to_r2_filters_scope(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_provisions(
        store.provisions_path("us-co", "policy", "2026-04-30"),
        [
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="policy",
                citation_path="us-co/policy/doc",
                body="Text.",
            )
        ],
    )
    store.write_provisions(
        store.provisions_path("us", "statute", "2026-04-29"),
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="statute",
                citation_path="us/statute/1",
                body="Text.",
            )
        ],
    )
    client = FakeR2Client()
    config = R2Config(
        bucket="axiom-corpus",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
    )

    report = sync_artifacts_to_r2(
        store.root,
        config=config,
        client=client,
        prefixes=("provisions",),
        jurisdiction="us-co",
        document_class="policy",
        version="2026-04-30",
        dry_run=True,
    )

    assert report.local_count == 1
    assert report.planned_upload_count == 1
    assert report.bytes_planned == (
        store.provisions_path("us-co", "policy", "2026-04-30").stat().st_size
    )


def test_artifact_report_flags_r2_and_supabase_mismatches(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-co/policy/doc")],
    )
    store.write_provisions(
        store.provisions_path("us-co", "policy", "2026-04-30"),
        [
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="policy",
                citation_path="us-co/policy/doc",
                body="Text.",
            )
        ],
    )
    store.write_json(
        store.coverage_path("us-co", "policy", "2026-04-30"),
        {
            "complete": True,
            "source_count": 1,
            "provision_count": 1,
            "matched_count": 1,
            "missing_from_provisions": [],
            "extra_provisions": [],
        },
    )
    counts = tmp_path / "counts.json"
    counts.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "policy",
                        "provision_count": 2,
                    }
                ]
            }
        )
    )

    report = build_artifact_report(
        store.root,
        prefixes=("inventory", "provisions", "coverage"),
        supabase_counts_path=counts,
        remote={
            "inventory/us-co/policy/2026-04-30.json": RemoteArtifact(
                key="inventory/us-co/policy/2026-04-30.json",
                size=1,
            )
        },
    )

    row = report.rows[0]
    assert row.coverage_complete is True
    assert row.supabase_count == 2
    assert row.remote_inventory is True
    assert row.remote_provisions is False
    assert set(row.mismatch_reasons()) == {
        "missing_r2_provisions",
        "missing_r2_coverage",
        "supabase_count_mismatch",
    }


def test_artifact_report_compares_supabase_counts_across_release_scopes(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    for version, count in (("2026-05-01-a", 1), ("2026-05-02-b", 2)):
        records = [
            ProvisionRecord(
                jurisdiction="us",
                document_class="guidance",
                citation_path=f"us/guidance/{version}/{index}",
                body="Text.",
            )
            for index in range(count)
        ]
        store.write_inventory(
            store.inventory_path("us", "guidance", version),
            [SourceInventoryItem(citation_path=record.citation_path) for record in records],
        )
        store.write_provisions(store.provisions_path("us", "guidance", version), records)
        store.write_json(
            store.coverage_path("us", "guidance", version),
            {
                "complete": True,
                "source_count": count,
                "provision_count": count,
                "matched_count": count,
                "missing_from_provisions": [],
                "extra_provisions": [],
            },
        )
    counts = tmp_path / "counts.json"
    counts.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "jurisdiction": "us",
                        "document_class": "guidance",
                        "provision_count": 3,
                    }
                ]
            }
        )
    )

    report = build_artifact_report(
        store.root,
        prefixes=("inventory", "provisions", "coverage"),
        supabase_counts_path=counts,
        release_scopes=(
            ("us", "guidance", "2026-05-01-a"),
            ("us", "guidance", "2026-05-02-b"),
        ),
    )
    payload = report.to_mapping()

    assert [row.supabase_count for row in report.rows] == [None, None]
    assert payload["mismatch_count"] == 0
    assert payload["supabase_mismatch_count"] == 0
    assert payload["supabase_groups"] == [
        {
            "document_class": "guidance",
            "jurisdiction": "us",
            "mismatch_reasons": [],
            "provision_count": 3,
            "scope_count": 2,
            "supabase_count": 3,
            "supabase_matches_provisions": True,
            "versions": ["2026-05-01-a", "2026-05-02-b"],
        }
    ]


def test_artifact_report_with_r2_reads_remote_coverage_counts(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    coverage_key = "coverage/us-co/statute/2026-04-30.json"
    client = FakeR2Client(
        {
            "inventory/us-co/statute/2026-04-30.json": 1,
            "provisions/us-co/statute/2026-04-30.jsonl": 1,
            coverage_key: json.dumps(
                {
                    "complete": True,
                    "source_count": 7,
                    "provision_count": 7,
                    "matched_count": 7,
                    "missing_from_provisions": [],
                    "extra_provisions": [],
                }
            ),
        }
    )
    config = R2Config(
        bucket="axiom-corpus",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
    )

    report = build_artifact_report_with_r2(
        store.root,
        config=config,
        client=client,
        prefixes=("inventory", "provisions", "coverage"),
        release_scopes=(("us-co", "statute", "2026-04-30"),),
    )

    row = report.rows[0]
    assert row.remote_inventory is True
    assert row.remote_provisions is True
    assert row.remote_coverage is True
    assert row.coverage_complete is True
    assert row.provision_count == 7
    assert row.mismatch_reasons() == ()


def test_sync_artifacts_to_r2_can_scope_uploads(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-co/policy/doc")],
    )
    store.write_inventory(
        store.inventory_path("us-ny", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-ny/policy/doc")],
    )
    client = FakeR2Client()
    config = R2Config(
        bucket="axiom-corpus",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
    )

    report = sync_artifacts_to_r2(
        store.root,
        config=config,
        client=client,
        prefixes=("inventory",),
        jurisdiction="us-co",
        document_class="policy",
        version="2026-04-30",
        dry_run=False,
    )

    assert report.local_count == 1
    assert report.remote_count == 0
    assert report.uploaded_keys == ("inventory/us-co/policy/2026-04-30.json",)


def test_sync_artifacts_to_r2_supports_parallel_workers(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    for jurisdiction in ("us-co", "us-ny", "us-tx"):
        store.write_inventory(
            store.inventory_path(jurisdiction, "policy", "2026-04-30"),
            [SourceInventoryItem(citation_path=f"{jurisdiction}/policy/doc")],
        )
    client = FakeR2Client()
    config = R2Config(
        bucket="axiom-corpus",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
    )

    report = sync_artifacts_to_r2(
        store.root,
        config=config,
        client=client,
        prefixes=("inventory",),
        dry_run=False,
        workers=2,
    )

    assert report.uploaded_count == 3
    assert report.uploaded_keys == (
        "inventory/us-co/policy/2026-04-30.json",
        "inventory/us-ny/policy/2026-04-30.json",
        "inventory/us-tx/policy/2026-04-30.json",
    )


def test_artifact_report_scope_filters_counts_and_rows(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-co/policy/doc")],
    )
    store.write_inventory(
        store.inventory_path("us-ny", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-ny/policy/doc")],
    )
    remote = {
        "inventory/us-co/policy/2026-04-30.json": RemoteArtifact(
            key="inventory/us-co/policy/2026-04-30.json",
            size=1,
        ),
        "inventory/us-ny/policy/2026-04-30.json": RemoteArtifact(
            key="inventory/us-ny/policy/2026-04-30.json",
            size=1,
        ),
    }

    report = build_artifact_report(
        store.root,
        prefixes=("inventory",),
        jurisdiction="us-co",
        document_class="policy",
        version="2026-04-30",
        remote=remote,
    )

    assert report.local_count == 1
    assert report.remote_count == 1
    assert len(report.rows) == 1
    assert report.rows[0].jurisdiction == "us-co"


def test_artifact_report_release_filters_and_seeds_missing_scopes(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-co/policy/doc")],
    )
    store.write_inventory(
        store.inventory_path("us-ny", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-ny/policy/doc")],
    )

    report = build_artifact_report(
        store.root,
        prefixes=("inventory",),
        release_name="current",
        release_scopes=(
            ("us-co", "policy", "2026-04-30"),
            ("us-tx", "policy", "2026-04-30"),
        ),
    )

    assert report.release_name == "current"
    assert report.release_scope_count == 2
    assert report.local_count == 1
    assert [(row.jurisdiction, row.document_class, row.version) for row in report.rows] == [
        ("us-co", "policy", "2026-04-30"),
        ("us-tx", "policy", "2026-04-30"),
    ]
    assert report.rows[0].local_inventory is True
    assert report.rows[1].local_inventory is False


def test_release_artifact_manifest_hashes_release_scoped_artifacts(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    source = store.source_path("us-co", "policy", "2026-04-30", "source.html")
    source_sha = store.write_text(source, "<p>Text.</p>")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [
            SourceInventoryItem(
                citation_path="us-co/policy/doc",
                source_path=source.relative_to(store.root).as_posix(),
                sha256=source_sha,
            )
        ],
    )
    store.write_provisions(
        store.provisions_path("us-co", "policy", "2026-04-30"),
        [
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="policy",
                citation_path="us-co/policy/doc",
                version="2026-04-30",
                body="Text.",
            )
        ],
    )
    store.write_json(
        store.coverage_path("us-co", "policy", "2026-04-30"),
        {
            "complete": True,
            "source_count": 1,
            "provision_count": 1,
            "matched_count": 1,
            "missing_from_provisions": [],
            "extra_provisions": [],
        },
    )

    manifest = build_release_artifact_manifest(
        store.root,
        release_name="current",
        release_scopes=(("us-co", "policy", "2026-04-30"),),
        created_at="2026-05-02T00:00:00+00:00",
    )
    payload = manifest.to_mapping()

    assert payload["release"] == "current"
    assert payload["artifact_count"] == 4
    assert payload["release_scope_count"] == 1
    assert payload["by_prefix"] == {
        "coverage": {
            "bytes": store.coverage_path("us-co", "policy", "2026-04-30").stat().st_size,
            "count": 1,
        },
        "inventory": {
            "bytes": store.inventory_path("us-co", "policy", "2026-04-30").stat().st_size,
            "count": 1,
        },
        "provisions": {
            "bytes": store.provisions_path("us-co", "policy", "2026-04-30").stat().st_size,
            "count": 1,
        },
        "sources": {"bytes": source.stat().st_size, "count": 1},
    }
    assert {entry["key"] for entry in payload["artifacts"]} == {
        "coverage/us-co/policy/2026-04-30.json",
        "inventory/us-co/policy/2026-04-30.json",
        "provisions/us-co/policy/2026-04-30.jsonl",
        "sources/us-co/policy/2026-04-30/source.html",
    }
    assert all(entry["sha256"] for entry in payload["artifacts"])
