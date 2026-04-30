import json
from pathlib import Path

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.r2 import (
    R2Config,
    RemoteArtifact,
    build_artifact_report,
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
            {"Key": key, "Size": size, "ETag": f'"{key}"'}
            for key, size in sorted(self.objects.items())
            if key.startswith(kwargs["Prefix"])
        ]
        return [{"Contents": contents}]

    def upload_file(self, filename, bucket, key, **kwargs):
        assert bucket == "axiom-corpus"
        self.uploads.append((Path(filename), key, kwargs["ExtraArgs"]))
        self.objects[key] = Path(filename).stat().st_size


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
