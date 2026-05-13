import json

import pytest

from axiom_corpus.corpus.models import ProvisionRecord
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope
from axiom_corpus.corpus.supabase import (
    delete_supabase_provisions_scope,
    deterministic_provision_id,
    fetch_provision_counts,
    list_release_scopes,
    load_provisions_to_supabase,
    provision_to_supabase_row,
    refresh_corpus_analytics,
    resolve_service_key,
    set_release_scope_active,
    sync_release_scopes_to_supabase,
    verify_release_coverage,
    write_supabase_rows_jsonl,
)


def test_supabase_projection_derives_stable_ids_and_parent_ids():
    record = ProvisionRecord(
        jurisdiction="us",
        document_class="regulation",
        citation_path="us/regulation/7/273/1",
        parent_citation_path="us/regulation/7/273",
        heading="Household concept",
        body="Text.",
    )

    row = provision_to_supabase_row(record)

    assert row["id"] == deterministic_provision_id("us/regulation/7/273/1")
    assert row["parent_id"] == deterministic_provision_id("us/regulation/7/273")
    assert row["doc_type"] == "regulation"
    assert row["version"] is None
    assert row["has_rulespec"] is False
    assert row["identifiers"] == {}


def test_supabase_projection_uses_versioned_ids_for_release_rows():
    record = ProvisionRecord(
        jurisdiction="us",
        document_class="regulation",
        citation_path="us/regulation/7/273/1",
        parent_citation_path="us/regulation/7/273",
        id=deterministic_provision_id("us/regulation/7/273/1"),
        parent_id=deterministic_provision_id("us/regulation/7/273"),
        version="2026-05-13",
    )

    row = provision_to_supabase_row(record)

    assert row["id"] == deterministic_provision_id(
        "us/regulation/7/273/1",
        "2026-05-13",
    )
    assert row["parent_id"] == deterministic_provision_id(
        "us/regulation/7/273",
        "2026-05-13",
    )
    assert row["version"] == "2026-05-13"


def test_supabase_projection_preserves_non_uuid_source_document_id_as_identifier():
    record = ProvisionRecord(
        jurisdiction="us-ny",
        document_class="regulation",
        citation_path="us-ny/regulation/title-1",
        source_document_id="I0dea9f40aab711ddae51f9dc2e7e68c4",
        identifiers={"nycrr:guid": "I0dea9f40aab711ddae51f9dc2e7e68c4"},
    )

    row = provision_to_supabase_row(record)

    assert row["source_document_id"] is None
    assert row["identifiers"] == {
        "nycrr:guid": "I0dea9f40aab711ddae51f9dc2e7e68c4",
        "source:document_id": "I0dea9f40aab711ddae51f9dc2e7e68c4",
    }


def test_supabase_projection_keeps_uuid_source_document_id_column():
    source_document_id = "11111111-1111-1111-1111-111111111111"
    record = ProvisionRecord(
        jurisdiction="us",
        document_class="policy",
        citation_path="us/policy/source-doc",
        source_document_id=source_document_id,
    )

    row = provision_to_supabase_row(record)

    assert row["source_document_id"] == source_document_id
    assert row["identifiers"] == {}


def test_write_supabase_rows_jsonl_uses_projection_contract(tmp_path):
    out = tmp_path / "rows.jsonl"
    count = write_supabase_rows_jsonl(
        out,
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273",
                heading="Certification of Eligible Households",
                level=0,
            )
        ],
    )

    row = json.loads(out.read_text())
    assert count == 1
    assert row["id"] == deterministic_provision_id("us/regulation/7/273")
    assert row["body"] is None


def test_fetch_provision_counts_reads_materialized_view(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return json.dumps(
                [
                    {
                        "jurisdiction": "us-wa",
                        "document_class": "statute",
                        "provision_count": 54631,
                        "body_count": 51768,
                        "top_level_count": 100,
                        "rulespec_count": 0,
                        "refreshed_at": "2026-05-04T17:00:00+00:00",
                    }
                ]
            ).encode()

    def fake_urlopen(req, timeout):
        calls.append((req.full_url, req.headers, timeout))
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    rows = fetch_provision_counts(
        service_key="service",
        supabase_url="https://example.supabase.co",
    )

    assert rows == (
        {
            "jurisdiction": "us-wa",
            "document_class": "statute",
            "provision_count": 54631,
            "body_count": 51768,
            "top_level_count": 100,
            "rulespec_count": 0,
            "refreshed_at": "2026-05-04T17:00:00+00:00",
        },
    )
    assert calls[0][0].startswith("https://example.supabase.co/rest/v1/current_provision_counts?")
    assert calls[0][1]["Accept-profile"] == "corpus"
    assert calls[0][2] == 180


def test_fetch_provision_counts_can_include_legacy(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return json.dumps(
                [
                    {
                        "jurisdiction": "us-wa",
                        "document_class": "statute",
                        "provision_count": 54631,
                    }
                ]
            ).encode()

    def fake_urlopen(req, timeout):
        calls.append(req.full_url)
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    fetch_provision_counts(
        service_key="service",
        supabase_url="https://example.supabase.co",
        include_legacy=True,
    )

    assert calls[0].startswith("https://example.supabase.co/rest/v1/provision_counts?")


class _SyncFakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return b"{}"


def _patch_sync_urlopen(monkeypatch, calls):
    import axiom_corpus.corpus.supabase as supabase

    def fake_urlopen(req, timeout):
        calls.append((req.get_method(), req.full_url, req.data, timeout))
        return _SyncFakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)


def test_sync_release_scopes_to_supabase_default_is_upsert_incremental(monkeypatch):
    """Default behavior is upsert-only — no PATCH to deactivate existing rows.

    This protects against the 2026-05-12 us-wa/regulation regression where
    a sync from a stale branch's manifest accidentally unpromoted a scope
    added on a different branch.
    """
    calls = []
    _patch_sync_urlopen(monkeypatch, calls)

    report = sync_release_scopes_to_supabase(
        ReleaseManifest(
            name="current",
            scopes=(
                ReleaseScope("us", "statute", "2026-04-30"),
                ReleaseScope("us-co", "statute", "2026-04-30"),
            ),
        ),
        service_key="service",
        supabase_url="https://example.supabase.co",
        chunk_size=1,
    )

    assert report.rows_total == 2
    assert report.rows_loaded == 2
    assert report.chunk_count == 2
    assert report.refreshed
    # No PATCH (no deactivate-all step) — only chunked inserts + refresh.
    assert [call[0] for call in calls] == ["POST", "POST", "POST"]
    assert calls[-1][1] == "https://example.supabase.co/rest/v1/rpc/refresh_corpus_analytics"
    first_insert = json.loads(calls[0][2])
    assert first_insert[0]["release_name"] == "current"
    assert first_insert[0]["jurisdiction"] == "us"
    assert first_insert[0]["active"] is True


def test_sync_release_scopes_to_supabase_exclusive_deactivates_first(monkeypatch):
    """exclusive=True opts into the historical 'deactivate-all then insert' flow."""
    calls = []
    _patch_sync_urlopen(monkeypatch, calls)

    sync_release_scopes_to_supabase(
        ReleaseManifest(
            name="current",
            scopes=(
                ReleaseScope("us", "statute", "2026-04-30"),
                ReleaseScope("us-co", "statute", "2026-04-30"),
            ),
        ),
        service_key="service",
        supabase_url="https://example.supabase.co",
        chunk_size=1,
        exclusive=True,
    )

    # PATCH first (deactivate-all), then chunked POSTs, then refresh.
    assert [call[0] for call in calls] == ["PATCH", "POST", "POST", "POST"]
    assert calls[0][1] == (
        "https://example.supabase.co/rest/v1/release_scopes?"
        "release_name=eq.current&active=eq.true"
    )


def test_load_provisions_to_supabase_upserts_chunks_and_refreshes(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __init__(self, body=b""):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return self.body

    def fake_urlopen(req, timeout):
        calls.append((req.full_url, req.data, timeout))
        if req.get_method() == "GET" and "/release_scopes" in req.full_url:
            return FakeResponse(json.dumps([{
                "release_name": "current",
                "jurisdiction": "us",
                "document_class": "regulation",
                "version": "2026-05-13",
                "active": True,
                "synced_at": "2026-05-13T12:00:00+00:00",
            }]).encode())
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = load_provisions_to_supabase(
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273",
                version="2026-05-13",
            ),
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
                version="2026-05-13",
            ),
        ],
        service_key="service",
        supabase_url="https://example.supabase.co",
        chunk_size=1,
    )

    assert report.rows_total == 2
    assert report.rows_loaded == 2
    assert report.chunk_count == 2
    assert report.refreshed
    # 2 provision chunks + 1 release_scopes auto-register + 1 readback + 1 refresh.
    assert [call[0] for call in calls] == [
        "https://example.supabase.co/rest/v1/provisions?on_conflict=id",
        "https://example.supabase.co/rest/v1/provisions?on_conflict=id",
        (
            "https://example.supabase.co/rest/v1/release_scopes?"
            "on_conflict=release_name,jurisdiction,document_class,version"
        ),
        (
            "https://example.supabase.co/rest/v1/release_scopes?"
            "release_name=eq.current&jurisdiction=eq.us&document_class=eq.regulation&"
            "version=eq.2026-05-13&select=release_name%2Cjurisdiction%2Cdocument_class%2C"
            "version%2Cactive%2Csynced_at&limit=1"
        ),
        "https://example.supabase.co/rest/v1/rpc/refresh_corpus_analytics",
    ]
    # Auto-registered scope appears in the report with active=true (default
    # is publish-on-load).
    assert len(report.auto_registered_scopes) == 1
    scope = report.auto_registered_scopes[0]
    assert scope["jurisdiction"] == "us"
    assert scope["document_class"] == "regulation"
    assert scope["version"] == "2026-05-13"
    assert scope["active"] is True
    first_payload = json.loads(calls[0][1])
    assert first_payload[0]["citation_path"] == "us/regulation/7/273"
    assert first_payload[0]["version"] == "2026-05-13"
    assert first_payload[0]["id"] == deterministic_provision_id(
        "us/regulation/7/273",
        "2026-05-13",
    )


def test_load_provisions_to_supabase_dry_run_does_not_call_network(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    def fake_urlopen(*args, **kwargs):
        raise AssertionError("dry-run should not call network")

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = load_provisions_to_supabase(
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273",
                version="2026-05-13",
            )
        ],
        service_key="",
        dry_run=True,
    )

    assert report.rows_total == 1
    assert report.rows_loaded == 0
    assert report.chunk_count == 1
    assert not report.refreshed


def test_load_provisions_to_supabase_requires_version_when_auto_registering(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    def fake_urlopen(*args, **kwargs):
        raise AssertionError("validation should fail before network")

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(ValueError, match="version is required"):
        load_provisions_to_supabase(
            [
                ProvisionRecord(
                    jurisdiction="us",
                    document_class="regulation",
                    citation_path="us/regulation/7/273",
                )
            ],
            service_key="",
            dry_run=True,
        )


def test_load_provisions_to_supabase_can_preserve_existing_ids(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    title_id = "11111111-1111-1111-1111-111111111111"
    section_id = "22222222-2222-2222-2222-222222222222"
    calls = []

    class FakeResponse:
        def __init__(self, body=b"{}"):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return self.body

    def fake_urlopen(req, timeout):
        calls.append((req.full_url, req.data, timeout))
        if "select=id%2Ccitation_path" in req.full_url:
            return FakeResponse(
                json.dumps(
                    [
                        {"citation_path": "us/statute/1", "id": title_id},
                        {"citation_path": "us/statute/1/1", "id": section_id},
                    ]
                ).encode()
            )
        if req.get_method() == "GET" and "/release_scopes" in req.full_url:
            return FakeResponse(json.dumps([{
                "release_name": "current",
                "jurisdiction": "us",
                "document_class": "statute",
                "version": "2026-05-13",
                "active": True,
                "synced_at": "2026-05-13T12:00:00+00:00",
            }]).encode())
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = load_provisions_to_supabase(
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="statute",
                citation_path="us/statute/1",
                version="2026-05-13",
            ),
            ProvisionRecord(
                jurisdiction="us",
                document_class="statute",
                citation_path="us/statute/1/1",
                parent_citation_path="us/statute/1",
                version="2026-05-13",
            ),
        ],
        service_key="service",
        supabase_url="https://example.supabase.co",
        chunk_size=2,
        preserve_existing_ids=True,
    )

    assert report.existing_id_count == 2
    upsert_payload = json.loads(calls[1][1])
    assert upsert_payload[0]["id"] == title_id
    assert upsert_payload[0]["version"] == "2026-05-13"
    assert upsert_payload[1]["id"] == section_id
    assert upsert_payload[1]["parent_id"] == title_id


def test_delete_supabase_provisions_scope_fetches_ids_then_deletes_chunks(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __init__(self, body=b"{}"):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return self.body

    pages = [
        [
            {"id": "11111111-1111-1111-1111-111111111111"},
            {"id": "22222222-2222-2222-2222-222222222222"},
        ],
        [{"id": "33333333-3333-3333-3333-333333333333"}],
    ]

    def fake_urlopen(req, timeout):
        calls.append((req.full_url, req.get_method(), timeout))
        if req.get_method() == "GET":
            return FakeResponse(json.dumps(pages.pop(0)).encode())
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = delete_supabase_provisions_scope(
        jurisdiction="us-ga",
        document_class="statute",
        service_key="service",
        supabase_url="https://example.supabase.co",
        fetch_page_size=2,
        delete_chunk_size=2,
    )

    assert report.intended_rows_deleted == 3
    assert report.delete_chunk_count == 2
    assert calls[0][0].startswith("https://example.supabase.co/rest/v1/provisions?select=id")
    assert calls[0][1] == "GET"
    assert calls[2][1] == "DELETE"
    assert "id=in." in calls[2][0]


def test_resolve_service_key_prefers_service_role_env():
    key = resolve_service_key(
        "https://example.supabase.co",
        environ={"SUPABASE_SERVICE_ROLE_KEY": "service", "SUPABASE_ACCESS_TOKEN": "token"},
    )

    assert key == "service"


def test_resolve_service_key_fetches_service_role_from_management_api(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b'[{"name": "service_role", "api_key": "service"}]'

    calls = []

    def fake_urlopen(req, timeout):
        calls.append((req.full_url, req.headers["Authorization"]))
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    key = resolve_service_key(
        "https://abc123.supabase.co",
        environ={"SUPABASE_ACCESS_TOKEN": "management"},
    )

    assert key == "service"
    assert calls == [("https://api.supabase.com/v1/projects/abc123/api-keys", "Bearer management")]


def test_refresh_corpus_analytics_calls_current_rpc(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __init__(self, body=b"{}"):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b"{}"

    def fake_urlopen(req, timeout):
        calls.append(req.full_url)
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    refresh_corpus_analytics(service_key="service", rest_url="https://example.supabase.co/rest/v1")

    assert calls == [
        "https://example.supabase.co/rest/v1/rpc/refresh_corpus_analytics",
    ]


def test_verify_release_coverage_flags_jurisdictions_missing_current_provisions(monkeypatch):
    """The historical UK regression: rows in navigation_nodes, zero in
    current_provisions. The check must catch this."""
    import axiom_corpus.corpus.supabase as supabase

    nav_count_rpc_rows = [
        {"jurisdiction": "us", "document_class": "statute", "node_count": 1000},
        {"jurisdiction": "uk", "document_class": "regulation", "node_count": 4705},
        {"jurisdiction": "us-ca", "document_class": "statute", "node_count": 7948},
    ]
    current_rows = [
        {"jurisdiction": "us", "document_class": "statute", "provision_count": 1000},
        {"jurisdiction": "us-ca", "document_class": "statute", "provision_count": 7948},
        # uk deliberately absent → should be flagged
    ]

    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return json.dumps(self.payload).encode()

    def fake_urlopen(req, timeout):
        url = req.full_url
        if "/rpc/get_navigation_node_counts" in url:
            return FakeResponse(nav_count_rpc_rows)
        if "/current_provision_counts" in url:
            return FakeResponse(current_rows)
        raise AssertionError(f"unexpected URL {url}")

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = verify_release_coverage(
        service_key="service",
        supabase_url="https://example.supabase.co",
    )

    assert report.ok is False
    assert len(report.missing_current_provisions) == 1
    finding = report.missing_current_provisions[0]
    assert finding.jurisdiction == "uk"
    assert finding.document_class == "regulation"
    assert finding.navigation_node_count == 4705
    assert finding.current_provision_count == 0


def test_verify_release_coverage_clean_when_all_jurisdictions_covered(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    nav_count_rpc_rows = [
        {"jurisdiction": "us", "document_class": "statute", "node_count": 1000},
        {"jurisdiction": "uk", "document_class": "regulation", "node_count": 4705},
    ]
    current_rows = [
        {"jurisdiction": "us", "document_class": "statute", "provision_count": 1000},
        {"jurisdiction": "uk", "document_class": "regulation", "provision_count": 4705},
    ]

    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return json.dumps(self.payload).encode()

    def fake_urlopen(req, timeout):
        url = req.full_url
        if "/rpc/get_navigation_node_counts" in url:
            return FakeResponse(nav_count_rpc_rows)
        if "/current_provision_counts" in url:
            return FakeResponse(current_rows)
        raise AssertionError(f"unexpected URL {url}")

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = verify_release_coverage(
        service_key="service",
        supabase_url="https://example.supabase.co",
    )

    assert report.ok is True
    assert report.missing_current_provisions == ()


def test_load_provisions_auto_publish_false_stages_load(monkeypatch):
    """auto_publish=False registers the scope as inactive (staged)."""
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __init__(self, body=b"{}"):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return self.body

    def fake_urlopen(req, timeout):
        calls.append((req.get_method(), req.full_url, req.data))
        if req.get_method() == "GET" and "/release_scopes" in req.full_url:
            return FakeResponse(json.dumps([{
                "release_name": "current",
                "jurisdiction": "us-co",
                "document_class": "regulation",
                "version": "2026-04-29",
                "active": False,
                "synced_at": "2026-05-13T12:00:00+00:00",
            }]).encode())
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = load_provisions_to_supabase(
        [
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="regulation",
                citation_path="us-co/regulation/10-ccr-2506-1/4.207.3",
                version="2026-04-29",
            ),
        ],
        service_key="service",
        supabase_url="https://example.supabase.co",
        chunk_size=10,
        auto_publish=False,
    )

    assert report.rows_loaded == 1
    assert len(report.auto_registered_scopes) == 1
    scope = report.auto_registered_scopes[0]
    assert scope["jurisdiction"] == "us-co"
    assert scope["active"] is False
    # Confirm the release_scopes POST went through with active=false
    rs_calls = [
        call for call in calls if call[0] == "POST" and "release_scopes" in call[1]
    ]
    assert len(rs_calls) == 1
    payload = json.loads(rs_calls[0][2])
    assert payload[0]["active"] is False


def test_load_provisions_no_auto_register_skips_release_scopes(monkeypatch):
    """auto_register_scopes=False skips the release_scopes upsert entirely."""
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b"{}"

    def fake_urlopen(req, timeout):
        calls.append(req.full_url)
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = load_provisions_to_supabase(
        [
            ProvisionRecord(
                jurisdiction="us-ny",
                document_class="statute",
                citation_path="us-ny/statute/labor/650",
                version="2026-05-06",
            ),
        ],
        service_key="service",
        supabase_url="https://example.supabase.co",
        chunk_size=10,
        auto_register_scopes=False,
    )

    assert report.auto_registered_scopes == ()
    assert not any("release_scopes" in c for c in calls)


def test_set_release_scope_active_with_version_pins_the_patch(monkeypatch):
    """Explicit version causes a single PATCH on that exact row."""
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __init__(self, body):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return self.body

    def fake_urlopen(req, timeout):
        calls.append((req.get_method(), req.full_url, req.data))
        if "/release_scopes" in req.full_url and req.get_method() == "PATCH":
            return FakeResponse(json.dumps([{
                "release_name": "current",
                "jurisdiction": "us-ms",
                "document_class": "statute",
                "version": "2026-05-12",
                "active": True,
                "synced_at": "2026-05-12T18:00:00+00:00",
            }]).encode())
        # Refresh RPC
        return FakeResponse(b"")

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    result = set_release_scope_active(
        jurisdiction="us-ms",
        document_class="statute",
        active=True,
        version="2026-05-12",
        service_key="service",
        supabase_url="https://example.supabase.co",
    )

    assert result["scope"]["active"] is True
    assert result["refreshed"] is True
    patch_calls = [c for c in calls if c[0] == "PATCH"]
    assert len(patch_calls) == 1
    assert "version=eq.2026-05-12" in patch_calls[0][1]


def test_set_release_scope_active_finds_latest_when_version_omitted(monkeypatch):
    """If --version is not specified, the function queries for the latest row first."""
    import axiom_corpus.corpus.supabase as supabase

    calls = []

    class FakeResponse:
        def __init__(self, body):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return self.body

    def fake_urlopen(req, timeout):
        method = req.get_method()
        url = req.full_url
        calls.append((method, url))
        if method == "GET" and "/release_scopes" in url:
            return FakeResponse(json.dumps([{
                "version": "2026-05-12",
                "synced_at": "2026-05-12T18:00:00+00:00",
                "active": False,
            }]).encode())
        if method == "PATCH":
            return FakeResponse(json.dumps([{
                "version": "2026-05-12",
                "active": True,
            }]).encode())
        return FakeResponse(b"")

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    set_release_scope_active(
        jurisdiction="us-ar",
        document_class="statute",
        active=True,
        service_key="service",
        supabase_url="https://example.supabase.co",
    )

    # First call: GET to find latest. Second: PATCH on that version.
    assert calls[0][0] == "GET"
    assert "order=synced_at.desc" in calls[0][1]
    assert calls[1][0] == "PATCH"
    assert "version=eq.2026-05-12" in calls[1][1]


def test_list_release_scopes_filters_by_active(monkeypatch):
    import axiom_corpus.corpus.supabase as supabase

    captured_url = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return json.dumps([
                {
                    "release_name": "current",
                    "jurisdiction": "us-ar",
                    "document_class": "statute",
                    "version": "2026-04-22",
                    "active": False,
                }
            ]).encode()

    def fake_urlopen(req, timeout):
        captured_url.append(req.full_url)
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    rows = list_release_scopes(
        active=False,
        service_key="service",
        supabase_url="https://example.supabase.co",
    )

    assert len(rows) == 1
    assert rows[0]["jurisdiction"] == "us-ar"
    assert "active=is.false" in captured_url[0]


def test_load_provisions_auto_register_uses_ignore_duplicates(monkeypatch):
    """Re-load should never silently flip an existing row's active flag.

    Auto-register uses Prefer: resolution=ignore-duplicates so that a
    re-load of an already-promoted scope leaves the existing row alone.
    This protects against:
      * --stage on a re-load demoting a previously-published scope
      * a default load undoing an explicit unpublish

    State changes always require explicit publish/unpublish.
    """
    import axiom_corpus.corpus.supabase as supabase

    captured_headers = []

    class FakeResponse:
        def __init__(self, body=b""):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return self.body

    def fake_urlopen(req, timeout):
        if "release_scopes" in req.full_url and req.get_method() == "POST":
            captured_headers.append(dict(req.headers))
        if "release_scopes" in req.full_url and req.get_method() == "GET":
            return FakeResponse(json.dumps([{
                "release_name": "current",
                "jurisdiction": "us-mo",
                "document_class": "statute",
                "version": "2026-05-13",
                "active": False,
                "synced_at": "2026-05-12T12:00:00+00:00",
            }]).encode())
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = load_provisions_to_supabase(
        [
            ProvisionRecord(
                jurisdiction="us-mo",
                document_class="statute",
                citation_path="us-mo/statute/akn/135.212",
                version="2026-05-13",
            ),
        ],
        service_key="service",
        supabase_url="https://example.supabase.co",
        chunk_size=10,
    )

    # One release_scopes POST should have been captured, with
    # ignore-duplicates resolution.
    assert len(captured_headers) == 1
    prefer = captured_headers[0].get("Prefer") or ""
    assert "resolution=ignore-duplicates" in prefer
    assert "resolution=merge-duplicates" not in prefer
    assert report.auto_registered_scopes[0]["active"] is False
