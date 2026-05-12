import json

from axiom_corpus.corpus.models import ProvisionRecord
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope
from axiom_corpus.corpus.supabase import (
    delete_supabase_provisions_scope,
    deterministic_provision_id,
    fetch_provision_counts,
    load_provisions_to_supabase,
    provision_to_supabase_row,
    refresh_corpus_analytics,
    resolve_service_key,
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
    assert row["has_rulespec"] is False
    assert row["identifiers"] == {}


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


def test_sync_release_scopes_to_supabase_replaces_active_scope_set(monkeypatch):
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
        calls.append((req.get_method(), req.full_url, req.data, timeout))
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

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
    assert [call[0] for call in calls] == ["PATCH", "POST", "POST", "POST"]
    assert calls[0][1] == (
        "https://example.supabase.co/rest/v1/release_scopes?"
        "release_name=eq.current&active=eq.true"
    )
    assert calls[-1][1] == "https://example.supabase.co/rest/v1/rpc/refresh_corpus_analytics"
    first_insert = json.loads(calls[1][2])
    assert first_insert[0]["release_name"] == "current"
    assert first_insert[0]["jurisdiction"] == "us"
    assert first_insert[0]["active"] is True


def test_load_provisions_to_supabase_upserts_chunks_and_refreshes(monkeypatch):
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
        calls.append((req.full_url, req.data, timeout))
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = load_provisions_to_supabase(
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273",
            ),
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
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
    assert [call[0] for call in calls] == [
        "https://example.supabase.co/rest/v1/provisions?on_conflict=id",
        "https://example.supabase.co/rest/v1/provisions?on_conflict=id",
        "https://example.supabase.co/rest/v1/rpc/refresh_corpus_analytics",
    ]
    assert json.loads(calls[0][1])[0]["citation_path"] == "us/regulation/7/273"


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
            )
        ],
        service_key="",
        dry_run=True,
    )

    assert report.rows_total == 1
    assert report.rows_loaded == 0
    assert report.chunk_count == 1
    assert not report.refreshed


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
        return FakeResponse()

    monkeypatch.setattr(supabase.urllib.request, "urlopen", fake_urlopen)

    report = load_provisions_to_supabase(
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="statute",
                citation_path="us/statute/1",
            ),
            ProvisionRecord(
                jurisdiction="us",
                document_class="statute",
                citation_path="us/statute/1/1",
                parent_citation_path="us/statute/1",
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
