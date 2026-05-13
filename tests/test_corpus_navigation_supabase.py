"""Tests for the `corpus.navigation_nodes` Supabase writer."""

from __future__ import annotations

import json

from axiom_corpus.corpus.models import ProvisionRecord
from axiom_corpus.corpus.navigation import build_navigation_nodes
from axiom_corpus.corpus.navigation_supabase import (
    fetch_navigation_statuses,
    fetch_provisions_for_navigation,
    write_navigation_nodes_to_supabase,
)


class _FakeResponse:
    def __init__(self, payload: bytes = b"{}"):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return self._payload


def _record(citation_path: str, **kwargs) -> ProvisionRecord:
    return ProvisionRecord(
        jurisdiction=kwargs.pop("jurisdiction", "us-co"),
        document_class=kwargs.pop("document_class", "statute"),
        citation_path=citation_path,
        **kwargs,
    )


def test_write_navigation_nodes_chunks_upserts_and_replaces_scope(monkeypatch):
    import axiom_corpus.corpus.navigation_supabase as module

    nodes = build_navigation_nodes(
        [
            _record("us-co/statute/title-39"),
            _record(
                "us-co/statute/title-39/article-22",
                parent_citation_path="us-co/statute/title-39",
            ),
        ]
    )

    calls: list[tuple[str, str | None, bytes | None]] = []
    fetch_responses = iter(
        [
            json.dumps(
                [
                    {"path": "us-co/statute/title-39"},
                    {"path": "us-co/statute/title-39/old-article"},
                ]
            ).encode(),
        ]
    )

    def fake_urlopen(req, timeout):  # noqa: ARG001
        method = getattr(req, "method", None) or "GET"
        body = req.data if req.data is not None else None
        calls.append((req.full_url, method, body))
        if "/navigation_nodes?" in req.full_url and method == "GET":
            return _FakeResponse(next(fetch_responses))
        return _FakeResponse()

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    report = write_navigation_nodes_to_supabase(
        nodes,
        service_key="service",
        supabase_url="https://example.supabase.co",
        chunk_size=1,
    )

    methods = [(call[0], call[1]) for call in calls]
    # First fetch then delete the stale row only, then upsert chunks.
    assert any(method == "GET" and "navigation_nodes" in url for url, method in methods)
    assert any(method == "DELETE" for _, method in methods)
    upsert_urls = [url for url, method, _ in calls if method == "POST"]
    assert upsert_urls and all(
        url.startswith("https://example.supabase.co/rest/v1/navigation_nodes?on_conflict=id")
        for url in upsert_urls
    )
    assert report.rows_total == 2
    assert report.rows_loaded == 2
    assert report.chunk_count == 2
    assert report.scopes_replaced == (("us-co", "statute", None),)
    assert report.rows_deleted == 1


def test_write_navigation_nodes_dry_run_makes_no_network_calls(monkeypatch):
    import axiom_corpus.corpus.navigation_supabase as module

    def fake_urlopen(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("dry-run should not call network")

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    nodes = build_navigation_nodes([_record("alpha")])
    report = write_navigation_nodes_to_supabase(
        nodes,
        service_key="",
        dry_run=True,
    )
    assert report.rows_total == 1
    assert report.rows_loaded == 0
    assert report.chunk_count == 1


def test_write_navigation_nodes_replace_scope_only_touches_input_scopes(monkeypatch):
    import axiom_corpus.corpus.navigation_supabase as module

    nodes = build_navigation_nodes(
        [_record("us-co/statute/x", jurisdiction="us-co", document_class="statute")]
    )

    seen_scope_filters: list[tuple[str | None, str | None, str | None]] = []

    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        method = getattr(req, "method", None) or "GET"
        if "/navigation_nodes?" in url and method == "GET":
            jurisdiction = "us-co" if "jurisdiction=eq.us-co" in url else None
            doc_type = "statute" if "doc_type=eq.statute" in url else None
            version = None if "version=is.null" in url else "unexpected"
            seen_scope_filters.append((jurisdiction, doc_type, version))
            return _FakeResponse(b"[]")
        return _FakeResponse()

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    write_navigation_nodes_to_supabase(
        nodes,
        service_key="service",
        supabase_url="https://example.supabase.co",
    )

    # Only the (us-co, statute) scope was queried for prune; the unrelated
    # (us, regulation) scope is never touched.
    assert seen_scope_filters == [("us-co", "statute", None)]


def test_write_navigation_nodes_can_replace_explicit_empty_scope(monkeypatch):
    import axiom_corpus.corpus.navigation_supabase as module

    calls: list[tuple[str, str]] = []
    fetch_responses = iter(
        [
            json.dumps(
                [
                    {"path": "us-co/statute/old-title"},
                    {"path": "us-co/statute/old-title/old-section"},
                ]
            ).encode(),
        ]
    )

    def fake_urlopen(req, timeout):  # noqa: ARG001
        method = getattr(req, "method", None) or "GET"
        calls.append((req.full_url, method))
        if "/navigation_nodes?" in req.full_url and method == "GET":
            return _FakeResponse(next(fetch_responses))
        return _FakeResponse()

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    report = write_navigation_nodes_to_supabase(
        (),
        service_key="service",
        supabase_url="https://example.supabase.co",
        replace_scopes=(("us-co", "statute", "2026-05-13"),),
    )

    assert any(method == "DELETE" for _, method in calls)
    assert report.rows_total == 0
    assert report.rows_loaded == 0
    assert report.rows_deleted == 2
    assert report.scopes_replaced == (("us-co", "statute", "2026-05-13"),)


def test_fetch_provisions_for_navigation_resolves_parent_paths(monkeypatch):
    import axiom_corpus.corpus.navigation_supabase as module

    page = json.dumps(
        [
            {
                "id": "title-id",
                "jurisdiction": "us-co",
                "doc_type": "statute",
                "parent_id": None,
                "level": 0,
                "ordinal": 1,
                "heading": "Title 39",
                "citation_path": "us-co/statute/title-39",
                "version": "2026-05-13",
                "rulespec_path": None,
                "has_rulespec": False,
                "language": "en",
                "legal_identifier": None,
                "identifiers": {},
            },
            {
                "id": "article-id",
                "jurisdiction": "us-co",
                "doc_type": "statute",
                "parent_id": "title-id",
                "level": 1,
                "ordinal": 1,
                "heading": "Article 22",
                "citation_path": "us-co/statute/title-39/article-22",
                "version": "2026-05-13",
                "rulespec_path": None,
                "has_rulespec": True,
                "language": "en",
                "legal_identifier": None,
                "identifiers": {},
            },
        ]
    ).encode()

    pages = iter([page, b"[]"])

    urls: list[str] = []

    def fake_urlopen(req, timeout):  # noqa: ARG001
        urls.append(req.full_url)
        return _FakeResponse(next(pages))

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    records = fetch_provisions_for_navigation(
        service_key="service",
        supabase_url="https://example.supabase.co",
        jurisdiction="us-co",
        doc_type="statute",
        version="2026-05-13",
        page_size=2,
    )
    by_path = {r.citation_path: r for r in records}
    assert by_path["us-co/statute/title-39"].parent_citation_path is None
    assert (
        by_path["us-co/statute/title-39/article-22"].parent_citation_path
        == "us-co/statute/title-39"
    )
    assert by_path["us-co/statute/title-39"].version == "2026-05-13"
    assert by_path["us-co/statute/title-39/article-22"].has_rulespec is True
    assert "version=eq.2026-05-13" in urls[0]


def test_fetch_navigation_statuses_reads_non_empty_statuses(monkeypatch):
    import axiom_corpus.corpus.navigation_supabase as module

    pages = iter(
        [
            json.dumps(
                [
                    {"path": "us-co/statute/a", "status": " current "},
                    {"path": "us-co/statute/b", "status": None},
                ]
            ).encode(),
            b"[]",
        ]
    )

    def fake_urlopen(req, timeout):  # noqa: ARG001
        assert "select=path%2Cstatus" in req.full_url
        assert "jurisdiction=eq.us-co" in req.full_url
        assert "doc_type=eq.statute" in req.full_url
        assert "version=eq.2026-05-13" in req.full_url
        return _FakeResponse(next(pages))

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    statuses = fetch_navigation_statuses(
        service_key="service",
        supabase_url="https://example.supabase.co",
        jurisdiction="us-co",
        doc_type="statute",
        version="2026-05-13",
        page_size=2,
    )

    assert statuses == {"us-co/statute/a": "current"}
