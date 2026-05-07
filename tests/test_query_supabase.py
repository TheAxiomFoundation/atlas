"""Tests for the query supabase module.

Tests cover Rule/Section dataclasses and SupabaseQuery class.
HTTP calls are mocked.
"""

import os
from unittest.mock import patch

from axiom_corpus.query.supabase import Rule, Section, SupabaseQuery


def _make_rule(**kwargs):
    defaults = {
        "id": "us/statute/26/32",
        "jurisdiction": "us",
        "doc_type": "statute",
        "parent_id": None,
        "level": 0,
        "ordinal": 1,
        "heading": "Earned income tax credit",
        "body": "A tax credit is allowed...",
        "effective_date": "2024-01-01",
        "repeal_date": None,
        "source_url": "https://uscode.house.gov",
        "source_path": "26/32",
        "rulespec_path": "rules-us/statute/26/32.yaml",
        "has_rulespec": True,
        "citation_path": "us/statute/26/32",
    }
    defaults.update(kwargs)
    return Rule(**defaults)


class TestRule:
    def test_create(self):
        rule = _make_rule()
        assert rule.id == "us/statute/26/32"
        assert rule.jurisdiction == "us"
        assert rule.has_rulespec is True

    def test_optional_fields(self):
        rule = _make_rule(
            heading=None,
            body=None,
            effective_date=None,
            repeal_date=None,
            source_url=None,
            source_path=None,
            rulespec_path=None,
            citation_path=None,
            ordinal=None,
        )
        assert rule.heading is None
        assert rule.body is None


class TestSection:
    def test_create(self):
        rule = _make_rule()
        section = Section(rule=rule, children=[])
        assert section.rule is rule
        assert section.children == []

    def test_full_text_no_children(self):
        rule = _make_rule()
        section = Section(rule=rule, children=[])
        text = section.full_text
        assert "Earned income tax credit" in text
        assert "A tax credit" in text

    def test_full_text_with_children(self):
        parent = _make_rule(heading="Main heading", body="Main body")
        child1 = _make_rule(heading="Sub A", body="Sub A body")
        child2 = _make_rule(heading="Sub B", body="Sub B body")

        section = Section(rule=parent, children=[child1, child2])
        text = section.full_text
        assert "Main heading" in text
        assert "Sub A" in text
        assert "Sub B" in text

    def test_full_text_no_heading(self):
        rule = _make_rule(heading=None)
        section = Section(rule=rule, children=[])
        text = section.full_text
        assert "A tax credit" in text

    def test_full_text_no_body(self):
        rule = _make_rule(body=None)
        section = Section(rule=rule, children=[])
        text = section.full_text
        assert "Earned income tax credit" in text

    def test_citation_with_source_path(self):
        rule = _make_rule(source_path="26/32")
        section = Section(rule=rule, children=[])
        assert section.citation == "us/statute/26/32"

    def test_citation_without_source_path(self):
        rule = _make_rule(source_path=None)
        section = Section(rule=rule, children=[])
        assert section.citation == "us/statute/26/32"


class TestSupabaseQuery:
    def test_init_defaults(self):
        with patch.dict(os.environ, {"SUPABASE_ANON_KEY": "test-key"}):
            query = SupabaseQuery()
        assert query.url is not None
        assert query.anon_key is not None
        assert "rest/v1" in query.rest_url

    def test_init_has_public_anon_key_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            query = SupabaseQuery()
        assert query.url is not None
        assert query.anon_key
        assert query.headers["Accept-Profile"] == "corpus"

    def test_init_with_explicit_url(self):
        query = SupabaseQuery(
            url="https://test.supabase.co",
            anon_key="test-key",
        )
        assert query.url == "https://test.supabase.co"
        assert query.anon_key == "test-key"
        assert query.rest_url == "https://test.supabase.co/rest/v1"
        assert query.headers["apikey"] == "test-key"
        assert query.provisions_table == "current_provisions"
        assert query.provision_counts_table == "current_provision_counts"

    def test_init_can_include_legacy_rows(self):
        query = SupabaseQuery(
            url="https://test.supabase.co",
            anon_key="test-key",
            include_legacy=True,
        )
        assert query.provisions_table == "provisions"
        assert query.provision_counts_table == "provision_counts"

    def test_init_from_env(self):
        with patch.dict(
            os.environ,
            {"AXIOM_SUPABASE_URL": "https://env.supabase.co", "SUPABASE_ANON_KEY": "env-key"},
        ):
            query = SupabaseQuery()
            assert query.url == "https://env.supabase.co"

    def test_normalizes_us_code_shorthand(self):
        assert SupabaseQuery._normalize_citation_path("26/32") == "us/statute/26/32"
        assert SupabaseQuery._normalize_citation_path("usc/26/32") == "us/statute/26/32"

    def test_normalizes_full_citation_path(self):
        assert (
            SupabaseQuery._normalize_citation_path("us/statute/26/32")
            == "us/statute/26/32"
        )
        assert (
            SupabaseQuery._normalize_citation_path("statute/tax/606", jurisdiction="us-ny")
            == "us-ny/statute/tax/606"
        )

    def test_get_section_queries_citation_path(self):
        query = SupabaseQuery(url="https://test.supabase.co", anon_key="test-key")
        data = _make_rule().__dict__

        with patch.object(query, "_request", return_value=[data]) as request:
            result = query.get_section("26/32")

        assert result is not None
        assert result.citation_path == "us/statute/26/32"
        request.assert_called_once()
        table, params = request.call_args.args
        assert table == "current_provisions"
        assert params["citation_path"] == "eq.us/statute/26/32"
        assert params["jurisdiction"] == "eq.us"
        assert "source_path" not in params

    def test_get_section_deep_queries_descendants_by_citation_path(self):
        query = SupabaseQuery(url="https://test.supabase.co", anon_key="test-key")
        parent = _make_rule().__dict__
        child = _make_rule(
            id="us/statute/26/32/a",
            citation_path="us/statute/26/32/a",
            parent_id=parent["id"],
        ).__dict__

        with patch.object(query, "_request", side_effect=[[parent], [child]]) as request:
            section = query.get_section_with_children("26/32", deep=True)

        assert section is not None
        assert [c.citation_path for c in section.children] == ["us/statute/26/32/a"]
        child_table, child_params = request.call_args_list[1].args
        assert child_table == "current_provisions"
        assert child_params == [
            ("citation_path", "gte.us/statute/26/32/"),
            ("citation_path", "lt.us/statute/26/320"),
            ("jurisdiction", "eq.us"),
            ("order", "citation_path"),
            ("limit", "1000"),
        ]
        assert all(name != "source_path" for name, _ in child_params)

    def test_search_orders_by_citation_path(self):
        query = SupabaseQuery(url="https://test.supabase.co", anon_key="test-key")

        with patch.object(query, "_request", return_value=[]) as request:
            query.search("earned income", jurisdiction="us")

        table, params = request.call_args.args
        assert table == "current_provisions"
        assert params["order"] == "jurisdiction,citation_path"

    def test_get_stats_reads_current_counts_by_default(self):
        query = SupabaseQuery(url="https://test.supabase.co", anon_key="test-key")

        with patch.object(
            query,
            "_request",
            return_value=[
                {"jurisdiction": "us", "provision_count": 2},
                {"jurisdiction": "us-co", "provision_count": "3"},
            ],
        ) as request:
            stats = query.get_stats()

        table, params = request.call_args.args
        assert table == "current_provision_counts"
        assert params["select"] == "jurisdiction,provision_count"
        assert stats == {"us": 2, "us-co": 3, "total": 5}
