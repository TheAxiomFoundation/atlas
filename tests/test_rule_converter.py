"""Tests for rule_converter -- pure section-to-rules conversion."""

import pytest
from uuid import UUID

from atlas.ingest.rule_converter import section_to_rules, _deterministic_id
from atlas.models import Section, Subsection, Citation
from datetime import date


def _make_us_section(**kwargs) -> Section:
    defaults = {
        "citation": Citation(title=26, section="32"),
        "title_name": "Internal Revenue Code",
        "section_title": "Earned income",
        "text": "Test section text",
        "source_url": "https://example.com",
        "retrieved_at": date(2025, 1, 1),
    }
    defaults.update(kwargs)
    return Section(**defaults)


def _make_state_section(state="oh", section_num="5747.01", **kwargs) -> Section:
    defaults = {
        "citation": Citation(title=0, section=f"OH-{section_num}"),
        "title_name": "Ohio Revised Code",
        "section_title": "Definitions",
        "text": "Test section text",
        "source_url": "https://codes.ohio.gov/section-5747.01",
        "retrieved_at": date(2025, 1, 1),
    }
    defaults.update(kwargs)
    return Section(**defaults)


class TestDeterministicId:
    def test_same_path_same_id(self):
        path = "us/statute/26/32"
        assert _deterministic_id(path) == _deterministic_id(path)

    def test_different_path_different_id(self):
        assert _deterministic_id("us/statute/26/32") != _deterministic_id(
            "us/statute/26/36B"
        )

    def test_returns_valid_uuid(self):
        result = _deterministic_id("us/statute/26/32")
        UUID(result)

    def test_uses_atlas_namespace(self):
        from uuid import uuid5, NAMESPACE_URL

        path = "us/statute/26/32"
        expected = str(uuid5(NAMESPACE_URL, f"atlas:{path}"))
        assert _deterministic_id(path) == expected


class TestSectionToRulesBasic:
    def test_us_federal_section_no_subsections(self):
        section = _make_us_section()
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert len(rules) == 1
        rule = rules[0]
        assert rule["jurisdiction"] == "us"
        assert rule["doc_type"] == "statute"
        assert rule["level"] == 0
        assert rule["heading"] == "Earned income"
        assert rule["body"] == "Test section text"
        assert rule["parent_id"] is None
        assert rule["citation_path"] == "us/statute/26/32"
        assert rule["has_rac"] is False

    def test_state_section(self):
        section = _make_state_section()
        rules = list(section_to_rules(section, jurisdiction="us-oh"))
        assert len(rules) == 1
        rule = rules[0]
        assert rule["jurisdiction"] == "us-oh"
        assert rule["citation_path"] == "us-oh/statute/0/OH-5747.01"

    def test_id_is_deterministic(self):
        section = _make_us_section()
        rules1 = list(section_to_rules(section, jurisdiction="us"))
        rules2 = list(section_to_rules(section, jurisdiction="us"))
        assert rules1[0]["id"] == rules2[0]["id"]


class TestSectionToRulesWithSubsections:
    def test_subsections_create_child_rules(self):
        section = _make_us_section(
            subsections=[
                Subsection(identifier="a", text="First subsection", children=[]),
                Subsection(identifier="b", text="Second subsection", children=[]),
            ]
        )
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert len(rules) == 3
        parent_id = rules[0]["id"]
        assert rules[1]["parent_id"] == parent_id
        assert rules[2]["parent_id"] == parent_id
        assert rules[1]["level"] == 1

    def test_nested_subsections(self):
        section = _make_us_section(
            subsections=[
                Subsection(
                    identifier="a",
                    text="Top level",
                    children=[
                        Subsection(
                            identifier="1",
                            text="Second level",
                            children=[
                                Subsection(
                                    identifier="A",
                                    text="Third level",
                                    children=[],
                                ),
                            ],
                        ),
                    ],
                ),
            ]
        )
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert len(rules) == 4
        assert [r["level"] for r in rules] == [0, 1, 2, 3]

    def test_subsection_citation_paths(self):
        section = _make_us_section(
            subsections=[Subsection(identifier="a", text="Sub a", children=[])]
        )
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert rules[0]["citation_path"] == "us/statute/26/32"
        assert rules[1]["citation_path"] == "us/statute/26/32/a"


class TestOrdinalExtraction:
    def test_numeric_section(self):
        section = _make_us_section(citation=Citation(title=26, section="32"))
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert rules[0]["ordinal"] == 32

    def test_alphanumeric_section(self):
        section = _make_us_section(citation=Citation(title=26, section="36B"))
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert rules[0]["ordinal"] == 36

    def test_non_numeric_section(self):
        section = _make_us_section(citation=Citation(title=26, section="ABC"))
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert rules[0]["ordinal"] is None


class TestEmptySubsections:
    def test_empty_subsection_list(self):
        section = _make_us_section(subsections=[])
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert len(rules) == 1


class TestCitationPathFormat:
    def test_us_federal_format(self):
        section = _make_us_section()
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert rules[0]["citation_path"] == "us/statute/26/32"

    def test_state_format(self):
        section = _make_state_section()
        rules = list(section_to_rules(section, jurisdiction="us-oh"))
        assert rules[0]["citation_path"] == "us-oh/statute/0/OH-5747.01"

    def test_custom_doc_type(self):
        section = _make_us_section()
        rules = list(
            section_to_rules(section, jurisdiction="us", doc_type="regulation")
        )
        assert rules[0]["citation_path"] == "us/regulation/26/32"
        assert rules[0]["doc_type"] == "regulation"


class TestEffectiveDate:
    def test_effective_date_serialized(self):
        section = _make_us_section(effective_date=date(2025, 1, 1))
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert rules[0]["effective_date"] == "2025-01-01"

    def test_no_effective_date(self):
        section = _make_us_section()
        rules = list(section_to_rules(section, jurisdiction="us"))
        assert rules[0]["effective_date"] is None
