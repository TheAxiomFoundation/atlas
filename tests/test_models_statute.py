"""Tests for the statute models module."""

import pytest

from axiom_corpus.models_statute import (
    JURISDICTIONS,
    CodeType,
    JurisdictionInfo,
    JurisdictionType,
    Statute,
    StatuteSearchResult,
    StatuteSubsection,
)


class TestEnums:
    def test_jurisdiction_type_values(self):
        assert JurisdictionType.FEDERAL == "federal"
        assert JurisdictionType.STATE == "state"
        assert JurisdictionType.TERRITORY == "territory"
        assert JurisdictionType.LOCAL == "local"

    def test_code_type_values(self):
        assert CodeType.STATUTE == "statute"
        assert CodeType.REGULATION == "regulation"
        assert CodeType.CONSTITUTION == "constitution"
        assert CodeType.RULE == "rule"


class TestJurisdictions:
    def test_has_federal(self):
        assert "us" in JURISDICTIONS

    def test_has_many_states(self):
        state_keys = [k for k in JURISDICTIONS if k.startswith("us-")]
        assert len(state_keys) >= 50

    def test_california_metadata(self):
        assert JURISDICTIONS["us-ca"]["name"] == "California"
        assert JURISDICTIONS["us-ca"]["type"] == JurisdictionType.STATE

    def test_federal_metadata(self):
        assert JURISDICTIONS["us"]["name"] == "United States"
        assert JURISDICTIONS["us"]["type"] == JurisdictionType.FEDERAL


class TestStatuteSubsection:
    def test_create(self):
        sub = StatuteSubsection(identifier="a", text="Tax is imposed.")
        assert sub.identifier == "a"
        assert sub.heading is None
        assert sub.children == []

    def test_with_heading(self):
        sub = StatuteSubsection(identifier="a", heading="General rule", text="...")
        assert sub.heading == "General rule"

    def test_with_children(self):
        child = StatuteSubsection(identifier="1", text="First item")
        parent = StatuteSubsection(identifier="a", text="Main", children=[child])
        assert len(parent.children) == 1
        assert parent.children[0].identifier == "1"


class TestStatute:
    def _make_statute(self, **kwargs):
        defaults = {
            "jurisdiction": "us-oh",
            "code": "ORC",
            "code_name": "Ohio Revised Code",
            "section": "5747.02",
            "title": "Tax rates",
            "text": "Tax imposed at the following rates.",
            "source_url": "https://codes.ohio.gov/orc/5747.02",
        }
        defaults.update(kwargs)
        return Statute(**defaults)

    def test_create_minimal(self):
        s = self._make_statute()
        assert s.jurisdiction == "us-oh"
        assert s.code == "ORC"
        assert s.section == "5747.02"

    def test_jurisdiction_validator_lowercase(self):
        s = self._make_statute(jurisdiction="US-OH")
        assert s.jurisdiction == "us-oh"

    def test_jurisdiction_name(self):
        s = self._make_statute(jurisdiction="us-oh")
        assert s.jurisdiction_name == "Ohio"

    def test_jurisdiction_name_unknown(self):
        s = self._make_statute(jurisdiction="zz-test")
        assert s.jurisdiction_name == "zz-test"

    def test_jurisdiction_type(self):
        s = self._make_statute(jurisdiction="us-oh")
        assert s.jurisdiction_type == JurisdictionType.STATE

    def test_jurisdiction_type_federal(self):
        s = self._make_statute(jurisdiction="us", code="26", code_name="IRC")
        assert s.jurisdiction_type == JurisdictionType.FEDERAL

    def test_jurisdiction_type_unknown(self):
        s = self._make_statute(jurisdiction="zz-test")
        assert s.jurisdiction_type is None

    def test_citation_federal(self):
        s = self._make_statute(jurisdiction="us", code="26", code_name="IRC", section="32")
        assert s.citation == "26 USC \u00a7 32"

    def test_citation_state(self):
        s = self._make_statute()
        assert "OH" in s.citation
        assert "5747.02" in s.citation

    def test_citation_international(self):
        s = self._make_statute(jurisdiction="uk", code="TAX", code_name="Tax Act")
        assert "UK" in s.citation

    def test_citation_with_subsection(self):
        s = self._make_statute(
            jurisdiction="us", code="26", code_name="IRC", section="32", subsection_path="a/1/A"
        )
        assert "(a)(1)(A)" in s.citation

    def test_rulespec_path(self):
        s = self._make_statute()
        assert s.rulespec_path == "rulespec-us-oh/statute/ORC/5747.02.yaml"

    def test_rulespec_path_with_subsection(self):
        s = self._make_statute(subsection_path="a/1")
        assert s.rulespec_path == "rulespec-us-oh/statute/ORC/5747.02/a/1.yaml"

    def test_db_path(self):
        s = self._make_statute()
        assert s.db_path == "us-oh/statute/ORC/5747.02"

    def test_db_path_with_subsection(self):
        s = self._make_statute(subsection_path="a")
        assert s.db_path == "us-oh/statute/ORC/5747.02/a"

    def test_optional_fields(self):
        s = self._make_statute(
            division="Div 1",
            part="Part A",
            chapter="5747",
            subchapter="I",
            article="1",
            history="Enacted 2024",
        )
        assert s.division == "Div 1"
        assert s.chapter == "5747"

    def test_extra_fields_forbidden(self):
        with pytest.raises(Exception):
            self._make_statute(unexpected_field="value")


class TestStatuteParseCitation:
    def test_federal_usc(self):
        result = Statute.parse_citation("26 USC 32")
        assert result["jurisdiction"] == "us"
        assert result["code"] == "26"
        assert result["section"] == "32"
        assert result["subsection_path"] is None

    def test_federal_usc_with_subsection(self):
        result = Statute.parse_citation("26 USC 32(a)(1)")
        assert result["jurisdiction"] == "us"
        assert result["section"] == "32"
        assert result["subsection_path"] == "a/1"

    def test_california(self):
        result = Statute.parse_citation("Cal. RTC 17041")
        assert result["jurisdiction"] == "us-ca"
        assert result["code"] == "RTC"
        assert result["section"] == "17041"

    def test_state_generic(self):
        result = Statute.parse_citation("NY Tax 601")
        assert result["jurisdiction"] == "us-ny"
        assert result["code"] == "TAX"
        assert result["section"] == "601"

    def test_invalid(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            Statute.parse_citation("not a citation")


class TestStatuteSearchResult:
    def test_create(self):
        result = StatuteSearchResult(
            jurisdiction="us-oh",
            code="ORC",
            section="5747.02",
            title="Tax rates",
            snippet="rates apply...",
            score=0.85,
            rulespec_path="rulespec-us-oh/statute/ORC/5747.02.yaml",
        )
        assert result.score == 0.85


class TestJurisdictionInfo:
    def test_create(self):
        info = JurisdictionInfo(
            jurisdiction="us-oh",
            name="Ohio",
            type=JurisdictionType.STATE,
            section_count=5000,
        )
        assert info.jurisdiction == "us-oh"
        assert info.section_count == 5000
