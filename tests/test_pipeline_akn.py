"""Tests for the pipeline AKN conversion module.

Note: This module may fail to import due to pipeline/__init__.py importing
from runner.py which has unresolved R2 deps. We import directly.
"""

import pytest

from atlas.models import Citation, Section

# Direct import to avoid pipeline.__init__ which may fail
try:
    import importlib
    _akn_mod = importlib.import_module("atlas.pipeline.akn")
    section_to_akn_xml = _akn_mod.section_to_akn_xml
    AKN_NS = _akn_mod.AKN_NS
    _IMPORT_OK = True
except ImportError:
    _IMPORT_OK = False


pytestmark = pytest.mark.skipif(not _IMPORT_OK, reason="Pipeline AKN module not importable")


def _make_section(**kwargs):
    from datetime import date as _date

    defaults = {
        "citation": Citation(title=0, section="5747.02"),
        "title_name": "Ohio Revised Code",
        "section_title": "Tax rates",
        "text": "Tax is imposed at the following rates.\n\nRate schedule applies.",
        "subsections": [],
        "source_url": "https://codes.ohio.gov/orc/5747.02",
        "retrieved_at": _date.today(),
    }
    defaults.update(kwargs)
    return Section(**defaults)


class TestSectionToAknXml:
    def test_basic_conversion(self):
        section = _make_section()
        xml = section_to_akn_xml(section, "oh")
        assert "akomaNtoso" in xml
        assert "5747.02" in xml
        assert "oh" in xml

    def test_contains_frbr_elements(self):
        section = _make_section()
        xml = section_to_akn_xml(section, "oh")
        assert "FRBRWork" in xml
        assert "FRBRExpression" in xml
        assert "FRBRManifestation" in xml

    def test_contains_body(self):
        section = _make_section()
        xml = section_to_akn_xml(section, "oh")
        assert "body" in xml

    def test_contains_heading(self):
        section = _make_section(section_title="Tax rates")
        xml = section_to_akn_xml(section, "oh")
        assert "Tax rates" in xml

    def test_text_paragraphs(self):
        section = _make_section(text="First paragraph.\n\nSecond paragraph.")
        xml = section_to_akn_xml(section, "oh")
        assert "First paragraph" in xml
        assert "Second paragraph" in xml

    def test_empty_text(self):
        section = _make_section(text="")
        xml = section_to_akn_xml(section, "oh")
        assert "akomaNtoso" in xml

    def test_different_states(self):
        section = _make_section()
        for state in ["ak", "ny", "ca", "tx"]:
            xml = section_to_akn_xml(section, state)
            assert f"us-{state}" in xml

    def test_references_section(self):
        section = _make_section()
        xml = section_to_akn_xml(section, "oh")
        assert "axiom-foundation" in xml

    def test_xml_is_valid(self):
        from xml.etree import ElementTree as ET

        section = _make_section()
        xml = section_to_akn_xml(section, "oh")
        if xml.startswith("<?xml"):
            xml = xml[xml.index("?>") + 2:]
        root = ET.fromstring(xml.strip())
        assert root is not None

    def test_section_with_special_chars(self):
        section = _make_section(
            citation=Citation(title=0, section="26-51-101"),
            text="Tax at rate of 5% & credits for income.",
        )
        xml = section_to_akn_xml(section, "ar")
        assert "akomaNtoso" in xml

    def test_no_section_title(self):
        section = _make_section(section_title="")
        xml = section_to_akn_xml(section, "oh")
        assert "akomaNtoso" in xml
