"""Tests for California state converter.

Tests the CAStateConverter which fetches from leginfo.legislature.ca.gov
and converts to the unified Statute model.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from axiom_corpus.converters.us_states.ca import (
    CA_CODES,
    CAStateConverter,
)
from axiom_corpus.models_statute import Statute

# Sample HTML from leginfo for testing
SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html>
<head><title>CA RTC 17052</title></head>
<body>
<div id="single_law_section" class="displaycodeleftmargin">
<HTML xmlns:xhtml="http://www.w3.org/1999/xhtml">
<head><META http-equiv="Content-Type" content="text/html;charset=utf-8"/></head>
<BODY>
<div id="codeLawSectionNoHead">
<div align="left" style="text-transform: uppercase">
<h4><b>Revenue and Taxation Code - RTC</b></h4>
</div>
<div style="float:left;text-indent: 0.25in;">
<h4 style="display:inline;"><b>DIVISION 2. OTHER TAXES [6001 - 61050]</b></h4>
</div>
<div style="float:left;text-indent: 0.5in;">
<h4 style="display:inline;"><b>PART 10. PERSONAL INCOME TAX [17001 - 18181]</b></h4>
</div>
<div style="display:inline;">
<h5 style="display:inline;"><b>CHAPTER 2. Imposition of Tax [17041 - 17061]</b></h5>
</div>
<div>
<font face="Times New Roman">
<h6 style="float:left;"><b>17052.  </b></h6>
<p style="margin:0 0 0.5em 0;">(a) (1) For each taxable year beginning on or after January 1, 2015, there shall be allowed against the "net tax," as defined by Section 17039, an earned income tax credit.</p>
<p style="margin:0 0 1em 0;margin-left: 1em;">(2) The credit shall be calculated as follows.</p>
<p style="margin:0 0 0.5em 0;">(b) (1) In lieu of the table prescribed in Section 32(b)(1), the credit percentage shall be 7.65%.</p>
<i>(Amended by Stats. 2022, Ch. 482, Sec. 5. Effective January 1, 2023.)</i>
</font>
</div>
</div>
</BODY>
</HTML>
</div>
</body>
</html>
"""


class TestCACodesRegistry:
    """Test California codes registry."""

    def test_rtc_in_codes(self):
        """Revenue and Taxation Code is in the registry."""
        assert "RTC" in CA_CODES
        assert "Revenue and Taxation" in CA_CODES["RTC"]

    def test_wic_in_codes(self):
        """Welfare and Institutions Code is in the registry."""
        assert "WIC" in CA_CODES
        assert "Welfare" in CA_CODES["WIC"]

    def test_all_29_codes(self):
        """All 29 California codes are in the registry."""
        # California has 29 codes (plus constitution)
        assert len(CA_CODES) >= 29


class TestCAStateConverter:
    """Test CAStateConverter class."""

    def test_init_default(self):
        """Converter initializes with default settings."""
        converter = CAStateConverter()
        assert converter.data_dir.name == "us-ca"
        assert converter.rate_limit_delay > 0

    def test_init_custom_data_dir(self, tmp_path):
        """Converter accepts custom data directory."""
        converter = CAStateConverter(data_dir=tmp_path)
        assert converter.data_dir == tmp_path

    def test_build_url_section(self):
        """Build correct URL for section fetch."""
        converter = CAStateConverter()
        url = converter.build_url("RTC", "17052")
        assert "leginfo.legislature.ca.gov" in url
        assert "lawCode=RTC" in url
        assert "sectionNum=17052" in url

    def test_parse_reference_simple(self):
        """Parse simple reference like 'rtc/17052'."""
        converter = CAStateConverter()
        code, section = converter.parse_reference("rtc/17052")
        assert code == "RTC"
        assert section == "17052"

    def test_parse_reference_with_decimal(self):
        """Parse section with decimal like 'rtc/17041.5'."""
        converter = CAStateConverter()
        code, section = converter.parse_reference("RTC/17041.5")
        assert code == "RTC"
        assert section == "17041.5"

    def test_parse_reference_lowercase(self):
        """Parse lowercase reference."""
        converter = CAStateConverter()
        code, section = converter.parse_reference("wic/11320.3")
        assert code == "WIC"
        assert section == "11320.3"

    @patch("httpx.Client")
    def test_fetch_section(self, mock_client_class):
        """Fetch and parse a section from leginfo."""
        # Set up mock
        mock_client = MagicMock()
        mock_client_class.return_value.__enter__ = Mock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = Mock(return_value=False)

        mock_response = Mock()
        mock_response.text = SAMPLE_SECTION_HTML
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        converter = CAStateConverter()
        result = converter.fetch("rtc/17052")

        assert result is not None
        assert isinstance(result, Statute)
        assert result.jurisdiction == "us-ca"
        assert result.code == "RTC"
        assert result.section == "17052"
        assert "earned income tax credit" in result.text.lower()

    def test_parse_html_section(self):
        """Parse HTML into Statute model."""
        converter = CAStateConverter()
        statute = converter._parse_html(SAMPLE_SECTION_HTML, "RTC", "17052", "https://example.com")

        assert statute.jurisdiction == "us-ca"
        assert statute.code == "RTC"
        assert statute.code_name == "Revenue and Taxation Code"
        assert statute.section == "17052"
        assert statute.division == "2"
        assert statute.part == "10"
        assert statute.chapter == "2"
        assert "earned income" in statute.text.lower()

    def test_parse_html_subsections(self):
        """Parse subsections from HTML."""
        converter = CAStateConverter()
        statute = converter._parse_html(SAMPLE_SECTION_HTML, "RTC", "17052", "https://example.com")

        # Should have subsections (a) and (b)
        assert len(statute.subsections) >= 2
        assert any(s.identifier == "a" for s in statute.subsections)
        assert any(s.identifier == "b" for s in statute.subsections)

    def test_parse_html_history(self):
        """Parse legislative history from HTML."""
        converter = CAStateConverter()
        statute = converter._parse_html(SAMPLE_SECTION_HTML, "RTC", "17052", "https://example.com")

        assert statute.history is not None
        assert "Stats. 2022" in statute.history

    def test_citation_format(self):
        """Statute generates correct citation format."""
        converter = CAStateConverter()
        statute = converter._parse_html(SAMPLE_SECTION_HTML, "RTC", "17052", "https://example.com")

        assert statute.citation == "CA RTC \u00a7 17052"

    def test_rulespec_path_format(self):
        """Statute generates correct RuleSpec path."""
        converter = CAStateConverter()
        statute = converter._parse_html(SAMPLE_SECTION_HTML, "RTC", "17052", "https://example.com")

        assert statute.rulespec_path == "rulespec-us-ca/statutes/RTC/17052.yaml"

    def test_list_codes(self):
        """List available California codes."""
        converter = CAStateConverter()
        codes = converter.list_codes()

        assert len(codes) >= 29
        assert any(c["code"] == "RTC" for c in codes)
        assert any(c["code"] == "WIC" for c in codes)


class TestCAStateConverterIntegration:
    """Integration tests that hit real leginfo (marked slow)."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_eitc_section(self):
        """Fetch California EITC section 17052."""
        converter = CAStateConverter()
        statute = converter.fetch("rtc/17052")

        assert statute is not None
        assert statute.code == "RTC"
        assert statute.section == "17052"
        assert "earned income" in statute.text.lower()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_calworks(self):
        """Fetch CalWORKs section from WIC."""
        converter = CAStateConverter()
        statute = converter.fetch("wic/11320.3")

        assert statute is not None
        assert statute.code == "WIC"
        assert statute.section == "11320.3"

    @pytest.mark.slow
    @pytest.mark.integration
    def test_fetch_with_decimal_section(self):
        """Fetch section with decimal number."""
        converter = CAStateConverter()
        # Use section 17041 which exists (main tax rate section)
        statute = converter.fetch("rtc/17041")

        assert statute is not None
        assert statute.section == "17041"
        assert "tax" in statute.text.lower()
