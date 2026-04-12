"""Tests for state_orchestrator — coordinates state statute ingestion."""

import inspect
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import date

from bs4 import BeautifulSoup

from atlas.ingest.state_orchestrator import StateOrchestrator
from atlas.models import Section, Subsection, Citation


def _make_section(section_num="5747.01", state="oh") -> Section:
    return Section(
        citation=Citation(title=0, section=f"{state.upper()}-{section_num}"),
        title_name="Test State Code",
        section_title="Test Section",
        text="Test text content",
        source_url=f"https://example.com/section-{section_num}",
        retrieved_at=date(2025, 1, 1),
    )


def _parse_parameters(converter) -> tuple[inspect.Parameter, ...]:
    return tuple(inspect.signature(converter._parse_section_html).parameters.values())


class TestIngestStateLocal:
    def test_ingest_state_local_parses_files(self, tmp_path):
        state_dir = tmp_path / "statutes" / "us-oh"
        state_dir.mkdir(parents=True)
        (state_dir / "ohio-revised-code_section-5747.01.html").write_text(
            "<html>test</html>"
        )
        (state_dir / "ohio-revised-code_section-5747.02.html").write_text(
            "<html>test2</html>"
        )
        mock_uploader = MagicMock()
        mock_uploader.upsert_all.return_value = 2

        mock_converter = MagicMock()
        mock_converter._parse_section_html = MagicMock()

        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)
        with patch.object(orch, "_get_converter", return_value=mock_converter), \
             patch.object(
                orch,
                "_parse_local_file",
                side_effect=[
                    _make_section("5747.01"),
                    _make_section("5747.02"),
                ],
             ):
            result = orch.ingest_state("oh")
        assert result > 0
        mock_uploader.upsert_all.assert_called_once()

    def test_ingest_state_no_data(self, tmp_path):
        mock_uploader = MagicMock()
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)
        result = orch.ingest_state("zz")
        assert result == 0


class TestIngestAllStates:
    def test_iterates_states_with_data(self, tmp_path):
        for state in ["oh", "ak", "ca"]:
            state_dir = tmp_path / "statutes" / f"us-{state}"
            state_dir.mkdir(parents=True)
            (state_dir / "test_section.html").write_text("<html>test</html>")
        mock_uploader = MagicMock()
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)
        with patch.object(orch, "ingest_state", return_value=10) as mock_ingest:
            result = orch.ingest_all_states()
        assert mock_ingest.call_count == 3
        states_called = {call.args[0] for call in mock_ingest.call_args_list}
        assert states_called == {"oh", "ak", "ca"}


class TestErrorResilience:
    def test_error_in_one_state_continues(self, tmp_path):
        for state in ["oh", "ak"]:
            state_dir = tmp_path / "statutes" / f"us-{state}"
            state_dir.mkdir(parents=True)
            (state_dir / "test.html").write_text("<html>test</html>")
        mock_uploader = MagicMock()
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)

        def side_effect(state):
            if state == "oh":
                raise RuntimeError("OH converter failed")
            return 5

        with patch.object(orch, "ingest_state", side_effect=side_effect):
            result = orch.ingest_all_states()
        assert "ak" in result
        assert result["ak"] == 5


class TestParseLocalFile:
    def test_parse_oh_filename_pattern(self, tmp_path):
        orch = StateOrchestrator(data_dir=tmp_path)
        section_num = orch._extract_section_number(
            "ohio-revised-code_section-5747.01.html", "oh"
        )
        assert section_num == "5747.01"

    def test_parse_ak_filename_pattern(self, tmp_path):
        orch = StateOrchestrator(data_dir=tmp_path)
        section_num = orch._extract_section_number(
            "statutes.asp_01-05-006.html", "ak"
        )
        assert section_num == "01.05.006"

    def test_parse_al_filename_pattern(self, tmp_path):
        """AL: code-of-alabama_section-1-1-1.1.html → 1-1-1.1"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "code-of-alabama_section-1-1-1.1.html", "al"
        ) == "1-1-1.1"

    def test_parse_az_filename_pattern(self, tmp_path):
        """AZ: viewdocument_...ars_20_00259.htm.html → 20-00259"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "viewdocument_docName-www.azleg.gov_ars_20_00259.htm.html", "az"
        ) == "20-00259"

    def test_parse_mn_filename_pattern(self, tmp_path):
        """MN: statutes_cite_105.63.html → 105.63"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "statutes_cite_105.63.html", "mn"
        ) == "105.63"

    def test_parse_wa_filename_pattern(self, tmp_path):
        """WA: RCW_default.aspx_cite-10.89.html → 10.89"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "RCW_default.aspx_cite-10.89.html", "wa"
        ) == "10.89"

    def test_parse_wi_filename_pattern(self, tmp_path):
        """WI: document_statutes_100.52.html → 100.52"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "document_statutes_100.52.html", "wi"
        ) == "100.52"

    def test_parse_ne_filename_pattern(self, tmp_path):
        """NE: laws_statutes.php_statute-13-2501.html → 13-2501"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "laws_statutes.php_statute-13-2501.html", "ne"
        ) == "13-2501"

    def test_parse_ne_print_variant(self, tmp_path):
        """NE: laws_statutes.php_statute-44-911_print-true.html → 44-911"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "laws_statutes.php_statute-44-911_print-true.html", "ne"
        ) == "44-911"

    def test_parse_tx_filename_pattern(self, tmp_path):
        """TX: Docs_AG_htm_AG.1.htm_1-001.html → AG/1.001"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert orch._extract_section_number(
            "Docs_AG_htm_AG.1.htm_1-001.html", "tx"
        ) == "AG/1.001"

    def test_parse_me_filename_pattern(self, tmp_path):
        """ME: statutes_1_title1ch0sec0.html.html → 1-0"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "statutes_1_title1ch0sec0.html.html", "me"
        )
        assert result == "1-0"

    def test_parse_ri_filename_pattern(self, tmp_path):
        """RI: Statutes_TITLE1_INDEX.HTM.html → TITLE1"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "Statutes_TITLE1_INDEX.HTM.html", "ri"
        )
        assert result == "TITLE1"

    def test_parse_sc_filename_pattern(self, tmp_path):
        """SC: code_t02c003.php.html → 2-3"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "code_t02c003.php.html", "sc"
        )
        assert result == "2-3"

    def test_parse_la_filename_pattern(self, tmp_path):
        """LA: Legis_Laws_Toc.aspx_folder-1.html → 1"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "Legis_Laws_Toc.aspx_folder-1.html", "la"
        )
        assert result == "1"

    def test_parse_nh_filename_pattern(self, tmp_path):
        """NH: rsa_html_NHTOC_NHTOC-I.htm.html → I"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "rsa_html_NHTOC_NHTOC-I.htm.html", "nh"
        )
        assert result == "I"

    def test_parse_nv_filename_pattern(self, tmp_path):
        """NV: NRS_NRS-000.html.html → 000"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "NRS_NRS-000.html.html", "nv"
        )
        assert result == "000"

    def test_parse_pa_filename_pattern(self, tmp_path):
        """PA: statutes_consolidated_view-statute_txtType-HTM_ttl-10.html → 10"""
        orch = StateOrchestrator(data_dir=tmp_path)
        result = orch._extract_section_number(
            "statutes_consolidated_view-statute_txtType-HTM_ttl-10.html", "pa"
        )
        assert result == "10"

    def test_parse_ma_url_filename_pattern(self, tmp_path):
        """MA: Laws_GeneralLaws_PartI_TitleIX_Chapter62_Section5A.html → 62-5A"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert (
            orch._extract_section_number(
                "Laws_GeneralLaws_PartI_TitleIX_Chapter62_Section5A.html",
                "ma",
            )
            == "62-5A"
        )

    def test_parse_md_query_filename_pattern(self, tmp_path):
        """MD: ...article-gtg_section-10-105...html → gtg/10-105"""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert (
            orch._extract_section_number(
                "mgawebsite_Laws_StatuteText_article-gtg_section-10-105_enactments-false.html",
                "md",
            )
            == "gtg/10-105"
        )

    def test_parse_il_docname_filename_pattern(self, tmp_path):
        """IL doc names should decode to chapter-act-section."""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert (
            orch._extract_section_number(
                "Documents_legislation_ilcs_documents_003500050K201.htm.html",
                "il",
            )
            == "35-5-201"
        )

    def test_parse_vt_url_filename_pattern(self, tmp_path):
        """VT section URLs should decode title, chapter, and section."""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert (
            orch._extract_section_number(
                "statutes_section_32_151_05828b.html",
                "vt",
            )
            == "32-151-5828b"
        )

    def test_parse_ut_url_filename_pattern(self, tmp_path):
        """UT content URLs should decode back to section numbers."""
        orch = StateOrchestrator(data_dir=tmp_path)
        assert (
            orch._extract_section_number(
                "xcode_Title59_Chapter10_C59-10-S104_1800010118000101.html",
                "ut",
            )
            == "59-10-104"
        )

    def test_skips_converters_without_parse_section_html(self, tmp_path):
        """States without _parse_section_html should be skipped gracefully."""
        state_dir = tmp_path / "statutes" / "us-de"
        state_dir.mkdir(parents=True)
        (state_dir / "title10_c009_index.html.html").write_text("<html>test</html>")
        mock_uploader = MagicMock()
        mock_uploader.upsert_all.return_value = 0
        orch = StateOrchestrator(data_dir=tmp_path, uploader=mock_uploader)
        # Should not crash, just return 0
        result = orch.ingest_state("de")
        assert result == 0


class TestMultiArgConverters:
    """Test handling of converters with non-standard _parse_section_html signatures."""

    def test_tx_4arg_parse_splits_code_and_section(self, tmp_path):
        """TX converter takes (html, code, section_number, url) — 4 args."""
        orch = StateOrchestrator(data_dir=tmp_path)
        context = orch._build_parse_context("AG/1.001", "tx")
        assert context["code"] == "AG"
        assert context["section_number"] == "1.001"

    def test_me_4arg_parse_splits_title_and_section(self, tmp_path):
        """ME converter takes (html, title, section_number, url) — 4 args."""
        orch = StateOrchestrator(data_dir=tmp_path)
        context = orch._build_parse_context("1-0", "me")
        assert context["title"] == 1
        assert context["section_number"] == "0"

    def test_me_lettered_title_splits_on_last_hyphen(self, tmp_path):
        """ME lettered titles should preserve the title suffix."""
        orch = StateOrchestrator(data_dir=tmp_path)
        context = orch._build_parse_context("13-A-0", "me")
        assert context["title"] == "13-A"
        assert context["section_number"] == "0"

    def test_tx_parse_local_file_calls_4arg(self, tmp_path):
        """Integration: _parse_local_file passes correct args to TX converter."""
        state_dir = tmp_path / "statutes" / "us-tx"
        state_dir.mkdir(parents=True)
        html_file = state_dir / "Docs_AG_htm_AG.1.htm_1-001.html"
        html_file.write_text("<html>test</html>")
        orch = StateOrchestrator(data_dir=tmp_path)

        captured_args = []

        class FakeConverter:
            def _parse_section_html(self, html, code, section_number, url):
                captured_args.extend([html, code, section_number, url])
                return MagicMock()

            def _to_section(self, parsed):
                return _make_section()

        converter = FakeConverter()
        result = orch._parse_local_file(
            html_file,
            "tx",
            converter,
            _parse_parameters(converter),
        )

        assert result is not None
        assert captured_args[1] == "AG"  # code
        assert captured_args[2] == "1.001"  # section_number

    def test_standard_3arg_still_works(self, tmp_path):
        """Standard converters with (html, section_number, url) still work."""
        state_dir = tmp_path / "statutes" / "us-oh"
        state_dir.mkdir(parents=True)
        html_file = state_dir / "ohio-revised-code_section-5747.01.html"
        html_file.write_text("<html>test</html>")
        orch = StateOrchestrator(data_dir=tmp_path)

        captured_args = []

        class FakeConverter:
            def _parse_section_html(self, html, section_number, url):
                captured_args.extend([html, section_number, url])
                return MagicMock()

            def _to_section(self, parsed):
                return _make_section()

        converter = FakeConverter()
        result = orch._parse_local_file(
            html_file,
            "oh",
            converter,
            _parse_parameters(converter),
        )

        assert result is not None
        assert captured_args[0] == "<html>test</html>"  # html
        assert captured_args[1] == "5747.01"  # section_number

    def test_ms_parse_local_file_passes_beautifulsoup(self, tmp_path):
        """Soup-based parsers should receive a parsed BeautifulSoup document."""
        state_dir = tmp_path / "statutes" / "us-ms"
        state_dir.mkdir(parents=True)
        html_file = state_dir / "section-27-7-1.html"
        html_file.write_text("<html><body><h1>Section</h1></body></html>")
        orch = StateOrchestrator(data_dir=tmp_path)

        captured_args = []

        class FakeConverter:
            def _parse_section_html(self, soup, section_number, url):
                captured_args.extend([soup, section_number, url])
                return MagicMock()

            def _to_section(self, parsed):
                return _make_section("27-7-1", state="ms")

        converter = FakeConverter()
        result = orch._parse_local_file(
            html_file,
            "ms",
            converter,
            _parse_parameters(converter),
        )

        assert result is not None
        assert isinstance(captured_args[0], BeautifulSoup)
        assert captured_args[1] == "27-7-1"

    def test_ma_parse_local_file_calls_4arg(self, tmp_path):
        """Non-TX/ME four-arg parsers should receive split chapter/section args."""
        state_dir = tmp_path / "statutes" / "us-ma"
        state_dir.mkdir(parents=True)
        html_file = state_dir / "Laws_GeneralLaws_PartI_TitleIX_Chapter62_Section5A.html"
        html_file.write_text("<html>test</html>")
        orch = StateOrchestrator(data_dir=tmp_path)

        captured_args = []

        class FakeConverter:
            def _parse_section_html(self, html, chapter, section, url):
                captured_args.extend([html, chapter, section, url])
                return MagicMock()

            def _to_section(self, parsed):
                return _make_section("62-5A", state="ma")

        converter = FakeConverter()
        result = orch._parse_local_file(
            html_file,
            "ma",
            converter,
            _parse_parameters(converter),
        )

        assert result is not None
        assert captured_args[1] == "62"
        assert captured_args[2] == "5A"

    def test_ok_parse_local_file_uses_optional_default(self, tmp_path):
        """Optional trailing parser args should use their declared defaults."""
        state_dir = tmp_path / "statutes" / "us-ok"
        state_dir.mkdir(parents=True)
        html_file = state_dir / "section-68-101.html"
        html_file.write_text("<html>test</html>")
        orch = StateOrchestrator(data_dir=tmp_path)

        captured_args = []

        class FakeConverter:
            def _parse_section_html(self, html, section_number, url, cite_id=None):
                captured_args.extend([html, section_number, url, cite_id])
                return MagicMock()

            def _to_section(self, parsed):
                return _make_section("68-101", state="ok")

        converter = FakeConverter()
        result = orch._parse_local_file(
            html_file,
            "ok",
            converter,
            _parse_parameters(converter),
        )

        assert result is not None
        assert captured_args[1] == "68-101"
        assert captured_args[3] is None

    def test_il_parse_local_file_calls_5arg(self, tmp_path):
        """IL five-arg parsers should receive chapter, act, and section separately."""
        state_dir = tmp_path / "statutes" / "us-il"
        state_dir.mkdir(parents=True)
        html_file = state_dir / "Documents_legislation_ilcs_documents_003500050K201.htm.html"
        html_file.write_text("<html>test</html>")
        orch = StateOrchestrator(data_dir=tmp_path)

        captured_args = []

        class FakeConverter:
            def _parse_section_html(self, html, chapter, act, section, url):
                captured_args.extend([html, chapter, act, section, url])
                return MagicMock()

            def _to_section(self, parsed):
                return _make_section("35-5-201", state="il")

        converter = FakeConverter()
        result = orch._parse_local_file(
            html_file,
            "il",
            converter,
            _parse_parameters(converter),
        )

        assert result is not None
        assert captured_args[1] == 35
        assert captured_args[2] == 5
        assert captured_args[3] == "201"

    def test_vt_parse_local_file_calls_5arg(self, tmp_path):
        """VT five-arg parsers should receive title, chapter, and section separately."""
        state_dir = tmp_path / "statutes" / "us-vt"
        state_dir.mkdir(parents=True)
        html_file = state_dir / "statutes_section_32_151_05828b.html"
        html_file.write_text("<html>test</html>")
        orch = StateOrchestrator(data_dir=tmp_path)

        captured_args = []

        class FakeConverter:
            def _parse_section_html(self, html, title, chapter, section, url):
                captured_args.extend([html, title, chapter, section, url])
                return MagicMock()

            def _to_section(self, parsed):
                return _make_section("32-151-5828b", state="vt")

        converter = FakeConverter()
        result = orch._parse_local_file(
            html_file,
            "vt",
            converter,
            _parse_parameters(converter),
        )

        assert result is not None
        assert captured_args[1] == 32
        assert captured_args[2] == 151
        assert captured_args[3] == "5828b"


class TestGetAvailableStates:
    def test_lists_states_from_data_dirs(self, tmp_path):
        for state in ["oh", "ak", "ca"]:
            (tmp_path / "statutes" / f"us-{state}").mkdir(parents=True)
        (tmp_path / "statutes" / "state").mkdir(parents=True)
        orch = StateOrchestrator(data_dir=tmp_path)
        states = orch.get_available_states()
        assert set(states) == {"oh", "ak", "ca"}
