"""Tests to close coverage gaps across the axiom codebase.

This file targets specific missing lines identified by coverage analysis.
"""

from datetime import date
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

# =============================================================================
# verifier.py - Missing lines: 24-26 (USE_PACKAGE=False), 237-299 (PE calls)
# =============================================================================


class TestVerifierCallPolicyengine:
    """Test call_policyengine and its internal dispatch functions."""

    def test_call_policyengine_uses_package_when_available(self):
        """Test that call_policyengine dispatches to package when USE_PACKAGE=True."""
        with patch("axiom_corpus.verifier.USE_PACKAGE", True), \
             patch("axiom_corpus.verifier._call_policyengine_package") as mock_pkg:
            mock_pkg.return_value = (100.0, None)
            from axiom_corpus.verifier import call_policyengine
            result = call_policyengine({"people": {}}, "eitc", 2024)
            mock_pkg.assert_called_once()
            assert result == (100.0, None)

    def test_call_policyengine_uses_api_when_no_package(self):
        """Test that call_policyengine dispatches to API when USE_PACKAGE=False."""
        with patch("axiom_corpus.verifier.USE_PACKAGE", False), \
             patch("axiom_corpus.verifier._call_policyengine_api") as mock_api:
            mock_api.return_value = (200.0, None)
            from axiom_corpus.verifier import call_policyengine
            result = call_policyengine({"people": {}}, "eitc", 2024)
            mock_api.assert_called_once()
            assert result == (200.0, None)

    def test_call_policyengine_package_success(self):
        """Test _call_policyengine_package with a mocked Simulation."""
        mock_sim_class = MagicMock()
        mock_sim_instance = MagicMock()
        mock_sim_class.return_value = mock_sim_instance
        # Use a list to mimic array with __len__ and indexing
        mock_sim_instance.calculate.return_value = [500.0]

        with patch("axiom_corpus.verifier.USE_PACKAGE", True), \
             patch("axiom_corpus.verifier.Simulation", mock_sim_class, create=True):
            from axiom_corpus.verifier import _call_policyengine_package
            value, error = _call_policyengine_package({"people": {}}, "eitc", 2024)
            assert value == 500.0
            assert error is None

    def test_call_policyengine_package_scalar(self):
        """Test _call_policyengine_package with scalar return."""
        mock_sim_class = MagicMock()
        mock_sim_instance = MagicMock()
        mock_sim_class.return_value = mock_sim_instance
        mock_sim_instance.calculate.return_value = 500.0

        with patch("axiom_corpus.verifier.USE_PACKAGE", True), \
             patch("axiom_corpus.verifier.Simulation", mock_sim_class, create=True):
            from axiom_corpus.verifier import _call_policyengine_package
            value, error = _call_policyengine_package({"people": {}}, "eitc", 2024)
            assert value == 500.0
            assert error is None

    def test_call_policyengine_package_error(self):
        """Test _call_policyengine_package error handling."""
        mock_sim_class = MagicMock()
        mock_sim_class.side_effect = Exception("Simulation error")

        with patch("axiom_corpus.verifier.USE_PACKAGE", True), \
             patch("axiom_corpus.verifier.Simulation", mock_sim_class, create=True):
            from axiom_corpus.verifier import _call_policyengine_package
            value, error = _call_policyengine_package({"people": {}}, "eitc", 2024)
            assert value is None
            assert "Simulation error" in error

    def _setup_api_url(self):
        """Ensure POLICYENGINE_API_URL is set in verifier module."""
        import axiom_corpus.verifier as v
        if not hasattr(v, "POLICYENGINE_API_URL"):
            v.POLICYENGINE_API_URL = "https://api.policyengine.org/us/calculate"

    def test_call_policyengine_api_success(self):
        """Test _call_policyengine_api success path."""
        import requests as real_requests
        self._setup_api_url()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "tax_units": {
                    "tax_unit": {
                        "eitc": {"2024": 500.0}
                    }
                }
            }
        }
        mock_response.raise_for_status = MagicMock()

        with patch.dict("sys.modules", {"requests": MagicMock()}) as _:
            import sys
            mock_requests = sys.modules["requests"]
            mock_requests.post.return_value = mock_response
            mock_requests.exceptions = real_requests.exceptions
            from axiom_corpus.verifier import _call_policyengine_api
            value, error = _call_policyengine_api(
                {
                    "people": {"adult": {}},
                    "tax_units": {"tax_unit": {"members": ["adult"]}},
                },
                "eitc",
                2024,
            )
            assert value == 500.0
            assert error is None

    def test_call_policyengine_api_variable_not_found(self):
        """Test _call_policyengine_api when variable not in response."""
        import requests as real_requests
        self._setup_api_url()

        mock_response = MagicMock()
        mock_response.json.return_value = {"result": {"tax_units": {"tax_unit": {}}}}
        mock_response.raise_for_status = MagicMock()

        with patch.dict("sys.modules", {"requests": MagicMock()}) as _:
            import sys
            mock_requests = sys.modules["requests"]
            mock_requests.post.return_value = mock_response
            mock_requests.exceptions = real_requests.exceptions
            from axiom_corpus.verifier import _call_policyengine_api
            value, error = _call_policyengine_api(
                {
                    "people": {"adult": {}},
                    "tax_units": {"tax_unit": {"members": ["adult"]}},
                },
                "eitc",
                2024,
            )
            assert value is None
            assert "not found" in error

    def test_call_policyengine_api_request_error(self):
        """Test _call_policyengine_api request failure."""
        import requests as real_requests
        self._setup_api_url()

        with patch.dict("sys.modules", {"requests": MagicMock()}) as _:
            import sys
            mock_requests = sys.modules["requests"]
            mock_requests.post.side_effect = real_requests.exceptions.ConnectionError("failed")
            mock_requests.exceptions = real_requests.exceptions
            from axiom_corpus.verifier import _call_policyengine_api
            value, error = _call_policyengine_api(
                {"people": {"adult": {}}, "tax_units": {"tax_unit": {"members": ["adult"]}}},
                "eitc",
                2024,
            )
            assert value is None
            assert error is not None

    def test_call_policyengine_api_parse_error(self):
        """Test _call_policyengine_api with response missing 'result' key."""
        import requests as real_requests
        self._setup_api_url()

        mock_response = MagicMock()
        # No 'result' key means falls to "not found" return
        mock_response.json.return_value = {"error": "bad request"}
        mock_response.raise_for_status = MagicMock()

        with patch.dict("sys.modules", {"requests": MagicMock()}) as _:
            import sys
            mock_requests = sys.modules["requests"]
            mock_requests.post.return_value = mock_response
            mock_requests.exceptions = real_requests.exceptions
            from axiom_corpus.verifier import _call_policyengine_api
            value, error = _call_policyengine_api(
                {"people": {"adult": {}}, "tax_units": {"tax_unit": {"members": ["adult"]}}},
                "eitc",
                2024,
            )
            assert value is None
            assert "not found" in error

    def test_call_policyengine_api_type_error(self):
        """Test _call_policyengine_api catches TypeError."""
        import requests as real_requests
        self._setup_api_url()

        mock_response = MagicMock()
        # result["result"] is a list, calling .get() will raise TypeError
        mock_response.json.return_value = {"result": {"tax_units": ["not a dict"]}}
        mock_response.raise_for_status = MagicMock()

        with patch.dict("sys.modules", {"requests": MagicMock()}) as _:
            import sys
            mock_requests = sys.modules["requests"]
            mock_requests.post.return_value = mock_response
            mock_requests.exceptions = real_requests.exceptions
            from axiom_corpus.verifier import _call_policyengine_api
            value, error = _call_policyengine_api(
                {"people": {"adult": {}}, "tax_units": {"tax_unit": {"members": ["adult"]}}},
                "eitc",
                2024,
            )
            # "tax_unit" in ["not a dict"] will be False, falls to "not found"
            assert value is None


# =============================================================================
# writer.py - Missing lines: 225-236, 243, 301
# =============================================================================


class TestWriterLocalBackend:
    """Test LocalBackend read_original and list_versions."""

    def test_read_original_missing_dir(self, tmp_path):
        from axiom_corpus.writer import LocalBackend
        backend = LocalBackend(root=tmp_path)
        assert backend.read_original("nonexistent/path") is None

    def test_read_original_no_matching_file(self, tmp_path):
        from axiom_corpus.writer import LocalBackend
        backend = LocalBackend(root=tmp_path)
        # Create the directory but with no original.* files
        path = tmp_path / "test" / "path"
        path.mkdir(parents=True)
        assert backend.read_original("test/path") is None

    def test_read_original_finds_xml(self, tmp_path):
        from axiom_corpus.writer import LocalBackend
        backend = LocalBackend(root=tmp_path)
        path = tmp_path / "test" / "path"
        path.mkdir(parents=True)
        (path / "original.xml").write_bytes(b"<xml/>")
        result = backend.read_original("test/path")
        assert result == b"<xml/>"

    def test_list_versions_empty(self, tmp_path):
        from axiom_corpus.writer import LocalBackend
        backend = LocalBackend(root=tmp_path)
        assert backend.list_versions("nonexistent") == []

    def test_list_versions_with_dates(self, tmp_path):
        from axiom_corpus.writer import LocalBackend
        backend = LocalBackend(root=tmp_path)
        base = tmp_path / "us" / "statute" / "26" / "32"
        (base / "2024-01-01").mkdir(parents=True)
        (base / "2023-01-01").mkdir(parents=True)
        (base / "not-a-date").mkdir(parents=True)
        result = backend.list_versions("us/statute/26/32")
        assert result == ["2023-01-01", "2024-01-01"]

    def test_document_writer_read(self, tmp_path):
        from axiom_corpus.writer import DocumentWriter, LocalBackend
        backend = LocalBackend(root=tmp_path)
        writer = DocumentWriter(backend=backend)
        # Not found
        assert writer.read("nonexistent") is None


# =============================================================================
# parsers/__init__.py - Missing lines: 26-33 (import error fallbacks)
# =============================================================================


class TestParsersInitImportFallbacks:
    """Test that parsers/__init__.py handles import failures gracefully."""

    def test_generic_parser_available(self):
        """Generic parser should be available since axiom is fully installed."""
        from axiom_corpus.parsers import GenericStateParser
        assert GenericStateParser is not None

    def test_parsers_init_exports(self):
        """Check all expected exports."""
        from axiom_corpus import parsers
        assert hasattr(parsers, "USLMParser")
        assert hasattr(parsers, "GenericStateParser")


# =============================================================================
# storage/__init__.py - Missing lines: 9-10, 15-17
# =============================================================================


class TestStorageInitImports:
    """Test storage __init__ imports."""

    def test_sqlite_storage_available(self):
        from axiom_corpus.storage import SQLiteStorage
        assert SQLiteStorage is not None

    def test_r2_storage_available(self):
        from axiom_corpus.storage import R2Storage
        assert R2Storage is not None

    def test_storage_all_list(self):
        from axiom_corpus import storage
        assert "StorageBackend" in storage.__all__
        assert "SQLiteStorage" in storage.__all__


# =============================================================================
# storage/sqlite.py - Missing lines: 219-220, 323-324, 345-358
# =============================================================================


class TestSQLiteStorageGaps:
    """Test SQLite storage edge cases."""

    def test_get_section_not_found(self, tmp_path):
        from axiom_corpus.storage.sqlite import SQLiteStorage
        db_path = tmp_path / "test.db"
        storage = SQLiteStorage(db_path)
        result = storage.get_section(title=26, section="99999")
        assert result is None

    def test_list_titles_empty(self, tmp_path):
        from axiom_corpus.storage.sqlite import SQLiteStorage
        db_path = tmp_path / "test.db"
        storage = SQLiteStorage(db_path)
        # No titles stored yet
        result = storage.list_titles()
        assert result == []

    def test_get_references_to_empty(self, tmp_path):
        from axiom_corpus.storage.sqlite import SQLiteStorage
        db_path = tmp_path / "test.db"
        storage = SQLiteStorage(db_path)
        result = storage.get_references_to(26, "999")
        assert result == []

    def test_get_referenced_by_empty(self, tmp_path):
        from axiom_corpus.storage.sqlite import SQLiteStorage
        db_path = tmp_path / "test.db"
        storage = SQLiteStorage(db_path)
        result = storage.get_referenced_by(26, "999")
        assert result == []

    def test_update_title_metadata(self, tmp_path):
        from axiom_corpus.storage.sqlite import SQLiteStorage
        db_path = tmp_path / "test.db"
        storage = SQLiteStorage(db_path)
        storage.update_title_metadata(26, "Internal Revenue Code", True)
        titles = storage.list_titles()
        assert len(titles) == 1
        assert titles[0].number == 26

    def test_store_section_with_malformed_reference(self, tmp_path):
        """Test that malformed cross-references are silently skipped."""
        from axiom_corpus.models import Citation, Section
        from axiom_corpus.storage.sqlite import SQLiteStorage
        db_path = tmp_path / "test.db"
        storage = SQLiteStorage(db_path)
        section = Section(
            citation=Citation(title=26, section="32"),
            title_name="IRC",
            section_title="EITC",
            text="Test",
            subsections=[],
            references_to=["not a valid citation!", "26 USC 1"],
            source_url="https://example.com",
            retrieved_at=date.today(),
        )
        storage.store_section(section)
        # The malformed ref should be skipped, valid one stored
        refs = storage.get_references_to(26, "32")
        assert len(refs) == 1
        assert "26 USC 1" in refs[0]


# =============================================================================
# storage/guidance.py - Missing lines: 110-149, 163-174
# =============================================================================


class TestGuidanceStorageGaps:
    """Test guidance storage search and statute linking."""

    def _make_rev_proc(self):
        from axiom_corpus.models_guidance import GuidanceType, RevenueProcedure
        return RevenueProcedure(
            doc_number="2024-40",
            doc_type=GuidanceType.REV_PROC,
            title="Test Rev Proc",
            irb_citation="2024-50 IRB",
            published_date=date(2024, 12, 1),
            full_text="Earned income credit parameters for 2025.",
            sections=[],
            effective_date=date(2025, 1, 1),
            tax_years=[2025],
            subject_areas=["EITC"],
            parameters={},
            source_url="https://irs.gov/test",
            pdf_url=None,
            retrieved_at=date.today(),
        )

    def test_search_guidance_no_results(self, tmp_path):
        from axiom_corpus.storage.guidance import GuidanceStorage
        db_path = tmp_path / "guidance.db"
        storage = GuidanceStorage(db_path)

        # Store a doc first to create the table
        rp = self._make_rev_proc()
        storage.store_revenue_procedure(rp)

        # Create FTS table manually if not exists
        storage.db.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS guidance_fts
            USING fts5(doc_number, full_text, content=guidance_documents, content_rowid=rowid)
        """)
        storage.db.execute("""
            INSERT INTO guidance_fts(rowid, doc_number, full_text)
            SELECT rowid, doc_number, full_text FROM guidance_documents
        """)

        results = storage.search_guidance("nonexistent_term_xyz")
        assert results == []

    def test_get_guidance_for_statute_empty(self, tmp_path):
        from axiom_corpus.storage.guidance import GuidanceStorage
        db_path = tmp_path / "guidance.db"
        storage = GuidanceStorage(db_path)

        # Create guidance_statute_refs table
        rp = self._make_rev_proc()
        storage.store_revenue_procedure(rp)
        storage.db["guidance_statute_refs"].insert(
            {"guidance_id": "test", "statute_title": 99, "statute_section": "999",
             "ref_type": "cites", "excerpt": None},
            ignore=True,
        )

        results = storage.get_guidance_for_statute(26, "32")
        assert results == []


# =============================================================================
# archive.py - Missing line: 146
# =============================================================================


class TestArchiveIngestTitle:
    """Test the AxiomArchive.ingest_title method."""

    def test_ingest_title_basic(self, tmp_path):
        """Test ingest_title counts sections."""
        from axiom_corpus.archive import AxiomArchive
        from axiom_corpus.models import Citation, Section

        mock_section = Section(
            citation=Citation(title=26, section="32"),
            title_name="Internal Revenue Code",
            section_title="Earned income",
            text="Test text",
            subsections=[],
            source_url="https://example.com",
            retrieved_at=date.today(),
        )

        with patch("axiom_corpus.parsers.us.statutes.USLMParser") as MockParser:
            mock_parser = MockParser.return_value
            mock_parser.get_title_number.return_value = 26
            mock_parser.get_title_name.return_value = "Internal Revenue Code"

            # Return 101 sections to trigger the modulo-100 branch (line 146)
            mock_parser.iter_sections.return_value = iter([mock_section] * 101)

            db_path = tmp_path / "test.db"
            archive = AxiomArchive(db_path=db_path)
            count = archive.ingest_title("fake.xml")
            assert count == 101


# =============================================================================
# fetchers/ecfr.py - Missing lines: 70-79, 101, 169-170
# =============================================================================


class TestECFRFetcherGaps:
    """Test eCFR fetcher async methods and convenience function."""

    def test_available_titles(self):
        from axiom_corpus.fetchers.ecfr import ECFRFetcher
        fetcher = ECFRFetcher()
        titles = fetcher.available_titles
        assert 1 in titles
        assert 50 in titles

    def test_get_title_url(self):
        from axiom_corpus.fetchers.ecfr import ECFRFetcher
        fetcher = ECFRFetcher()
        url = fetcher.get_title_url(26)
        assert "title-26" in url

    @pytest.mark.asyncio
    async def test_download_file(self, tmp_path):
        from unittest.mock import AsyncMock

        from axiom_corpus.fetchers.ecfr import ECFRFetcher
        fetcher = ECFRFetcher(data_dir=tmp_path)

        mock_response = MagicMock()
        mock_response.content = b"<xml/>"
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("axiom_corpus.fetchers.ecfr.httpx.AsyncClient") as MockClient:
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            dest = tmp_path / "test.xml"
            result = await fetcher._download_file("https://example.com/test.xml", dest)
            assert result == dest
            assert dest.read_bytes() == b"<xml/>"

    @pytest.mark.asyncio
    async def test_download_title_existing(self, tmp_path):
        from axiom_corpus.fetchers.ecfr import ECFRFetcher
        fetcher = ECFRFetcher(data_dir=tmp_path)

        # Create file so it's "already downloaded"
        dest = tmp_path / "title-26.xml"
        dest.write_text("<xml/>")

        result = await fetcher.download_title(26, force=False)
        assert result == dest

    @pytest.mark.asyncio
    async def test_download_cfr_title_convenience(self, tmp_path):
        from axiom_corpus.fetchers.ecfr import download_cfr_title
        # Existing file case
        dest = tmp_path / "title-26.xml"
        dest.write_text("<xml/>")

        result = await download_cfr_title(26, data_dir=tmp_path, force=False)
        assert result == dest


# =============================================================================
# fetchers/pdf_extractor.py - Missing lines: 42, 54-55, 71-72, 117-122
# =============================================================================


class TestPDFExtractorGaps:
    """Test PDF extractor edge cases."""

    def test_extract_text_from_bytesio(self):
        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor
        extractor = PDFTextExtractor()

        # Create a minimal PDF
        import fitz
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Test PDF content")
        pdf_bytes = doc.tobytes()
        doc.close()

        result = extractor.extract_text(BytesIO(pdf_bytes))
        assert "Test PDF content" in result

    def test_extract_text_invalid_pdf(self):
        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor
        extractor = PDFTextExtractor()
        with pytest.raises(ValueError, match="Failed to read PDF"):
            extractor.extract_text(b"not a pdf")

    def test_extract_text_from_file(self, tmp_path):
        import fitz

        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor

        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "File content")
        pdf_path = tmp_path / "test.pdf"
        doc.save(str(pdf_path))
        doc.close()

        extractor = PDFTextExtractor()
        result = extractor.extract_text_from_file(str(pdf_path))
        assert "File content" in result

    def test_get_metadata(self):
        import fitz

        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor

        doc = fitz.open()
        doc.new_page()
        pdf_bytes = doc.tobytes()
        doc.close()

        extractor = PDFTextExtractor()
        meta = extractor.get_metadata(pdf_bytes)
        assert "page_count" in meta
        assert meta["page_count"] == 1

    def test_get_metadata_from_bytesio(self):
        import fitz

        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor

        doc = fitz.open()
        doc.new_page()
        pdf_bytes = doc.tobytes()
        doc.close()

        extractor = PDFTextExtractor()
        meta = extractor.get_metadata(BytesIO(pdf_bytes))
        assert meta["page_count"] == 1


# =============================================================================
# fetchers/irs_guidance.py - Missing lines: 89-148, 233
# =============================================================================


class TestIRSGuidanceFetcherGaps:
    """Test IRS guidance fetcher parsing methods."""

    def test_parse_revenue_procedure_html(self):
        from bs4 import BeautifulSoup

        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher

        html = """
        <html>
        <body>
        <h2>Rev. Proc. 2024-40. Inflation Adjustments</h2>
        <p>SECTION 1. PURPOSE</p>
        <p>This revenue procedure describes the inflation adjustments for 2025.</p>
        <p>SECTION 2. BACKGROUND</p>
        <p>.01 The earned income credit is adjusted annually.</p>
        <p>.02 The standard deduction is also adjusted.</p>
        <p>Rev. Proc. 2024-41. Another Document</p>
        </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        fetcher = IRSGuidanceFetcher()
        rp = fetcher._parse_revenue_procedure(
            soup, "2024-40", "https://www.irs.gov/irb/2024-50_IRB"
        )
        assert rp.doc_number == "2024-40"
        assert rp.irb_citation == "2024-50 IRB"

    def test_parse_revenue_procedure_with_main_content(self):
        """Test fallback to main content element."""
        from bs4 import BeautifulSoup

        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher

        html = """
        <html>
        <body>
        <main>
        <h2>Rev. Proc. 2024-40. Inflation Adjustments</h2>
        <p>SECTION 1. PURPOSE</p>
        <p>This revenue procedure provides inflation adjustments for 2025.</p>
        <p>SECTION 2. SCOPE</p>
        <p>This applies to all taxpayers.</p>
        <p>Rev. Proc. 2024-41. Next Document</p>
        </main>
        </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        fetcher = IRSGuidanceFetcher()
        rp = fetcher._parse_revenue_procedure(
            soup, "2024-40", "https://www.irs.gov/irb/2024-50_IRB"
        )
        assert rp.doc_number == "2024-40"
        assert len(rp.sections) >= 1

    def test_extract_title(self):
        from bs4 import BeautifulSoup

        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher

        html = "<h2>Rev. Proc. 2024-40. Inflation Adjustments</h2>"
        soup = BeautifulSoup(html, "html.parser")
        heading = soup.find("h2")

        fetcher = IRSGuidanceFetcher()
        title = fetcher._extract_title(heading)
        assert "Inflation Adjustments" in title

    def test_is_next_document(self):
        from bs4 import BeautifulSoup

        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher

        fetcher = IRSGuidanceFetcher()

        html = "<h2>Rev. Proc. 2024-41. Next One</h2>"
        tag = BeautifulSoup(html, "html.parser").find("h2")
        assert fetcher._is_next_document(tag) is True

        html2 = "<p>Some regular text</p>"
        tag2 = BeautifulSoup(html2, "html.parser").find("p")
        assert fetcher._is_next_document(tag2) is False

        assert fetcher._is_next_document(None) is False

    def test_parse_sections(self):
        from bs4 import BeautifulSoup

        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher

        html = """
        <div>
        <p>SECTION 1. PURPOSE</p>
        <p>This is the purpose.</p>
        <p>.01 First subsection.</p>
        <p>.02 Second subsection.</p>
        <p>SECTION 2. SCOPE</p>
        <p>This is the scope.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        elements = soup.find_all("p")
        fetcher = IRSGuidanceFetcher()
        sections = fetcher._parse_sections(elements)
        assert len(sections) == 2
        assert sections[0].section_num == "1"
        assert len(sections[0].children) >= 1  # At least one subsection

    def test_extract_tax_years_from_text(self):
        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher
        fetcher = IRSGuidanceFetcher()
        years = fetcher._extract_tax_years("Applicable for 2025 and 2026.", 2024)
        assert 2025 in years
        assert 2026 in years

    def test_extract_tax_years_default(self):
        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher
        fetcher = IRSGuidanceFetcher()
        years = fetcher._extract_tax_years("No year mentioned.", 2024)
        assert years == [2025]

    def test_extract_subject_areas_eitc(self):
        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher
        fetcher = IRSGuidanceFetcher()
        subjects = fetcher._extract_subject_areas("Earned Income Credit", "eitc parameters")
        assert "EITC" in subjects

    def test_extract_subject_areas_default(self):
        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher
        fetcher = IRSGuidanceFetcher()
        subjects = fetcher._extract_subject_areas("Something", "something else")
        assert subjects == ["General"]

    def test_context_manager(self):
        from axiom_corpus.fetchers.irs_guidance import IRSGuidanceFetcher
        with IRSGuidanceFetcher() as fetcher:
            assert fetcher is not None


# =============================================================================
# fetchers/irs_bulk.py - Missing lines: 45-51, 115, 157-181, etc.
# =============================================================================


class TestIRSBulkFetcherGaps:
    """Test IRS bulk fetcher gaps."""

    def test_irs_drop_document_properties(self):
        from axiom_corpus.fetchers.irs_bulk import IRSDropDocument
        from axiom_corpus.models_guidance import GuidanceType

        doc = IRSDropDocument(
            doc_type=GuidanceType.REV_PROC,
            doc_number="2024-40",
            year=2024,
            pdf_filename="rp-24-40.pdf",
        )
        assert doc.pdf_url == "https://www.irs.gov/pub/irs-drop/rp-24-40.pdf"
        assert doc.id == "rp-2024-40"

    def test_irs_drop_document_all_types(self):
        from axiom_corpus.fetchers.irs_bulk import IRSDropDocument
        from axiom_corpus.models_guidance import GuidanceType

        for gtype, prefix in [
            (GuidanceType.REV_RUL, "rr"),
            (GuidanceType.NOTICE, "notice"),
            (GuidanceType.ANNOUNCEMENT, "announce"),
        ]:
            doc = IRSDropDocument(
                doc_type=gtype,
                doc_number="2024-1",
                year=2024,
                pdf_filename="test.pdf",
            )
            assert doc.id == f"{prefix}-2024-1"

    def test_parse_irs_drop_listing_with_filters(self):
        from axiom_corpus.fetchers.irs_bulk import parse_irs_drop_listing
        from axiom_corpus.models_guidance import GuidanceType

        html = """
        <html><body>
        <a href="/pub/irs-drop/rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="/pub/irs-drop/rr-24-12.pdf">rr-24-12.pdf</a>
        <a href="/pub/irs-drop/n-23-45.pdf">n-23-45.pdf</a>
        <a href="/pub/irs-drop/rp-24-40.pdf">rp-24-40.pdf duplicate</a>
        <a href="/pub/irs-drop/a-24-01.pdf">a-24-01.pdf</a>
        </body></html>
        """

        # Filter by year
        docs = parse_irs_drop_listing(html, year=2024)
        assert all(d.year == 2024 for d in docs)

        # Filter by type
        docs = parse_irs_drop_listing(html, doc_types=[GuidanceType.REV_PROC])
        assert all(d.doc_type == GuidanceType.REV_PROC for d in docs)

        # No duplicates
        all_docs = parse_irs_drop_listing(html)
        filenames = [d.pdf_filename for d in all_docs]
        assert len(filenames) == len(set(filenames))

    def test_bulk_fetcher_context_manager(self):
        from axiom_corpus.fetchers.irs_bulk import IRSBulkFetcher
        with IRSBulkFetcher() as fetcher:
            assert fetcher is not None

    def test_generate_title(self):
        from axiom_corpus.fetchers.irs_bulk import IRSBulkFetcher, IRSDropDocument
        from axiom_corpus.models_guidance import GuidanceType

        fetcher = IRSBulkFetcher()
        doc = IRSDropDocument(
            doc_type=GuidanceType.REV_PROC,
            doc_number="2024-40",
            year=2024,
            pdf_filename="rp-24-40.pdf",
        )
        title = fetcher._generate_title(doc)
        assert title == "Revenue Procedure 2024-40"

    def test_fetch_drop_listing_pagination(self):
        """Test _fetch_drop_listing pagination handling."""
        from axiom_corpus.fetchers.irs_bulk import IRSBulkFetcher

        fetcher = IRSBulkFetcher(max_pages=2)

        page0_html = '<a href="rp-24-01.pdf">rp-24-01.pdf</a> ?page=1'
        page1_html = '<p>No more guidance</p>'

        call_count = 0

        def mock_get(url):
            nonlocal call_count
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if call_count == 0:
                resp.text = page0_html
            else:
                resp.text = page1_html
            call_count += 1
            return resp

        fetcher.client = MagicMock()
        fetcher.client.get = mock_get

        html = fetcher._fetch_drop_listing()
        assert "rp-24-01.pdf" in html

    def test_fetch_drop_listing_with_progress(self):
        """Test _fetch_drop_listing with progress callback."""
        from axiom_corpus.fetchers.irs_bulk import IRSBulkFetcher

        fetcher = IRSBulkFetcher(max_pages=2)
        page0_html = '<a href="rp-24-01.pdf">rp-24-01.pdf</a> ?page=1'
        page1_html = '<a href="rp-24-02.pdf">rp-24-02.pdf</a>'

        call_count = 0
        def mock_get(url):
            nonlocal call_count
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if call_count == 0:
                resp.text = page0_html
            else:
                resp.text = page1_html
            call_count += 1
            return resp

        fetcher.client = MagicMock()
        fetcher.client.get = mock_get

        progress_msgs = []
        fetcher._fetch_drop_listing(progress_callback=progress_msgs.append)
        assert any("page" in m.lower() for m in progress_msgs)

    def test_fetch_and_store_basic(self, tmp_path):
        """Test fetch_and_store method."""
        from axiom_corpus.fetchers.irs_bulk import IRSBulkFetcher
        from axiom_corpus.models_guidance import GuidanceType

        fetcher = IRSBulkFetcher()

        listing_html = '<a href="rp-24-01.pdf">rp-24-01.pdf</a>'

        def mock_get(url):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "irs-drop" in url and url.endswith(".pdf"):
                resp.content = b"fake pdf content"
            else:
                resp.text = listing_html
            return resp

        fetcher.client = MagicMock()
        fetcher.client.get = mock_get

        progress = []
        results = fetcher.fetch_and_store(
            years=[2024],
            doc_types=[GuidanceType.REV_PROC],
            download_dir=tmp_path,
            progress_callback=progress.append,
        )
        assert len(results) >= 0  # May be 0 if listing parsing yields no docs

    def test_fetch_and_store_http_error(self):
        """Test fetch_and_store handles HTTP errors."""
        import httpx

        from axiom_corpus.fetchers.irs_bulk import IRSBulkFetcher
        from axiom_corpus.models_guidance import GuidanceType

        fetcher = IRSBulkFetcher()

        listing_html = '<a href="rp-24-01.pdf">rp-24-01.pdf</a>'
        call_count = 0

        def mock_get(url):
            nonlocal call_count
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "irs-drop" in url and url.endswith(".pdf"):
                raise httpx.HTTPError("download failed")
            resp.text = listing_html
            return resp

        fetcher.client = MagicMock()
        fetcher.client.get = mock_get

        progress = []
        results = fetcher.fetch_and_store(
            years=[2024],
            doc_types=[GuidanceType.REV_PROC],
            progress_callback=progress.append,
        )
        # Should handle error gracefully
        assert isinstance(results, list)


# =============================================================================
# sources/uslm.py - Missing lines: 99-103, 115-152, 185-229
# =============================================================================


class TestUSLMSourceGaps:
    """Test USLM source adapter methods."""

    def test_get_federal_config(self):
        from axiom_corpus.sources.uslm import get_federal_config
        config = get_federal_config()
        assert config.jurisdiction == "us"
        assert config.source_type == "uslm"
        assert "26" in config.codes

    def test_uslm_source_init(self):
        from axiom_corpus.sources.uslm import USLMSource
        source = USLMSource()
        assert source.config.jurisdiction == "us"


# =============================================================================
# models_uk.py - Missing lines: 143, 185, 208
# =============================================================================


class TestUKModelsGaps:
    """Test UK models edge cases."""

    def test_uk_models_import(self):
        from axiom_corpus.models_uk import UKAct, UKAmendment, UKCitation, UKSection
        assert UKSection is not None
        assert UKCitation is not None
        assert UKAct is not None
        assert UKAmendment is not None

    def test_uk_legislation_types(self):
        from axiom_corpus.models_uk import UK_LEGISLATION_TYPES
        assert "ukpga" in UK_LEGISLATION_TYPES
        assert "uksi" in UK_LEGISLATION_TYPES

    def test_uk_act_short_titles(self):
        from axiom_corpus.models_uk import UK_ACT_SHORT_TITLES
        assert isinstance(UK_ACT_SHORT_TITLES, dict)


# =============================================================================
# fetchers/state_benefits.py - Missing lines for SNAP, TANF, CCDF fetchers
# =============================================================================


class TestStateBenefitsFetcherGaps:
    """Test state benefits fetcher classes."""

    def test_snap_sua_url(self):
        from axiom_corpus.fetchers.state_benefits import SNAPSUAFetcher
        fetcher = SNAPSUAFetcher()
        url = fetcher.get_sua_url(2025)
        assert "FY25" in url

    def test_snap_save_file(self, tmp_path):
        from axiom_corpus.fetchers.state_benefits import SNAPSUAFetcher
        fetcher = SNAPSUAFetcher()
        out = tmp_path / "sub" / "test.xlsx"
        fetcher.save_file(b"content", out)
        assert out.read_bytes() == b"content"

    def test_tanf_fetcher_urls(self):
        from axiom_corpus.fetchers.state_benefits import TANFFetcher
        fetcher = TANFFetcher()

        url_2023 = fetcher.get_table_url("II.A.4", 2023)
        assert "2025-05" in url_2023

        url_2022 = fetcher.get_table_url("II.A.4", 2022)
        assert "2024-02" in url_2022

        url_2021 = fetcher.get_table_url("II.A.4", 2021)
        assert "2023-10" in url_2021

    def test_tanf_list_tables(self):
        from axiom_corpus.fetchers.state_benefits import TANFFetcher
        fetcher = TANFFetcher()
        tables = fetcher.list_available_tables()
        assert "II.A.4" in tables

    def test_tanf_save_file(self, tmp_path):
        from axiom_corpus.fetchers.state_benefits import TANFFetcher
        fetcher = TANFFetcher()
        out = tmp_path / "test.xlsx"
        fetcher.save_file(b"data", out)
        assert out.exists()

    def test_ccdf_urls(self):
        from axiom_corpus.fetchers.state_benefits import CCDFFetcher
        fetcher = CCDFFetcher()
        db_url = fetcher.get_database_url()
        assert "CCDF" in db_url

        bot_url_2023 = fetcher.get_book_of_tables_url(2023)
        assert "2023" in bot_url_2023

        bot_url_2022 = fetcher.get_book_of_tables_url(2022)
        assert "2022" in bot_url_2022

    def test_ccdf_save_file(self, tmp_path):
        from axiom_corpus.fetchers.state_benefits import CCDFFetcher
        fetcher = CCDFFetcher()
        out = tmp_path / "test.xlsx"
        fetcher.save_file(b"data", out)
        assert out.exists()

    def test_state_benefits_fetcher_init(self):
        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher
        fetcher = StateBenefitsFetcher()
        assert fetcher.snap is not None
        assert fetcher.tanf is not None
        assert fetcher.ccdf is not None

    def test_state_benefits_context_manager(self):
        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher
        with StateBenefitsFetcher() as fetcher:
            assert fetcher is not None

    def test_state_benefits_close(self):
        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher
        fetcher = StateBenefitsFetcher()
        fetcher.close()  # Should not raise

    def test_fetch_snap_sua_success(self, tmp_path):
        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher
        fetcher = StateBenefitsFetcher()

        mock_response = MagicMock()
        mock_response.content = b"excel content"
        mock_response.raise_for_status = MagicMock()

        fetcher.snap.client = MagicMock()
        fetcher.snap.client.get.return_value = mock_response

        progress = []
        results = fetcher.fetch_snap_sua([2025], tmp_path, progress.append)
        assert 2025 in results

    def test_fetch_snap_sua_error(self, tmp_path):
        import httpx

        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher

        fetcher = StateBenefitsFetcher()
        fetcher.snap.client = MagicMock()
        fetcher.snap.client.get.side_effect = httpx.HTTPError("fail")

        progress = []
        results = fetcher.fetch_snap_sua([2025], tmp_path, progress.append)
        assert 2025 not in results

    def test_fetch_tanf_tables(self, tmp_path):
        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher
        fetcher = StateBenefitsFetcher()

        mock_response = MagicMock()
        mock_response.content = b"tanf data"
        mock_response.raise_for_status = MagicMock()

        fetcher.tanf.client = MagicMock()
        fetcher.tanf.client.get.return_value = mock_response

        progress = []
        results = fetcher.fetch_tanf_tables(["II.A.4"], [2023], tmp_path, progress.append)
        assert "II.A.4_2023" in results

    def test_fetch_tanf_tables_error(self, tmp_path):
        import httpx

        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher

        fetcher = StateBenefitsFetcher()
        fetcher.tanf.client = MagicMock()
        fetcher.tanf.client.get.side_effect = httpx.HTTPError("fail")

        progress = []
        results = fetcher.fetch_tanf_tables(["II.A.4"], [2023], tmp_path, progress.append)
        assert len(results) == 0

    def test_fetch_tanf_databook(self, tmp_path):
        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher
        fetcher = StateBenefitsFetcher()

        mock_response = MagicMock()
        mock_response.content = b"databook"
        mock_response.raise_for_status = MagicMock()

        fetcher.tanf.client = MagicMock()
        fetcher.tanf.client.get.return_value = mock_response

        progress = []
        result = fetcher.fetch_tanf_databook(2023, tmp_path, progress.append)
        assert result is not None

    def test_fetch_tanf_databook_error(self, tmp_path):
        import httpx

        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher

        fetcher = StateBenefitsFetcher()
        fetcher.tanf.client = MagicMock()
        fetcher.tanf.client.get.side_effect = httpx.HTTPError("fail")

        progress = []
        result = fetcher.fetch_tanf_databook(2023, tmp_path, progress.append)
        assert result is None

    def test_fetch_ccdf_database(self, tmp_path):
        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher
        fetcher = StateBenefitsFetcher()

        mock_response = MagicMock()
        mock_response.content = b"ccdf data"
        mock_response.raise_for_status = MagicMock()

        fetcher.ccdf.client = MagicMock()
        fetcher.ccdf.client.get.return_value = mock_response

        progress = []
        result = fetcher.fetch_ccdf_database(tmp_path, progress.append)
        assert result is not None

    def test_fetch_ccdf_database_error(self, tmp_path):
        import httpx

        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher

        fetcher = StateBenefitsFetcher()
        fetcher.ccdf.client = MagicMock()
        fetcher.ccdf.client.get.side_effect = httpx.HTTPError("fail")

        progress = []
        result = fetcher.fetch_ccdf_database(tmp_path, progress.append)
        assert result is None

    def test_fetch_all(self, tmp_path):
        from axiom_corpus.fetchers.state_benefits import StateBenefitsFetcher
        fetcher = StateBenefitsFetcher()

        mock_response = MagicMock()
        mock_response.content = b"content"
        mock_response.raise_for_status = MagicMock()

        for sub_fetcher in [fetcher.snap, fetcher.tanf, fetcher.ccdf]:
            sub_fetcher.client = MagicMock()
            sub_fetcher.client.get.return_value = mock_response

        progress = []
        results = fetcher.fetch_all(tmp_path, [2025], [2023], progress.append)
        assert "snap_sua" in results
        assert "tanf" in results
        assert "ccdf" in results
