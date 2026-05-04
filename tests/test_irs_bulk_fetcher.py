"""Tests for IRS bulk guidance fetcher."""

from unittest.mock import MagicMock, patch

import pytest

from axiom_corpus.fetchers.irs_bulk import (
    IRSBulkFetcher,
    IRSDropDocument,
    parse_irs_drop_listing,
)
from axiom_corpus.models_guidance import GuidanceType


class TestParseIrsDropListing:
    """Tests for parsing IRS drop folder listings."""

    def test_parse_revenue_procedure(self):
        """Parse a Revenue Procedure filename."""
        html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        """
        docs = parse_irs_drop_listing(html)
        assert len(docs) == 1
        doc = docs[0]
        assert doc.doc_type == GuidanceType.REV_PROC
        assert doc.doc_number == "2024-40"
        assert doc.year == 2024
        assert doc.pdf_filename == "rp-24-40.pdf"

    def test_parse_revenue_ruling(self):
        """Parse a Revenue Ruling filename."""
        html = """
        <a href="rr-23-12.pdf">rr-23-12.pdf</a>
        """
        docs = parse_irs_drop_listing(html)
        assert len(docs) == 1
        doc = docs[0]
        assert doc.doc_type == GuidanceType.REV_RUL
        assert doc.doc_number == "2023-12"
        assert doc.year == 2023

    def test_parse_notice(self):
        """Parse a Notice filename."""
        html = """
        <a href="n-22-45.pdf">n-22-45.pdf</a>
        """
        docs = parse_irs_drop_listing(html)
        assert len(docs) == 1
        doc = docs[0]
        assert doc.doc_type == GuidanceType.NOTICE
        assert doc.doc_number == "2022-45"
        assert doc.year == 2022

    def test_parse_multiple_documents(self):
        """Parse multiple document types from listing."""
        html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="rr-24-15.pdf">rr-24-15.pdf</a>
        <a href="n-24-78.pdf">n-24-78.pdf</a>
        <a href="a-24-10.pdf">a-24-10.pdf</a>
        """
        docs = parse_irs_drop_listing(html)
        assert len(docs) == 4  # All 4 types parsed

    def test_filter_by_year(self):
        """Filter documents by year."""
        html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="rp-23-34.pdf">rp-23-34.pdf</a>
        <a href="rp-22-38.pdf">rp-22-38.pdf</a>
        """
        docs = parse_irs_drop_listing(html, year=2024)
        assert len(docs) == 1
        assert docs[0].year == 2024

    def test_filter_by_doc_type(self):
        """Filter documents by type."""
        html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="rr-24-15.pdf">rr-24-15.pdf</a>
        <a href="n-24-78.pdf">n-24-78.pdf</a>
        """
        docs = parse_irs_drop_listing(html, doc_types=[GuidanceType.REV_PROC])
        assert len(docs) == 1
        assert docs[0].doc_type == GuidanceType.REV_PROC

    def test_ignore_non_guidance_files(self):
        """Ignore non-guidance PDF files."""
        html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="p1544.pdf">p1544.pdf</a>
        <a href="i1040.pdf">i1040.pdf</a>
        """
        docs = parse_irs_drop_listing(html)
        assert len(docs) == 1  # Only the Rev. Proc.


class TestIRSBulkFetcher:
    """Tests for the IRS bulk fetcher."""

    @pytest.fixture
    def fetcher(self):
        """Create a fetcher instance."""
        return IRSBulkFetcher()

    def test_list_documents_for_year(self, fetcher):
        """List all guidance documents for a year."""
        # Mock the HTTP response
        mock_html = """
        <html>
        <body>
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="rp-24-39.pdf">rp-24-39.pdf</a>
        <a href="rr-24-15.pdf">rr-24-15.pdf</a>
        <a href="n-24-78.pdf">n-24-78.pdf</a>
        </body>
        </html>
        """
        with patch.object(fetcher, "_fetch_drop_listing", return_value=mock_html):
            docs = fetcher.list_documents(year=2024)

        assert len(docs) == 4
        assert all(d.year == 2024 for d in docs)

    def test_list_documents_filter_type(self, fetcher):
        """List documents filtered by type."""
        mock_html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="rr-24-15.pdf">rr-24-15.pdf</a>
        """
        with patch.object(fetcher, "_fetch_drop_listing", return_value=mock_html):
            docs = fetcher.list_documents(year=2024, doc_types=[GuidanceType.REV_PROC])

        assert len(docs) == 1
        assert docs[0].doc_type == GuidanceType.REV_PROC

    def test_fetch_document_pdf(self, fetcher):
        """Fetch PDF content for a document."""
        mock_pdf = b"%PDF-1.4 test content"

        with patch.object(fetcher.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.content = mock_pdf
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            doc = IRSDropDocument(
                doc_type=GuidanceType.REV_PROC,
                doc_number="2024-40",
                year=2024,
                pdf_filename="rp-24-40.pdf",
            )
            content = fetcher.fetch_pdf(doc)

        assert content == mock_pdf

    def test_document_url(self, fetcher):
        """Test PDF URL construction."""
        doc = IRSDropDocument(
            doc_type=GuidanceType.REV_PROC,
            doc_number="2024-40",
            year=2024,
            pdf_filename="rp-24-40.pdf",
        )
        assert doc.pdf_url == "https://www.irs.gov/pub/irs-drop/rp-24-40.pdf"


class TestBulkDownloadWithExtraction:
    """Tests for bulk download with text extraction."""

    @pytest.fixture
    def fetcher(self):
        """Create a fetcher instance."""
        return IRSBulkFetcher()

    def test_download_and_extract_document(self, fetcher, tmp_path):
        """Test downloading and extracting text from a single document."""
        # Skip if we don't have a real PDF to test with
        # This tests the integration with PDF extraction
        doc = IRSDropDocument(
            doc_type=GuidanceType.REV_PROC,
            doc_number="2024-40",
            year=2024,
            pdf_filename="rp-24-40.pdf",
        )

        # Create a minimal fake PDF for testing
        # Real PDF would require actual IRS download
        mock_pdf_content = b"%PDF-1.4 mock content"

        with patch.object(fetcher, "fetch_pdf", return_value=mock_pdf_content):
            # Mock PDF extraction since we're using a fake PDF
            # The imports are local in fetch_and_extract, so patch at the source modules
            with patch(
                "axiom_corpus.fetchers.pdf_extractor.PDFTextExtractor"
            ) as mock_extractor_class:
                mock_extractor = MagicMock()
                mock_extractor.extract_text.return_value = (
                    "Rev. Proc. 2024-40\nSECTION 1. PURPOSE\nTest content"
                )
                mock_extractor_class.return_value = mock_extractor

                with patch(
                    "axiom_corpus.fetchers.irs_parser.IRSDocumentParser"
                ) as mock_parser_class:
                    mock_parser = MagicMock()
                    mock_parser.parse.return_value = MagicMock(
                        sections=[],
                        effective_year=2025,
                    )
                    mock_parser_class.return_value = mock_parser

                    with patch(
                        "axiom_corpus.fetchers.irs_parser.IRSParameterExtractor"
                    ) as mock_param_class:
                        mock_param = MagicMock()
                        mock_param.extract.return_value = {}
                        mock_param_class.return_value = mock_param

                        result = fetcher.fetch_and_extract(doc, save_pdf=tmp_path / "test.pdf")

        assert result.doc_number == "2024-40"
        assert result.doc_type == GuidanceType.REV_PROC
        assert "Test content" in result.full_text

    def test_bulk_download_progress_callback(self, fetcher, tmp_path):
        """Test progress callback during bulk download."""
        mock_html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="rp-24-39.pdf">rp-24-39.pdf</a>
        """

        progress_messages = []

        def progress_callback(msg):
            progress_messages.append(msg)

        with patch.object(fetcher, "_fetch_drop_listing", return_value=mock_html):
            mock_pdf = b"%PDF-1.4 test"
            with patch.object(fetcher, "fetch_pdf", return_value=mock_pdf):
                fetcher.fetch_and_store(
                    years=[2024],
                    doc_types=[GuidanceType.REV_PROC],
                    download_dir=tmp_path,
                    progress_callback=progress_callback,
                )

        # Should have progress messages
        assert len(progress_messages) > 0
        assert any("Found" in msg for msg in progress_messages)

    def test_bulk_download_saves_pdfs(self, fetcher, tmp_path):
        """Test that PDFs are saved to the specified directory."""
        mock_html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        """

        with patch.object(fetcher, "_fetch_drop_listing", return_value=mock_html):
            mock_pdf = b"%PDF-1.4 test content for saving"
            with patch.object(fetcher, "fetch_pdf", return_value=mock_pdf):
                fetcher.fetch_and_store(
                    years=[2024],
                    doc_types=[GuidanceType.REV_PROC],
                    download_dir=tmp_path,
                )

        # Check PDF was saved
        pdf_file = tmp_path / "rp-24-40.pdf"
        assert pdf_file.exists()
        assert pdf_file.read_bytes() == mock_pdf

    def test_bulk_download_uses_extracted_guidance_text(self, fetcher, tmp_path):
        """Bulk storage must use extracted text, not synthetic PDF markers."""
        mock_html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        """
        extracted = MagicMock(
            doc_number="2024-40",
            full_text="Rev. Proc. 2024-40\nSECTION 1. PURPOSE\nActual source text",
        )

        with patch.object(fetcher, "_fetch_drop_listing", return_value=mock_html):
            with patch.object(fetcher, "fetch_and_extract", return_value=extracted) as mock_extract:
                results = fetcher.fetch_and_store(
                    years=[2024],
                    doc_types=[GuidanceType.REV_PROC],
                    download_dir=tmp_path,
                )

        assert results == [extracted]
        assert "[PDF content" not in results[0].full_text
        mock_extract.assert_called_once()

    def test_bulk_download_handles_errors_gracefully(self, fetcher, tmp_path):
        """Test that errors during download don't stop the entire process."""
        import httpx

        mock_html = """
        <a href="rp-24-40.pdf">rp-24-40.pdf</a>
        <a href="rp-24-39.pdf">rp-24-39.pdf</a>
        """

        call_count = [0]

        def mock_fetch_and_extract(doc, save_pdf=None):
            call_count[0] += 1
            if call_count[0] == 1:
                raise httpx.HTTPError("404 Not Found")
            if save_pdf is not None:
                save_pdf.parent.mkdir(parents=True, exist_ok=True)
                save_pdf.write_bytes(b"%PDF-1.4 success")
            return MagicMock(doc_number=doc.doc_number)

        error_messages = []

        def progress_callback(msg):
            if "ERROR" in msg:
                error_messages.append(msg)

        with patch.object(fetcher, "_fetch_drop_listing", return_value=mock_html):
            with patch.object(fetcher, "fetch_and_extract", side_effect=mock_fetch_and_extract):
                results = fetcher.fetch_and_store(
                    years=[2024],
                    doc_types=[GuidanceType.REV_PROC],
                    download_dir=tmp_path,
                    progress_callback=progress_callback,
                )

        # Should have gotten one successful result despite one failure
        assert len(results) == 1
        assert len(error_messages) == 1
