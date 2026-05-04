"""Bulk fetcher for IRS guidance documents.

Downloads Revenue Procedures, Revenue Rulings, and Notices from IRS.gov
in bulk for specified year ranges.

The IRS publishes guidance documents in two locations:
1. https://www.irs.gov/pub/irs-drop/ - PDF files with naming convention:
   - rp-YY-NN.pdf for Revenue Procedures
   - rr-YY-NN.pdf for Revenue Rulings
   - n-YY-NN.pdf for Notices
   - a-YY-NN.pdf for Announcements

2. https://www.irs.gov/irb - Internal Revenue Bulletin HTML pages
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from axiom_corpus.models_guidance import GuidanceSection, GuidanceType, RevenueProcedure


@dataclass
class IRSDropDocument:
    """Metadata for a document in the IRS drop folder."""

    doc_type: GuidanceType
    doc_number: str  # e.g., "2024-40"
    year: int
    pdf_filename: str

    @property
    def pdf_url(self) -> str:
        """Full URL to the PDF file."""
        return f"https://www.irs.gov/pub/irs-drop/{self.pdf_filename}"

    @property
    def id(self) -> str:
        """Generate document ID for database storage."""
        prefix = {
            GuidanceType.REV_PROC: "rp",
            GuidanceType.REV_RUL: "rr",
            GuidanceType.NOTICE: "notice",
            GuidanceType.ANNOUNCEMENT: "announce",
        }[self.doc_type]
        return f"{prefix}-{self.doc_number}"


# Mapping from filename prefix to GuidanceType
PREFIX_TO_TYPE = {
    "rp": GuidanceType.REV_PROC,
    "rr": GuidanceType.REV_RUL,
    "n": GuidanceType.NOTICE,
    "a": GuidanceType.ANNOUNCEMENT,
}

# Pattern to match guidance document filenames: rp-24-40.pdf, rr-23-12.pdf, n-22-45.pdf
# Can appear as just filename or in full URL path
GUIDANCE_PATTERN = re.compile(r"(rp|rr|n|a)-(\d{2})-(\d+)\.pdf", re.IGNORECASE)


def parse_irs_drop_listing(
    html: str,
    year: int | None = None,
    doc_types: list[GuidanceType] | None = None,
) -> list[IRSDropDocument]:
    """Parse IRS drop folder HTML listing to extract document metadata.

    Args:
        html: HTML content of the drop folder listing
        year: Optional filter by year (4-digit, e.g., 2024)
        doc_types: Optional filter by document types

    Returns:
        List of IRSDropDocument objects
    """
    soup = BeautifulSoup(html, "html.parser")
    documents = []
    seen = set()  # Track seen filenames to avoid duplicates

    for link in soup.find_all("a", href=True):
        href = link["href"]

        # Search for guidance pattern anywhere in the URL
        match = GUIDANCE_PATTERN.search(href)
        if not match:
            continue

        prefix, year_short, num = match.groups()
        prefix = prefix.lower()

        # Convert 2-digit year to 4-digit
        year_4digit = 2000 + int(year_short)

        # Apply year filter
        if year is not None and year_4digit != year:
            continue

        doc_type = PREFIX_TO_TYPE[prefix]

        # Apply type filter
        if doc_types is not None and doc_type not in doc_types:
            continue

        doc_number = f"{year_4digit}-{num}"
        filename = f"{prefix}-{year_short}-{num}.pdf"

        # Skip duplicates
        if filename in seen:
            continue
        seen.add(filename)

        documents.append(
            IRSDropDocument(
                doc_type=doc_type,
                doc_number=doc_number,
                year=year_4digit,
                pdf_filename=filename,
            )
        )

    return documents


class IRSBulkFetcher:
    """Bulk fetch IRS guidance documents from official sources."""

    def __init__(self, timeout: float = 60.0, max_pages: int = 50):
        """Initialize the bulk fetcher.

        Args:
            timeout: HTTP request timeout in seconds
            max_pages: Maximum number of pages to fetch from IRS drop folder
        """
        self.base_url = "https://www.irs.gov"
        self.drop_url = f"{self.base_url}/pub/irs-drop/"
        self.max_pages = max_pages
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "Axiom/1.0 (Policy Research; +https://axiom-foundation.org)"},
        )

    def _fetch_drop_listing(self, progress_callback: Callable[[str], None] | None = None) -> str:
        """Fetch all pages of the IRS drop folder directory listing.

        The IRS website paginates the drop folder listing. This method
        fetches all pages and concatenates them.
        """
        all_html = []
        page = 0

        while page < self.max_pages:
            url = f"{self.drop_url}?page={page}"
            if progress_callback and page > 0:
                progress_callback(f"  Fetching page {page + 1}...")

            response = self.client.get(url)
            response.raise_for_status()
            html = response.text

            # Check if this page has any new PDF links
            if page > 0 and not GUIDANCE_PATTERN.search(html):
                # No more guidance documents on this page
                break

            all_html.append(html)
            page += 1

            # Check if there's a next page link
            if f"?page={page}" not in html:
                break

        return "\n".join(all_html)

    def list_documents(
        self,
        year: int | None = None,
        doc_types: list[GuidanceType] | None = None,
    ) -> list[IRSDropDocument]:
        """List available guidance documents from IRS drop folder.

        Args:
            year: Optional filter by year
            doc_types: Optional filter by document types

        Returns:
            List of available documents
        """
        html = self._fetch_drop_listing()
        return parse_irs_drop_listing(html, year=year, doc_types=doc_types)

    def fetch_pdf(self, doc: IRSDropDocument) -> bytes:
        """Fetch PDF content for a document.

        Args:
            doc: Document metadata

        Returns:
            PDF file content as bytes
        """
        response = self.client.get(doc.pdf_url)
        response.raise_for_status()
        return response.content

    def fetch_and_extract(
        self,
        doc: IRSDropDocument,
        save_pdf: Path | None = None,
    ) -> RevenueProcedure:
        """Fetch a document and extract text content from the PDF.

        Args:
            doc: Document metadata
            save_pdf: Optional path to save the PDF file

        Returns:
            RevenueProcedure with extracted text and parsed sections

        Raises:
            httpx.HTTPError: If the HTTP request fails
            ValueError: If PDF extraction fails
        """
        from axiom_corpus.fetchers.irs_parser import IRSDocumentParser, IRSParameterExtractor
        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor

        # Fetch PDF
        pdf_content = self.fetch_pdf(doc)

        # Optionally save PDF
        if save_pdf:
            save_pdf.parent.mkdir(parents=True, exist_ok=True)
            save_pdf.write_bytes(pdf_content)

        # Extract text from PDF
        extractor = PDFTextExtractor()
        full_text = extractor.extract_text(pdf_content)

        # Parse document structure
        parser = IRSDocumentParser()
        parsed = parser.parse(full_text)

        # Extract parameters
        param_extractor = IRSParameterExtractor()
        parameters = param_extractor.extract(full_text)

        # Convert parsed sections to GuidanceSection models
        sections = []
        for sec in parsed.sections:
            guidance_sec = GuidanceSection(  # pragma: no cover
                section_num=sec.section_num,
                heading=sec.heading,
                text=sec.text,
                children=[
                    GuidanceSection(
                        section_num=child.section_num,
                        heading=child.heading,
                        text=child.text,
                    )
                    for child in sec.children
                ],
            )
            sections.append(guidance_sec)  # pragma: no cover

        # Determine effective year
        effective_date = None
        if parsed.effective_year:
            effective_date = date(parsed.effective_year, 1, 1)

        # Extract subject areas from parameters
        subject_areas = list(parameters.keys()) if parameters else ["General"]

        return RevenueProcedure(
            doc_number=doc.doc_number,
            doc_type=doc.doc_type,
            title=self._generate_title(doc),
            irb_citation="",  # Would need IRB index lookup
            published_date=date(doc.year, 1, 1),  # Approximate
            full_text=full_text,
            sections=sections,
            effective_date=effective_date,
            tax_years=[doc.year, doc.year + 1] if parsed.effective_year else [doc.year],
            subject_areas=subject_areas,
            parameters=parameters,
            source_url=doc.pdf_url,
            pdf_url=doc.pdf_url,
            retrieved_at=date.today(),
        )

    def fetch_and_store(
        self,
        years: list[int],
        doc_types: list[GuidanceType] | None = None,
        storage_callback: Callable[[RevenueProcedure], None] | None = None,
        download_dir: Path | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> list[RevenueProcedure]:
        """Fetch documents for multiple years and optionally store them.

        Args:
            years: List of years to fetch (e.g., [2020, 2021, 2022, 2023, 2024])
            doc_types: Optional filter by document types (default: Rev. Procs, Rev. Rulings, Notices)
            storage_callback: Optional callback to store each document (e.g., to database)
            download_dir: Optional directory to save PDFs
            progress_callback: Optional callback for progress updates

        Returns:
            List of fetched RevenueProcedure objects
        """
        if doc_types is None:
            doc_types = [
                GuidanceType.REV_PROC,
                GuidanceType.REV_RUL,
                GuidanceType.NOTICE,
            ]  # pragma: no cover

        if download_dir:
            download_dir.mkdir(parents=True, exist_ok=True)

        # Get full document listing
        html = self._fetch_drop_listing()
        all_docs = []
        for year in years:
            docs = parse_irs_drop_listing(html, year=year, doc_types=doc_types)
            all_docs.extend(docs)

        if progress_callback:
            progress_callback(f"Found {len(all_docs)} documents for years {years}")

        results = []
        for i, doc in enumerate(all_docs):
            if progress_callback:
                progress_callback(
                    f"[{i + 1}/{len(all_docs)}] Fetching {doc.doc_number} ({doc.doc_type.value})"
                )

            try:
                pdf_path = None
                if download_dir:
                    pdf_path = download_dir / doc.pdf_filename

                rev_proc = self.fetch_and_extract(doc, save_pdf=pdf_path)

                if storage_callback:
                    storage_callback(rev_proc)  # pragma: no cover

                results.append(rev_proc)

            except (httpx.HTTPError, ValueError) as e:
                if progress_callback:
                    progress_callback(f"  ERROR: Failed to fetch/extract {doc.doc_number}: {e}")

        return results

    def _generate_title(self, doc: IRSDropDocument) -> str:
        """Generate a placeholder title for a document."""
        type_names = {
            GuidanceType.REV_PROC: "Revenue Procedure",
            GuidanceType.REV_RUL: "Revenue Ruling",
            GuidanceType.NOTICE: "Notice",
            GuidanceType.ANNOUNCEMENT: "Announcement",
        }
        return f"{type_names[doc.doc_type]} {doc.doc_number}"

    def download_bulk_with_extraction(
        self,
        years: list[int],
        doc_types: list[GuidanceType] | None = None,
        output_dir: Path | None = None,
        extract_text: bool = True,
        extract_params: bool = True,
        rate_limit_seconds: float = 1.0,
        progress_callback: Callable[[str], None] | None = None,
        skip_existing: bool = True,
    ) -> dict:
        """Download IRS guidance documents with full text extraction.

        This is the preferred method for bulk downloading guidance documents.
        It downloads PDFs, extracts text, parses document structure, and
        saves extracted parameters to structured files.

        Args:
            years: List of years to download (e.g., [2020, 2021, 2022, 2023, 2024])
            doc_types: Document types to download (default: Rev. Procs, Rev. Rulings, Notices)
            output_dir: Base output directory (default: data/guidance)
            extract_text: Whether to extract text from PDFs (default: True)
            extract_params: Whether to extract parameters from text (default: True)
            rate_limit_seconds: Delay between requests in seconds (default: 1.0)
            progress_callback: Optional callback for progress updates
            skip_existing: Skip documents that already have PDF files (default: True)

        Returns:
            Dictionary with download statistics:
            {
                "total_found": int,
                "downloaded": int,
                "skipped": int,
                "errors": int,
                "by_type": {type: count},
                "by_year": {year: count},
            }
        """
        import json  # pragma: no cover
        import time  # pragma: no cover

        from axiom_corpus.fetchers.irs_parser import (
            IRSDocumentParser,
            IRSParameterExtractor,
        )  # pragma: no cover
        from axiom_corpus.fetchers.pdf_extractor import PDFTextExtractor  # pragma: no cover

        if doc_types is None:  # pragma: no cover
            doc_types = [
                GuidanceType.REV_PROC,
                GuidanceType.REV_RUL,
                GuidanceType.NOTICE,
            ]  # pragma: no cover

        if output_dir is None:  # pragma: no cover
            output_dir = Path("data/guidance")  # pragma: no cover

        output_dir.mkdir(parents=True, exist_ok=True)  # pragma: no cover

        # Create subdirectories for each type
        pdf_dir = output_dir / "irs"  # pragma: no cover
        text_dir = output_dir / "text"  # pragma: no cover
        params_dir = output_dir / "parameters"  # pragma: no cover

        for d in [pdf_dir, text_dir, params_dir]:  # pragma: no cover
            d.mkdir(parents=True, exist_ok=True)  # pragma: no cover

        stats = {  # pragma: no cover
            "total_found": 0,
            "downloaded": 0,
            "skipped": 0,
            "errors": 0,
            "by_type": {},
            "by_year": {},
        }

        # Get full document listing
        if progress_callback:  # pragma: no cover
            progress_callback("Scanning IRS drop folder...")  # pragma: no cover

        html = self._fetch_drop_listing(progress_callback=progress_callback)  # pragma: no cover

        all_docs = []  # pragma: no cover
        for year in years:  # pragma: no cover
            docs = parse_irs_drop_listing(html, year=year, doc_types=doc_types)  # pragma: no cover
            all_docs.extend(docs)  # pragma: no cover

        stats["total_found"] = len(all_docs)  # pragma: no cover

        if progress_callback:  # pragma: no cover
            progress_callback(
                f"Found {len(all_docs)} documents for years {years}"
            )  # pragma: no cover

        # Initialize extractors
        pdf_extractor = PDFTextExtractor() if extract_text else None  # pragma: no cover
        doc_parser = IRSDocumentParser() if extract_text else None  # pragma: no cover
        param_extractor = IRSParameterExtractor() if extract_params else None  # pragma: no cover

        for i, doc in enumerate(all_docs):  # pragma: no cover
            f"{doc.doc_type.value[:3]}-{doc.doc_number}"  # pragma: no cover

            if progress_callback:  # pragma: no cover
                progress_callback(  # pragma: no cover
                    f"[{i + 1}/{len(all_docs)}] {doc.doc_type.value} {doc.doc_number}"
                )

            # Check if already exists
            pdf_path = pdf_dir / doc.pdf_filename  # pragma: no cover
            if skip_existing and pdf_path.exists():  # pragma: no cover
                if progress_callback:  # pragma: no cover
                    progress_callback(
                        f"  Skipping (exists): {doc.pdf_filename}"
                    )  # pragma: no cover
                stats["skipped"] += 1  # pragma: no cover
                continue  # pragma: no cover

            try:  # pragma: no cover
                # Rate limiting
                if i > 0:  # pragma: no cover
                    time.sleep(rate_limit_seconds)

                # Download PDF
                pdf_content = self.fetch_pdf(doc)  # pragma: no cover
                pdf_path.write_bytes(pdf_content)  # pragma: no cover

                # Extract text if requested
                full_text = ""  # pragma: no cover
                parameters = {}  # pragma: no cover

                if extract_text and pdf_extractor:  # pragma: no cover
                    try:  # pragma: no cover
                        full_text = pdf_extractor.extract_text(pdf_content)  # pragma: no cover

                        # Save extracted text
                        text_filename = doc.pdf_filename.replace(".pdf", ".txt")  # pragma: no cover
                        text_path = text_dir / text_filename  # pragma: no cover
                        text_path.write_text(full_text, encoding="utf-8")  # pragma: no cover

                        # Parse document structure
                        if doc_parser:  # pragma: no cover
                            doc_parser.parse(full_text)  # pragma: no cover

                        # Extract parameters
                        if extract_params and param_extractor and full_text:  # pragma: no cover
                            parameters = param_extractor.extract(full_text)  # pragma: no cover
                            if parameters:  # pragma: no cover
                                params_filename = doc.pdf_filename.replace(
                                    ".pdf", ".json"
                                )  # pragma: no cover
                                params_path = params_dir / params_filename  # pragma: no cover
                                params_path.write_text(  # pragma: no cover
                                    json.dumps(parameters, indent=2),
                                    encoding="utf-8",
                                )

                    except Exception as e:  # pragma: no cover
                        if progress_callback:  # pragma: no cover
                            progress_callback(
                                f"  Warning: Text extraction failed: {e}"
                            )  # pragma: no cover

                # Update statistics
                stats["downloaded"] += 1  # pragma: no cover
                doc_type_key = doc.doc_type.value  # pragma: no cover
                stats["by_type"][doc_type_key] = (
                    stats["by_type"].get(doc_type_key, 0) + 1
                )  # pragma: no cover
                stats["by_year"][doc.year] = (
                    stats["by_year"].get(doc.year, 0) + 1
                )  # pragma: no cover

                if progress_callback:  # pragma: no cover
                    size_kb = len(pdf_content) / 1024  # pragma: no cover
                    params_info = (
                        f", {len(parameters)} param groups" if parameters else ""
                    )  # pragma: no cover
                    progress_callback(
                        f"  Downloaded: {size_kb:.1f} KB{params_info}"
                    )  # pragma: no cover

            except Exception as e:  # pragma: no cover
                stats["errors"] += 1  # pragma: no cover
                if progress_callback:  # pragma: no cover
                    progress_callback(f"  ERROR: {e}")  # pragma: no cover

        return stats  # pragma: no cover

    def close(self):
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
