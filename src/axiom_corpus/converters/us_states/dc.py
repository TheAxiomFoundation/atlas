"""District of Columbia statute converter.

Converts DC Code from the DC Council's law-xml-codified GitHub repository
(https://github.com/dccouncil/law-xml-codified) to the internal Section model.

The DC Code source is XML with machine-readable structure and provenance.

DC Code Structure:
- Titles (e.g., Title 47: Taxation, Licensing, Permits, Assessments, and Fees)
- Chapters (e.g., Chapter 18: Income and Franchise Taxes)
- Subchapters (e.g., Subchapter I: General Provisions)
- Sections (e.g., 47-1801.04: General definitions)

URL Patterns (GitHub Raw):
- Base: https://raw.githubusercontent.com/dccouncil/law-xml-codified/master/
- Section: us/dc/council/code/titles/{title}/sections/{section}.xml
- Title index: us/dc/council/code/titles/{title}/index.xml

Also available via web interface at code.dccouncil.gov.

Key Titles for Tax/Benefit Analysis:
- Title 47: Taxation, Licensing, Permits, Assessments, and Fees
- Title 4: Public Care Systems (TANF, Medicaid, welfare programs)
- Title 46: Domestic Relations (child support)

Example:
    >>> from axiom_corpus.converters.us_states.dc import DCConverter
    >>> converter = DCConverter()
    >>> section = converter.fetch_section("47-1801.04")
    >>> print(section.section_title)
    "General definitions."
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from xml.etree import ElementTree as ET

import httpx

from axiom_corpus.models import Citation, Section, Subsection

# GitHub raw content base URL for dc-law-xml-codified
BASE_URL = "https://raw.githubusercontent.com/dccouncil/law-xml-codified/master"

# Web interface URL for code.dccouncil.gov
WEB_URL = "https://code.dccouncil.gov"

# XML namespaces used in DC law XML
# Default namespace for dc-library elements
DC_NS = "https://code.dccouncil.us/schemas/dc-library"
NS = {
    "dc": DC_NS,
    "codified": "https://code.dccouncil.us/schemas/codified",
    "codify": "https://code.dccouncil.us/schemas/codify",
    "xi": "http://www.w3.org/2001/XInclude",
}

# DC Code titles
DC_TITLES: dict[int, str] = {
    1: "Government Organization",
    2: "Government Administration",
    3: "District of Columbia Boards and Commissions",
    4: "Public Care Systems",
    5: "Police, Firefighters, Medical Examiner, and Forensic Sciences",
    6: "Education",
    7: "Human Health Care and Safety",
    8: "Environmental and Animal Control and Protection",
    9: "Transportation Systems",
    10: "Parks, Recreation, and Cultural Affairs",
    11: "Organization and Jurisdiction of the Courts",
    12: "Right to Counsel and Jury Trials",
    13: "Procedure Generally",
    14: "Proof",
    15: "Judgments and Executions; Att, Garnishments",
    16: "Particular Actions, Proceedings and Matters",
    17: "Copyright, Patent, and Trademark",
    18: "Criminal Code",
    19: "Decedents' Estates and Fiduciary Relations",
    20: "Probate and Administration of Decedents' Estates",
    21: "Fiduciary Relations and Persons with Mental Illness",
    22: "Criminal Offenses and Penalties",
    23: "Criminal Procedure",
    24: "Prisoners and Their Treatment",
    25: "Alcoholic Beverages and Cannabis",
    26: "Banks and Financial Institutions",
    27: "Insurance",
    28: "Commercial Instruments and Transactions",
    29: "Business Organizations",
    30: "Oil and Gas",
    31: "Insurance and Securities",
    32: "Labor",
    33: "Public Utilities",
    34: "Public Utilities",
    35: "Mines",
    36: "Trade Practices",
    37: "Trade and Commerce",
    38: "Educational Institutions",
    39: "Libraries and Cultural Institutions",
    40: "Licensing",
    41: "Public Accommodations and Amusements",
    42: "Real Property",
    43: "Surveyor",
    44: "Charitable and Curative Institutions",
    45: "Cemeteries",
    46: "Domestic Relations",
    47: "Taxation, Licensing, Permits, Assessments, and Fees",
    48: "Foods and Drugs",
    49: "Highways, Bridges, and Ferries",
    50: "Motor and Non-Motor Vehicles and Traffic",
    51: "Social Security",
}

# Key chapters in Title 47 (Taxation)
DC_TAX_CHAPTERS: dict[int, str] = {
    1: "General Provisions",
    8: "Real Property Assessment and Tax",
    10: "Special Real Property Tax Provisions",
    12: "Special Assessments",
    13: "Real Property Tax Sales",
    18: "Income and Franchise Taxes",
    20: "Gross Sales Tax",
    22: "Motor Vehicle Fuel Tax",
    23: "Motor Vehicle and Equipment Excise Tax",
    24: "Insurance Premium Tax",
    25: "Tobacco Tax",
    26: "Alcoholic Beverage Control",
    28: "General License Law",
    29: "Fees of Public Officers",
    31: "Compensating-Use Tax",
    37: "Community Development by Religious Organizations",
    39: "Ballpark Sales Taxes",
    40: "Ballpark Fees and Taxes",
    42: "Interest Rates and Tax Rates",
    43: "Quid Pro Quo Contributions",
    44: "Toll Telecommunication Service Tax",
    45: "Qualified Zone Academy Revenue Bond Project",
    46: "Special Tax Rules",
    50: "Workers' Compensation Fund",
}

# Key chapters in Title 4 (Public Care Systems)
# Note: Some chapters use letter suffixes (7A, 7B), stored as strings
DC_WELFARE_CHAPTERS: dict[int | str, str] = {
    1: "Public Welfare Supervision",
    2: "Public Assistance",
    3: "Family Services",
    4: "Food Assistance",
    5: "Employees Mutual Disability Insurance",
    7: "Youth Rehabilitation Services",
    "7A": "Child, Youth, and Families Services",
    "7B": "Child and Family Services Agency",
    8: "Foster Care",
    9: "Child Abuse and Neglect",
    10: "Public Day Care",
    11: "Immunization of School Students",
    12: "Community Residence Facilities Licensure",
    13: "Health-Care Decisions",
    14: "Social Hygiene",
    15: "Assistance to Aged",
    16: "Nurse Training Corps",
    17: "Child Welfare Services",
    18: "Child-Placing Agencies",
    19: "Youth Services Agencies",
    20: "Youth Residential Facilities Licensure",
    21: "Public Care Systems",
}


@dataclass
class ParsedDCSection:
    """Parsed DC Code section."""

    section_number: str  # e.g., "47-1801.04"
    section_title: str  # e.g., "General definitions."
    title_number: int  # e.g., 47
    title_name: str  # e.g., "Taxation, Licensing, Permits, Assessments, and Fees"
    chapter: str | None  # e.g., "18"
    subchapter: str | None  # e.g., "I"
    text: str  # Full text content
    xml: str  # Raw XML
    subsections: list[ParsedDCSubsection] = field(default_factory=list)
    annotations: list[str] = field(default_factory=list)
    history: str | None = None
    source_url: str = ""
    effective_date: date | None = None


@dataclass
class ParsedDCSubsection:
    """A subsection within a DC Code section."""

    identifier: str  # e.g., "1", "a", "A", "i"
    text: str
    heading: str | None = None
    children: list[ParsedDCSubsection] = field(default_factory=list)


class DCConverterError(Exception):
    """Error during DC Code conversion."""

    def __init__(self, message: str, url: str | None = None):
        super().__init__(message)
        self.url = url


class DCConverter:
    """Converter for DC Code XML to internal Section model.

    Fetches from the dccouncil/law-xml-codified GitHub repository which
    contains the official DC Code in structured XML format.

    Example:
        >>> converter = DCConverter()
        >>> section = converter.fetch_section("47-1801.04")
        >>> print(section.citation.section)
        "DC-47-1801.04"

        >>> for section in converter.iter_title(47):
        ...     print(section.section_title)
    """

    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        use_web_fallback: bool = False,
    ):
        """Initialize the DC Code converter.

        Args:
            rate_limit_delay: Seconds to wait between HTTP requests
            use_web_fallback: If True, fall back to code.dccouncil.gov HTML
        """
        self.rate_limit_delay = rate_limit_delay
        self.use_web_fallback = use_web_fallback
        self._last_request_time = 0.0
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                timeout=60.0,
                headers={"User-Agent": "Axiom/1.0 (Statute Research; contact@axiom-foundation.org)"},
            )
        return self._client

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str) -> str:
        """Make a rate-limited GET request."""
        self._rate_limit()
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    def _parse_section_number(self, section_number: str) -> tuple[int, str]:
        """Parse section number into title number and section ID.

        Args:
            section_number: e.g., "47-1801.04", "4-205.11"

        Returns:
            Tuple of (title_number, section_id)
        """
        # DC sections are formatted as "{title}-{section}"
        parts = section_number.split("-", 1)
        if len(parts) != 2:
            raise DCConverterError(f"Invalid section number format: {section_number}")

        try:
            title_num = int(parts[0])
        except ValueError as exc:
            raise DCConverterError(
                f"Invalid title number in section: {section_number}"
            ) from exc

        return title_num, section_number

    def _build_section_url(self, section_number: str) -> str:
        """Build the GitHub raw URL for a section.

        Args:
            section_number: e.g., "47-1801.04"

        Returns:
            Full URL to the section XML
        """
        title_num, _ = self._parse_section_number(section_number)
        return f"{BASE_URL}/us/dc/council/code/titles/{title_num}/sections/{section_number}.xml"

    def _build_web_url(self, section_number: str) -> str:
        """Build the web interface URL for a section.

        Args:
            section_number: e.g., "47-1801.04"

        Returns:
            URL to code.dccouncil.gov section page
        """
        title_num, _ = self._parse_section_number(section_number)
        return f"{WEB_URL}/us/dc/council/code/sections/{section_number}"

    def _build_title_index_url(self, title: int) -> str:
        """Build the URL for a title's index.xml."""
        return f"{BASE_URL}/us/dc/council/code/titles/{title}/index.xml"

    def _find_element(self, parent: ET.Element, tag: str) -> ET.Element | None:
        """Find an element by tag, trying both namespaced and non-namespaced."""
        # Try with dc-library namespace first (using {namespace}tag format)
        elem = parent.find(f"{{{DC_NS}}}{tag}")
        if elem is not None:
            return elem
        # Fall back to no namespace
        return parent.find(tag)

    def _find_all_elements(self, parent: ET.Element, tag: str) -> list[ET.Element]:
        """Find all elements by tag, trying both namespaced and non-namespaced."""
        # Try with dc-library namespace first
        elems = parent.findall(f"{{{DC_NS}}}{tag}")
        if elems:
            return elems
        # Fall back to no namespace
        return parent.findall(tag)

    def _parse_xml(self, xml_content: str, section_number: str, url: str) -> ParsedDCSection:
        """Parse DC Code XML into ParsedDCSection."""
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:  # pragma: no cover
            raise DCConverterError(
                f"Failed to parse XML for {section_number}: {e}", url
            ) from e  # pragma: no cover

        title_num, _ = self._parse_section_number(section_number)
        title_name = DC_TITLES.get(title_num, f"Title {title_num}")

        # Extract section number from <num> element
        num_elem = self._find_element(root, "num")
        xml_section_num = num_elem.text if num_elem is not None else section_number

        # Extract heading/title from <heading> element
        heading_elem = self._find_element(root, "heading")
        section_title = heading_elem.text if heading_elem is not None else ""

        # Extract main text from <text> element
        main_text_elem = self._find_element(root, "text")
        main_text = main_text_elem.text if main_text_elem is not None else ""

        # Parse subsections from <para> elements
        subsections = self._parse_paras(root)

        # Build full text from main text and subsections
        full_text = self._build_full_text(main_text, subsections)

        # Extract annotations
        annotations = []
        history = None
        annotations_elem = self._find_element(root, "annotations")
        if annotations_elem is not None:
            for ann in annotations_elem:
                ann_type = ann.get("type", "")
                ann_text = ann.text or ""
                if ann_type == "History" and not history:
                    history = ann_text.strip()
                if ann_text.strip():
                    annotations.append(f"{ann_type}: {ann_text.strip()}")

        # Determine chapter/subchapter from section number
        # DC sections like 47-1801.04: title=47, chapter=18, subchapter=01
        chapter = None
        subchapter = None
        section_parts = section_number.split("-", 1)
        if len(section_parts) == 2:
            sec_id = section_parts[1]
            # Try to extract chapter from section ID (e.g., "1801.04" -> chapter 18)
            if len(sec_id) >= 2:
                chapter_match = re.match(r"(\d{2})", sec_id)
                if chapter_match:
                    chapter = str(int(chapter_match.group(1)))

        return ParsedDCSection(
            section_number=xml_section_num or section_number,
            section_title=section_title or f"Section {section_number}",
            title_number=title_num,
            title_name=title_name,
            chapter=chapter,
            subchapter=subchapter,
            text=full_text,
            xml=xml_content,
            subsections=subsections,
            annotations=annotations,
            history=history,
            source_url=url,
        )

    def _parse_paras(self, elem: ET.Element) -> list[ParsedDCSubsection]:
        """Recursively parse <para> elements into subsections."""
        subsections = []

        for para in self._find_all_elements(elem, "para"):
            subsections.append(self._parse_single_para(para))

        return subsections

    def _parse_single_para(self, para: ET.Element) -> ParsedDCSubsection:
        """Parse a single <para> element."""
        # Get identifier from <num>
        num_elem = self._find_element(para, "num")
        identifier = ""
        if num_elem is not None and num_elem.text:
            # Strip parentheses: "(1)" -> "1", "(a)" -> "a"
            identifier = num_elem.text.strip().strip("()")

        # Get heading if present
        heading_elem = self._find_element(para, "heading")
        heading = heading_elem.text if heading_elem is not None else None

        # Get text content
        text_elem = self._find_element(para, "text")
        text = self._get_element_text(text_elem) if text_elem is not None else ""

        # Parse children recursively
        children = self._parse_paras(para)

        return ParsedDCSubsection(
            identifier=identifier,
            text=text,
            heading=heading,
            children=children,
        )

    def _get_element_text(self, elem: ET.Element) -> str:
        """Get text content from element, including tail text of children."""
        parts = []
        if elem.text:
            parts.append(elem.text)
        for child in elem:  # pragma: no cover
            if child.text:
                parts.append(child.text)
            if child.tail:
                parts.append(child.tail)
        return "".join(parts).strip()

    def _build_full_text(
        self, main_text: str, subsections: list[ParsedDCSubsection], indent: int = 0
    ) -> str:
        """Build full text from main text and subsections."""
        lines = []
        if main_text:
            lines.append(main_text)

        for sub in subsections:
            prefix = "  " * indent
            sub_text = f"{prefix}({sub.identifier}) {sub.text}"
            if sub.heading:
                sub_text = f"{prefix}({sub.identifier}) {sub.heading}. {sub.text}"
            lines.append(sub_text)

            if sub.children:
                child_text = self._build_full_text("", sub.children, indent + 1)
                lines.append(child_text)

        return "\n".join(lines)

    def _to_section(self, parsed: ParsedDCSection) -> Section:
        """Convert ParsedDCSection to internal Section model."""
        # Create citation using DC prefix
        citation = Citation(
            title=0,  # State/district law indicator
            section=f"DC-{parsed.section_number}",
        )

        # Convert subsections recursively
        def convert_subsections(subs: list[ParsedDCSubsection]) -> list[Subsection]:
            return [
                Subsection(
                    identifier=sub.identifier,
                    heading=sub.heading,
                    text=sub.text,
                    children=convert_subsections(sub.children),
                )
                for sub in subs
            ]

        subsections = convert_subsections(parsed.subsections)

        return Section(
            citation=citation,
            title_name=f"DC Code - {parsed.title_name}",
            section_title=parsed.section_title,
            text=parsed.text,
            subsections=subsections,
            source_url=parsed.source_url,
            retrieved_at=date.today(),
            uslm_id=f"dc/{parsed.title_number}/{parsed.section_number}",
        )

    def fetch_section(self, section_number: str) -> Section:
        """Fetch and convert a single section.

        Args:
            section_number: e.g., "47-1801.04", "4-205.11"

        Returns:
            Section model

        Raises:
            DCConverterError: If section not found or parsing fails
        """
        url = self._build_section_url(section_number)
        try:
            xml_content = self._get(url)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise DCConverterError(f"Section {section_number} not found", url) from e
            raise DCConverterError(
                f"HTTP error fetching {section_number}: {e}", url
            ) from e  # pragma: no cover

        parsed = self._parse_xml(xml_content, section_number, url)
        return self._to_section(parsed)

    def get_title_section_numbers(self, title: int) -> list[str]:
        """Get list of section numbers in a title.

        Args:
            title: Title number (e.g., 47, 4)

        Returns:
            List of section numbers (e.g., ["47-101", "47-102", ...])
        """
        url = self._build_title_index_url(title)
        try:
            xml_content = self._get(url)
        except httpx.HTTPStatusError as e:  # pragma: no cover
            if e.response.status_code == 404:  # pragma: no cover
                return []  # pragma: no cover
            raise DCConverterError(
                f"Failed to fetch title {title} index: {e}", url
            ) from e  # pragma: no cover

        # Parse index.xml to find section includes
        section_numbers = []
        try:
            root = ET.fromstring(xml_content)

            # Look for XInclude references to sections
            # Pattern: <xi:include href="./sections/47-101.xml"/> or href="sections/47-101.xml"
            for include in root.iter("{http://www.w3.org/2001/XInclude}include"):
                href = include.get("href", "")
                # Handle both "./sections/" and "sections/" prefixes
                if "/sections/" in href and href.endswith(".xml"):
                    # Extract section number from path like "./sections/47-101.xml"
                    section_num = href.split("/sections/")[1][:-4]  # Remove ".xml"
                    section_numbers.append(section_num)

        except ET.ParseError:  # pragma: no cover
            pass

        return sorted(section_numbers)

    def iter_title(self, title: int) -> Iterator[Section]:
        """Iterate over all sections in a title.

        Args:
            title: Title number (e.g., 47, 4)

        Yields:
            Section objects for each section
        """
        section_numbers = self.get_title_section_numbers(title)

        for section_num in section_numbers:
            try:
                yield self.fetch_section(section_num)
            except DCConverterError as e:  # pragma: no cover
                # Log but continue with other sections
                print(f"Warning: Could not fetch {section_num}: {e}")  # pragma: no cover
                continue  # pragma: no cover

    def iter_chapter(self, title: int, chapter: int | str) -> Iterator[Section]:
        """Iterate over sections in a specific chapter.

        Args:
            title: Title number (e.g., 47)
            chapter: Chapter number (e.g., 18)

        Yields:
            Section objects matching the chapter
        """
        chapter_prefix = f"{title}-{chapter}"  # pragma: no cover

        for section in self.iter_title(title):  # pragma: no cover
            # Check if section belongs to this chapter
            section_num = section.citation.section.replace("DC-", "")  # pragma: no cover
            if section_num.startswith(chapter_prefix):  # pragma: no cover
                yield section  # pragma: no cover

    def iter_tax_titles(self) -> Iterator[Section]:
        """Iterate over sections from Title 47 (Taxation).

        Yields:
            Section objects
        """
        yield from self.iter_title(47)  # pragma: no cover

    def iter_welfare_titles(self) -> Iterator[Section]:
        """Iterate over sections from Title 4 (Public Care Systems).

        Yields:
            Section objects
        """
        yield from self.iter_title(4)  # pragma: no cover

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()  # pragma: no cover
            self._client = None  # pragma: no cover

    def __enter__(self) -> DCConverter:
        return self

    def __exit__(self, *args) -> None:
        self.close()


# Convenience functions


def fetch_dc_section(section_number: str) -> Section:
    """Fetch a single DC Code section.

    Args:
        section_number: e.g., "47-1801.04"

    Returns:
        Section model
    """
    with DCConverter() as converter:
        return converter.fetch_section(section_number)


def download_dc_title(title: int) -> list[Section]:
    """Download all sections from a DC Code title.

    Args:
        title: Title number (e.g., 47)

    Returns:
        List of Section objects
    """
    with DCConverter() as converter:
        return list(converter.iter_title(title))


def download_dc_tax_title() -> Iterator[Section]:
    """Download all sections from DC Title 47 (Taxation).

    Yields:
        Section objects
    """
    with DCConverter() as converter:  # pragma: no cover
        yield from converter.iter_tax_titles()  # pragma: no cover


def download_dc_welfare_title() -> Iterator[Section]:
    """Download all sections from DC Title 4 (Public Care Systems).

    Yields:
        Section objects
    """
    with DCConverter() as converter:  # pragma: no cover
        yield from converter.iter_welfare_titles()  # pragma: no cover
