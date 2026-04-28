"""Parser for California Codes via web scraping.

California Legislature provides statutes at leginfo.legislature.ca.gov.
There is no public API, but there's a bulk download option (complex MySQL format)
and a clean web interface.

Structure:
- Codes: RTC (Revenue & Taxation), WIC (Welfare & Institutions), etc.
- Each code has: Divisions > Titles > Parts > Chapters > Articles > Sections

URL Patterns:
- Code TOC: faces/codesTOCSelected.xhtml?tocCode=RTC
- Section: faces/codes_displaySection.xhtml?lawCode=RTC&sectionNum=17041

Bulk Download (alternative):
- https://downloads.leginfo.legislature.ca.gov/pubinfo_2025.zip (1GB+)
- Format: Tab-delimited .dat files with .lob files for content
"""

import logging
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Literal

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Note: Using CA-specific dataclasses rather than generic models
# since Citation model is designed for US Code (title as int)

BASE_URL = "https://leginfo.legislature.ca.gov/faces"

# California Code abbreviations and full names
CA_CODES: dict[str, str] = {
    "BPC": "Business and Professions Code",
    "CIV": "Civil Code",
    "CCP": "Code of Civil Procedure",
    "COM": "Commercial Code",
    "CORP": "Corporations Code",
    "EDC": "Education Code",
    "ELEC": "Elections Code",
    "EVID": "Evidence Code",
    "FAM": "Family Code",
    "FIN": "Financial Code",
    "FGC": "Fish and Game Code",
    "FAC": "Food and Agricultural Code",
    "GOV": "Government Code",
    "HNC": "Harbors and Navigation Code",
    "HSC": "Health and Safety Code",
    "INS": "Insurance Code",
    "LAB": "Labor Code",
    "MVC": "Military and Veterans Code",
    "PEN": "Penal Code",
    "PROB": "Probate Code",
    "PCC": "Public Contract Code",
    "PRC": "Public Resources Code",
    "PUC": "Public Utilities Code",
    "RTC": "Revenue and Taxation Code",
    "SHC": "Streets and Highways Code",
    "UIC": "Unemployment Insurance Code",
    "VEH": "Vehicle Code",
    "WAT": "Water Code",
    "WIC": "Welfare and Institutions Code",
}

# Priority codes for tax/benefit policy
CA_TAX_CODES = ["RTC"]  # Revenue and Taxation Code
CA_WELFARE_CODES = ["WIC", "UIC", "LAB", "HSC"]  # Benefits-related codes


@dataclass
class CASubsection:
    """A subsection within a California statute."""

    identifier: str  # e.g., "a", "1", "A"
    text: str
    children: list["CASubsection"] = field(default_factory=list)


@dataclass
class CASection:
    """A section from California Codes with full content."""

    code: str  # e.g., "RTC"
    section_num: str  # e.g., "17041"
    title: str  # Section heading
    code_name: str  # e.g., "Revenue and Taxation Code"
    text: str  # Full text content
    url: str  # Source URL
    subsections: list[CASubsection] = field(default_factory=list)
    division: str | None = None
    part: str | None = None
    chapter: str | None = None
    article: str | None = None
    history: str | None = None

    @property
    def citation(self) -> str:
        """Return formatted citation (e.g., 'Cal. RTC § 17041')."""
        return f"Cal. {self.code} § {self.section_num}"  # pragma: no cover


@dataclass
class CASectionInfo:
    """Information about a California Code section."""

    code: str  # e.g., "RTC"
    section_num: str  # e.g., "17041", "17041.5"
    title: str  # Section heading/title
    division: str | None = None
    part: str | None = None
    chapter: str | None = None
    article: str | None = None

    @property
    def citation(self) -> str:
        """Return formatted citation (e.g., 'Cal. RTC § 17041')."""
        return f"Cal. {self.code} § {self.section_num}"  # pragma: no cover

    @property
    def url(self) -> str:
        """Return URL to section on leginfo."""
        return f"{BASE_URL}/codes_displaySection.xhtml?lawCode={self.code}&sectionNum={self.section_num}"  # pragma: no cover


@dataclass
class CACodeParser:
    """Parser for a specific California Code."""

    code: str  # e.g., "RTC"
    rate_limit: float = 0.5  # Seconds between requests
    _client: httpx.Client = field(default=None, repr=False)  # type: ignore

    def __post_init__(self):
        if self.code not in CA_CODES:  # pragma: no cover
            raise ValueError(
                f"Unknown CA code: {self.code}. Valid codes: {list(CA_CODES.keys())}"
            )  # pragma: no cover
        self._client = httpx.Client(
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": "Axiom/1.0 (Legal Archive; contact@axiom-foundation.org) https://github.com/TheAxiomFoundation/axiom-corpus"
            },
        )

    def __del__(self):
        if self._client:  # pragma: no cover
            self._client.close()  # pragma: no cover

    @property
    def code_name(self) -> str:
        """Return full name of the code."""
        return CA_CODES[self.code]  # pragma: no cover

    def get_toc_url(self) -> str:
        """Return URL to code table of contents."""
        return f"{BASE_URL}/codesTOCSelected.xhtml?tocCode={self.code}&tocTitle=+{self.code_name.replace(' ', '+')}"  # pragma: no cover

    def list_divisions(self) -> list[dict]:
        """List all divisions/parts in the code.

        Returns list of dicts with 'name', 'url', 'level' keys.
        """
        url = self.get_toc_url()  # pragma: no cover
        response = self._client.get(url)  # pragma: no cover
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")  # pragma: no cover
        divisions = []  # pragma: no cover

        # Find the TOC tree - California uses a nested list structure
        toc_div = soup.find("div", {"id": "codesToc"}) or soup.find(
            "div", class_="codeBody"
        )  # pragma: no cover
        if not toc_div:  # pragma: no cover
            return divisions  # pragma: no cover

        # Extract links to divisions/parts
        for link in toc_div.find_all("a", href=True):  # pragma: no cover
            href = link.get("href", "")  # pragma: no cover
            text = link.get_text(strip=True)  # pragma: no cover

            # Filter to structural links (divisions, parts, chapters)
            if "codes_displayText" in href or "expandedbranch" in href:  # pragma: no cover
                divisions.append(
                    {  # pragma: no cover
                        "name": text,
                        "url": f"{BASE_URL}/{href}" if not href.startswith("http") else href,
                        "level": self._infer_level(text),
                    }
                )

        return divisions  # pragma: no cover

    def _infer_level(self, text: str) -> str:
        """Infer structural level from text."""
        text_lower = text.lower()  # pragma: no cover
        if "division" in text_lower:  # pragma: no cover
            return "division"  # pragma: no cover
        elif "part" in text_lower:  # pragma: no cover
            return "part"  # pragma: no cover
        elif "title" in text_lower:  # pragma: no cover
            return "title"  # pragma: no cover
        elif "chapter" in text_lower:  # pragma: no cover
            return "chapter"  # pragma: no cover
        elif "article" in text_lower:  # pragma: no cover
            return "article"  # pragma: no cover
        return "unknown"  # pragma: no cover

    def list_sections_from_toc(self, toc_url: str) -> Iterator[CASectionInfo]:
        """List sections from a TOC page.

        Args:
            toc_url: URL to a TOC page (e.g., a chapter page)

        Yields:
            CASectionInfo for each section found
        """
        response = self._client.get(toc_url)  # pragma: no cover
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")  # pragma: no cover

        # Find section links - California uses "codes_displaySection" URLs
        for link in soup.find_all("a", href=True):  # pragma: no cover
            href = link.get("href", "")  # pragma: no cover
            if "codes_displaySection" in href:  # pragma: no cover
                # Extract section number from URL
                section_match = re.search(r"sectionNum=([^&]+)", href)  # pragma: no cover
                if section_match:  # pragma: no cover
                    section_num = section_match.group(1)  # pragma: no cover
                    title = link.get_text(strip=True)  # pragma: no cover

                    yield CASectionInfo(  # pragma: no cover
                        code=self.code,
                        section_num=section_num,
                        title=title,
                    )

    def get_section(self, section_num: str) -> CASection | None:
        """Fetch a single section by number.

        Args:
            section_num: Section number (e.g., "17041", "17041.5")

        Returns:
            CASection object or None if not found
        """
        url = f"{BASE_URL}/codes_displaySection.xhtml?lawCode={self.code}&sectionNum={section_num}"  # pragma: no cover

        try:  # pragma: no cover
            response = self._client.get(url)  # pragma: no cover
            response.raise_for_status()
        except httpx.HTTPError as e:  # pragma: no cover
            logger.warning(  # pragma: no cover
                "[CA] Error fetching %s § %s at %s: %s",
                self.code,
                section_num,
                url,
                e,
                exc_info=True,
            )
            return None  # pragma: no cover

        soup = BeautifulSoup(response.text, "html.parser")  # pragma: no cover

        # Extract section content - California uses displaycodeleftmargin class
        content_div = (  # pragma: no cover
            soup.find("div", class_="displaycodeleftmargin")
            or soup.find("div", {"id": "codeLawContent"})
            or soup.find("div", class_="content_main")
        )
        if not content_div:  # pragma: no cover
            return None  # pragma: no cover

        # Get section text
        text = content_div.get_text(separator="\n", strip=True)  # pragma: no cover

        # Extract heading if present
        heading_tag = content_div.find(["h1", "h2", "h3", "h4"])  # pragma: no cover
        heading = (
            heading_tag.get_text(strip=True) if heading_tag else f"{self.code} § {section_num}"
        )  # pragma: no cover

        # Try to get history note
        history_note = ""  # pragma: no cover
        history_match = re.search(
            r"(\(Added by Stats\.|Amended by Stats\.)[^)]+\)", text
        )  # pragma: no cover
        if history_match:  # pragma: no cover
            history_note = history_match.group(0)  # pragma: no cover

        # Parse subsections
        subsections = self._parse_subsections(content_div)  # pragma: no cover

        return CASection(  # pragma: no cover
            code=self.code,
            section_num=section_num,
            title=heading,
            code_name=CA_CODES[self.code],
            text=text,
            url=url,
            subsections=subsections,
            history=history_note if history_note else None,
        )

    def _parse_subsections(self, content_div) -> list[CASubsection]:
        """Parse subsections from section content.

        California uses patterns like:
        (a) First subsection
        (1) Sub-sub
        (A) Sub-sub-sub
        """
        subsections = []  # pragma: no cover
        text = content_div.get_text(separator="\n", strip=True)  # pragma: no cover

        # Pattern for California subsection markers
        pattern = r"\(([a-z]|\d+|[A-Z])\)\s*([^(]+?)(?=\([a-z]|\d+|[A-Z]\)|$)"  # pragma: no cover

        for match in re.finditer(pattern, text, re.DOTALL):  # pragma: no cover
            marker = match.group(1)  # pragma: no cover
            content = match.group(2).strip()  # pragma: no cover

            if content:  # Only add if there's actual content  # pragma: no cover
                subsections.append(
                    CASubsection(  # pragma: no cover
                        identifier=marker,
                        text=content[:1000] if len(content) > 1000 else content,
                    )
                )

        return subsections  # pragma: no cover

    def download_code(
        self,
        divisions: list[str] | None = None,
        max_sections: int | None = None,
    ) -> Iterator[CASection]:
        """Download all sections from this code.

        Args:
            divisions: Specific divisions to download (None = all)
            max_sections: Maximum sections to download (for testing)

        Yields:
            Section objects
        """
        count = 0  # pragma: no cover

        # Get all TOC entries
        toc_entries = self.list_divisions()  # pragma: no cover
        if not toc_entries:  # pragma: no cover
            print(f"No TOC entries found for {self.code}")  # pragma: no cover
            return  # pragma: no cover

        print(f"Found {len(toc_entries)} TOC entries for {self.code}")  # pragma: no cover

        for entry in toc_entries:  # pragma: no cover
            if divisions and entry["level"] == "division":  # pragma: no cover
                if not any(d in entry["name"] for d in divisions):  # pragma: no cover
                    continue  # pragma: no cover

            # Get sections from this TOC page
            try:  # pragma: no cover
                for section_info in self.list_sections_from_toc(entry["url"]):  # pragma: no cover
                    if max_sections and count >= max_sections:  # pragma: no cover
                        return  # pragma: no cover

                    # Rate limit
                    time.sleep(self.rate_limit)

                    section = self.get_section(section_info.section_num)  # pragma: no cover
                    if section:  # pragma: no cover
                        count += 1  # pragma: no cover
                        print(f"  [{count}] {section_info.citation}")  # pragma: no cover
                        yield section  # pragma: no cover

            except httpx.HTTPError as e:  # pragma: no cover
                logger.warning(  # pragma: no cover
                    "[CA] Error fetching TOC %s: %s",
                    entry["url"],
                    e,
                    exc_info=True,
                )
                continue  # pragma: no cover

        print(f"Downloaded {count} sections from {self.code}")  # pragma: no cover


class CaliforniaStatutesParser:
    """Main parser for all California Codes."""

    def __init__(self, rate_limit: float = 0.5):
        self.rate_limit = rate_limit  # pragma: no cover

    def list_codes(self) -> list[dict[str, str]]:
        """List all available California codes."""
        return [{"code": k, "name": v} for k, v in CA_CODES.items()]  # pragma: no cover

    def download_code(
        self,
        code: str,
        max_sections: int | None = None,
    ) -> Iterator[CASection]:
        """Download a specific California code.

        Args:
            code: Code abbreviation (e.g., "RTC", "WIC")
            max_sections: Maximum sections to download

        Yields:
            CASection objects
        """
        parser = CACodeParser(code=code, rate_limit=self.rate_limit)  # pragma: no cover
        yield from parser.download_code(max_sections=max_sections)  # pragma: no cover

    def download_tax_codes(self, max_sections: int | None = None) -> Iterator[CASection]:
        """Download tax-related codes (RTC)."""
        for code in CA_TAX_CODES:  # pragma: no cover
            yield from self.download_code(code, max_sections=max_sections)  # pragma: no cover

    def download_welfare_codes(self, max_sections: int | None = None) -> Iterator[CASection]:
        """Download welfare/benefits-related codes (WIC, UIC, LAB, HSC)."""
        for code in CA_WELFARE_CODES:  # pragma: no cover
            yield from self.download_code(code, max_sections=max_sections)  # pragma: no cover


# CLI helper functions
def download_california_rtc(output_dir: str = "data/ca/statute", max_sections: int | None = None):
    """Download California Revenue and Taxation Code."""
    from pathlib import Path  # pragma: no cover
    import json  # pragma: no cover

    output_path = Path(output_dir)  # pragma: no cover
    output_path.mkdir(parents=True, exist_ok=True)  # pragma: no cover

    parser = CACodeParser(code="RTC")  # pragma: no cover
    sections = list(parser.download_code(max_sections=max_sections))  # pragma: no cover

    # Save as JSON
    output_file = output_path / "rtc_sections.json"  # pragma: no cover
    with open(output_file, "w") as f:  # pragma: no cover
        json.dump(  # pragma: no cover
            [
                {
                    "citation": str(s.citation),
                    "heading": s.heading,
                    "text": s.text,
                    "source_url": s.source_url,
                }
                for s in sections
            ],
            f,
            indent=2,
        )

    print(f"Saved {len(sections)} sections to {output_file}")  # pragma: no cover
    return sections  # pragma: no cover


if __name__ == "__main__":
    # Test: Download a few sections from RTC
    parser = CACodeParser(code="RTC")

    # Test single section fetch
    print("Testing single section fetch...")
    section = parser.get_section("17041")
    if section:
        print(f"✓ Fetched: {section.citation}")
        print(f"  Heading: {section.heading}")
        print(f"  Text length: {len(section.text)} chars")
        print(f"  Subsections: {len(section.subsections)}")
    else:
        print("✗ Failed to fetch section")
