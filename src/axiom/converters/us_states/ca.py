"""California state statute converter.

Fetches from leginfo.legislature.ca.gov and converts to unified Statute model.

California provides statutes via:
1. Web scraping (leginfo.legislature.ca.gov) - used here
2. Bulk download (downloads.leginfo.legislature.ca.gov) - MySQL format

URL Patterns:
- Section: faces/codes_displaySection.xhtml?lawCode=RTC&sectionNum=17052
- Code TOC: faces/codesTOCSelected.xhtml?tocCode=RTC

Example usage:
    converter = CAStateConverter()

    # Fetch a section
    statute = converter.fetch("rtc/17052")  # CA EITC section
    print(statute.citation)  # "CA RTC 17052"
    print(statute.rulespec_path)  # "rules-us-ca/statute/RTC/17052.yaml"

    # Fetch with caching
    statute = converter.fetch("wic/11320.3", cache=True)

California has 29 codes:
- RTC: Revenue and Taxation Code (tax policy)
- WIC: Welfare and Institutions Code (benefits)
- UIC: Unemployment Insurance Code
- LAB: Labor Code
- HSC: Health and Safety Code
- And 24 others
"""

import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import httpx
from bs4 import BeautifulSoup

from axiom.models_statute import Statute, StatuteSubsection


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

# Priority codes for tax/benefit policy work
CA_TAX_CODES = ["RTC"]
CA_WELFARE_CODES = ["WIC", "UIC", "LAB", "HSC"]

BASE_URL = "https://leginfo.legislature.ca.gov/faces"


class CAStateConverter:
    """Converter for California statutes from leginfo.legislature.ca.gov.

    Fetches and parses California Code sections, converting them to the
    unified Statute model used across all jurisdictions.

    Attributes:
        base_url: Base URL for leginfo website.
        data_dir: Directory for caching downloaded HTML files.
        rate_limit_delay: Minimum seconds between requests.
    """

    def __init__(
        self,
        data_dir: Path | None = None,
        base_url: str = BASE_URL,
        rate_limit_delay: float = 0.5,
    ):
        """Initialize the converter.

        Args:
            data_dir: Directory to cache downloaded files.
                     Defaults to ~/.axiom/us-ca/
            base_url: Base URL for leginfo website.
            rate_limit_delay: Seconds between requests (default 0.5).
        """
        self.base_url = base_url
        self.data_dir = data_dir or Path.home() / ".axiom" / "us-ca"
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0.0

    def build_url(self, code: str, section: str) -> str:
        """Build the URL for a section.

        Args:
            code: Code abbreviation (e.g., "RTC")
            section: Section number (e.g., "17052", "17041.5")

        Returns:
            Full URL to fetch the section HTML.
        """
        return f"{self.base_url}/codes_displaySection.xhtml?lawCode={code}&sectionNum={section}"

    def parse_reference(self, ref: str) -> tuple[str, str]:
        """Parse a reference string into code and section.

        Args:
            ref: Reference string like "rtc/17052" or "RTC/17041.5"

        Returns:
            Tuple of (code, section)

        Raises:
            ValueError: If the reference cannot be parsed
        """
        ref = ref.strip().strip("/")

        # Pattern: code/section
        pattern = r"^([a-zA-Z]+)/(.+)$"
        match = re.match(pattern, ref)

        if not match:
            raise ValueError(
                f"Invalid CA reference: {ref}. Expected format: CODE/SECTION (e.g., rtc/17052)"
            )  # pragma: no cover

        code = match.group(1).upper()
        section = match.group(2)

        if code not in CA_CODES:
            raise ValueError(
                f"Unknown CA code: {code}. Valid codes: {list(CA_CODES.keys())}"
            )  # pragma: no cover

        return code, section

    def _cache_path(self, code: str, section: str) -> Path:
        """Get cache file path for a section.

        Args:
            code: Code abbreviation
            section: Section number

        Returns:
            Path to cache file
        """
        # Replace dots with underscores for filesystem safety
        safe_section = section.replace(".", "_")
        return self.data_dir / code / f"{safe_section}.html"

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _fetch_html(self, url: str) -> str:
        """Fetch HTML from URL with rate limiting.

        Args:
            url: URL to fetch

        Returns:
            HTML string

        Raises:
            httpx.HTTPError: If request fails
        """
        self._rate_limit()  # pragma: no cover

        with httpx.Client(
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": "Axiom/1.0 (https://github.com/TheAxiomFoundation/axiom-corpus; contact@axiom-foundation.org)"
            },
        ) as client:
            response = client.get(url)
            response.raise_for_status()
            return response.text

    def _parse_html(
        self,
        html: str,
        code: str,
        section: str,
        source_url: str,
    ) -> Statute:
        """Parse HTML into Statute model.

        Args:
            html: Raw HTML content
            code: Code abbreviation
            section: Section number
            source_url: URL where HTML was fetched

        Returns:
            Statute object
        """
        soup = BeautifulSoup(html, "html.parser")

        # Find the main content div
        content_div = (
            soup.find("div", {"id": "codeLawSectionNoHead"})
            or soup.find("div", class_="displaycodeleftmargin")
            or soup.find("div", {"id": "codeLawContent"})
        )

        if not content_div:
            # Fallback to body
            content_div = soup.find("body")  # pragma: no cover

        # Extract structural hierarchy
        division = self._extract_structure(content_div, "DIVISION")
        part = self._extract_structure(content_div, "PART")
        chapter = self._extract_structure(content_div, "CHAPTER")
        article = self._extract_structure(content_div, "ARTICLE")

        # Extract section title - look for h6 with section number
        title = f"Section {section}"
        h6 = content_div.find("h6") if content_div else None
        if h6:
            # Title is often in the next sibling p tag or same div
            title_p = h6.find_next_sibling("p")
            if title_p:
                # Get first sentence as title
                title_text = title_p.get_text(strip=True)
                if title_text:
                    # Limit to first sentence
                    first_sentence = title_text.split(".")[0]
                    if len(first_sentence) < 200:
                        title = first_sentence

        # Extract full text
        text = content_div.get_text(separator="\n", strip=True) if content_div else ""

        # Extract legislative history
        history = self._extract_history(text)

        # Parse subsections
        subsections = self._parse_subsections(content_div)

        return Statute(
            jurisdiction="us-ca",
            code=code,
            code_name=CA_CODES.get(code, code),
            section=section,
            title=title,
            text=text,
            subsections=subsections,
            division=division,
            part=part,
            chapter=chapter,
            article=article,
            history=history,
            source_url=source_url,
            retrieved_at=datetime.now(timezone.utc),
        )

    def _extract_structure(self, content_div, level: str) -> str | None:
        """Extract structural level (division, part, chapter) from HTML.

        Args:
            content_div: BeautifulSoup element containing content
            level: Level name to extract (e.g., "DIVISION", "PART")

        Returns:
            Level number or None if not found
        """
        if not content_div:
            return None  # pragma: no cover

        # Look for h4/h5 elements with the level name
        for heading in content_div.find_all(["h4", "h5"]):
            text = heading.get_text(strip=True).upper()
            if level in text:
                # Extract number from text like "DIVISION 2. OTHER TAXES"
                match = re.search(rf"{level}\s+(\d+)", text, re.IGNORECASE)
                if match:
                    return match.group(1)
        return None

    def _extract_history(self, text: str) -> str | None:
        """Extract legislative history note from text.

        Args:
            text: Full section text

        Returns:
            History note or None
        """
        # Look for patterns like "(Amended by Stats. 2022, Ch. 482...)"
        patterns = [
            r"\((?:Added|Amended|Repealed).*?Stats\.\s*\d{4}.*?\)",
            r"\((?:Added|Amended|Repealed) by.*?Effective.*?\)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(0)
        return None  # pragma: no cover

    def _parse_subsections(self, content_div) -> list[StatuteSubsection]:
        """Parse subsections from content.

        California uses patterns like:
        (a) First subsection
        (1) Paragraph
        (A) Subparagraph

        Args:
            content_div: BeautifulSoup element containing content

        Returns:
            List of StatuteSubsection objects
        """
        if not content_div:
            return []  # pragma: no cover

        subsections = []
        text = content_div.get_text(separator="\n", strip=True)

        # Find top-level subsections (a), (b), etc.
        # Pattern matches (letter) anywhere in text, not just at newlines
        # Split text by top-level subsection markers
        parts = re.split(r"\(([a-z])\)\s*", text)

        # parts[0] is text before first (a), then alternating identifier, content
        for i in range(1, len(parts) - 1, 2):
            identifier = parts[i]
            content = parts[i + 1].strip() if i + 1 < len(parts) else ""

            # Stop if we hit another top-level marker in the content
            # (this happens when content contains (1), (2), etc.)
            next_marker = re.search(r"\([a-z]\)\s", content)
            if next_marker:
                content = content[: next_marker.start()].strip()  # pragma: no cover

            # Parse nested subsections
            children = self._parse_nested_subsections(content)

            # Clean content - remove nested subsection text
            if children:
                first_marker = re.search(r"\(\d+\)", content)
                if first_marker:
                    content = content[: first_marker.start()].strip()

            subsections.append(
                StatuteSubsection(
                    identifier=identifier,
                    text=content[:2000] if len(content) > 2000 else content,
                    children=children,
                )
            )

        return subsections

    def _parse_nested_subsections(self, text: str) -> list[StatuteSubsection]:
        """Parse nested subsections (1), (2), etc.

        Args:
            text: Text containing nested subsections

        Returns:
            List of StatuteSubsection objects
        """
        children = []

        # Pattern for numbered subsections
        pattern = r"\((\d+)\)\s*([^(]*?)(?=\(\d+\)|$)"

        for match in re.finditer(pattern, text, re.DOTALL):
            identifier = match.group(1)
            content = match.group(2).strip()

            children.append(
                StatuteSubsection(
                    identifier=identifier,
                    text=content[:1000] if len(content) > 1000 else content,
                    children=[],  # Could add (A), (B) parsing here
                )
            )

        return children

    def fetch(
        self,
        ref: str,
        cache: bool = True,
        force: bool = False,
    ) -> Statute:
        """Fetch and parse a California statute section.

        Args:
            ref: Reference string like "rtc/17052"
            cache: Whether to cache the HTML to disk
            force: Re-fetch even if cached

        Returns:
            Statute object

        Raises:
            ValueError: If reference is invalid
            httpx.HTTPError: If request fails
        """
        code, section = self.parse_reference(ref)
        cache_path = self._cache_path(code, section)
        url = self.build_url(code, section)

        # Check cache
        if not force and cache_path.exists():  # pragma: no cover
            html = cache_path.read_text()
        else:
            html = self._fetch_html(url)  # pragma: no cover

            # Save to cache
            if cache:  # pragma: no cover
                cache_path.parent.mkdir(parents=True, exist_ok=True)  # pragma: no cover
                cache_path.write_text(html)  # pragma: no cover

        return self._parse_html(html, code, section, url)

    def fetch_sync(
        self,
        ref: str,
        cache: bool = True,
        force: bool = False,
    ) -> Statute:
        """Synchronous fetch (alias for fetch since we don't use async here).

        Args:
            ref: Reference string like "rtc/17052"
            cache: Whether to cache the HTML to disk
            force: Re-fetch even if cached

        Returns:
            Statute object
        """
        return self.fetch(ref, cache=cache, force=force)  # pragma: no cover

    def list_codes(self) -> list[dict[str, str]]:
        """List all available California codes.

        Returns:
            List of dicts with 'code' and 'name' keys.
        """
        return [{"code": k, "name": v} for k, v in CA_CODES.items()]

    def fetch_code_sections(
        self,
        code: str,
        sections: list[str] | None = None,
        max_sections: int | None = None,
    ) -> Iterator[Statute]:
        """Fetch multiple sections from a code.

        Args:
            code: Code abbreviation (e.g., "RTC")
            sections: Specific section numbers to fetch (None = use TOC)
            max_sections: Maximum sections to fetch (for testing)

        Yields:
            Statute objects
        """
        code = code.upper()  # pragma: no cover
        if code not in CA_CODES:  # pragma: no cover
            raise ValueError(f"Unknown code: {code}")  # pragma: no cover

        if sections:  # pragma: no cover
            # Fetch specific sections
            for i, section in enumerate(sections):  # pragma: no cover
                if max_sections and i >= max_sections:  # pragma: no cover
                    break  # pragma: no cover
                try:  # pragma: no cover
                    yield self.fetch(f"{code}/{section}")  # pragma: no cover
                except Exception as e:  # pragma: no cover
                    print(f"Error fetching {code}/{section}: {e}")  # pragma: no cover
        else:
            # Would need to implement TOC crawling here
            # For now, raise not implemented
            raise NotImplementedError(
                "TOC crawling not yet implemented. Please provide specific section numbers."
            )


# Convenience function for quick access
def fetch_ca_statute(ref: str) -> Statute:
    """Fetch a California statute by reference.

    Args:
        ref: Reference string like "rtc/17052"

    Returns:
        Statute object

    Example:
        statute = fetch_ca_statute("rtc/17052")
        print(statute.citation)  # "CA RTC 17052"
    """
    converter = CAStateConverter()  # pragma: no cover
    return converter.fetch(ref)  # pragma: no cover


if __name__ == "__main__":
    # Test: Fetch California EITC section
    print("Fetching California RTC 17052 (EITC)...")
    converter = CAStateConverter()

    try:
        statute = converter.fetch("rtc/17052")
        print(f"Citation: {statute.citation}")
        print(f"RuleSpec Path: {statute.rulespec_path}")
        print(f"Title: {statute.title}")
        print(f"Division: {statute.division}")
        print(f"Part: {statute.part}")
        print(f"Chapter: {statute.chapter}")
        print(f"Text length: {len(statute.text)} chars")
        print(f"Subsections: {len(statute.subsections)}")
        if statute.subsections:
            for sub in statute.subsections[:3]:
                print(f"  ({sub.identifier}): {sub.text[:50]}...")
    except Exception as e:
        print(f"Error: {e}")
