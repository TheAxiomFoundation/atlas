"""UK CLML (Crown Legislation Markup Language) converter.

Converts UK legislation from legislation.gov.uk CLML XML format to AxiomArchive models.

This converter:
1. Fetches CLML XML from legislation.gov.uk API
2. Parses the hierarchical structure (Part, Chapter, Section, Subsection)
3. Extracts metadata (EnactmentDate, ComingIntoForce, extent, etc.)
4. Converts to UKSection/UKAct models

Example usage:
    converter = UKCLMLConverter()

    # Fetch a section
    section = await converter.fetch("ukpga/2024/3/section/1")

    # Fetch Act metadata
    act = await converter.fetch("ukpga/2024/3")

    # Or use sync wrapper
    section = converter.fetch_sync("ukpga/2024/3/section/1")

Legislation types:
    - ukpga: UK Public General Acts
    - uksi: UK Statutory Instruments
    - asp: Acts of Scottish Parliament
    - asc: Acts of Senedd Cymru (Welsh Parliament)
    - nia: Acts of Northern Ireland Assembly

API Documentation: https://legislation.github.io/data-documentation/
Rate Limit: 3,000 requests per 5 minutes per IP
"""

import asyncio
import re
import time
from pathlib import Path
from typing import Optional, Union

import httpx

from axiom.models_uk import UKAct, UKCitation, UKSection
from axiom.parsers.clml import parse_act_metadata, parse_section


class UKCLMLConverter:
    """Converter for UK legislation from legislation.gov.uk CLML XML.

    Provides a unified interface to fetch and parse UK legislation,
    with caching and rate limiting.

    Attributes:
        base_url: Base URL for legislation.gov.uk API.
        data_dir: Directory for caching downloaded XML files.
        rate_limit_delay: Minimum seconds between requests.
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        base_url: str = "https://www.legislation.gov.uk",
        rate_limit_delay: float = 0.2,
    ):
        """Initialize the converter.

        Args:
            data_dir: Directory to cache downloaded files.
                     Defaults to ~/.axiom/uk/
            base_url: Base URL for legislation.gov.uk API.
            rate_limit_delay: Seconds between requests (default 0.2 = 5/sec).
        """
        self.base_url = base_url
        self.data_dir = data_dir or Path.home() / ".axiom" / "uk"
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0.0

    def build_url(self, ref: str) -> str:
        """Build the XML data URL for a reference.

        Args:
            ref: Reference string like "ukpga/2024/3" or "ukpga/2024/3/section/1"

        Returns:
            URL to fetch XML data, e.g.:
            https://www.legislation.gov.uk/ukpga/2024/3/data.xml
        """
        # Normalize ref (remove leading slash if present)
        ref = ref.lstrip("/")
        return f"{self.base_url}/{ref}/data.xml"

    def parse_reference(self, ref: str) -> UKCitation:
        """Parse a reference string into a UKCitation.

        Args:
            ref: Reference string like "ukpga/2024/3" or "ukpga/2024/3/section/1"

        Returns:
            UKCitation object

        Raises:
            ValueError: If the reference cannot be parsed
        """
        ref = ref.lstrip("/")

        # Pattern: type/year/number[/section/num]
        pattern = r"^([a-z]+)/(\d+)/(\d+)(?:/section/(\d+[A-Za-z]?))?$"
        match = re.match(pattern, ref, re.IGNORECASE)

        if not match:
            raise ValueError(f"Invalid UK legislation reference: {ref}")  # pragma: no cover

        return UKCitation(
            type=match.group(1).lower(),
            year=int(match.group(2)),
            number=int(match.group(3)),
            section=match.group(4),
        )

    def _cache_path(self, ref: str) -> Path:
        """Get cache file path for a reference.

        Args:
            ref: Reference string

        Returns:
            Path to cache file
        """
        citation = self.parse_reference(ref)
        path = self.data_dir / citation.type / str(citation.year) / str(citation.number)
        if citation.section:
            return path / f"section-{citation.section}.xml"
        return path / "act.xml"

    async def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    async def _fetch_xml(self, url: str) -> str:
        """Fetch XML from URL with rate limiting.

        Args:
            url: URL to fetch

        Returns:
            XML string

        Raises:
            httpx.HTTPError: If request fails
        """
        await self._rate_limit()  # pragma: no cover

        async with httpx.AsyncClient() as client:  # pragma: no cover
            response = await client.get(  # pragma: no cover
                url,
                headers={
                    "User-Agent": "Axiom/1.0 (https://github.com/TheAxiomFoundation/axiom-corpus; contact@axiom-foundation.org)"
                },
                follow_redirects=True,
                timeout=60,
            )
            response.raise_for_status()
            return response.text  # pragma: no cover

    def parse_section_xml(self, xml_str: str) -> UKSection:
        """Parse a section from CLML XML string.

        Args:
            xml_str: CLML XML content

        Returns:
            UKSection object
        """
        return parse_section(xml_str)

    def parse_act_xml(self, xml_str: str) -> UKAct:
        """Parse Act metadata from CLML XML string.

        Args:
            xml_str: CLML XML content

        Returns:
            UKAct object
        """
        return parse_act_metadata(xml_str)

    def _is_section_ref(self, ref: str) -> bool:
        """Check if reference points to a specific section.

        Args:
            ref: Reference string

        Returns:
            True if reference includes a section number
        """
        return "/section/" in ref.lower()

    async def fetch(
        self,
        ref: str,
        cache: bool = True,
        force: bool = False,
    ) -> Union[UKSection, UKAct]:
        """Fetch and parse UK legislation.

        Args:
            ref: Reference string like "ukpga/2024/3" or "ukpga/2024/3/section/1"
            cache: Whether to cache the XML to disk
            force: Re-fetch even if cached

        Returns:
            UKSection if ref includes section, otherwise UKAct
        """
        cache_path = self._cache_path(ref)

        # Check cache
        if not force and cache_path.exists():
            xml_str = cache_path.read_text()
        else:
            url = self.build_url(ref)
            xml_str = await self._fetch_xml(url)

            # Save to cache
            if cache:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(xml_str)

        # Parse based on reference type
        if self._is_section_ref(ref):
            return self.parse_section_xml(xml_str)
        else:
            return self.parse_act_xml(xml_str)

    def fetch_sync(
        self,
        ref: str,
        cache: bool = True,
        force: bool = False,
    ) -> Union[UKSection, UKAct]:
        """Synchronous wrapper for fetch().

        Args:
            ref: Reference string like "ukpga/2024/3" or "ukpga/2024/3/section/1"
            cache: Whether to cache the XML to disk
            force: Re-fetch even if cached

        Returns:
            UKSection if ref includes section, otherwise UKAct
        """
        return asyncio.run(self.fetch(ref, cache=cache, force=force))

    async def fetch_section(
        self,
        citation: UKCitation,
        cache: bool = True,
        force: bool = False,
    ) -> UKSection:
        """Fetch a single section by citation.

        Args:
            citation: UKCitation with section number
            cache: Whether to cache the XML
            force: Re-fetch even if cached

        Returns:
            UKSection object
        """
        ref = f"{citation.type}/{citation.year}/{citation.number}/section/{citation.section}"  # pragma: no cover
        result = await self.fetch(ref, cache=cache, force=force)  # pragma: no cover
        assert isinstance(result, UKSection)  # pragma: no cover
        return result  # pragma: no cover

    async def fetch_act(
        self,
        citation: UKCitation,
        cache: bool = True,
        force: bool = False,
    ) -> UKAct:
        """Fetch Act metadata by citation.

        Args:
            citation: UKCitation without section
            cache: Whether to cache the XML
            force: Re-fetch even if cached

        Returns:
            UKAct object
        """
        ref = f"{citation.type}/{citation.year}/{citation.number}"  # pragma: no cover
        result = await self.fetch(ref, cache=cache, force=force)  # pragma: no cover
        assert isinstance(result, UKAct)  # pragma: no cover
        return result  # pragma: no cover


# Convenience function for quick access
async def fetch_uk_legislation(ref: str) -> Union[UKSection, UKAct]:
    """Fetch UK legislation by reference.

    Args:
        ref: Reference string like "ukpga/2024/3" or "ukpga/2024/3/section/1"

    Returns:
        UKSection if ref includes section, otherwise UKAct

    Example:
        section = await fetch_uk_legislation("ukpga/2024/3/section/1")
    """
    converter = UKCLMLConverter()  # pragma: no cover
    return await converter.fetch(ref)  # pragma: no cover
