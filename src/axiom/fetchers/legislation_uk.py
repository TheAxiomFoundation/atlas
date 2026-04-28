"""Fetcher for UK legislation from legislation.gov.uk.

Downloads legislation in CLML XML format via the REST API.

API Documentation: https://legislation.github.io/data-documentation/
Rate Limit: 3,000 requests per 5 minutes per IP (we use 5 req/sec to be safe)
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator, Callable, Iterator, Optional
from xml.etree import ElementTree as ET

import httpx

from axiom.models_uk import UKAct, UKCitation, UKSection
from axiom.parsers.clml import parse_act_metadata, parse_section


logger = logging.getLogger(__name__)


# Priority Acts for PolicyEngine UK
UK_PRIORITY_ACTS = [
    "ukpga/2003/1",  # Income Tax (Earnings and Pensions) Act 2003
    "ukpga/2007/3",  # Income Tax Act 2007
    "ukpga/2009/4",  # Corporation Tax Act 2009
    "ukpga/1992/4",  # Social Security Contributions and Benefits Act 1992
    "ukpga/2012/5",  # Welfare Reform Act 2012
    "ukpga/2002/21",  # Tax Credits Act 2002
    "ukpga/1992/12",  # Taxation of Chargeable Gains Act 1992
    "ukpga/1994/23",  # Value Added Tax Act 1994
    "ukpga/1984/51",  # Inheritance Tax Act 1984
    "ukpga/2017/32",  # Finance Act 2017
]


# Atom feed namespaces
ATOM_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "ukm": "http://www.legislation.gov.uk/namespaces/metadata",
}


class UKActReference:
    """Reference to a UK Act from the legislation.gov.uk feed."""

    def __init__(
        self,
        act_id: str,
        title: str,
        year: int,
        number: int,
        updated: Optional[datetime] = None,
    ):
        self.act_id = act_id  # e.g., "ukpga/2003/1"
        self.title = title
        self.year = year
        self.number = number
        self.updated = updated

    @property
    def citation(self) -> UKCitation:
        """Convert to UKCitation."""
        return UKCitation(type="ukpga", year=self.year, number=self.number)

    def __repr__(self) -> str:
        return f"UKActReference({self.act_id!r}, {self.title!r})"


class BulkDownloadProgress:
    """Tracks progress of bulk UK legislation download."""

    def __init__(self, progress_file: Path):
        self.progress_file = progress_file
        self.downloaded: set[str] = set()
        self.failed: dict[str, str] = {}  # act_id -> error message
        self.total_acts: int = 0
        self.total_sections: int = 0
        self.started_at: Optional[datetime] = None
        self.load()

    def load(self) -> None:
        """Load progress from file."""
        if self.progress_file.exists():
            try:
                data = json.loads(self.progress_file.read_text())
                self.downloaded = set(data.get("downloaded", []))
                self.failed = data.get("failed", {})
                self.total_acts = data.get("total_acts", 0)
                self.total_sections = data.get("total_sections", 0)
                if data.get("started_at"):
                    self.started_at = datetime.fromisoformat(data["started_at"])
            except json.JSONDecodeError, KeyError:
                pass

    def save(self) -> None:
        """Save progress to file."""
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "downloaded": list(self.downloaded),
            "failed": self.failed,
            "total_acts": self.total_acts,
            "total_sections": self.total_sections,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_updated": datetime.now().isoformat(),
        }
        self.progress_file.write_text(json.dumps(data, indent=2))

    def mark_downloaded(self, act_id: str, section_count: int = 0) -> None:
        """Mark an act as downloaded."""
        self.downloaded.add(act_id)
        self.total_sections += section_count
        # Remove from failed if it was there
        self.failed.pop(act_id, None)

    def mark_failed(self, act_id: str, error: str) -> None:
        """Mark an act as failed."""
        self.failed[act_id] = error

    def is_downloaded(self, act_id: str) -> bool:
        """Check if act has been downloaded."""
        return act_id in self.downloaded

    @property
    def summary(self) -> str:
        """Return progress summary."""
        elapsed = ""
        if self.started_at:
            delta = datetime.now() - self.started_at
            hours, remainder = divmod(delta.seconds, 3600)
            minutes, _ = divmod(remainder, 60)
            elapsed = f" ({hours}h {minutes}m elapsed)"

        return (
            f"Downloaded: {len(self.downloaded)}/{self.total_acts} acts, "
            f"{self.total_sections} sections, "
            f"{len(self.failed)} failed{elapsed}"
        )


class UKLegislationFetcher:
    """Fetcher for UK legislation from legislation.gov.uk.

    Downloads legislation XML and parses into UKSection/UKAct objects.
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        base_url: str = "https://www.legislation.gov.uk",
        rate_limit_delay: float = 0.2,
    ):
        """Initialize the fetcher.

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

    def build_url(self, citation: UKCitation, version: str = "") -> str:
        """Build the XML data URL for a citation.

        Args:
            citation: UK legislation citation
            version: Version specifier (e.g., "enacted", "2020-01-01")

        Returns:
            URL to fetch XML data
        """
        url = f"{self.base_url}/{citation.type}/{citation.year}/{citation.number}"
        if citation.section:
            url += f"/section/{citation.section}"
        if version:
            url += f"/{version}"
        url += "/data.xml"
        return url

    def build_search_url(
        self,
        query: str,
        type: Optional[str] = None,
        year: Optional[int] = None,
        limit: int = 20,
    ) -> str:
        """Build search API URL.

        Args:
            query: Search query text
            type: Legislation type filter (e.g., "ukpga")
            year: Year filter
            limit: Max results (default 20)

        Returns:
            Search URL
        """
        params = [f"text={query}", f"results-count={limit}"]
        if type:
            params.append(f"type={type}")
        if year:
            params.append(f"year={year}")
        return f"{self.base_url}/search?{'&'.join(params)}"

    async def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        import time

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
        await self._rate_limit()

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers={
                    "User-Agent": "Axiom/1.0 (https://github.com/TheAxiomFoundation/axiom-corpus; contact@axiom-foundation.org)"
                },
                follow_redirects=True,
                timeout=60,
            )
            response.raise_for_status()
            return response.text

    def _cache_path(self, citation: UKCitation) -> Path:
        """Get cache file path for a citation."""
        path = self.data_dir / citation.type / str(citation.year) / str(citation.number)
        if citation.section:
            return path / f"section-{citation.section}.xml"
        return path / "act.xml"

    async def fetch_section(
        self,
        citation: UKCitation,
        cache: bool = True,
        force: bool = False,
    ) -> UKSection:
        """Fetch a single section.

        Args:
            citation: Citation with section number
            cache: Whether to cache the XML
            force: Re-fetch even if cached

        Returns:
            UKSection object
        """
        cache_path = self._cache_path(citation)

        # Check cache
        if not force and cache_path.exists():
            xml_str = cache_path.read_text()
        else:
            url = self.build_url(citation)
            xml_str = await self._fetch_xml(url)

            # Save to cache
            if cache:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(xml_str)

        return parse_section(xml_str)

    async def fetch_act_metadata(
        self,
        citation: UKCitation,
        cache: bool = True,
        force: bool = False,
    ) -> UKAct:
        """Fetch Act-level metadata.

        Args:
            citation: Citation without section
            cache: Whether to cache the XML
            force: Re-fetch even if cached

        Returns:
            UKAct object with metadata
        """
        cache_path = self._cache_path(citation)

        if not force and cache_path.exists():
            xml_str = cache_path.read_text()
        else:
            url = self.build_url(citation)
            xml_str = await self._fetch_xml(url)

            if cache:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(xml_str)

        return parse_act_metadata(xml_str)

    async def fetch_act_sections(
        self,
        citation: UKCitation,
        max_sections: Optional[int] = None,
    ) -> Iterator[UKSection]:
        """Fetch all sections from an Act.

        Args:
            citation: Citation for the Act
            max_sections: Maximum sections to fetch

        Yields:
            UKSection objects
        """
        # First, get the Act metadata to find section count
        act = await self.fetch_act_metadata(citation)
        section_count = act.section_count or 1000  # Default max

        if max_sections:
            section_count = min(section_count, max_sections)

        # Fetch sections (legislation.gov.uk uses numeric sections)
        sections = []
        for i in range(1, section_count + 1):
            try:
                section_citation = UKCitation(
                    type=citation.type,
                    year=citation.year,
                    number=citation.number,
                    section=str(i),
                )
                section = await self.fetch_section(section_citation)
                sections.append(section)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    # Section doesn't exist, skip
                    continue
                raise

        return iter(sections)

    async def list_all_ukpga_acts(
        self,
        page_size: int = 50,
        max_pages: Optional[int] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> list[UKActReference]:
        """List all UK Public General Acts from the legislation.gov.uk feed.

        Args:
            page_size: Number of results per page (max 100)
            max_pages: Maximum pages to fetch (None for all)
            progress_callback: Optional callback for progress updates

        Returns:
            List of UKActReference objects
        """
        acts = []
        page = 1
        base_url = f"{self.base_url}/ukpga/data.feed"

        async with httpx.AsyncClient() as client:
            while True:
                await self._rate_limit()

                url = f"{base_url}?results-count={page_size}&page={page}"
                if progress_callback:
                    progress_callback(f"Fetching page {page}...")

                response = await client.get(
                    url,
                    headers={
                        "User-Agent": "Axiom/1.0 (https://github.com/TheAxiomFoundation/axiom-corpus; contact@axiom-foundation.org)"
                    },
                    follow_redirects=True,
                    timeout=60,
                )
                response.raise_for_status()

                # Parse Atom feed
                root = ET.fromstring(response.text)

                # Extract entries
                entries_found = 0
                for entry in root.findall("atom:entry", ATOM_NS):
                    entries_found += 1
                    act_ref = self._parse_feed_entry(entry)
                    if act_ref:
                        acts.append(act_ref)

                if progress_callback:
                    progress_callback(
                        f"Page {page}: found {entries_found} entries, total {len(acts)} acts"
                    )

                # Check for next page
                next_link = root.find("atom:link[@rel='next']", ATOM_NS)
                if next_link is None:
                    # No more pages
                    break

                page += 1
                if max_pages and page > max_pages:
                    break

        return acts

    def _parse_feed_entry(self, entry: ET.Element) -> Optional[UKActReference]:
        """Parse an Atom feed entry into a UKActReference.

        Args:
            entry: XML Element for an Atom entry

        Returns:
            UKActReference or None if parsing fails
        """
        try:
            # Get the id (URL) to extract act reference
            id_elem = entry.find("atom:id", ATOM_NS)
            if id_elem is None or id_elem.text is None:
                return None

            # Extract act_id from URL: http://www.legislation.gov.uk/id/ukpga/2025/36
            match = re.search(r"legislation\.gov\.uk/(?:id/)?(ukpga/\d+/\d+)", id_elem.text)
            if not match:
                return None

            act_id = match.group(1)

            # Extract year and number
            parts = act_id.split("/")
            year = int(parts[1])
            number = int(parts[2])

            # Get title
            title_elem = entry.find("atom:title", ATOM_NS)
            title = (
                title_elem.text
                if title_elem is not None and title_elem.text
                else f"ukpga/{year}/{number}"
            )

            # Get updated timestamp
            updated_elem = entry.find("atom:updated", ATOM_NS)
            updated = None
            if updated_elem is not None and updated_elem.text:
                try:
                    # Handle ISO format with timezone
                    updated_str = updated_elem.text.replace("Z", "+00:00")
                    updated = datetime.fromisoformat(updated_str)
                except ValueError:
                    pass

            return UKActReference(
                act_id=act_id,
                title=title,
                year=year,
                number=number,
                updated=updated,
            )
        except (ValueError, IndexError, AttributeError) as e:
            logger.warning(f"Failed to parse feed entry: {e}")
            return None

    async def download_act_full_xml(
        self,
        act_ref: UKActReference,
        output_dir: Path,
    ) -> tuple[Path, int]:
        """Download the full XML for an act (all sections in one file).

        Args:
            act_ref: Reference to the act
            output_dir: Directory to save the XML

        Returns:
            Tuple of (path to saved file, approximate section count)
        """
        # Build URL for full act XML
        url = f"{self.base_url}/{act_ref.act_id}/data.xml"

        await self._rate_limit()

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers={
                    "User-Agent": "Axiom/1.0 (https://github.com/TheAxiomFoundation/axiom-corpus; contact@axiom-foundation.org)"
                },
                follow_redirects=True,
                timeout=120,  # Longer timeout for full acts
            )
            response.raise_for_status()
            xml_content = response.text

        # Save to file
        output_path = output_dir / f"{act_ref.year}" / f"{act_ref.number}.xml"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(xml_content)

        # Count sections (rough estimate from P1 elements)
        section_count = xml_content.count("<P1 ")

        return output_path, section_count

    async def bulk_download_ukpga(
        self,
        output_dir: Optional[Path] = None,
        progress: Optional[BulkDownloadProgress] = None,
        act_refs: Optional[list[UKActReference]] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        log_file: Optional[Path] = None,
    ) -> BulkDownloadProgress:
        """Bulk download all UK Public General Acts.

        Args:
            output_dir: Directory to save XML files. Defaults to ~/.axiom/uk/ukpga/
            progress: Progress tracker (creates new one if not provided)
            act_refs: List of acts to download (fetches all if not provided)
            progress_callback: Optional callback for progress updates
            log_file: Optional file to write detailed log

        Returns:
            BulkDownloadProgress object with download status
        """
        output_dir = output_dir or self.data_dir / "ukpga"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize progress tracking
        if progress is None:
            progress_file = output_dir / "progress.json"
            progress = BulkDownloadProgress(progress_file)

        # Open log file if specified
        log_handle = None
        if log_file:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            log_handle = open(log_file, "a")

        def log(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            if log_handle:
                log_handle.write(f"{datetime.now().isoformat()} {msg}\n")
                log_handle.flush()

        try:
            # Get list of all acts if not provided
            if act_refs is None:
                log("Enumerating all UK Public General Acts...")
                act_refs = await self.list_all_ukpga_acts(
                    page_size=50,
                    progress_callback=log,
                )
                log(f"Found {len(act_refs)} UK Public General Acts")

            # Update progress totals
            if progress.started_at is None:
                progress.started_at = datetime.now()
            progress.total_acts = len(act_refs)
            progress.save()

            # Download each act
            for i, act_ref in enumerate(act_refs, 1):
                # Skip if already downloaded
                if progress.is_downloaded(act_ref.act_id):
                    log(f"[{i}/{len(act_refs)}] Skip {act_ref.act_id} (already downloaded)")
                    continue

                try:
                    log(
                        f"[{i}/{len(act_refs)}] Downloading {act_ref.act_id}: {act_ref.title[:50]}..."
                    )
                    path, section_count = await self.download_act_full_xml(act_ref, output_dir)
                    progress.mark_downloaded(act_ref.act_id, section_count)
                    log(f"  -> Saved to {path} ({section_count} sections)")

                except httpx.HTTPStatusError as e:
                    error_msg = f"HTTP {e.response.status_code}"
                    progress.mark_failed(act_ref.act_id, error_msg)
                    log(f"  -> FAILED: {error_msg}")

                except Exception as e:
                    error_msg = str(e)[:100]
                    progress.mark_failed(act_ref.act_id, error_msg)
                    log(f"  -> FAILED: {error_msg}")

                # Save progress periodically (every 10 acts)
                if i % 10 == 0:
                    progress.save()
                    log(f"Progress: {progress.summary}")

            # Final save
            progress.save()
            log(f"COMPLETE: {progress.summary}")

        finally:
            if log_handle:
                log_handle.close()

        return progress


async def download_uk_act(
    act_ref: str,
    data_dir: Optional[Path] = None,
    max_sections: Optional[int] = None,
) -> list[UKSection]:
    """Convenience function to download an entire UK Act.

    Args:
        act_ref: Act reference like "ukpga/2003/1"
        data_dir: Optional data directory
        max_sections: Optional limit on sections

    Returns:
        List of UKSection objects
    """
    citation = UKCitation.from_string(act_ref)
    fetcher = UKLegislationFetcher(data_dir=data_dir)
    sections = await fetcher.fetch_act_sections(citation, max_sections=max_sections)
    return list(sections)
