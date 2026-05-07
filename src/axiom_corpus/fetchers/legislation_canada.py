"""Fetcher for Canadian federal legislation from laws-lois.justice.gc.ca.

Downloads consolidated Acts in XML format from the Justice Laws Website.

Data source:
- URL: https://laws-lois.justice.gc.ca
- Format: XML (custom schema with LIMS namespace)
- Coverage: All consolidated federal Acts (~956 acts)
- Update frequency: Every two weeks

Usage:
    fetcher = CanadaLegislationFetcher()

    # Enumerate all acts
    acts = fetcher.list_all_acts()

    # Download a single act
    xml = fetcher.download_act("I-3.3")  # Income Tax Act

    # Bulk download all acts
    fetcher.bulk_download(output_dir="~/.axiom/canada")
"""

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx


@dataclass
class CanadaActReference:
    """Reference to a Canadian federal Act."""

    code: str  # e.g., "I-3.3", "A-1", "C-46"
    title: str | None = None  # e.g., "Income Tax Act"

    @property
    def xml_url(self) -> str:
        """URL to download XML version."""
        return f"https://laws-lois.justice.gc.ca/eng/XML/{self.code}.xml"

    @property
    def html_url(self) -> str:
        """URL to view HTML version."""
        return f"https://laws-lois.justice.gc.ca/eng/acts/{self.code}/"


class CanadaLegislationFetcher:
    """Fetcher for Canadian federal legislation."""

    BASE_URL = "https://laws-lois.justice.gc.ca"
    ACTS_INDEX_URL = f"{BASE_URL}/eng/acts"
    XML_BASE_URL = f"{BASE_URL}/eng/XML"

    def __init__(
        self,
        timeout: float = 60.0,
        rate_limit: float = 0.5,  # seconds between requests
    ):
        self.timeout = timeout
        self.rate_limit = rate_limit
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=self.timeout,
                headers={
                    "User-Agent": "Axiom/1.0 (legislation archiver; contact@axiom-foundation.org)"
                },
                follow_redirects=True,
            )
        return self._client

    def list_all_acts(self) -> list[CanadaActReference]:
        """Enumerate all federal Acts by browsing alphabetical index pages.

        Returns:
            List of CanadaActReference objects for all Acts.
        """
        all_acts: dict[str, CanadaActReference] = {}

        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            url = f"{self.ACTS_INDEX_URL}/{letter}.html"
            try:
                response = self.client.get(url)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                html = response.text

                # Extract act codes and titles from links
                # Pattern: <a ... href="CODE/index.html">Title</a>
                pattern = r'href="([A-Z][A-Za-z0-9.-]*)/index\.html[^"]*"[^>]*>([^<]+)</a>'
                matches = re.findall(pattern, html)

                for code, title in matches:
                    if code not in all_acts:
                        # Clean up title
                        title = title.strip()
                        all_acts[code] = CanadaActReference(code=code, title=title)

                time.sleep(self.rate_limit)

            except httpx.HTTPError as e:
                print(f"Error fetching {letter}: {e}")
                continue

        return sorted(all_acts.values(), key=lambda a: a.code)

    def download_act(self, code: str) -> bytes:
        """Download XML for a single Act.

        Args:
            code: Act code (e.g., "I-3.3", "A-1")

        Returns:
            XML content as bytes.

        Raises:
            httpx.HTTPError: If download fails.
        """
        # We previously used the shared httpx client, but on darwin the SSL
        # read for the larger LIMS XML payloads (e.g. I-3.3 at ~13MB) reliably
        # hangs in `_ssl__SSLSocket_read`. Falling back to `requests` for the
        # streaming download avoids the issue without touching the rest of
        # the fetcher's API.
        import requests

        url = f"{self.XML_BASE_URL}/{code}.xml"
        with requests.get(
            url,
            headers={
                "User-Agent": ("Axiom/1.0 (legislation archiver; contact@axiom-foundation.org)")
            },
            timeout=self.timeout,
            stream=True,
        ) as response:
            response.raise_for_status()
            chunks: list[bytes] = []
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if chunk:
                    chunks.append(chunk)
            return b"".join(chunks)

    def bulk_download(
        self,
        output_dir: str | Path,
        acts: list[CanadaActReference] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        resume: bool = True,
    ) -> dict[str, int]:
        """Bulk download all Acts in XML format.

        Args:
            output_dir: Directory to save XML files.
            acts: List of acts to download (default: all acts).
            progress_callback: Called with (current, total, act_code) for each act.
            resume: Skip already-downloaded files.

        Returns:
            Dict with download statistics.
        """
        output_path = Path(output_dir).expanduser()
        output_path.mkdir(parents=True, exist_ok=True)

        if acts is None:
            print("Enumerating all acts...")
            acts = self.list_all_acts()
            print(f"Found {len(acts)} acts")

        stats = {"downloaded": 0, "skipped": 0, "failed": 0}
        total = len(acts)

        for i, act in enumerate(acts, 1):
            output_file = output_path / f"{act.code}.xml"

            if resume and output_file.exists():
                stats["skipped"] += 1
                if progress_callback:
                    progress_callback(i, total, f"{act.code} (skipped)")
                continue

            try:
                xml_content = self.download_act(act.code)
                output_file.write_bytes(xml_content)
                stats["downloaded"] += 1

                if progress_callback:
                    progress_callback(i, total, act.code)

                time.sleep(self.rate_limit)

            except httpx.HTTPError as e:
                stats["failed"] += 1
                print(f"Failed to download {act.code}: {e}")

        return stats

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> "CanadaLegislationFetcher":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()
