"""Base class for statute source adapters.

Each jurisdiction has a source adapter that knows how to:
1. Fetch statutes from the official source (API, HTML, XML)
2. Parse them into the unified Statute model
3. Handle rate limiting and error recovery

Source Types:
- USLMSource: XML from uscode.house.gov (federal)
- APISource: JSON APIs (NY Open Legislation, LegiScan)
- HTMLSource: Web scraping (most states)
- BulkSource: Bulk downloads (CA, some federal)
"""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import httpx

from axiom.models_statute import Statute, StatuteSubsection


@dataclass
class SourceConfig:
    """Configuration for a statute source.

    Loaded from YAML files in sources/ directory.
    """

    # Identification
    jurisdiction: str  # e.g., "us-ca"
    name: str  # e.g., "California"
    source_type: str  # "uslm", "api", "html", "bulk"

    # Connection
    base_url: str
    api_key: str | None = None

    # URL patterns (for HTML sources)
    section_url_pattern: str | None = None  # e.g., "/section-{section}"
    toc_url_pattern: str | None = None

    # Selectors (for HTML sources)
    content_selector: str | None = None
    title_selector: str | None = None
    history_selector: str | None = None

    # Codes available in this jurisdiction
    codes: dict[str, str] = field(default_factory=dict)  # code_id -> code_name

    # Rate limiting
    rate_limit: float = 0.5  # seconds between requests
    max_retries: int = 3

    # Custom parsing
    custom_parser: str | None = None  # module path to custom parser function


class StatuteSource(ABC):
    """Abstract base class for statute sources.

    Implementations:
    - USLMSource: Parses USLM XML (federal)
    - APISource: Calls JSON APIs (NY, LegiScan)
    - HTMLSource: Scrapes HTML (most states)
    """

    def __init__(self, config: SourceConfig):
        self.config = config
        self._client: httpx.Client | None = None
        self._last_request = 0.0

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialize HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                timeout=30,
                follow_redirects=True,
                headers={
                    "User-Agent": "Axiom/1.0 (Legal Archive; contact@axiom-foundation.org) https://github.com/TheAxiomFoundation/axiom-corpus"
                },
            )
        return self._client

    def close(self):
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __del__(self):
        self.close()

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        import time

        elapsed = time.time() - self._last_request
        if elapsed < self.config.rate_limit:
            time.sleep(self.config.rate_limit - elapsed)
        self._last_request = time.time()

    def _get(self, url: str, **kwargs) -> httpx.Response:
        """Make a rate-limited GET request."""
        self._rate_limit()
        return self.client.get(url, **kwargs)

    @abstractmethod
    def get_section(self, code: str, section: str, **kwargs) -> Statute | None:
        """Fetch a single section.

        Args:
            code: Code identifier (e.g., "26" for IRC, "RTC" for CA)
            section: Section number (e.g., "32", "17041")
            **kwargs: Additional source-specific parameters

        Returns:
            Statute object or None if not found
        """
        pass

    @abstractmethod
    def list_sections(self, code: str, **kwargs) -> Iterator[str]:
        """List all section numbers in a code.

        Args:
            code: Code identifier
            **kwargs: Additional parameters (e.g., chapter filter)

        Yields:
            Section numbers
        """
        pass

    def download_code(
        self,
        code: str,
        max_sections: int | None = None,
        progress_callback: Callable[[int, str], None] | None = None,
    ) -> Iterator[Statute]:
        """Download all sections from a code.

        Args:
            code: Code identifier
            max_sections: Maximum sections to download (for testing)
            progress_callback: Called with (count, section_num) for progress

        Yields:
            Statute objects
        """
        count = 0
        for section_num in self.list_sections(code):
            if max_sections and count >= max_sections:
                return

            statute = self.get_section(code, section_num)
            if statute:
                count += 1
                if progress_callback:
                    progress_callback(count, section_num)
                yield statute

    def download_jurisdiction(
        self,
        codes: list[str] | None = None,
        max_sections_per_code: int | None = None,
    ) -> Iterator[Statute]:
        """Download all statutes from this jurisdiction.

        Args:
            codes: Specific codes to download (None = all)
            max_sections_per_code: Limit per code (for testing)

        Yields:
            Statute objects
        """
        target_codes = codes or list(self.config.codes.keys())

        for code in target_codes:
            print(f"Downloading {self.config.jurisdiction}/{code}...")
            yield from self.download_code(code, max_sections=max_sections_per_code)

    def get_code_name(self, code: str) -> str:
        """Get human-readable name for a code."""
        return self.config.codes.get(code, code)

    def _create_statute(
        self,
        code: str,
        section: str,
        title: str,
        text: str,
        source_url: str,
        subsections: list[StatuteSubsection] | None = None,
        **kwargs,
    ) -> Statute:
        """Helper to create a Statute with common fields filled in."""
        return Statute(
            jurisdiction=self.config.jurisdiction,
            code=code,
            code_name=self.get_code_name(code),
            section=section,
            title=title,
            text=text,
            source_url=source_url,
            subsections=subsections or [],
            retrieved_at=datetime.utcnow(),
            **kwargs,
        )


def load_source(jurisdiction: str) -> StatuteSource:
    """Load source adapter for a jurisdiction.

    Args:
        jurisdiction: Jurisdiction ID (e.g., "us", "us-ca")

    Returns:
        StatuteSource instance

    Raises:
        ValueError: If jurisdiction not configured
    """
    from axiom.sources.registry import get_source_for_jurisdiction

    source = get_source_for_jurisdiction(jurisdiction)
    if not source:
        raise ValueError(f"No source configured for jurisdiction: {jurisdiction}")
    return source
