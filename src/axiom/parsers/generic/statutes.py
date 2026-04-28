"""Generic state statute parser with configurable URL patterns.

This module provides a configurable parser that can be adapted for different
state statute websites with minimal per-state code. States with complex or
unique formats may still need custom parsers.

Supported patterns:
- Path-based URLs: /code/title-1/section-123
- Query-string URLs: /view?title=1&section=123
- Nested HTML structure with consistent selectors
"""

import logging
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class StateSection:
    """A section from a state statute."""

    state: str  # State code (e.g., "PA", "OH")
    code: str  # Code/title identifier
    code_name: str  # Full name of the code
    section_num: str  # Section number
    title: str  # Section heading
    text: str  # Full text content
    url: str  # Source URL
    subsections: list["StateSubsection"] = field(default_factory=list)
    chapter: str | None = None
    history: str | None = None

    @property
    def citation(self) -> str:
        """Return formatted citation."""
        return f"{self.state} {self.code} § {self.section_num}"


@dataclass
class StateSubsection:
    """A subsection within a state statute."""

    identifier: str
    text: str
    children: list["StateSubsection"] = field(default_factory=list)


@dataclass
class StateConfig:
    """Configuration for a state's statute website."""

    state_code: str  # e.g., "PA", "OH", "IL"
    state_name: str  # e.g., "Pennsylvania"
    base_url: str  # e.g., "https://codes.ohio.gov"

    # URL patterns - use {placeholders} for dynamic values
    section_url_pattern: str  # e.g., "/ohio-revised-code/section-{section}"
    toc_url_pattern: str  # e.g., "/ohio-revised-code/title-{title}"

    # CSS selectors for content extraction
    content_selector: str  # e.g., "div.section-content"
    title_selector: str | None = None  # e.g., "h1.section-title"
    history_selector: str | None = None

    # Code/title structure
    codes: dict[str, str] = field(default_factory=dict)  # code_id -> code_name

    # Rate limiting
    rate_limit: float = 0.5

    # Custom parsers (optional)
    section_parser: Callable[[BeautifulSoup, str], StateSection | None] | None = None


# Pre-configured state settings
OHIO_CONFIG = StateConfig(
    state_code="OH",
    state_name="Ohio",
    base_url="https://codes.ohio.gov",
    section_url_pattern="/ohio-revised-code/section-{section}",
    toc_url_pattern="/ohio-revised-code/title-{title}",
    content_selector="main",
    title_selector="title",
    codes={
        "57": "Taxation",
        "51": "Public Welfare",
        "41": "Labor and Industry",
        "33": "Education-Libraries",
        "37": "Health-Safety-Morals",
    },
)

PENNSYLVANIA_CONFIG = StateConfig(
    state_code="PA",
    state_name="Pennsylvania",
    base_url="https://www.palegis.us",
    section_url_pattern="/statutes/consolidated/view-statute?txtType=HTM&ttl={title}&chpt={chapter}&sctn={section}",
    toc_url_pattern="/statutes/consolidated/view-statute?txtType=HTM&ttl={title}",
    content_selector="div.statute-content, div#content, body",
    title_selector="h1, h2.title",
    codes={
        "72": "Taxation and Fiscal Affairs",
        "62": "Public Welfare",
        "43": "Labor",
        "40": "Insurance",
        "24": "Education",
    },
)

ILLINOIS_CONFIG = StateConfig(
    state_code="IL",
    state_name="Illinois",
    base_url="https://www.ilga.gov",
    section_url_pattern="/legislation/ilcs/ilcs4.asp?ActID={act}&ChapterID={chapter}&SeqStart={seq}&SeqEnd={seq_end}",
    toc_url_pattern="/legislation/ilcs/ilcs2.asp?ChapterID={chapter}",
    content_selector="div.ilcs-content, td.content, body",
    title_selector="h1, h2",
    codes={
        "35": "Revenue",
        "305": "Public Aid",
        "820": "Employment",
        "215": "Insurance",
        "105": "Schools",
    },
)

NORTH_CAROLINA_CONFIG = StateConfig(
    state_code="NC",
    state_name="North Carolina",
    base_url="https://www.ncleg.gov",
    section_url_pattern="/EnactedLegislation/Statutes/HTML/BySection/Chapter_{chapter}/GS_{section}.html",
    toc_url_pattern="/EnactedLegislation/Statutes/HTML/ByChapter/Chapter_{chapter}.html",
    content_selector="body",
    title_selector="title",
    codes={
        "105": "Taxation",
        "108A": "Social Services",
        "95": "Department of Labor",
        "96": "Employment Security",
        "58": "Insurance",
    },
)

MICHIGAN_CONFIG = StateConfig(
    state_code="MI",
    state_name="Michigan",
    base_url="https://www.legislature.mi.gov",
    section_url_pattern="/Laws/MCL?objectId=mcl-{section}",
    toc_url_pattern="/Laws/MCL?chapter={chapter}",
    content_selector="div.content, main, body",
    title_selector="title, h1",
    codes={
        "206": "Income Tax Act",
        "400": "Social Welfare",
        "408": "Labor",
        "421": "Michigan Employment Security Act",
        "500": "Insurance Code",
    },
)

GEORGIA_CONFIG = StateConfig(
    state_code="GA",
    state_name="Georgia",
    base_url="https://www.legis.ga.gov",
    # Georgia's code is in PDFs, harder to scrape - using placeholder
    section_url_pattern="/api/legislation/document/{session}/{doc_id}",
    toc_url_pattern="/legislation/en-US/Search/Legislation",
    content_selector="body",
    title_selector="title",
    codes={
        "48": "Revenue and Taxation",
        "49": "Social Services",
        "34": "Labor and Industrial Relations",
        "33": "Insurance",
    },
)


class GenericStateParser:
    """Generic parser for state statutes.

    Can be configured for different states using StateConfig.

    Example:
        >>> parser = GenericStateParser(OHIO_CONFIG)
        >>> section = parser.get_section("5747.02")
        >>> print(section.citation)
        OH 57 § 5747.02
    """

    def __init__(self, config: StateConfig):
        self.config = config
        self._client = httpx.Client(
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": "Axiom/1.0 (Legal Archive; contact@axiom-foundation.org) https://github.com/TheAxiomFoundation/axiom-corpus"
            },
        )
        self._last_request = 0.0

    def __del__(self):
        if hasattr(self, "_client") and self._client:
            self._client.close()

    def _rate_limit(self):
        """Enforce rate limiting."""
        elapsed = time.time() - self._last_request
        if elapsed < self.config.rate_limit:
            time.sleep(self.config.rate_limit - elapsed)
        self._last_request = time.time()

    def _get(self, url: str) -> str:
        """Make a rate-limited GET request."""
        self._rate_limit()
        response = self._client.get(url)
        response.raise_for_status()
        return response.text

    def _build_section_url(self, section: str, **kwargs) -> str:
        """Build URL for a section."""
        url = self.config.section_url_pattern.format(section=section, **kwargs)
        if not url.startswith("http"):
            url = self.config.base_url + url
        return url

    def _build_toc_url(self, **kwargs) -> str:
        """Build URL for a TOC page."""
        url = self.config.toc_url_pattern.format(**kwargs)
        if not url.startswith("http"):
            url = self.config.base_url + url
        return url

    def _infer_code_from_section(self, section_num: str) -> tuple[str, str]:
        """Infer code/title from section number.

        Many states use section numbers that encode the title.
        E.g., Ohio 5747.02 -> Title 57, Chapter 5747
        """
        # Try to extract title from section number prefix
        for code_id, code_name in self.config.codes.items():
            if section_num.startswith(code_id):
                return code_id, code_name

        # Default to first code if can't infer
        if self.config.codes:
            first = next(iter(self.config.codes.items()))
            return first

        return "", "Unknown"

    def get_section(self, section_num: str, **kwargs) -> StateSection | None:
        """Fetch a single section.

        Args:
            section_num: Section number (e.g., "5747.02")
            **kwargs: Additional URL parameters

        Returns:
            StateSection or None if not found
        """
        # Use custom parser if provided
        if self.config.section_parser:
            url = self._build_section_url(section_num, **kwargs)
            try:
                html = self._get(url)
                soup = BeautifulSoup(html, "html.parser")
                return self.config.section_parser(soup, url)
            except httpx.HTTPError as e:
                logger.warning(
                    "[%s] Error fetching section %s at %s: %s",
                    self.config.state_code,
                    section_num,
                    url,
                    e,
                    exc_info=True,
                )
                return None

        # Default parsing logic
        url = self._build_section_url(section_num, **kwargs)

        try:
            html = self._get(url)
        except httpx.HTTPError as e:
            logger.warning(
                "[%s] Error fetching section %s at %s: %s",
                self.config.state_code,
                section_num,
                url,
                e,
                exc_info=True,
            )
            return None

        soup = BeautifulSoup(html, "html.parser")

        # Find content
        content_div = None
        for selector in self.config.content_selector.split(","):
            content_div = soup.select_one(selector.strip())
            if content_div:
                break

        if not content_div:
            # Silent-failure trap: the page loaded but no selector in
            # ``content_selector`` matched. Usually means the site changed
            # its markup or this state needs a custom selector.
            logger.warning(
                "[%s] No content matched selector %r for section %s at %s",
                self.config.state_code,
                self.config.content_selector,
                section_num,
                url,
            )
            return None

        text = content_div.get_text(separator="\n", strip=True)

        # Find title
        title = f"§ {section_num}"
        if self.config.title_selector:
            for selector in self.config.title_selector.split(","):
                title_el = soup.select_one(selector.strip())
                if title_el:
                    title = title_el.get_text(strip=True)
                    break

        # Find history
        history = None
        if self.config.history_selector:
            history_el = soup.select_one(self.config.history_selector)
            if history_el:
                history = history_el.get_text(strip=True)

        # Infer code
        code_id, code_name = self._infer_code_from_section(section_num)

        # Parse subsections
        subsections = self._parse_subsections(content_div)

        return StateSection(
            state=self.config.state_code,
            code=code_id,
            code_name=code_name,
            section_num=section_num,
            title=title,
            text=text,
            url=url,
            subsections=subsections,
            history=history,
        )

    def _parse_subsections(self, content_div) -> list[StateSubsection]:
        """Parse subsections using common patterns."""
        subsections = []
        text = content_div.get_text(separator="\n", strip=True)

        # Common subsection pattern: (a), (1), (A), (i)
        pattern = r"\(([a-zA-Z]|\d+|[ivx]+)\)\s*([^(]+?)(?=\([a-zA-Z]|\d+|[ivx]+\)|$)"

        for match in re.finditer(pattern, text, re.DOTALL):
            marker = match.group(1)
            content = match.group(2).strip()
            if content:
                subsections.append(
                    StateSubsection(
                        identifier=marker,
                        text=content[:1000] if len(content) > 1000 else content,
                    )
                )

        return subsections

    def list_sections_from_toc(self, **kwargs) -> Iterator[str]:
        """List section numbers from a TOC page.

        Args:
            **kwargs: URL parameters for the TOC page

        Yields:
            Section numbers found on the page
        """
        url = self._build_toc_url(**kwargs)

        try:
            html = self._get(url)
        except httpx.HTTPError as e:
            logger.warning(
                "[%s] Error fetching TOC at %s: %s",
                self.config.state_code,
                url,
                e,
                exc_info=True,
            )
            return

        soup = BeautifulSoup(html, "html.parser")

        # Find links that look like section references
        section_pattern = re.compile(r"section[-_]?(\d+\.?\d*)", re.I)

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = section_pattern.search(href)
            if match:
                yield match.group(1)

    def download_code(
        self,
        code_id: str,
        max_sections: int | None = None,
    ) -> Iterator[StateSection]:
        """Download sections from a specific code.

        Args:
            code_id: Code/title identifier
            max_sections: Maximum sections to download

        Yields:
            StateSection objects
        """
        count = 0

        # Get sections from TOC
        for section_num in self.list_sections_from_toc(title=code_id):
            if max_sections and count >= max_sections:
                return

            section = self.get_section(section_num)
            if section:
                count += 1
                print(f"  [{count}] {section.citation}")
                yield section


# Convenience functions for specific states
def get_ohio_parser() -> GenericStateParser:
    """Get a parser for Ohio Revised Code."""
    return GenericStateParser(OHIO_CONFIG)


def get_pennsylvania_parser() -> GenericStateParser:
    """Get a parser for Pennsylvania Consolidated Statutes."""
    return GenericStateParser(PENNSYLVANIA_CONFIG)


def get_illinois_parser() -> GenericStateParser:
    """Get a parser for Illinois Compiled Statutes."""
    return GenericStateParser(ILLINOIS_CONFIG)


def get_north_carolina_parser() -> GenericStateParser:
    """Get a parser for North Carolina General Statutes."""
    return GenericStateParser(NORTH_CAROLINA_CONFIG)


def get_michigan_parser() -> GenericStateParser:
    """Get a parser for Michigan Compiled Laws."""
    return GenericStateParser(MICHIGAN_CONFIG)


def get_georgia_parser() -> GenericStateParser:
    """Get a parser for Georgia Code (O.C.G.A.)."""
    return GenericStateParser(GEORGIA_CONFIG)


# Registry of all available state parsers
STATE_PARSERS: dict[str, StateConfig] = {
    "OH": OHIO_CONFIG,
    "PA": PENNSYLVANIA_CONFIG,
    "IL": ILLINOIS_CONFIG,
    "NC": NORTH_CAROLINA_CONFIG,
    "MI": MICHIGAN_CONFIG,
    "GA": GEORGIA_CONFIG,
}


def get_parser_for_state(state_code: str) -> GenericStateParser | None:
    """Get a parser for a specific state by code.

    Args:
        state_code: Two-letter state code (e.g., "OH", "PA")

    Returns:
        GenericStateParser if state is supported, None otherwise
    """
    config = STATE_PARSERS.get(state_code.upper())
    if config:
        return GenericStateParser(config)
    return None


def list_supported_states() -> list[dict[str, str]]:
    """List all states with configured parsers."""
    return [{"code": code, "name": config.state_name} for code, config in STATE_PARSERS.items()]


if __name__ == "__main__":
    # Test Ohio parser
    print("Testing Ohio parser...")
    parser = get_ohio_parser()
    section = parser.get_section("5747.02")
    if section:
        print(f"✓ {section.citation}")
        print(f"  Title: {section.title[:60]}...")
        print(f"  Text: {len(section.text)} chars")
        print(f"  Subsections: {len(section.subsections)}")
    else:
        print("✗ Failed")
