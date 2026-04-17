"""Query statute data from Supabase.

This module provides a simple interface to query the akn.rules table in Supabase,
which contains parsed statute text from US, UK, and Canada.

Usage:
    from atlas.query import SupabaseQuery

    query = SupabaseQuery()

    # Get a specific section by citation path
    section = query.get_section("26/32")  # 26 USC § 32

    # Search for sections
    results = query.search("earned income credit", jurisdiction="us")

    # Get section with children (subsections)
    section = query.get_section_with_children("26/32")
"""

import os
from dataclasses import dataclass
from typing import Optional

import httpx


@dataclass
class Rule:
    """A rule from the Supabase rules table."""

    id: str
    jurisdiction: str
    doc_type: str
    parent_id: Optional[str]
    level: int
    ordinal: Optional[int]
    heading: Optional[str]
    body: Optional[str]
    effective_date: Optional[str]
    repeal_date: Optional[str]
    source_url: Optional[str]
    source_path: Optional[str]
    rac_path: Optional[str]
    has_rac: bool
    citation_path: Optional[str]


@dataclass
class Section:
    """A statute section with optional children."""

    rule: Rule
    children: list[Rule]

    @property
    def full_text(self) -> str:
        """Get the full text of the section including children."""
        parts = []
        if self.rule.heading:
            parts.append(f"# {self.rule.heading}")
        if self.rule.body:
            parts.append(self.rule.body)

        for child in self.children:
            if child.heading:
                parts.append(f"\n## {child.heading}")
            if child.body:
                parts.append(child.body)

        return "\n\n".join(parts)

    @property
    def citation(self) -> str:
        """Get the citation string."""
        return self.rule.source_path or self.rule.id


class SupabaseQuery:
    """Query the Supabase rules table."""

    def __init__(
        self,
        url: Optional[str] = None,
        anon_key: Optional[str] = None,
    ):
        """Initialize the query client.

        Args:
            url: Supabase project URL. Defaults to COSILICO_SUPABASE_URL env var.
            anon_key: Supabase anon key. Defaults to SUPABASE_ANON_KEY env var.
        """
        self.url = url or os.environ.get(
            "COSILICO_SUPABASE_URL",
            "https://nsupqhfchdtqclomlrgs.supabase.co"
        )
        self.anon_key = anon_key or os.environ.get(
            "SUPABASE_ANON_KEY",
            # Default anon key (safe to include - it's public)
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5zdXBxaGZjaGR0cWNsb21scmdzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY5MzExMDgsImV4cCI6MjA4MjUwNzEwOH0.BPdUadtBCdKfWZrKbfxpBQUqSGZ4hd34Dlor8kMBrVI"
        )
        self.rest_url = f"{self.url}/rest/v1"
        self.headers = {
            "apikey": self.anon_key,
            "Authorization": f"Bearer {self.anon_key}",
            "Accept-Profile": "akn",  # Query arch schema
        }

    def _request(
        self,
        table: str,
        params: Optional[dict] = None,
        single: bool = False,
    ) -> dict | list[dict] | None:
        """Make a request to the Supabase REST API."""
        url = f"{self.rest_url}/{table}"
        headers = self.headers.copy()
        if single:
            headers["Accept"] = "application/vnd.pgrst.object+json"

        with httpx.Client() as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

    def _to_rule(self, data: dict) -> Rule:
        """Convert a dict to a Rule object."""
        return Rule(
            id=data["id"],
            jurisdiction=data["jurisdiction"],
            doc_type=data["doc_type"],
            parent_id=data.get("parent_id"),
            level=data["level"],
            ordinal=data.get("ordinal"),
            heading=data.get("heading"),
            body=data.get("body"),
            effective_date=data.get("effective_date"),
            repeal_date=data.get("repeal_date"),
            source_url=data.get("source_url"),
            source_path=data.get("source_path"),
            rac_path=data.get("rac_path"),
            has_rac=data.get("has_rac", False),
            citation_path=data.get("citation_path"),
        )

    def get_section(self, source_path: str, jurisdiction: str = "us") -> Optional[Rule]:
        """Get a section by its source path.

        Args:
            source_path: The source path (e.g., "26/32" for 26 USC § 32)
            jurisdiction: The jurisdiction (us, uk, canada)

        Returns:
            The Rule or None if not found
        """
        # Try exact match first
        params = {
            "source_path": f"eq.{source_path}",
            "jurisdiction": f"eq.{jurisdiction}",
            "limit": "1",
        }

        results = self._request("rules", params)
        if results and len(results) > 0:
            return self._to_rule(results[0])

        # Try with "usc/" prefix for US Code
        if jurisdiction == "us" and not source_path.startswith("usc/"):
            params["source_path"] = f"eq.usc/{source_path}"
            results = self._request("rules", params)
            if results and len(results) > 0:
                return self._to_rule(results[0])

        return None

    def get_section_with_children(
        self,
        source_path: str,
        jurisdiction: str = "us",
        deep: bool = False,
    ) -> Optional[Section]:
        """Get a section with its children (subsections).

        Args:
            source_path: The source path (e.g., "26/32")
            jurisdiction: The jurisdiction
            deep: If True, fetch ALL descendants recursively (not just direct children)

        Returns:
            Section with rule and children, or None
        """
        rule = self.get_section(source_path, jurisdiction)
        if not rule:
            return None

        if deep:
            # Fetch ALL descendants by source_path prefix
            # e.g., "usc/26/32" gets "usc/26/32/a", "usc/26/32/a/1", etc.
            base_path = rule.source_path or source_path
            params = {
                "source_path": f"like.{base_path}/*",
                "jurisdiction": f"eq.{jurisdiction}",
                "order": "source_path",
                "limit": "1000",  # Reasonable limit for deep fetch
            }
            children_data = self._request("rules", params) or []
            children = [self._to_rule(c) for c in children_data]
        else:
            # Fetch only direct children
            params = {
                "parent_id": f"eq.{rule.id}",
                "order": "ordinal",
            }
            children_data = self._request("rules", params) or []
            children = [self._to_rule(c) for c in children_data]

        return Section(rule=rule, children=children)

    def get_section_deep(
        self,
        source_path: str,
        jurisdiction: str = "us",
    ) -> Optional[str]:
        """Get a section with ALL descendants as concatenated text.

        This is optimized for encoder agents that need the full statute text
        in a single response.

        Args:
            source_path: The source path (e.g., "usc/26/32")
            jurisdiction: The jurisdiction

        Returns:
            Concatenated text of the section and all subsections, or None
        """
        section = self.get_section_with_children(source_path, jurisdiction, deep=True)
        if not section:
            return None

        parts = []

        # Add main section
        if section.rule.heading:
            parts.append(f"# {section.rule.source_path}: {section.rule.heading}")
        if section.rule.body:
            parts.append(section.rule.body)

        # Add all descendants, preserving hierarchy via indentation
        for child in section.children:
            # Calculate depth from path
            child_path = child.source_path or ""
            base_path = section.rule.source_path or source_path
            depth = child_path.replace(base_path, "").count("/")
            indent = "  " * depth

            if child.heading:
                parts.append(f"\n{indent}## {child.source_path}: {child.heading}")
            if child.body:
                # Indent the body text
                indented_body = "\n".join(f"{indent}{line}" for line in (child.body or "").split("\n"))
                parts.append(indented_body)

        return "\n\n".join(parts)

    def search(
        self,
        query: str,
        jurisdiction: Optional[str] = None,
        limit: int = 20,
    ) -> list[Rule]:
        """Search for rules by text.

        Args:
            query: Search query
            jurisdiction: Optional jurisdiction filter
            limit: Maximum results

        Returns:
            List of matching rules
        """
        params = {
            "fts": f"wfts.{query}",  # websearch full-text search
            "limit": str(limit),
            "order": "jurisdiction,source_path",
        }

        if jurisdiction:
            params["jurisdiction"] = f"eq.{jurisdiction}"

        results = self._request("rules", params) or []
        return [self._to_rule(r) for r in results]

    def get_by_citation(
        self,
        title: int,
        section: str,
        jurisdiction: str = "us",
    ) -> Optional[Section]:
        """Get a US Code section by title and section number.

        Args:
            title: USC title number (e.g., 26 for IRC)
            section: Section number (e.g., "32" for EITC)
            jurisdiction: Jurisdiction (default "us")

        Returns:
            Section with children or None
        """
        source_path = f"usc/{title}/{section}"
        return self.get_section_with_children(source_path, jurisdiction)

    def get_stats(self) -> dict[str, int]:
        """Get counts by jurisdiction.

        Returns:
            Dict with jurisdiction counts
        """
        stats = {}
        for jur in ["us", "uk", "canada"]:
            params = {
                "jurisdiction": f"eq.{jur}",
                "select": "id",
                "limit": "1",
            }
            # Use HEAD request with Prefer: count=exact
            url = f"{self.rest_url}/rules"
            headers = {
                **self.headers,
                "Prefer": "count=exact",
                "Accept-Profile": "akn",  # Query arch schema
            }
            with httpx.Client() as client:
                response = client.head(url, params=params, headers=headers)
                content_range = response.headers.get("content-range", "")
                # Parse "0-0/12345" to get 12345
                if "/" in content_range:
                    stats[jur] = int(content_range.split("/")[1])
                else:
                    stats[jur] = 0

        stats["total"] = sum(stats.values())
        return stats
