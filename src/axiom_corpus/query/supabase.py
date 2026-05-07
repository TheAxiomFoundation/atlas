"""Query statute data from Supabase.

This module provides a simple interface to query the corpus.provisions table in Supabase,
which contains parsed statute text from US, UK, and Canada.

Usage:
    from axiom_corpus.query import SupabaseQuery

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

import httpx

DEFAULT_AXIOM_SUPABASE_URL = "https://swocpijqqahhuwtuahwc.supabase.co"
DEFAULT_AXIOM_SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN3b2NwaWpxcWFoaHV3dHVhaHdjI"
    "iwicm9sZSI6ImFub24iLCJpYXQiOjE3NzczMzU3NzcsImV4cCI6MjA5Mjkx"
    "MTc3N30."
    "spiF6Z6LLJmETL8eI0z_QbwgXce7J5CIqHTiXZ6K9Zk"
)
DEFAULT_DOC_TYPE = "statute"
DOC_TYPE_SEGMENTS = {"statute", "regulation", "rulemaking"}
QueryParams = dict[str, str] | list[tuple[str, str]]


@dataclass
class Rule:
    """A provision row from the Supabase corpus."""

    id: str
    jurisdiction: str
    doc_type: str
    parent_id: str | None
    level: int
    ordinal: int | None
    heading: str | None
    body: str | None
    effective_date: str | None
    repeal_date: str | None
    source_url: str | None
    source_path: str | None
    rulespec_path: str | None
    has_rulespec: bool
    citation_path: str | None


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
        return self.rule.citation_path or self.rule.source_path or self.rule.id


class SupabaseQuery:
    """Query the Supabase source corpus."""

    def __init__(
        self,
        url: str | None = None,
        anon_key: str | None = None,
        include_legacy: bool = False,
    ):
        """Initialize the query client.

        Args:
            url: Supabase project URL. Defaults to AXIOM_SUPABASE_URL env var.
            anon_key: Supabase anon key. Defaults to SUPABASE_ANON_KEY env var.
            include_legacy: Query all rows, including scopes outside the current release.
        """
        self.url = url or os.environ.get("AXIOM_SUPABASE_URL", DEFAULT_AXIOM_SUPABASE_URL)
        self.anon_key = (
            anon_key
            or os.environ.get("SUPABASE_ANON_KEY")
            or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
            or DEFAULT_AXIOM_SUPABASE_ANON_KEY
        )
        if not self.anon_key:
            raise ValueError("SUPABASE_ANON_KEY env var required")
        self.rest_url = f"{self.url}/rest/v1"
        self.provisions_table = "provisions" if include_legacy else "current_provisions"
        self.provision_counts_table = (
            "provision_counts" if include_legacy else "current_provision_counts"
        )
        self.headers = {
            "apikey": self.anon_key,
            "Authorization": f"Bearer {self.anon_key}",
            "Accept-Profile": "corpus",  # Query corpus schema
        }

    def _request(
        self,
        table: str,
        params: QueryParams | None = None,
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
            rulespec_path=data.get("rulespec_path"),
            has_rulespec=data.get("has_rulespec", False),
            citation_path=data.get("citation_path"),
        )

    @staticmethod
    def _normalize_citation_path(path: str, jurisdiction: str = "us") -> str:
        """Normalize CLI shorthand into the canonical corpus.provisions citation_path."""
        normalized = path.strip().strip("/")
        if not normalized:
            return normalized

        parts = normalized.split("/")

        # Already canonical, e.g. us/statute/26/32 or us/regulation/26/1.32-1.
        if len(parts) >= 2 and parts[1] in DOC_TYPE_SEGMENTS:
            return normalized

        # Allow users to omit the jurisdiction but include the document type.
        if parts[0] in DOC_TYPE_SEGMENTS:
            return f"{jurisdiction}/{normalized}"

        # Old US Code shorthand used by the CLI and encoder prompts.
        if jurisdiction == "us" and parts[0] == "usc":
            return f"us/{DEFAULT_DOC_TYPE}/{'/'.join(parts[1:])}"

        return f"{jurisdiction}/{DEFAULT_DOC_TYPE}/{normalized}"

    @staticmethod
    def _path_label(rule: Rule, fallback: str = "") -> str:
        return rule.citation_path or rule.source_path or fallback or rule.id

    def get_section(self, path: str, jurisdiction: str = "us") -> Rule | None:
        """Get a section by its canonical citation path or CLI shorthand.

        Args:
            path: Citation path or shorthand (e.g., "26/32" for 26 USC § 32)
            jurisdiction: The jurisdiction (us, uk, canada)

        Returns:
            The Rule or None if not found
        """
        citation_path = self._normalize_citation_path(path, jurisdiction)
        params = {
            "citation_path": f"eq.{citation_path}",
            "jurisdiction": f"eq.{citation_path.split('/', 1)[0]}",
            "limit": "1",
        }

        results = self._request(self.provisions_table, params)
        if results and len(results) > 0:
            return self._to_rule(results[0])

        return None

    def get_section_with_children(
        self,
        path: str,
        jurisdiction: str = "us",
        deep: bool = False,
    ) -> Section | None:
        """Get a section with its children (subsections).

        Args:
            path: Citation path or shorthand (e.g., "26/32")
            jurisdiction: The jurisdiction
            deep: If True, fetch ALL descendants recursively (not just direct children)

        Returns:
            Section with rule and children, or None
        """
        rule = self.get_section(path, jurisdiction)
        if not rule:
            return None

        if deep:
            # Fetch ALL descendants by indexed citation_path range.
            base_path = self._path_label(rule, self._normalize_citation_path(path, jurisdiction))
            params = [
                ("citation_path", f"gte.{base_path}/"),
                ("citation_path", f"lt.{base_path}0"),
                ("jurisdiction", f"eq.{base_path.split('/', 1)[0]}"),
                ("order", "citation_path"),
                ("limit", "1000"),  # Reasonable limit for deep fetch
            ]
            children_data = self._request(self.provisions_table, params) or []
            children = [self._to_rule(c) for c in children_data]
        else:
            # Fetch only direct children
            params = {
                "parent_id": f"eq.{rule.id}",
                "order": "ordinal",
            }
            children_data = self._request(self.provisions_table, params) or []
            children = [self._to_rule(c) for c in children_data]

        return Section(rule=rule, children=children)

    def get_section_deep(
        self,
        path: str,
        jurisdiction: str = "us",
    ) -> str | None:
        """Get a section with ALL descendants as concatenated text.

        This is optimized for encoder agents that need the full statute text
        in a single response.

        Args:
            path: Citation path or shorthand (e.g., "usc/26/32")
            jurisdiction: The jurisdiction

        Returns:
            Concatenated text of the section and all subsections, or None
        """
        section = self.get_section_with_children(path, jurisdiction, deep=True)
        if not section:
            return None

        parts = []

        # Add main section
        if section.rule.heading:
            parts.append(f"# {self._path_label(section.rule, path)}: {section.rule.heading}")
        if section.rule.body:
            parts.append(section.rule.body)

        # Add all descendants, preserving hierarchy via indentation
        for child in section.children:
            # Calculate depth from path
            child_path = self._path_label(child)
            base_path = self._path_label(section.rule, path)
            depth = child_path.replace(base_path, "").count("/")
            indent = "  " * depth

            if child.heading:
                parts.append(f"\n{indent}## {child_path}: {child.heading}")
            if child.body:
                # Indent the body text
                indented_body = "\n".join(
                    f"{indent}{line}" for line in (child.body or "").split("\n")
                )
                parts.append(indented_body)

        return "\n\n".join(parts)

    def search(
        self,
        query: str,
        jurisdiction: str | None = None,
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
            "order": "jurisdiction,citation_path",
        }

        if jurisdiction:
            params["jurisdiction"] = f"eq.{jurisdiction}"

        results = self._request(self.provisions_table, params) or []
        return [self._to_rule(r) for r in results]

    def get_by_citation(
        self,
        title: int,
        section: str,
        jurisdiction: str = "us",
    ) -> Section | None:
        """Get a US Code section by title and section number.

        Args:
            title: USC title number (e.g., 26 for IRC)
            section: Section number (e.g., "32" for EITC)
            jurisdiction: Jurisdiction (default "us")

        Returns:
            Section with children or None
        """
        path = f"{jurisdiction}/{DEFAULT_DOC_TYPE}/{title}/{section}"
        return self.get_section_with_children(path, jurisdiction)

    def get_stats(self) -> dict[str, int]:
        """Get counts by jurisdiction.

        Returns:
            Dict with jurisdiction counts
        """
        rows = self._request(
            self.provision_counts_table,
            {
                "select": "jurisdiction,provision_count",
                "order": "jurisdiction.asc",
            },
        )
        stats: dict[str, int] = {}
        for row in rows or []:
            jurisdiction = row.get("jurisdiction")
            provision_count = row.get("provision_count")
            if jurisdiction is None or provision_count is None:
                continue
            stats[str(jurisdiction)] = stats.get(str(jurisdiction), 0) + int(provision_count)
        stats["total"] = sum(stats.values())
        return stats
