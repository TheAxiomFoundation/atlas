"""Ingest parsed statutes into Supabase arch.rules table.

This module pushes parsed statute sections to the PostgreSQL `arch.rules` table
via the Supabase REST API.

Usage:
    from atlas.ingest.supabase import SupabaseIngestor

    ingestor = SupabaseIngestor()
    ingestor.ingest_canada_act("I-3.3")
    ingestor.ingest_usc_title(26)
    ingestor.ingest_uk_act(2020, 1)
"""

import os
import re
from pathlib import Path
from typing import Iterator
from uuid import uuid4, uuid5, NAMESPACE_URL

import httpx

from atlas.parsers.canada import CanadaStatuteParser
from atlas.parsers.us.statutes import USLMParser
from atlas.parsers.clml import parse_act_metadata, parse_section
from atlas.models_canada import CanadaSection, CanadaSubsection
from atlas.models import Section, Subsection
from atlas.models_uk import UKSection, UKSubsection


def _deterministic_id(citation_path: str) -> str:
    """Generate deterministic UUID from citation path for idempotent upserts."""
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


class SupabaseIngestor:
    """Ingest parsed statutes into Supabase rules table."""

    def __init__(
        self,
        url: str | None = None,
        key: str | None = None,
    ):
        """Initialize with Supabase credentials.

        Args:
            url: Supabase project URL (or COSILICO_SUPABASE_URL env var)
            key: Supabase service role key (or from Management API)
        """
        self.url = url or os.environ.get(
            "COSILICO_SUPABASE_URL",
            "https://nsupqhfchdtqclomlrgs.supabase.co",
        )
        # Get service role key from Management API if not provided
        self.key = key or self._get_service_key()
        self.rest_url = f"{self.url}/rest/v1"

    def _get_service_key(self) -> str:
        """Get service role key from Supabase Management API."""
        access_token = os.environ.get("SUPABASE_ACCESS_TOKEN")
        if not access_token:
            raise ValueError(
                "SUPABASE_ACCESS_TOKEN env var required to get service key"
            )

        # Extract project ref from URL
        project_ref = self.url.split("//")[1].split(".")[0]

        with httpx.Client() as client:
            response = client.get(
                f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            response.raise_for_status()
            keys = response.json()

            # Find service_role key
            for key in keys:
                if key.get("name") == "service_role" and key.get("api_key"):
                    return key["api_key"]

        raise ValueError("Could not find service_role key")

    def _upsert_rules(self, rules: list[dict], max_retries: int = 5) -> int:
        """Upsert rules into Supabase (insert or update on citation_path conflict).

        Args:
            rules: List of rule dictionaries with citation_path
            max_retries: Maximum retry attempts on timeout/error

        Returns:
            Number of rows upserted
        """
        import time

        if not rules:
            return 0

        # Add line_count to all rules based on body text
        for rule in rules:
            body = rule.get("body") or ""
            rule["line_count"] = len(body.split("\n"))

        timeout = httpx.Timeout(180.0, connect=30.0, read=180.0, write=180.0)

        for attempt in range(max_retries):
            try:
                with httpx.Client(timeout=timeout) as client:
                    response = client.post(
                        f"{self.rest_url}/rules",
                        headers={
                            "apikey": self.key,
                            "Authorization": f"Bearer {self.key}",
                            "Content-Type": "application/json",
                            "Content-Profile": "arch",  # Write to arch schema
                            # Upsert: on conflict with citation_path, update
                            "Prefer": "resolution=merge-duplicates,return=minimal",
                        },
                        json=rules,
                    )
                    response.raise_for_status()
                return len(rules)
            except (httpx.ReadTimeout, httpx.HTTPStatusError) as e:
                is_server_error = isinstance(e, httpx.HTTPStatusError) and e.response.status_code >= 500
                is_timeout = isinstance(e, httpx.ReadTimeout)

                if (is_server_error or is_timeout) and attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"    Retry {attempt + 1}/{max_retries} after {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                raise

        return len(rules)

    # Keep old method for backwards compatibility
    def _insert_rules(self, rules: list[dict], max_retries: int = 5) -> int:
        """Insert rules (deprecated, use _upsert_rules)."""
        return self._upsert_rules(rules, max_retries)

    def _section_to_rules(
        self,
        section: CanadaSection,
        parent_id: str | None = None,
        act_id: str | None = None,
    ) -> Iterator[dict]:
        """Convert a CanadaSection to rule dictionaries.

        Yields rule dicts for the section and all its subsections.
        """
        # Build citation path: ca/statute/{act_id}/{section_number}
        citation_path = f"ca/statute/{act_id}/{section.section_number}" if act_id else None
        section_id = _deterministic_id(citation_path) if citation_path else str(uuid4())

        # Section-level rule
        yield {
            "id": section_id,
            "jurisdiction": "canada",
            "doc_type": "statute",
            "parent_id": parent_id,
            "level": 0,
            "ordinal": int(section.section_number.split(".")[0]) if section.section_number.replace(".", "").isdigit() else None,
            "heading": section.marginal_note,
            "body": section.text,
            "effective_date": section.in_force_date.isoformat() if section.in_force_date else None,
            "source_url": section.source_url,
            "source_path": section.source_path,
            "citation_path": citation_path,
            "rac_path": None,
            "has_rac": False,
        }

        # Recursively yield subsections
        yield from self._subsections_to_rules(
            section.subsections,
            parent_id=section_id,
            level=1,
            parent_path=citation_path,
        )

    def _subsections_to_rules(
        self,
        subsections: list[CanadaSubsection],
        parent_id: str,
        level: int,
        parent_path: str | None = None,
    ) -> Iterator[dict]:
        """Convert subsections to rule dictionaries recursively."""
        for i, sub in enumerate(subsections):
            # Build citation path: {parent_path}/{ordinal}
            ordinal = i + 1
            citation_path = f"{parent_path}/{ordinal}" if parent_path else None
            sub_id = _deterministic_id(citation_path) if citation_path else str(uuid4())

            yield {
                "id": sub_id,
                "jurisdiction": "canada",
                "doc_type": "statute",
                "parent_id": parent_id,
                "level": level,
                "ordinal": ordinal,
                "heading": sub.marginal_note,
                "body": sub.text,
                "effective_date": None,
                "source_url": None,
                "source_path": None,
                "citation_path": citation_path,
                "rac_path": None,
                "has_rac": False,
            }

            # Recursively handle children
            if sub.children:
                yield from self._subsections_to_rules(
                    sub.children,
                    parent_id=sub_id,
                    level=level + 1,
                    parent_path=citation_path,
                )

    def ingest_canada_act(
        self,
        consolidated_number: str,
        arch_path: Path | None = None,
        batch_size: int = 50,
    ) -> int:
        """Ingest a Canadian federal act into the rules table.

        Args:
            consolidated_number: e.g., "I-3.3" for Income Tax Act
            arch_path: Path to arch directory (default ~/.arch)
            batch_size: Number of rules to insert per batch

        Returns:
            Total number of rules inserted
        """
        if arch_path is None:
            arch_path = Path.home() / ".arch"

        xml_path = arch_path / "canada" / f"{consolidated_number}.xml"
        if not xml_path.exists():
            raise FileNotFoundError(f"Not found: {xml_path}")

        parser = CanadaStatuteParser(xml_path)
        total_inserted = 0
        batch: list[dict] = []

        print(f"Ingesting {consolidated_number}...")

        for section in parser.iter_sections():
            for rule in self._section_to_rules(section, act_id=consolidated_number):
                batch.append(rule)

                if len(batch) >= batch_size:
                    inserted = self._insert_rules(batch)
                    total_inserted += inserted
                    print(f"  Inserted {total_inserted} rules...")
                    batch = []

        # Insert remaining
        if batch:
            inserted = self._insert_rules(batch)
            total_inserted += inserted

        print(f"Done! Inserted {total_inserted} rules for {consolidated_number}")
        return total_inserted

    def ingest_all_canada(
        self,
        arch_path: Path | None = None,
        limit: int | None = None,
    ) -> int:
        """Ingest all Canadian federal acts.

        Args:
            arch_path: Path to arch directory
            limit: Max number of acts to process (for testing)

        Returns:
            Total number of rules inserted
        """
        if arch_path is None:
            arch_path = Path.home() / ".arch"

        canada_path = arch_path / "canada"
        xml_files = sorted(canada_path.glob("*.xml"))

        if limit:
            xml_files = xml_files[:limit]

        total = 0
        for xml_file in xml_files:
            cons_num = xml_file.stem
            try:
                count = self.ingest_canada_act(cons_num, arch_path)
                total += count
            except Exception as e:
                print(f"Error ingesting {cons_num}: {e}")

        return total

    # -------------------------------------------------------------------------
    # US Code Ingestion
    # -------------------------------------------------------------------------

    def _usc_section_to_rules(
        self,
        section: Section,
        parent_id: str | None = None,
    ) -> Iterator[dict]:
        """Convert a US Code Section to rule dictionaries."""
        # Parse ordinal from section number
        ordinal = None
        sec_num = section.citation.section
        if sec_num.isdigit():
            ordinal = int(sec_num)

        # Build citation path: us/statute/{title}/{section}
        title = section.citation.title
        citation_path = f"us/statute/{title}/{sec_num}"
        section_id = _deterministic_id(citation_path)

        yield {
            "id": section_id,
            "jurisdiction": "us",
            "doc_type": "statute",
            "parent_id": parent_id,
            "level": 0,
            "ordinal": ordinal,
            "heading": section.section_title,
            "body": section.text,
            "effective_date": section.effective_date.isoformat() if section.effective_date else None,
            "source_url": section.source_url,
            "source_path": None,
            "citation_path": citation_path,
            "rac_path": None,
            "has_rac": False,
        }

        # Recursively yield subsections
        yield from self._usc_subsections_to_rules(
            section.subsections,
            parent_id=section_id,
            level=1,
            parent_path=citation_path,
        )

    def _usc_subsections_to_rules(
        self,
        subsections: list[Subsection],
        parent_id: str,
        level: int,
        parent_path: str | None = None,
    ) -> Iterator[dict]:
        """Convert US Code subsections to rule dictionaries."""
        for i, sub in enumerate(subsections):
            # Use identifier if available (a, b, 1, 2, etc.), else ordinal
            sub_key = sub.identifier if hasattr(sub, 'identifier') and sub.identifier else str(i + 1)
            citation_path = f"{parent_path}/{sub_key}" if parent_path else None
            sub_id = _deterministic_id(citation_path) if citation_path else str(uuid4())

            yield {
                "id": sub_id,
                "jurisdiction": "us",
                "doc_type": "statute",
                "parent_id": parent_id,
                "level": level,
                "ordinal": i + 1,
                "heading": sub.heading,
                "body": sub.text,
                "effective_date": None,
                "source_url": None,
                "source_path": None,
                "citation_path": citation_path,
                "rac_path": None,
                "has_rac": False,
            }

            if sub.children:
                yield from self._usc_subsections_to_rules(
                    sub.children,
                    parent_id=sub_id,
                    level=level + 1,
                    parent_path=citation_path,
                )

    def ingest_usc_title(
        self,
        title_num: int,
        uscode_path: Path | None = None,
        batch_size: int = 50,
    ) -> int:
        """Ingest a US Code title into the rules table.

        Args:
            title_num: Title number (e.g., 26 for IRC)
            uscode_path: Path to uscode directory (default arch/data/uscode)
            batch_size: Number of rules to insert per batch

        Returns:
            Total number of rules inserted
        """
        if uscode_path is None:
            # Default to arch repo's data/uscode directory
            uscode_path = Path(__file__).parent.parent.parent.parent / "data" / "uscode"

        xml_path = uscode_path / f"usc{title_num}.xml"
        if not xml_path.exists():
            raise FileNotFoundError(f"Not found: {xml_path}")

        parser = USLMParser(xml_path)
        total_inserted = 0
        batch: list[dict] = []

        title_name = parser.get_title_name()
        print(f"Ingesting Title {title_num}: {title_name}...")

        for section in parser.iter_sections():
            for rule in self._usc_section_to_rules(section):
                batch.append(rule)

                if len(batch) >= batch_size:
                    inserted = self._upsert_rules(batch)
                    total_inserted += inserted
                    print(f"  Upserted {total_inserted} rules...")
                    batch = []

        if batch:
            inserted = self._upsert_rules(batch)
            total_inserted += inserted

        print(f"Done! Upserted {total_inserted} rules for Title {title_num}")
        return total_inserted

    def ingest_all_usc(
        self,
        uscode_path: Path | None = None,
        titles: list[int] | None = None,
    ) -> int:
        """Ingest all US Code titles.

        Args:
            uscode_path: Path to uscode directory
            titles: Specific titles to ingest (default: all available)

        Returns:
            Total number of rules inserted
        """
        if uscode_path is None:
            uscode_path = Path(__file__).parent.parent.parent.parent / "data" / "uscode"

        if titles is None:
            # Find all available titles
            xml_files = sorted(uscode_path.glob("usc*.xml"))
            titles = []
            for f in xml_files:
                # Extract title number from filename like usc26.xml
                try:
                    title_num = int(f.stem.replace("usc", ""))
                    titles.append(title_num)
                except ValueError:
                    continue

        total = 0
        for title_num in titles:
            try:
                count = self.ingest_usc_title(title_num, uscode_path)
                total += count
            except Exception as e:
                print(f"Error ingesting Title {title_num}: {e}")

        return total

    # -------------------------------------------------------------------------
    # UK Legislation Ingestion
    # -------------------------------------------------------------------------

    def _uk_section_to_rules(
        self,
        section: UKSection,
        parent_id: str | None = None,
    ) -> Iterator[dict]:
        """Convert a UK Section to rule dictionaries."""
        # Determine jurisdiction from extent
        jurisdiction = "uk"
        if section.extent:
            # E+W+S+N.I. -> uk, just E -> uk-eng, etc.
            if section.extent == ["E"]:
                jurisdiction = "uk-eng"
            elif section.extent == ["S"]:
                jurisdiction = "uk-sct"

        ordinal = None
        sec_num = section.citation.section
        if sec_num and sec_num.isdigit():
            ordinal = int(sec_num)

        # Build citation path: uk/statute/{type}/{year}/{chapter}/{section}
        cite = section.citation
        citation_path = f"uk/statute/{cite.type}/{cite.year}/{cite.number}"
        if sec_num:
            citation_path += f"/{sec_num}"
        section_id = _deterministic_id(citation_path)

        yield {
            "id": section_id,
            "jurisdiction": jurisdiction,
            "doc_type": "statute",
            "parent_id": parent_id,
            "level": 0,
            "ordinal": ordinal,
            "heading": section.title,
            "body": section.text,
            "effective_date": section.enacted_date.isoformat() if section.enacted_date else None,
            "source_url": section.source_url,
            "source_path": None,
            "citation_path": citation_path,
            "rac_path": None,
            "has_rac": False,
        }

        yield from self._uk_subsections_to_rules(
            section.subsections,
            parent_id=section_id,
            level=1,
            jurisdiction=jurisdiction,
            parent_path=citation_path,
        )

    def _uk_subsections_to_rules(
        self,
        subsections: list[UKSubsection],
        parent_id: str,
        level: int,
        jurisdiction: str,
        parent_path: str | None = None,
    ) -> Iterator[dict]:
        """Convert UK subsections to rule dictionaries."""
        for i, sub in enumerate(subsections):
            # Use id if available, else ordinal
            sub_key = sub.id if sub.id else str(i + 1)
            citation_path = f"{parent_path}/{sub_key}" if parent_path else None
            sub_id = _deterministic_id(citation_path) if citation_path else str(uuid4())

            yield {
                "id": sub_id,
                "jurisdiction": jurisdiction,
                "doc_type": "statute",
                "parent_id": parent_id,
                "level": level,
                "ordinal": i + 1,
                "heading": None,
                "body": sub.text,
                "effective_date": None,
                "source_url": None,
                "source_path": None,
                "citation_path": citation_path,
                "rac_path": None,
                "has_rac": False,
            }

            if sub.children:
                yield from self._uk_subsections_to_rules(
                    sub.children,
                    parent_id=sub_id,
                    level=level + 1,
                    jurisdiction=jurisdiction,
                    parent_path=citation_path,
                )

    def ingest_uk_act(
        self,
        year: int,
        chapter: int,
        uk_path: Path | None = None,
        batch_size: int = 50,
    ) -> int:
        """Ingest a UK Act into the rules table.

        Args:
            year: Year of the act (e.g., 2020)
            chapter: Chapter number (e.g., 1)
            uk_path: Path to UK legislation directory
            batch_size: Number of rules to insert per batch

        Returns:
            Total number of rules inserted
        """
        if uk_path is None:
            uk_path = Path.home() / ".arch" / "uk" / "ukpga"

        xml_path = uk_path / str(year) / f"{chapter}.xml"
        if not xml_path.exists():
            raise FileNotFoundError(f"Not found: {xml_path}")

        # Read and parse the XML
        xml_content = xml_path.read_text()

        total_inserted = 0
        batch: list[dict] = []

        # Parse act metadata
        try:
            act = parse_act_metadata(xml_content)
            print(f"Ingesting {act.citation.type}/{year}/{chapter}: {act.title}...")
        except Exception:
            print(f"Ingesting ukpga/{year}/{chapter}...")

        # Parse sections (the XML contains the full act)
        try:
            section = parse_section(xml_content)
            for rule in self._uk_section_to_rules(section):
                batch.append(rule)

                if len(batch) >= batch_size:
                    inserted = self._insert_rules(batch)
                    total_inserted += inserted
                    print(f"  Inserted {total_inserted} rules...")
                    batch = []
        except Exception as e:
            print(f"  Warning: Could not parse sections: {e}")

        if batch:
            inserted = self._insert_rules(batch)
            total_inserted += inserted

        print(f"Done! Inserted {total_inserted} rules for ukpga/{year}/{chapter}")
        return total_inserted

    def ingest_all_uk(
        self,
        uk_path: Path | None = None,
        limit: int | None = None,
    ) -> int:
        """Ingest all UK legislation.

        Args:
            uk_path: Path to UK legislation directory
            limit: Max number of acts to process

        Returns:
            Total number of rules inserted
        """
        if uk_path is None:
            uk_path = Path.home() / ".arch" / "uk" / "ukpga"

        # Find all XML files
        xml_files = sorted(uk_path.glob("*/*.xml"))

        if limit:
            xml_files = xml_files[:limit]

        total = 0
        for xml_file in xml_files:
            try:
                year = int(xml_file.parent.name)
                chapter = int(xml_file.stem)
                count = self.ingest_uk_act(year, chapter, uk_path)
                total += count
            except Exception as e:
                print(f"Error ingesting {xml_file}: {e}")

        return total

    # -------------------------------------------------------------------------
    # US State Statute Ingestion
    # -------------------------------------------------------------------------

    def _state_section_to_rules(
        self,
        section: Section,
        state_code: str,
        parent_id: str | None = None,
    ) -> Iterator[dict]:
        """Convert a state statute Section to rule dictionaries.

        Args:
            section: Parsed Section from USLM-style XML
            state_code: Two-letter state code (e.g., "oh", "ca")
            parent_id: Parent rule ID for hierarchy
        """
        # Parse ordinal from section number
        ordinal = None
        sec_num = section.citation.section
        # Try to extract a numeric prefix for ordering
        match = re.match(r'(\d+)', sec_num) if sec_num else None
        if match:
            ordinal = int(match.group(1))

        # Build citation path: us-{state}/statute/{title}/{section}
        title = section.citation.title
        citation_path = f"us-{state_code}/statute/{title}/{sec_num}"
        section_id = _deterministic_id(citation_path)

        yield {
            "id": section_id,
            "jurisdiction": f"us-{state_code}",
            "doc_type": "statute",
            "parent_id": parent_id,
            "level": 0,
            "ordinal": ordinal,
            "heading": section.section_title,
            "body": section.text,
            "effective_date": section.effective_date.isoformat() if section.effective_date else None,
            "source_url": section.source_url,
            "source_path": None,
            "citation_path": citation_path,
            "rac_path": None,
            "has_rac": False,
        }

        # Recursively yield subsections
        yield from self._state_subsections_to_rules(
            section.subsections,
            state_code=state_code,
            parent_id=section_id,
            level=1,
            parent_path=citation_path,
        )

    def _state_subsections_to_rules(
        self,
        subsections: list[Subsection],
        state_code: str,
        parent_id: str,
        level: int,
        parent_path: str | None = None,
    ) -> Iterator[dict]:
        """Convert state statute subsections to rule dictionaries."""
        for i, sub in enumerate(subsections):
            sub_key = sub.identifier if hasattr(sub, 'identifier') and sub.identifier else str(i + 1)
            citation_path = f"{parent_path}/{sub_key}" if parent_path else None
            sub_id = _deterministic_id(citation_path) if citation_path else str(uuid4())

            yield {
                "id": sub_id,
                "jurisdiction": f"us-{state_code}",
                "doc_type": "statute",
                "parent_id": parent_id,
                "level": level,
                "ordinal": i + 1,
                "heading": sub.heading,
                "body": sub.text,
                "effective_date": None,
                "source_url": None,
                "source_path": None,
                "citation_path": citation_path,
                "rac_path": None,
                "has_rac": False,
            }

            if sub.children:
                yield from self._state_subsections_to_rules(
                    sub.children,
                    state_code=state_code,
                    parent_id=sub_id,
                    level=level + 1,
                    parent_path=citation_path,
                )

    def ingest_state_uslm(
        self,
        xml_path: Path | str,
        state_code: str,
        batch_size: int = 50,
    ) -> int:
        """Ingest a state statute USLM XML file into the rules table.

        Args:
            xml_path: Path to USLM-style XML file
            state_code: Two-letter state code (e.g., "oh", "ca")
            batch_size: Number of rules to insert per batch

        Returns:
            Total number of rules inserted
        """
        xml_path = Path(xml_path)
        if not xml_path.exists():
            raise FileNotFoundError(f"Not found: {xml_path}")

        parser = USLMParser(xml_path)
        total_inserted = 0

        title_name = parser.get_title_name()
        print(f"Ingesting {state_code.upper()} - {title_name}...")

        for section in parser.iter_sections():
            # Collect all rules for this section
            all_rules = list(self._state_section_to_rules(section, state_code))

            # Deduplicate by citation_path (HTML parsing may create duplicates)
            seen_paths: set[str] = set()
            unique_rules: list[dict] = []
            for rule in all_rules:
                path = rule.get("citation_path", "")
                if path and path not in seen_paths:
                    seen_paths.add(path)
                    unique_rules.append(rule)

            # Insert in batches
            for i in range(0, len(unique_rules), batch_size):
                batch = unique_rules[i : i + batch_size]
                inserted = self._upsert_rules(batch)
                total_inserted += inserted
                print(f"  Upserted {total_inserted} rules...")

        print(f"Done! Upserted {total_inserted} rules for {state_code.upper()}")
        return total_inserted

    # -------------------------------------------------------------------------
    # State Ingestion (via StateOrchestrator)
    # -------------------------------------------------------------------------

    def ingest_state(self, state_code: str) -> int:
        """Ingest all statutes for a state from local HTML files.

        Delegates to StateOrchestrator for file discovery, parsing, and upload.

        Args:
            state_code: Two-letter state code (e.g., "oh", "ca")

        Returns:
            Number of rules upserted
        """
        from atlas.ingest.state_orchestrator import StateOrchestrator
        from atlas.ingest.rule_uploader import RuleUploader

        uploader = RuleUploader(url=self.url, key=self.key)
        orch = StateOrchestrator(uploader=uploader)
        return orch.ingest_state(state_code)

    def ingest_all_states(self) -> dict[str, int]:
        """Ingest all states that have local HTML data.

        Returns:
            Dict mapping state code to number of rules upserted
        """
        from atlas.ingest.state_orchestrator import StateOrchestrator
        from atlas.ingest.rule_uploader import RuleUploader

        uploader = RuleUploader(url=self.url, key=self.key)
        orch = StateOrchestrator(uploader=uploader)
        return orch.ingest_all_states()
