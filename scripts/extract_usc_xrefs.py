#!/usr/bin/env python3
"""Extract cross-references from US Code XML and populate rac.rule_dependencies.

Uses structured <ref href="/us/usc/t26/s151"> tags instead of regex parsing.
"""

import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

import psycopg2
from psycopg2.extras import execute_values


# Namespace for USLM XML
NS = {"uslm": "http://xml.house.gov/schemas/uslm/1.0"}


def parse_usc_href(href: str) -> tuple[str, str, str] | None:
    """Parse /us/usc/t26/s151 into citation_path components.

    Returns None if not a valid USC reference.
    The DB stores paths as us/statute/26/151, not us/usc/26/151.
    """
    # Pattern: /us/usc/t{title}/s{section}[/{subsection}]
    match = re.match(r"/us/usc/t(\d+)/s(\d+[A-Za-z]?)(?:/(.+))?", href)
    if match:
        title = match.group(1)
        section = match.group(2)
        subsection = match.group(3)  # May be None
        if subsection:
            # Convert subsection path: a/1 -> a/1
            return ("us", title, f"{section}/{subsection}")
        return ("us", title, section)
    return None


def build_citation_path(jurisdiction: str, title: str, section: str) -> str:
    """Build citation_path in DB format: us/statute/26/32."""
    return f"{jurisdiction}/statute/{title}/{section}"


def extract_refs_from_section(
    section_elem: ET.Element, section_id: str
) -> Iterator[tuple[str, str, str, str]]:
    """Extract all <ref> cross-references from a section element.

    Yields: (from_citation_path, to_citation_path, to_citation_raw, reference_type)
    """
    # Parse the from section identifier
    from_parts = parse_section_id(section_id)
    if not from_parts:
        return

    from_jurisdiction, from_title, from_section = from_parts
    from_path = f"{from_jurisdiction}/statute/{from_title}/{from_section}"

    # Find all ref elements
    for ref in section_elem.iter("{http://xml.house.gov/schemas/uslm/1.0}ref"):
        href = ref.get("href", "")
        ref_text = "".join(ref.itertext()).strip()

        if not href or not ref_text:
            continue

        parsed = parse_usc_href(href)
        if not parsed:
            continue

        to_jurisdiction, to_title, to_section = parsed
        to_path = f"{to_jurisdiction}/statute/{to_title}/{to_section}"

        # Determine reference type
        if to_title == from_title:
            if to_section.startswith(from_section.split("/")[0]):
                ref_type = "internal_subsection"
            else:
                ref_type = "internal_section"
        else:
            ref_type = "external_title"

        yield (from_path, to_path, ref_text[:200], ref_type)


def parse_section_id(identifier: str) -> tuple[str, str, str] | None:
    """Parse /us/usc/t26/s32 into (jurisdiction, title, section).

    The XML identifier is like /us/usc/t26/s32 (section level only).
    We return just the section number, not subsections.
    """
    match = re.match(r"/us/usc/t(\d+)/s(\d+[A-Za-z]?)", identifier)
    if match:
        title = match.group(1)
        section = match.group(2)
        return ("us", title, section)
    return None


def process_xml_file(xml_path: Path) -> Iterator[tuple[str, str, str, str]]:
    """Process a single USC XML file and yield cross-references."""
    print(f"Processing {xml_path.name}...")

    # Parse with namespace handling
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Find all section elements
    for section in root.iter("{http://xml.house.gov/schemas/uslm/1.0}section"):
        section_id = section.get("identifier", "")
        if not section_id:
            continue

        yield from extract_refs_from_section(section, section_id)


def main():
    """Extract cross-references from all USC XML files."""
    # Database connection - use pooler URL
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        db_url = "postgresql://postgres.nsupqhfchdtqclomlrgs:YlpJXpBsy0NzLqxV@aws-0-us-west-2.pooler.supabase.com:5432/postgres"

    # USC XML directory
    usc_dir = Path(__file__).parent.parent / "data" / "uscode"

    if not usc_dir.exists():
        print(f"USC directory not found: {usc_dir}")
        return

    # Collect all cross-references
    all_refs = []

    # Process Title 26 first (most important for tax)
    title26_path = usc_dir / "usc26.xml"
    if title26_path.exists():
        for ref in process_xml_file(title26_path):
            all_refs.append(ref)

    print(f"Extracted {len(all_refs)} cross-references from Title 26")

    # Insert into database
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            # Clear existing dependencies (we'll repopulate from XML)
            cur.execute("DELETE FROM rac.rule_dependencies")
            deleted = cur.rowcount
            print(f"Deleted {deleted} existing dependencies")

            # Get all rules for Title 26 with their IDs
            cur.execute(
                """
                SELECT id, citation_path
                FROM akn.rules
                WHERE citation_path LIKE 'us/statute/26/%'
            """
            )
            rules = cur.fetchall()
            print(f"Found {len(rules)} rules in Title 26")

            # Build lookup: section_path -> [(rule_id, full_path), ...]
            section_to_rules = {}
            for rule_id, full_path in rules:
                # Extract section from path like us/statute/26/32/a/1 -> us/statute/26/32
                parts = full_path.split("/")
                if len(parts) >= 4:
                    section_path = "/".join(parts[:4])  # us/statute/26/32
                    if section_path not in section_to_rules:
                        section_to_rules[section_path] = []
                    section_to_rules[section_path].append((rule_id, full_path))

            print(f"Built lookup for {len(section_to_rules)} sections")

            # Prepare insert data - for each ref, apply to all subsections of that section
            insert_data = []
            for from_path, to_path, to_raw, ref_type in all_refs:
                # from_path is like us/statute/26/32
                matching_rules = section_to_rules.get(from_path, [])
                for rule_id, full_path in matching_rules:
                    insert_data.append((rule_id, full_path, to_path, to_raw, ref_type))

            print(f"Prepared {len(insert_data)} dependency records")

            # Bulk insert in batches
            batch_size = 10000
            total_inserted = 0
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i : i + batch_size]
                execute_values(
                    cur,
                    """
                    INSERT INTO rac.rule_dependencies
                    (from_rule_id, from_citation_path, to_citation_path, to_citation_raw, reference_type)
                    VALUES %s
                    ON CONFLICT (from_citation_path, to_citation_raw) DO NOTHING
                """,
                    batch,
                )
                total_inserted += cur.rowcount

            print(f"Inserted {total_inserted} cross-references")
            conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
