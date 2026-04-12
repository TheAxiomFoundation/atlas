"""Pure section-to-rules conversion, no IO.

Extracts the repeated pattern from SupabaseIngestor's 4 near-identical
conversion methods into a single reusable function.
"""

import re
from typing import Iterator
from uuid import uuid5, NAMESPACE_URL

from atlas.models import Section, Subsection


def _deterministic_id(citation_path: str) -> str:
    """Generate deterministic UUID from citation path for idempotent upserts."""
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


def section_to_rules(
    section: Section,
    jurisdiction: str,
    doc_type: str = "statute",
) -> Iterator[dict]:
    """Convert a Section to rule dictionaries.

    Yields rule dicts for the section and all its subsections.

    Args:
        section: Parsed Section model.
        jurisdiction: e.g. "us", "us-oh", "uk".
        doc_type: e.g. "statute", "regulation".
    """
    sec_num = section.citation.section
    title = section.citation.title
    citation_path = f"{jurisdiction}/{doc_type}/{title}/{sec_num}"
    section_id = _deterministic_id(citation_path)

    # Extract numeric prefix for ordinal
    ordinal = None
    match = re.match(r"(\d+)", sec_num) if sec_num else None
    if match:
        ordinal = int(match.group(1))

    yield {
        "id": section_id,
        "jurisdiction": jurisdiction,
        "doc_type": doc_type,
        "parent_id": None,
        "level": 0,
        "ordinal": ordinal,
        "heading": section.section_title,
        "body": section.text,
        "effective_date": (
            section.effective_date.isoformat() if section.effective_date else None
        ),
        "source_url": section.source_url,
        "source_path": None,
        "citation_path": citation_path,
        "rac_path": None,
        "has_rac": False,
    }

    yield from _subsections_to_rules(
        section.subsections,
        jurisdiction=jurisdiction,
        doc_type=doc_type,
        parent_id=section_id,
        level=1,
        parent_path=citation_path,
    )


def _subsections_to_rules(
    subsections: list[Subsection],
    jurisdiction: str,
    doc_type: str,
    parent_id: str,
    level: int,
    parent_path: str,
) -> Iterator[dict]:
    """Convert subsections to rule dictionaries recursively."""
    for i, sub in enumerate(subsections):
        sub_key = sub.identifier if sub.identifier else str(i + 1)
        citation_path = f"{parent_path}/{sub_key}"
        sub_id = _deterministic_id(citation_path)

        yield {
            "id": sub_id,
            "jurisdiction": jurisdiction,
            "doc_type": doc_type,
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
            yield from _subsections_to_rules(
                sub.children,
                jurisdiction=jurisdiction,
                doc_type=doc_type,
                parent_id=sub_id,
                level=level + 1,
                parent_path=citation_path,
            )
