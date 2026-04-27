#!/usr/bin/env python3
"""Convert Massachusetts General Laws to Akoma Ntoso XML format.

This script fetches Massachusetts statutes from malegislature.gov using the
MAConverter and converts them to Akoma Ntoso XML format following the OASIS standard.

Usage:
    python scripts/convert_ma_to_akn.py

Output:
    Creates AKN XML files in /tmp/rules-us-ma-akn/
"""

import re
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.ma import (
    MAConverter,
    MA_TAX_CHAPTERS,
    MA_WELFARE_CHAPTERS,
)

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-ma-akn")


def register_namespace():
    """Register the Akoma Ntoso namespace."""
    ET.register_namespace("", AKN_NS)
    ET.register_namespace("akn", AKN_NS)


def make_element(tag: str, attrib: dict = None, text: str = None) -> ET.Element:
    """Create an element in the AKN namespace."""
    elem = ET.Element(f"{{{AKN_NS}}}{tag}", attrib or {})
    if text:
        elem.text = text
    return elem


def make_subelement(
    parent: ET.Element, tag: str, attrib: dict = None, text: str = None
) -> ET.Element:
    """Create a subelement in the AKN namespace."""
    elem = ET.SubElement(parent, f"{{{AKN_NS}}}{tag}", attrib or {})
    if text:
        elem.text = text
    return elem


def sanitize_id(text: str) -> str:
    """Convert text to a valid XML ID."""
    text = re.sub(r"[^\w\s-]", "", str(text))
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"-+", "-", text)
    return text.lower()[:50]


def create_akn_chapter_document(chapter_num: str, chapter_name: str, sections: list) -> ET.Element:
    """Create an Akoma Ntoso document for a chapter."""
    # Root element
    root = make_element("akomaNtoso")

    # Act container
    act = make_subelement(root, "act", {"name": f"chapter-{chapter_num}"})

    # Meta section
    meta = make_subelement(act, "meta")

    # Identification
    today = date.today().isoformat()
    work_uri = f"/akn/us-ma/act/mgl/chapter-{chapter_num}"
    expr_uri = f"{work_uri}/eng@{today}"
    manif_uri = f"{expr_uri}/main"

    identification = make_subelement(meta, "identification", {"source": "#axiom-foundation"})

    # FRBRWork
    frbr_work = make_subelement(identification, "FRBRWork")
    make_subelement(frbr_work, "FRBRthis", {"value": work_uri})
    make_subelement(frbr_work, "FRBRuri", {"value": work_uri})
    make_subelement(frbr_work, "FRBRdate", {"date": today, "name": "generation"})
    make_subelement(frbr_work, "FRBRauthor", {"href": "#massachusetts-legislature"})
    make_subelement(frbr_work, "FRBRcountry", {"value": "us-ma"})
    make_subelement(frbr_work, "FRBRnumber", {"value": chapter_num})
    make_subelement(frbr_work, "FRBRname", {"value": "Massachusetts General Laws"})

    # FRBRExpression
    frbr_expr = make_subelement(identification, "FRBRExpression")
    make_subelement(frbr_expr, "FRBRthis", {"value": expr_uri})
    make_subelement(frbr_expr, "FRBRuri", {"value": expr_uri})
    make_subelement(frbr_expr, "FRBRdate", {"date": today, "name": "generation"})
    make_subelement(frbr_expr, "FRBRauthor", {"href": "#axiom-foundation"})
    make_subelement(frbr_expr, "FRBRlanguage", {"language": "eng"})

    # FRBRManifestation
    frbr_manif = make_subelement(identification, "FRBRManifestation")
    make_subelement(frbr_manif, "FRBRthis", {"value": manif_uri})
    make_subelement(frbr_manif, "FRBRuri", {"value": manif_uri})
    make_subelement(frbr_manif, "FRBRdate", {"date": today, "name": "generation"})
    make_subelement(frbr_manif, "FRBRauthor", {"href": "#axiom-foundation"})

    # References
    references = make_subelement(meta, "references", {"source": "#axiom-foundation"})
    make_subelement(
        references,
        "TLCOrganization",
        {
            "eId": "massachusetts-legislature",
            "href": "https://malegislature.gov",
            "showAs": "Massachusetts Legislature",
        },
    )
    make_subelement(
        references,
        "TLCOrganization",
        {
            "eId": "axiom-foundation",
            "href": "https://axiom-foundation.org",
            "showAs": "The Axiom Foundation",
        },
    )

    # Body
    body = make_subelement(act, "body")

    # Chapter as top-level container
    ch_id = f"chap_{sanitize_id(chapter_num)}"
    ch_elem = make_subelement(body, "chapter", {"eId": ch_id})
    make_subelement(ch_elem, "num", text=f"Chapter {chapter_num}")
    make_subelement(ch_elem, "heading", text=chapter_name)

    # Add sections
    for section in sections:
        add_section_element(ch_elem, section)

    return root


def add_section_element(parent: ET.Element, section):
    """Add a section element from the Section model."""
    # Extract section number from citation (e.g., "MA-62-2" -> "2")
    section_num = section.citation.section.split("-")[-1]
    sec_id = f"sec_{sanitize_id(section_num)}"

    sec_elem = make_subelement(parent, "section", {"eId": sec_id})
    make_subelement(sec_elem, "num", text=f"Section {section_num}")
    make_subelement(sec_elem, "heading", text=section.section_title or f"Section {section_num}")

    # Add subsections
    if section.subsections:
        for subsection in section.subsections:
            add_subsection_element(sec_elem, subsection)
    elif section.text:
        # No subsections, just add the text
        content = make_subelement(sec_elem, "content")
        # Split text into paragraphs
        paragraphs = section.text.split("\n\n")
        for para in paragraphs[:10]:  # Limit paragraphs
            if para.strip():
                p = make_subelement(content, "p")
                p.text = para.strip()[:2000]  # Limit text length


def add_subsection_element(parent: ET.Element, subsection, level: int = 1):
    """Add a subsection element recursively."""
    sub_id = f"subsec_{sanitize_id(subsection.identifier)}"

    # Use appropriate element based on level
    if level == 1:
        elem_name = "subsection"
    elif level == 2:
        elem_name = "paragraph"
    else:
        elem_name = "subparagraph"

    sub_elem = make_subelement(parent, elem_name, {"eId": sub_id})
    make_subelement(sub_elem, "num", text=f"({subsection.identifier})")

    if subsection.heading:
        make_subelement(sub_elem, "heading", text=subsection.heading)

    # Add text content
    if subsection.text:
        content = make_subelement(sub_elem, "content")
        p = make_subelement(content, "p")
        p.text = subsection.text[:2000]  # Limit text length

    # Add children recursively
    for child in subsection.children:
        add_subsection_element(sub_elem, child, level + 1)


def indent_xml(elem: ET.Element, level: int = 0):
    """Add indentation to XML for pretty printing."""
    i = "\n" + "  " * level
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for child in elem:
            indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def write_akn_file(root: ET.Element, output_path: Path):
    """Write the AKN XML to a file."""
    indent_xml(root)
    tree = ET.ElementTree(root)

    with open(output_path, "wb") as f:
        tree.write(f, encoding="UTF-8", xml_declaration=True)


def main():
    """Main conversion function."""
    register_namespace()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Combine all chapters to fetch
    all_chapters = {**MA_TAX_CHAPTERS, **MA_WELFARE_CHAPTERS}

    # Track statistics
    stats = {
        "total_chapters": len(all_chapters),
        "converted": 0,
        "sections": 0,
        "errors": 0,
        "chapters_with_errors": [],
    }

    print(f"Converting {len(all_chapters)} Massachusetts General Laws chapters to Akoma Ntoso XML")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 60)

    with MAConverter(rate_limit_delay=0.5) as converter:
        for chapter_num, chapter_name in all_chapters.items():
            print(f"\nProcessing Chapter {chapter_num}: {chapter_name}")

            try:
                # Fetch all sections in this chapter
                sections = list(converter.iter_chapter(chapter_num))

                if not sections:
                    print(f"  Warning: No sections found for Chapter {chapter_num}")
                    stats["chapters_with_errors"].append(chapter_num)
                    continue

                print(f"  Fetched {len(sections)} sections")

                # Create AKN document
                akn_root = create_akn_chapter_document(chapter_num, chapter_name, sections)

                # Write output
                output_filename = f"mgl-chapter-{chapter_num}.akn.xml"
                output_path = OUTPUT_DIR / output_filename
                write_akn_file(akn_root, output_path)

                # Update stats
                stats["converted"] += 1
                stats["sections"] += len(sections)

                print(f"  -> {output_filename} ({len(sections)} sections)")

            except Exception as e:
                print(f"  Error processing Chapter {chapter_num}: {e}")
                stats["errors"] += 1
                stats["chapters_with_errors"].append(chapter_num)

    # Print summary
    print("\n" + "=" * 60)
    print("Conversion Summary")
    print("=" * 60)
    print(f"Total chapters attempted: {stats['total_chapters']}")
    print(f"Successfully converted:   {stats['converted']}")
    print(f"Errors:                   {stats['errors']}")
    print(f"Total sections:           {stats['sections']}")
    print(f"Output directory:         {OUTPUT_DIR}")

    if stats["chapters_with_errors"]:
        print(f"\nChapters with errors: {', '.join(stats['chapters_with_errors'])}")

    print("=" * 60)


if __name__ == "__main__":
    main()
