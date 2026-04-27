#!/usr/bin/env python3
"""Convert Missouri Revised Statutes to Akoma Ntoso XML format.

This script uses the MOConverter to fetch statutes from revisor.mo.gov
and converts them to Akoma Ntoso XML format following the OASIS standard.

Usage:
    python scripts/convert_mo_to_akn.py               # All tax chapters
    python scripts/convert_mo_to_akn.py --chapter 143 # Single chapter

Output:
    Creates AKN XML files in /tmp/rules-us-mo-akn/
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.mo import (
    MOConverter,
    MOConverterError,
    MO_TAX_CHAPTERS,
    MO_WELFARE_CHAPTERS,
)
from atlas.models import Section, Subsection

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-mo-akn")


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
    import re

    # Remove special characters, replace spaces/dots with underscores
    text = re.sub(r"[^\w\s.-]", "", text)
    text = re.sub(r"[\s.]+", "_", text)
    text = re.sub(r"-+", "-", text)
    return text.lower()[:100]


def create_akn_document(section: Section) -> ET.Element:
    """Create an Akoma Ntoso document from a Section model."""
    # Root element
    root = make_element("akomaNtoso")

    # Extract section number from citation (e.g., "MO-143.011" -> "143.011")
    section_num = section.citation.section.replace("MO-", "")
    chapter_num = section_num.split(".")[0]

    # Act container
    act = make_subelement(root, "act", {"name": f"section-{section_num}"})

    # Meta section
    meta = make_subelement(act, "meta")

    # Identification
    today = date.today().isoformat()
    work_uri = f"/akn/us-mo/act/mrs/section-{section_num}"
    expr_uri = f"{work_uri}/eng@{today}"
    manif_uri = f"{expr_uri}/main"

    identification = make_subelement(meta, "identification", {"source": "#axiom-foundation"})

    # FRBRWork
    frbr_work = make_subelement(identification, "FRBRWork")
    make_subelement(frbr_work, "FRBRthis", {"value": work_uri})
    make_subelement(frbr_work, "FRBRuri", {"value": work_uri})
    make_subelement(frbr_work, "FRBRdate", {"date": today, "name": "generation"})
    make_subelement(frbr_work, "FRBRauthor", {"href": "#missouri-legislature"})
    make_subelement(frbr_work, "FRBRcountry", {"value": "us-mo"})
    make_subelement(frbr_work, "FRBRnumber", {"value": section_num})
    make_subelement(frbr_work, "FRBRname", {"value": "Missouri Revised Statutes"})

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
            "eId": "missouri-legislature",
            "href": "https://revisor.mo.gov",
            "showAs": "Missouri General Assembly",
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

    # Publication/lifecycle
    if section.source_url:
        references_elem = make_subelement(meta, "notes")
        note = make_subelement(references_elem, "note", {"eId": "source"})
        p = make_subelement(note, "p")
        p.text = f"Source: {section.source_url}"

    # Body
    body = make_subelement(act, "body")

    # Section element
    section_id = f"sec_{sanitize_id(section_num)}"
    section_elem = make_subelement(body, "section", {"eId": section_id})

    # Section number
    make_subelement(section_elem, "num", text=section_num)

    # Section heading
    if section.section_title:
        make_subelement(section_elem, "heading", text=section.section_title)

    # Add subsections
    if section.subsections:
        for subsec in section.subsections:
            add_subsection_element(section_elem, subsec, section_id)
    else:
        # Add content as paragraphs if no structured subsections
        content = make_subelement(section_elem, "content")
        # Split text into paragraphs
        paragraphs = section.text.split("\n\n") if section.text else []
        for para in paragraphs[:50]:  # Limit to prevent huge files
            if para.strip():
                p = make_subelement(content, "p")
                p.text = para.strip()[:5000]  # Truncate very long paragraphs

    return root


def add_subsection_element(parent: ET.Element, subsection: Subsection, parent_id: str):
    """Add a subsection element recursively."""
    subsec_id = f"{parent_id}__subsec_{sanitize_id(subsection.identifier)}"

    subsec_elem = make_subelement(parent, "subsection", {"eId": subsec_id})
    make_subelement(subsec_elem, "num", text=f"({subsection.identifier})")

    if subsection.heading:
        make_subelement(subsec_elem, "heading", text=subsection.heading)

    if subsection.text:
        content = make_subelement(subsec_elem, "content")
        p = make_subelement(content, "p")
        p.text = subsection.text[:5000]  # Truncate very long text

    # Add children recursively
    for child in subsection.children:
        add_subsection_element(subsec_elem, child, subsec_id)


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

    # Write with XML declaration
    with open(output_path, "wb") as f:
        tree.write(f, encoding="UTF-8", xml_declaration=True)


def convert_chapter(converter: MOConverter, chapter: int, output_dir: Path) -> tuple[int, int]:
    """Convert all sections in a chapter to AKN.

    Returns (sections_found, sections_converted).
    """
    chapter_dir = output_dir / f"chapter-{chapter}"
    chapter_dir.mkdir(parents=True, exist_ok=True)

    chapter_title = (
        MO_TAX_CHAPTERS.get(chapter) or MO_WELFARE_CHAPTERS.get(chapter) or f"Chapter {chapter}"
    )
    print(f"\nChapter {chapter}: {chapter_title}")
    print(f"  Fetching section list...", end=" ", flush=True)

    try:
        section_numbers = converter.get_chapter_section_numbers(chapter)
    except Exception as e:
        print(f"Error: {e}")
        return 0, 0

    print(f"{len(section_numbers)} sections found")

    converted = 0
    errors = 0

    for section_num in section_numbers:
        try:
            print(f"  {section_num}...", end=" ", flush=True)
            section = converter.fetch_section(section_num)

            # Create AKN document
            akn_root = create_akn_document(section)

            # Write file
            output_filename = f"{section_num.replace('.', '-')}.akn.xml"
            output_path = chapter_dir / output_filename
            write_akn_file(akn_root, output_path)

            print(
                f"OK ({section.section_title[:40]}...)"
                if len(section.section_title) > 40
                else f"OK ({section.section_title})"
            )
            converted += 1

        except MOConverterError as e:
            print(f"Skip: {e}")
            errors += 1
        except Exception as e:
            print(f"Error: {e}")
            errors += 1

    return len(section_numbers), converted


def main():
    parser = argparse.ArgumentParser(description="Convert Missouri statutes to Akoma Ntoso XML")
    parser.add_argument("--chapter", "-c", type=int, help="Convert a specific chapter")
    parser.add_argument("--chapters", "-C", type=str, help="Comma-separated list of chapters")
    parser.add_argument("--all-tax", action="store_true", help="Convert all tax chapters (135-155)")
    parser.add_argument("--all-welfare", action="store_true", help="Convert all welfare chapters")
    parser.add_argument("--output", "-o", type=Path, default=OUTPUT_DIR, help="Output directory")
    args = parser.parse_args()

    register_namespace()

    # Determine which chapters to convert
    if args.chapter:
        chapters = [args.chapter]
    elif args.chapters:
        chapters = [int(c.strip()) for c in args.chapters.split(",")]
    elif args.all_welfare:
        chapters = list(MO_WELFARE_CHAPTERS.keys())
    else:
        # Default to tax chapters
        chapters = list(MO_TAX_CHAPTERS.keys())

    output_dir = args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Missouri Revised Statutes to Akoma Ntoso Converter")
    print(f"=" * 60)
    print(f"Chapters to convert: {chapters}")
    print(f"Output directory: {output_dir}")

    # Track statistics
    stats = {
        "chapters": 0,
        "sections_found": 0,
        "sections_converted": 0,
    }

    with MOConverter(rate_limit_delay=0.3) as converter:
        for chapter in chapters:
            found, converted = convert_chapter(converter, chapter, output_dir)
            stats["chapters"] += 1
            stats["sections_found"] += found
            stats["sections_converted"] += converted

    # Print summary
    print(f"\n{'=' * 60}")
    print("Conversion Summary")
    print(f"{'=' * 60}")
    print(f"Chapters processed:    {stats['chapters']}")
    print(f"Sections found:        {stats['sections_found']}")
    print(f"Sections converted:    {stats['sections_converted']}")
    print(f"Output directory:      {output_dir}")
    print(f"{'=' * 60}")

    return 0 if stats["sections_converted"] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
