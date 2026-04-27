#!/usr/bin/env python3
"""Convert Hawaii Revised Statutes to Akoma Ntoso XML.

This script fetches Hawaii Revised Statutes from the Hawaii Capitol website
(capitol.hawaii.gov) using the HIConverter and converts them to Akoma Ntoso XML format.

Hawaii Revised Statutes Structure:
- Divisions (e.g., Division 1: Government)
- Titles (e.g., Title 14: Taxation)
- Chapters (e.g., Chapter 235: Income Tax Law)
- Parts (e.g., Part I: General Provisions)
- Sections (e.g., 235-1: Definitions)

Key chapters for tax/benefit analysis:
- Tax chapters: 231-257
- Welfare chapters: 346-398

Usage:
    python scripts/hi_to_akn.py               # Convert all tax chapters
    python scripts/hi_to_akn.py --chapter 235 # Convert specific chapter
    python scripts/hi_to_akn.py --welfare     # Convert welfare chapters
    python scripts/hi_to_akn.py --all         # Convert all tax + welfare
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.hi import (
    HIConverter,
    HI_TAX_CHAPTERS,
    HI_WELFARE_CHAPTERS,
)
from atlas.models import Section


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def section_to_akn_xml(section: Section, chapter_num: int, chapter_title: str) -> str:
    """Convert a Section model to Akoma Ntoso XML string.

    Args:
        section: Section model from HIConverter
        chapter_num: Chapter number (e.g., 235)
        chapter_title: Chapter title (e.g., "Income Tax Law")

    Returns:
        Akoma Ntoso XML string
    """
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Extract section number from citation (HI-235-51 -> 235-51)
    section_num = section.citation.section.replace("HI-", "")
    section_id = section_num.replace("-", "_").replace(".", "_")

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#hi-legislature")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/us-hi/act/hrs/{section_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/us-hi/act/hrs/{section_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", str(date.today()))
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#hi-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-hi")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/us-hi/act/hrs/{section_num}/eng")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/us-hi/act/hrs/{section_num}/eng")
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", str(date.today()))
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#hi-legislature")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", f"/us-hi/act/hrs/{section_num}/eng/akn")
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/us-hi/act/hrs/{section_num}/eng/akn")
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", str(date.today()))
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    org_legislature = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_legislature.set("eId", "hi-legislature")
    org_legislature.set("href", "/ontology/organization/us-hi/legislature")
    org_legislature.set("showAs", "Hawaii Legislature")

    org_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_rf.set("eId", "axiom-foundation")
    org_rf.set("href", "https://axiom-foundation.org")
    org_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Chapter container
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{chapter_num}")

    chap_num = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    chap_num.text = f"Chapter {chapter_num}"

    chap_heading = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    chap_heading.text = chapter_title

    # Section element
    sec = ET.SubElement(chapter, f"{{{AKN_NS}}}section")
    sec.set("eId", f"sec_{section_id}")

    # Section number
    num = ET.SubElement(sec, f"{{{AKN_NS}}}num")
    num.text = section_num

    # Section heading
    if section.section_title:
        heading = ET.SubElement(sec, f"{{{AKN_NS}}}heading")
        heading.text = section.section_title

    # Content
    if section.text:
        content = ET.SubElement(sec, f"{{{AKN_NS}}}content")
        # Split text into paragraphs
        paragraphs = section.text.split("\n\n")
        for para in paragraphs:
            para = para.strip()
            if para:
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para

    # Subsections
    for subsec in section.subsections:
        add_subsection(sec, subsec, section_id, 0)

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ")


def add_subsection(parent: ET.Element, subsec, parent_id: str, level: int) -> None:
    """Add a subsection element recursively.

    Args:
        parent: Parent XML element
        subsec: Subsection model
        parent_id: Parent element ID
        level: Nesting level (0=subsection, 1=paragraph, etc.)
    """
    # Element names by level
    level_names = ["subsection", "paragraph", "subparagraph", "clause", "subclause"]
    elem_name = level_names[min(level, len(level_names) - 1)]

    # Build eId
    level_abbrevs = ["subsec", "para", "subpara", "clause", "subclause"]
    level_abbrev = level_abbrevs[min(level, len(level_abbrevs) - 1)]
    subsec_id = f"{parent_id}__{level_abbrev}_{subsec.identifier}"

    subsection = ET.SubElement(parent, f"{{{AKN_NS}}}{elem_name}")
    subsection.set("eId", subsec_id)

    subsec_num = ET.SubElement(subsection, f"{{{AKN_NS}}}num")
    subsec_num.text = f"({subsec.identifier})"

    if subsec.heading:
        subsec_heading = ET.SubElement(subsection, f"{{{AKN_NS}}}heading")
        subsec_heading.text = subsec.heading

    if subsec.text:
        subsec_content = ET.SubElement(subsection, f"{{{AKN_NS}}}content")
        subsec_p = ET.SubElement(subsec_content, f"{{{AKN_NS}}}p")
        subsec_p.text = subsec.text

    # Recurse for children
    for child in subsec.children:
        add_subsection(subsection, child, subsec_id, level + 1)


def convert_chapter(
    converter: HIConverter,
    chapter_num: int,
    output_dir: Path,
) -> tuple[int, int]:
    """Convert all sections in a chapter to AKN XML.

    Args:
        converter: HIConverter instance
        chapter_num: Chapter number (e.g., 235)
        output_dir: Output directory path

    Returns:
        Tuple of (sections_converted, sections_failed)
    """
    chapter_title = (
        HI_TAX_CHAPTERS.get(chapter_num)
        or HI_WELFARE_CHAPTERS.get(chapter_num)
        or f"Chapter {chapter_num}"
    )

    print(f"  Converting Chapter {chapter_num}: {chapter_title}")

    # Create chapter directory
    chapter_dir = output_dir / f"chapter-{chapter_num}"
    chapter_dir.mkdir(parents=True, exist_ok=True)

    converted = 0
    failed = 0

    try:
        # Get section numbers in chapter
        section_numbers = converter.get_chapter_section_numbers(chapter_num)
        print(f"    Found {len(section_numbers)} sections")

        for section_num in section_numbers:
            try:
                section = converter.fetch_section(section_num)

                # Convert to AKN
                xml_content = section_to_akn_xml(section, chapter_num, chapter_title)

                # Write to file
                filename = f"{section_num.replace('.', '-')}.xml"
                filepath = chapter_dir / filename
                filepath.write_text(xml_content, encoding="utf-8")

                converted += 1

            except Exception as e:
                print(f"    Warning: Failed to convert {section_num}: {e}")
                failed += 1
                continue

    except Exception as e:
        print(f"    Error fetching chapter {chapter_num}: {e}")
        return 0, 1

    print(f"    Converted {converted} sections ({failed} failed)")
    return converted, failed


def main():
    parser = argparse.ArgumentParser(
        description="Convert Hawaii Revised Statutes to Akoma Ntoso XML"
    )
    parser.add_argument(
        "--chapter",
        type=int,
        help="Convert a specific chapter (e.g., 235)",
    )
    parser.add_argument(
        "--welfare",
        action="store_true",
        help="Convert welfare chapters (346-398)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Convert all tax and welfare chapters",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="/tmp/rules-us-hi-akn",
        help="Output directory (default: /tmp/rules-us-hi-akn)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.5,
        help="Seconds between requests (default: 0.5)",
    )

    args = parser.parse_args()

    # Determine which chapters to convert
    if args.chapter:
        chapters = [args.chapter]
    elif args.welfare:
        chapters = list(HI_WELFARE_CHAPTERS.keys())
    elif args.all:
        chapters = list(HI_TAX_CHAPTERS.keys()) + list(HI_WELFARE_CHAPTERS.keys())
    else:
        # Default: tax chapters
        chapters = list(HI_TAX_CHAPTERS.keys())

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Hawaii Revised Statutes to Akoma Ntoso Converter")
    print(f"  Output: {output_dir}")
    print(f"  Chapters: {len(chapters)}")
    print(f"  Rate limit: {args.rate_limit}s")
    print()

    total_converted = 0
    total_failed = 0

    with HIConverter(rate_limit_delay=args.rate_limit) as converter:
        for chapter in chapters:
            converted, failed = convert_chapter(converter, chapter, output_dir)
            total_converted += converted
            total_failed += failed

    print()
    print("=" * 50)
    print(f"Complete: {total_converted} sections converted, {total_failed} failed")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
