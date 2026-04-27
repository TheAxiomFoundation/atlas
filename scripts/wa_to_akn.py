#!/usr/bin/env python3
"""Convert Washington RCW statutes to Akoma Ntoso XML.

This script fetches Washington Revised Code (RCW) statutes from app.leg.wa.gov
and converts them to Akoma Ntoso XML format, outputting to /tmp/rules-us-wa-akn/.

Usage:
    python scripts/wa_to_akn.py                    # Convert key tax/benefit chapters
    python scripts/wa_to_akn.py --title 82         # Convert all of Title 82
    python scripts/wa_to_akn.py --chapter 82.04    # Convert single chapter
    python scripts/wa_to_akn.py --all              # Convert all titles (slow!)
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.wa import (
    WAConverter,
    WA_EXCISE_TAX_CHAPTERS,
    WA_PUBLIC_ASSISTANCE_CHAPTERS,
    WA_TITLES,
)
from atlas.models import Section, Subsection


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-wa-akn")


def section_to_akn_xml(section: Section) -> str:
    """Convert an arch Section to Akoma Ntoso XML.

    Args:
        section: Section model from WAConverter

    Returns:
        XML string in Akoma Ntoso format
    """
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Parse section number from citation (e.g., "WA-82.04.290" -> "82.04.290")
    section_num = section.citation.section.replace("WA-", "")
    section_id_safe = section_num.replace(".", "_")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#wa-legislature")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/us-wa/act/rcw/{section_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/us-wa/act/rcw/{section_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", str(date.today()))
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#wa-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-wa")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/us-wa/act/rcw/{section_num}/eng")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/us-wa/act/rcw/{section_num}/eng")
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", str(section.retrieved_at or date.today()))
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#wa-legislature")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", f"/us-wa/act/rcw/{section_num}/eng/akn")
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/us-wa/act/rcw/{section_num}/eng/akn")
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", str(date.today()))
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    org_legislature = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_legislature.set("eId", "wa-legislature")
    org_legislature.set("href", "/ontology/organization/us-wa/legislature")
    org_legislature.set("showAs", "Washington State Legislature")

    org_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_rf.set("eId", "axiom-foundation")
    org_rf.set("href", "https://axiom-foundation.org")
    org_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Section element
    section_elem = ET.SubElement(body, f"{{{AKN_NS}}}section")
    section_elem.set("eId", f"sec_{section_id_safe}")

    # Section number
    num = ET.SubElement(section_elem, f"{{{AKN_NS}}}num")
    num.text = f"RCW {section_num}"

    # Section heading
    if section.section_title:
        heading = ET.SubElement(section_elem, f"{{{AKN_NS}}}heading")
        heading.text = section.section_title

    # Add subsections recursively
    def add_subsection(parent_elem: ET.Element, subsec: Subsection, parent_id: str):
        """Add a subsection and its children to the XML."""
        subsec_id = f"{parent_id}__subsec_{subsec.identifier}"
        subsection_elem = ET.SubElement(parent_elem, f"{{{AKN_NS}}}subsection")
        subsection_elem.set("eId", subsec_id)

        subsec_num = ET.SubElement(subsection_elem, f"{{{AKN_NS}}}num")
        subsec_num.text = f"({subsec.identifier})"

        if subsec.text:
            content = ET.SubElement(subsection_elem, f"{{{AKN_NS}}}content")
            p = ET.SubElement(content, f"{{{AKN_NS}}}p")
            # Truncate very long text to keep XML manageable
            text = subsec.text[:5000] if len(subsec.text) > 5000 else subsec.text
            p.text = text

        # Add children recursively
        for child in subsec.children:
            add_subsection(subsection_elem, child, subsec_id)

    # Add all top-level subsections
    for subsec in section.subsections:
        add_subsection(section_elem, subsec, f"sec_{section_id_safe}")

    # If no subsections, add full text as content
    if not section.subsections and section.text:
        content = ET.SubElement(section_elem, f"{{{AKN_NS}}}content")
        # Split text into paragraphs
        paragraphs = section.text.split("\n\n")
        for para in paragraphs[:50]:  # Limit paragraphs
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                text = para.strip()[:5000]  # Truncate long paragraphs
                p.text = text

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ")


def get_chapter_from_section(section_num: str) -> str:
    """Extract chapter number from section number.

    Args:
        section_num: e.g., "82.04.290"

    Returns:
        Chapter number, e.g., "82.04"
    """
    parts = section_num.split(".")
    if len(parts) >= 2:
        return f"{parts[0]}.{parts[1]}"
    return parts[0]


def get_title_from_section(section_num: str) -> str:
    """Extract title number from section number.

    Args:
        section_num: e.g., "82.04.290"

    Returns:
        Title number, e.g., "82"
    """
    return section_num.split(".")[0]


def convert_chapter(converter: WAConverter, chapter: str, output_dir: Path) -> int:
    """Convert a single chapter to AKN.

    Args:
        converter: WAConverter instance
        chapter: Chapter number (e.g., "82.04")
        output_dir: Output directory

    Returns:
        Number of sections converted
    """
    title_num = chapter.split(".")[0]
    title_dir = output_dir / f"title-{title_num}"
    chapter_dir = title_dir / f"chapter-{chapter.replace('.', '-')}"
    chapter_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for section in converter.iter_chapter(chapter):
        section_num = section.citation.section.replace("WA-", "")
        filename = f"{section_num.replace('.', '-')}.xml"
        filepath = chapter_dir / filename

        xml_content = section_to_akn_xml(section)
        filepath.write_text(xml_content, encoding="utf-8")
        count += 1
        print(
            f"    {section_num}: {section.section_title[:50]}..."
            if len(section.section_title) > 50
            else f"    {section_num}: {section.section_title}"
        )

    return count


def convert_title(converter: WAConverter, title: int, output_dir: Path) -> tuple[int, int]:
    """Convert a single title to AKN.

    Args:
        converter: WAConverter instance
        title: Title number (e.g., 82)
        output_dir: Output directory

    Returns:
        Tuple of (chapters_converted, sections_converted)
    """
    chapters = converter.get_title_chapters(title)
    print(f"  Found {len(chapters)} chapters in Title {title}")

    total_sections = 0
    for chapter in chapters:
        print(f"  Chapter {chapter}...")
        count = convert_chapter(converter, chapter, output_dir)
        total_sections += count

    return len(chapters), total_sections


def convert_key_chapters(output_dir: Path) -> dict:
    """Convert key tax and benefit chapters.

    Args:
        output_dir: Output directory

    Returns:
        Statistics dict
    """
    stats = {
        "chapters_converted": 0,
        "sections_converted": 0,
        "errors": [],
    }

    # Combine excise tax and public assistance chapters
    chapters_to_convert = list(WA_EXCISE_TAX_CHAPTERS.keys()) + list(
        WA_PUBLIC_ASSISTANCE_CHAPTERS.keys()
    )

    print(f"Converting {len(chapters_to_convert)} key chapters...")

    with WAConverter(rate_limit_delay=0.3) as converter:
        for chapter in chapters_to_convert:
            chapter_name = WA_EXCISE_TAX_CHAPTERS.get(chapter) or WA_PUBLIC_ASSISTANCE_CHAPTERS.get(
                chapter, ""
            )
            print(f"\nChapter {chapter}: {chapter_name}")

            try:
                count = convert_chapter(converter, chapter, output_dir)
                stats["chapters_converted"] += 1
                stats["sections_converted"] += count
            except Exception as e:
                error_msg = f"Chapter {chapter}: {str(e)}"
                print(f"  ERROR: {e}")
                stats["errors"].append(error_msg)

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Convert Washington RCW statutes to Akoma Ntoso XML"
    )
    parser.add_argument(
        "--title",
        type=int,
        help="Convert all chapters in a specific title (e.g., 82)",
    )
    parser.add_argument(
        "--chapter",
        type=str,
        help="Convert a specific chapter (e.g., 82.04)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Convert all titles (very slow!)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )

    args = parser.parse_args()
    output_dir = args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Washington RCW -> Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output: {output_dir}")
    print()

    if args.chapter:
        # Convert single chapter
        print(f"Converting chapter {args.chapter}...")
        with WAConverter(rate_limit_delay=0.3) as converter:
            count = convert_chapter(converter, args.chapter, output_dir)
        print(f"\nConverted {count} sections")

    elif args.title:
        # Convert single title
        print(f"Converting Title {args.title}...")
        with WAConverter(rate_limit_delay=0.3) as converter:
            chapters, sections = convert_title(converter, args.title, output_dir)
        print(f"\nConverted {chapters} chapters, {sections} sections")

    elif args.all:
        # Convert all titles (slow!)
        print("Converting ALL titles (this will take a while)...")
        total_chapters = 0
        total_sections = 0

        with WAConverter(rate_limit_delay=0.5) as converter:
            for title_str in sorted(
                WA_TITLES.keys(), key=lambda x: (x.isdigit() and int(x) or 0, x)
            ):
                try:
                    title_int = int(title_str) if title_str.isdigit() else 0
                    if title_int > 0:
                        print(f"\nTitle {title_str}: {WA_TITLES[title_str]}")
                        chapters, sections = convert_title(converter, title_int, output_dir)
                        total_chapters += chapters
                        total_sections += sections
                except Exception as e:
                    print(f"  ERROR: {e}")

        print(f"\nTotal: {total_chapters} chapters, {total_sections} sections")

    else:
        # Default: convert key chapters
        stats = convert_key_chapters(output_dir)

        print()
        print("=" * 60)
        print("CONVERSION SUMMARY")
        print("=" * 60)
        print(f"Chapters converted: {stats['chapters_converted']}")
        print(f"Sections converted: {stats['sections_converted']}")
        print(f"Output directory:   {output_dir}")

        if stats["errors"]:
            print()
            print("ERRORS:")
            for err in stats["errors"]:
                print(f"  - {err}")

    # List output structure
    print()
    print("OUTPUT STRUCTURE:")
    for title_dir in sorted(output_dir.glob("title-*")):
        chapter_count = len(list(title_dir.glob("chapter-*")))
        section_count = len(list(title_dir.glob("*/*.xml")))
        print(f"  {title_dir.name}: {chapter_count} chapters, {section_count} sections")

    return 0


if __name__ == "__main__":
    sys.exit(main())
