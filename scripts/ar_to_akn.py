#!/usr/bin/env python3
"""Convert Arkansas Code sections to Akoma Ntoso XML.

This script fetches Arkansas Code sections from Justia using the ARConverter
and converts them to Akoma Ntoso XML format, outputting to /tmp/rules-us-ar-akn/.

Arkansas Code Structure:
- Title 26: Taxation
- Title 20: Public Health and Welfare

Usage:
    python scripts/ar_to_akn.py
    python scripts/ar_to_akn.py --title 26 --chapters 51,52  # Specific chapters
    python scripts/ar_to_akn.py --quick  # Just Title 26 Chapter 51 (income tax)
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.ar import (
    ARConverter,
    AR_TAX_CHAPTERS,
    AR_WELFARE_CHAPTERS,
    AR_TITLES,
)

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def section_to_akn_xml(section, title_num: int, chapter_num: int) -> str:
    """Convert an arch Section model to Akoma Ntoso XML.

    Args:
        section: Section model from ARConverter
        title_num: Title number (e.g., 26)
        chapter_num: Chapter number (e.g., 51)

    Returns:
        XML string in Akoma Ntoso format
    """
    # Register namespace
    ET.register_namespace("akn", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification block
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom-foundation")

    # Extract section number from citation (e.g., "AR-26-51-101" -> "26-51-101")
    section_id = section.citation.section.replace("AR-", "")
    section_id_safe = section_id.replace("-", "_")
    today = date.today().isoformat()

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")

    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-ar/act/aca/{section_id}")

    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-ar/act/aca/{section_id}")

    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")

    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#ar-legislature")

    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ar")

    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", section_id)

    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "aca")

    # FRBRExpression
    expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")

    expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/akn/us-ar/act/aca/{section_id}/eng@{today}")

    expr_uri = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-ar/act/aca/{section_id}/eng@{today}")

    expr_date = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", today)
    expr_date.set("name", "generation")

    expr_author = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#axiom-foundation")

    expr_lang = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manifestation = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")

    manif_this = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", f"/akn/us-ar/act/aca/{section_id}/eng@{today}/main.xml")

    manif_uri = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/akn/us-ar/act/aca/{section_id}/eng@{today}/main.xml")

    manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")

    manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    # TLC Organizations
    tlc_ar = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_ar.set("eId", "ar-legislature")
    tlc_ar.set("href", "/ontology/organization/us-ar/general-assembly")
    tlc_ar.set("showAs", "Arkansas General Assembly")

    tlc_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_rf.set("eId", "axiom-foundation")
    tlc_rf.set("href", "https://axiom-foundation.org")
    tlc_rf.set("showAs", "The Axiom Foundation")

    tlc_justia = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_justia.set("eId", "justia")
    tlc_justia.set("href", "https://law.justia.com")
    tlc_justia.set("showAs", "Justia")

    # Proprietary (source tracking)
    if section.source_url:
        proprietary = ET.SubElement(meta, f"{{{AKN_NS}}}proprietary")
        source_elem = ET.SubElement(proprietary, f"{{{AKN_NS}}}source")
        source_elem.set("href", section.source_url)
        source_elem.set("showAs", "Justia Arkansas Code")

    # Body section
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Section element
    section_elem = ET.SubElement(body, f"{{{AKN_NS}}}section")
    section_elem.set("eId", f"sec_{section_id_safe}")

    # Section number
    num_elem = ET.SubElement(section_elem, f"{{{AKN_NS}}}num")
    num_elem.text = section_id

    # Section heading
    if section.section_title:
        heading_elem = ET.SubElement(section_elem, f"{{{AKN_NS}}}heading")
        heading_elem.text = section.section_title

    # Content
    content_elem = ET.SubElement(section_elem, f"{{{AKN_NS}}}content")

    # Add main text as paragraphs
    if section.text:
        # Split into paragraphs and add (skip if we have subsections)
        if not section.subsections:
            paragraphs = section.text.split("\n\n")
            for i, para in enumerate(paragraphs):
                para = para.strip()
                if para:
                    p_elem = ET.SubElement(content_elem, f"{{{AKN_NS}}}p")
                    p_elem.text = para[:2000]  # Limit length

    # Add subsections
    for subsec in section.subsections:
        subsec_elem = ET.SubElement(section_elem, f"{{{AKN_NS}}}subsection")
        subsec_elem.set("eId", f"sec_{section_id_safe}__subsec_{subsec.identifier}")

        subsec_num = ET.SubElement(subsec_elem, f"{{{AKN_NS}}}num")
        subsec_num.text = f"({subsec.identifier})"

        if subsec.text:
            subsec_content = ET.SubElement(subsec_elem, f"{{{AKN_NS}}}content")
            subsec_p = ET.SubElement(subsec_content, f"{{{AKN_NS}}}p")
            subsec_p.text = subsec.text[:2000]

        # Add nested subsections (level 2)
        for child in subsec.children:
            child_elem = ET.SubElement(subsec_elem, f"{{{AKN_NS}}}paragraph")
            child_elem.set(
                "eId", f"sec_{section_id_safe}__subsec_{subsec.identifier}__para_{child.identifier}"
            )

            child_num = ET.SubElement(child_elem, f"{{{AKN_NS}}}num")
            child_num.text = f"({child.identifier})"

            if child.text:
                child_content = ET.SubElement(child_elem, f"{{{AKN_NS}}}content")
                child_p = ET.SubElement(child_content, f"{{{AKN_NS}}}p")
                child_p.text = child.text[:2000]

            # Add level 3 subsections
            for grandchild in child.children:
                gc_elem = ET.SubElement(child_elem, f"{{{AKN_NS}}}subparagraph")
                gc_elem.set(
                    "eId",
                    f"sec_{section_id_safe}__subsec_{subsec.identifier}__para_{child.identifier}__subpara_{grandchild.identifier}",
                )

                gc_num = ET.SubElement(gc_elem, f"{{{AKN_NS}}}num")
                gc_num.text = f"({grandchild.identifier})"

                if grandchild.text:
                    gc_content = ET.SubElement(gc_elem, f"{{{AKN_NS}}}content")
                    gc_p = ET.SubElement(gc_content, f"{{{AKN_NS}}}p")
                    gc_p.text = grandchild.text[:2000]

    # Convert to string with declaration
    ET.indent(root, space="  ")
    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def create_chapter_index(
    title_num: int, chapter_num: int, section_ids: list[str], chapter_name: str
) -> str:
    """Create an index file for a chapter listing all sections.

    Args:
        title_num: Title number
        chapter_num: Chapter number
        section_ids: List of section IDs in the chapter
        chapter_name: Name of the chapter

    Returns:
        XML string for chapter index
    """
    ET.register_namespace("akn", AKN_NS)

    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "chapter")

    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom-foundation")

    today = date.today().isoformat()

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#ar-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ar")

    # FRBRExpression
    expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}/eng@{today}"
    )
    expr_uri = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}/eng@{today}")
    expr_date = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", today)
    expr_date.set("name", "generation")
    expr_author = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#axiom-foundation")
    expr_lang = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manifestation = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set(
        "value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}/eng@{today}/main.xml"
    )
    manif_uri = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}/eng@{today}/main.xml"
    )
    manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{title_num}_{chapter_num}")

    num_elem = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    num_elem.text = f"Chapter {chapter_num}"

    heading_elem = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    heading_elem.text = chapter_name

    # List sections
    for section_id in section_ids:
        ref = ET.SubElement(chapter, f"{{{AKN_NS}}}componentRef")
        ref.set("src", f"./{section_id}.xml")
        ref.set("showAs", f"Section {section_id}")

    ET.indent(root, space="  ")
    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def convert_chapter(
    converter: ARConverter, title_num: int, chapter_num: int, output_dir: Path
) -> dict:
    """Convert all sections in a chapter to AKN XML.

    Args:
        converter: ARConverter instance
        title_num: Title number
        chapter_num: Chapter number
        output_dir: Output directory

    Returns:
        Dictionary with conversion statistics
    """
    stats = {
        "sections_fetched": 0,
        "sections_converted": 0,
        "sections_failed": 0,
        "section_ids": [],
        "errors": [],
    }

    # Get chapter name
    if title_num == 26:
        chapter_name = AR_TAX_CHAPTERS.get(chapter_num, f"Chapter {chapter_num}")
    elif title_num == 20:
        chapter_name = AR_WELFARE_CHAPTERS.get(chapter_num, f"Chapter {chapter_num}")
    else:
        chapter_name = f"Chapter {chapter_num}"

    print(f"\n  Chapter {chapter_num}: {chapter_name}")

    # Create chapter directory
    chapter_dir = output_dir / f"title-{title_num}" / f"chapter-{chapter_num}"
    chapter_dir.mkdir(parents=True, exist_ok=True)

    # Get section numbers
    try:
        section_numbers = converter.get_chapter_section_numbers(title_num, chapter_num)
        print(f"    Found {len(section_numbers)} sections")
    except Exception as e:
        print(f"    ERROR getting section list: {e}")
        stats["errors"].append(f"Chapter {title_num}-{chapter_num}: {str(e)}")
        return stats

    # Fetch and convert each section
    for section_num in section_numbers:
        try:
            print(f"    Fetching {section_num}...", end=" ", flush=True)
            section = converter.fetch_section(section_num)
            stats["sections_fetched"] += 1

            # Convert to AKN
            xml_content = section_to_akn_xml(section, title_num, chapter_num)

            # Write file
            output_filename = f"{section_num}.xml"
            output_path = chapter_dir / output_filename

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(xml_content)

            stats["sections_converted"] += 1
            stats["section_ids"].append(section_num)
            print("OK")

        except Exception as e:
            stats["sections_failed"] += 1
            stats["errors"].append(f"Section {section_num}: {str(e)}")
            print(f"FAILED: {e}")

    # Create chapter index
    if stats["section_ids"]:
        index_xml = create_chapter_index(title_num, chapter_num, stats["section_ids"], chapter_name)
        index_path = chapter_dir / "_index.xml"
        with open(index_path, "w", encoding="utf-8") as f:
            f.write(index_xml)
        print(f"    Created index: {index_path.name}")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Convert Arkansas Code to Akoma Ntoso XML")
    parser.add_argument("--title", type=int, default=26, help="Title number (default: 26)")
    parser.add_argument(
        "--chapters", type=str, help="Comma-separated chapter numbers (default: all tax chapters)"
    )
    parser.add_argument("--quick", action="store_true", help="Quick mode: just Title 26 Chapter 51")
    parser.add_argument(
        "--output", type=str, default="/tmp/rules-us-ar-akn", help="Output directory"
    )
    parser.add_argument("--delay", type=float, default=0.5, help="Rate limit delay in seconds")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Arkansas Code -> Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output: {output_dir}")
    print(f"Rate limit: {args.delay}s between requests")
    print()

    # Determine which chapters to convert
    if args.quick:
        chapters_to_convert = [(26, [51])]
        print("Quick mode: Converting Title 26 Chapter 51 (Income Taxes) only")
    elif args.chapters:
        chapter_nums = [int(c.strip()) for c in args.chapters.split(",")]
        chapters_to_convert = [(args.title, chapter_nums)]
        print(f"Converting Title {args.title} Chapters: {chapter_nums}")
    else:
        # Default: All tax chapters
        chapters_to_convert = [(26, list(AR_TAX_CHAPTERS.keys()))]
        print(f"Converting Title 26 (Taxation) - {len(AR_TAX_CHAPTERS)} chapters")

    # Stats
    total_stats = {
        "chapters_processed": 0,
        "sections_fetched": 0,
        "sections_converted": 0,
        "sections_failed": 0,
        "errors": [],
    }

    # Convert
    with ARConverter(rate_limit_delay=args.delay) as converter:
        for title_num, chapter_nums in chapters_to_convert:
            title_name = AR_TITLES.get(title_num, f"Title {title_num}")
            print(f"\nTitle {title_num}: {title_name}")
            print("-" * 40)

            for chapter_num in chapter_nums:
                chapter_stats = convert_chapter(converter, title_num, chapter_num, output_dir)
                total_stats["chapters_processed"] += 1
                total_stats["sections_fetched"] += chapter_stats["sections_fetched"]
                total_stats["sections_converted"] += chapter_stats["sections_converted"]
                total_stats["sections_failed"] += chapter_stats["sections_failed"]
                total_stats["errors"].extend(chapter_stats["errors"])

    # Print summary
    print()
    print("=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"Chapters processed:   {total_stats['chapters_processed']}")
    print(f"Sections fetched:     {total_stats['sections_fetched']}")
    print(f"Sections converted:   {total_stats['sections_converted']}")
    print(f"Sections failed:      {total_stats['sections_failed']}")
    print(f"Output directory:     {output_dir}")

    if total_stats["errors"]:
        print()
        print(f"ERRORS ({len(total_stats['errors'])}):")
        for err in total_stats["errors"][:10]:  # Show first 10
            print(f"  - {err}")
        if len(total_stats["errors"]) > 10:
            print(f"  ... and {len(total_stats['errors']) - 10} more")

    # List output structure
    print()
    print("OUTPUT STRUCTURE:")
    for title_dir in sorted(output_dir.glob("title-*")):
        print(f"  {title_dir.name}/")
        for chapter_dir in sorted(title_dir.glob("chapter-*")):
            xml_count = len(list(chapter_dir.glob("*.xml")))
            print(f"    {chapter_dir.name}/ ({xml_count} files)")

    return 0 if total_stats["sections_failed"] == 0 else 1


if __name__ == "__main__":
    exit(main())
