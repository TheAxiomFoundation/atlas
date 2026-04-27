#!/usr/bin/env python3
"""Convert Idaho Statutes from legislature.idaho.gov to Akoma Ntoso XML.

This script fetches Idaho Statutes directly from the official legislature website
and converts them to Akoma Ntoso XML format, outputting to /tmp/rules-us-id-akn/.

Uses the existing IDConverter from atlas.converters.us_states.id_.

Usage:
    python scripts/id_to_akn.py

    # Fetch specific titles only:
    python scripts/id_to_akn.py --titles 63,56

    # Fetch tax chapters only:
    python scripts/id_to_akn.py --tax-only

    # Fetch welfare chapters only:
    python scripts/id_to_akn.py --welfare-only
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.id_ import (
    ID_TAX_CHAPTERS,
    ID_TITLES,
    ID_WELFARE_CHAPTERS,
    IDConverter,
    IDConverterError,
)
from atlas.models import Section

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-id-akn")


def section_to_akn_dict(section: Section) -> dict:
    """Convert an arch Section model to a dictionary for AKN conversion.

    Args:
        section: Section model from IDConverter

    Returns:
        Dictionary with section info for AKN XML generation
    """
    # Extract section number from citation (e.g., "ID-63-3002" -> "63-3002")
    section_num = section.citation.section
    if section_num.startswith("ID-"):
        section_num = section_num[3:]

    # Parse subsections into dict format
    subsections = []
    for sub in section.subsections:
        children = []
        for child in sub.children:
            children.append(
                {
                    "identifier": child.identifier,
                    "text": child.text or "",
                    "children": [],
                }
            )
        subsections.append(
            {
                "identifier": sub.identifier,
                "text": sub.text or "",
                "children": children,
            }
        )

    return {
        "section_number": section_num,
        "title": section.section_title,
        "text": section.text or "",
        "history": None,  # History extracted separately if available
        "subsections": subsections,
        "source_url": section.source_url,
    }


def create_akn_xml(
    title_num: int, chapter_num: int, chapter_title: str, sections: list[dict]
) -> str:
    """Create Akoma Ntoso XML from parsed sections.

    Args:
        title_num: Title number (e.g., 63)
        chapter_num: Chapter number (e.g., 30)
        chapter_title: Chapter title (e.g., "Income Tax")
        sections: List of section dicts from section_to_akn_dict

    Returns:
        XML string in Akoma Ntoso format
    """
    # Register namespace
    ET.register_namespace("akn", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "chapter")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom-foundation")

    today = date.today().isoformat()

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")

    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-id/act/idaho-statutes/title-{title_num}/chapter-{chapter_num}")

    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-id/act/idaho-statutes/title-{title_num}/chapter-{chapter_num}")

    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")

    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#idaho-legislature")

    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-id")

    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", f"{title_num}-{chapter_num}")

    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "idaho-statutes")

    # FRBRExpression
    expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")

    expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value",
        f"/akn/us-id/act/idaho-statutes/title-{title_num}/chapter-{chapter_num}/eng@{today}",
    )

    expr_uri = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value",
        f"/akn/us-id/act/idaho-statutes/title-{title_num}/chapter-{chapter_num}/eng@{today}",
    )

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
        "value",
        f"/akn/us-id/act/idaho-statutes/title-{title_num}/chapter-{chapter_num}/eng@{today}/main.xml",
    )

    manif_uri = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value",
        f"/akn/us-id/act/idaho-statutes/title-{title_num}/chapter-{chapter_num}/eng@{today}/main.xml",
    )

    manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")

    manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    # TLC Organizations
    tlc_id = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_id.set("eId", "idaho-legislature")
    tlc_id.set("href", "https://legislature.idaho.gov")
    tlc_id.set("showAs", "Idaho State Legislature")

    tlc_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_rf.set("eId", "axiom-foundation")
    tlc_rf.set("href", "https://axiom-foundation.org")
    tlc_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Chapter container
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{title_num}_{chapter_num}")

    # Chapter number
    num_elem = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    num_elem.text = f"Chapter {chapter_num}"

    # Chapter heading
    heading_elem = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    heading_elem.text = chapter_title or f"Chapter {chapter_num}"

    # Add sections
    for sec in sections:
        add_section_to_xml(chapter, sec)

    # Convert to string with declaration
    ET.indent(root, space="  ")
    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def add_section_to_xml(parent: ET.Element, section: dict) -> None:
    """Add a section element to the parent XML element."""
    sec_num = section["section_number"].replace("-", "_").replace(".", "_")

    sec_elem = ET.SubElement(parent, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", f"sec_{sec_num}")

    # Section number
    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = section["section_number"]

    # Section heading
    heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    heading.text = section["title"] or f"Section {section['section_number']}"

    # Content
    if section["subsections"]:
        # Add subsections
        for sub in section["subsections"]:
            add_subsection_to_xml(sec_elem, sub, sec_num, level=1)
    elif section["text"]:
        # Plain content
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        # Limit text to reasonable size
        p.text = section["text"][:10000] if section["text"] else ""

    # Add history/source note as authorialNote
    if section.get("history"):
        note = ET.SubElement(sec_elem, f"{{{AKN_NS}}}authorialNote")
        note.set("marker", "history")
        p = ET.SubElement(note, f"{{{AKN_NS}}}p")
        p.text = section["history"]


def add_subsection_to_xml(parent: ET.Element, subsection: dict, parent_id: str, level: int) -> None:
    """Add a subsection element to the parent XML element."""
    identifier = subsection["identifier"]
    sub_id = f"{parent_id}__subsec_{identifier}"

    if level == 1:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
    elif level == 2:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}paragraph")
    else:
        elem = ET.SubElement(parent, f"{{{AKN_NS}}}subparagraph")

    elem.set("eId", sub_id)

    # Number/identifier
    num = ET.SubElement(elem, f"{{{AKN_NS}}}num")
    # Idaho uses (1), (a), etc.
    if identifier.isdigit():
        num.text = f"({identifier})"
    elif identifier.isalpha():
        num.text = f"({identifier})"
    else:
        num.text = identifier

    # Text content
    if subsection["text"]:
        content = ET.SubElement(elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = subsection["text"][:10000]

    # Children
    for child in subsection.get("children", []):
        add_subsection_to_xml(elem, child, sub_id, level + 1)


def fetch_and_convert_chapter(
    converter: IDConverter,
    title_num: int,
    chapter_num: int,
    output_dir: Path,
) -> dict:
    """Fetch a chapter and convert to AKN XML.

    Args:
        converter: IDConverter instance
        title_num: Title number
        chapter_num: Chapter number
        output_dir: Output directory

    Returns:
        Dictionary with conversion stats
    """
    result = {
        "title": title_num,
        "chapter": chapter_num,
        "sections": 0,
        "success": False,
        "error": None,
    }

    try:
        # Get chapter title
        if title_num == 63:
            chapter_title = ID_TAX_CHAPTERS.get(chapter_num, f"Chapter {chapter_num}")
        elif title_num == 56:
            chapter_title = ID_WELFARE_CHAPTERS.get(chapter_num, f"Chapter {chapter_num}")
        else:
            chapter_title = f"Chapter {chapter_num}"

        # Fetch sections
        sections_data = []
        for section in converter.iter_chapter(title_num, chapter_num):
            sections_data.append(section_to_akn_dict(section))

        if not sections_data:
            result["error"] = "No sections found"
            return result

        # Create AKN XML
        xml_content = create_akn_xml(title_num, chapter_num, chapter_title, sections_data)

        # Write output
        output_path = output_dir / f"idaho-title-{title_num}-chapter-{chapter_num:02d}.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        result["sections"] = len(sections_data)
        result["success"] = True

    except IDConverterError as e:
        result["error"] = str(e)
    except Exception as e:
        result["error"] = f"Unexpected error: {e}"

    return result


def fetch_title(
    converter: IDConverter,
    title_num: int,
    output_dir: Path,
) -> dict:
    """Fetch all chapters in a title and convert to AKN XML.

    Args:
        converter: IDConverter instance
        title_num: Title number
        output_dir: Output directory

    Returns:
        Dictionary with conversion stats
    """
    print(f"\nFetching Title {title_num}: {ID_TITLES.get(title_num, 'Unknown')}")
    print("-" * 60)

    try:
        chapters = converter.get_title_chapters(title_num)
        print(f"Found {len(chapters)} chapters")
    except Exception as e:
        print(f"ERROR: Could not get chapter list: {e}")
        return {"title": title_num, "chapters": 0, "sections": 0, "success": False}

    stats = {
        "title": title_num,
        "chapters": len(chapters),
        "sections": 0,
        "successful_chapters": 0,
        "failed_chapters": 0,
    }

    for chapter_num in chapters:
        result = fetch_and_convert_chapter(converter, title_num, chapter_num, output_dir)

        if result["success"]:
            stats["successful_chapters"] += 1
            stats["sections"] += result["sections"]
            print(f"  [OK] Chapter {chapter_num}: {result['sections']} sections")
        else:
            stats["failed_chapters"] += 1
            print(f"  [FAIL] Chapter {chapter_num}: {result['error']}")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Convert Idaho Statutes to Akoma Ntoso XML")
    parser.add_argument(
        "--titles",
        type=str,
        help="Comma-separated list of title numbers to fetch (e.g., '63,56')",
    )
    parser.add_argument(
        "--tax-only",
        action="store_true",
        help="Fetch only tax-related chapters (Title 63)",
    )
    parser.add_argument(
        "--welfare-only",
        action="store_true",
        help="Fetch only welfare-related chapters (Title 56)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.5,
        help="Delay between HTTP requests in seconds (default: 0.5)",
    )

    args = parser.parse_args()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Idaho Statutes -> Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Source: https://legislature.idaho.gov/statutesrules/idstat")
    print(f"Output: {OUTPUT_DIR}")
    print()

    # Initialize converter
    converter = IDConverter(rate_limit_delay=args.rate_limit)

    total_stats = {
        "titles_processed": 0,
        "chapters_processed": 0,
        "sections_processed": 0,
        "chapters_succeeded": 0,
        "chapters_failed": 0,
    }

    try:
        if args.tax_only:
            # Fetch tax chapters only
            print("Fetching tax-related chapters (Title 63)...")
            for chapter_num in ID_TAX_CHAPTERS:
                result = fetch_and_convert_chapter(converter, 63, chapter_num, OUTPUT_DIR)
                total_stats["chapters_processed"] += 1
                if result["success"]:
                    total_stats["chapters_succeeded"] += 1
                    total_stats["sections_processed"] += result["sections"]
                    print(f"  [OK] 63-{chapter_num}: {result['sections']} sections")
                else:
                    total_stats["chapters_failed"] += 1
                    print(f"  [FAIL] 63-{chapter_num}: {result['error']}")
            total_stats["titles_processed"] = 1

        elif args.welfare_only:
            # Fetch welfare chapters only
            print("Fetching welfare-related chapters (Title 56)...")
            for chapter_num in ID_WELFARE_CHAPTERS:
                result = fetch_and_convert_chapter(converter, 56, chapter_num, OUTPUT_DIR)
                total_stats["chapters_processed"] += 1
                if result["success"]:
                    total_stats["chapters_succeeded"] += 1
                    total_stats["sections_processed"] += result["sections"]
                    print(f"  [OK] 56-{chapter_num}: {result['sections']} sections")
                else:
                    total_stats["chapters_failed"] += 1
                    print(f"  [FAIL] 56-{chapter_num}: {result['error']}")
            total_stats["titles_processed"] = 1

        elif args.titles:
            # Fetch specific titles
            title_nums = [int(t.strip()) for t in args.titles.split(",")]
            for title_num in title_nums:
                stats = fetch_title(converter, title_num, OUTPUT_DIR)
                total_stats["titles_processed"] += 1
                total_stats["chapters_processed"] += stats.get("chapters", 0)
                total_stats["sections_processed"] += stats.get("sections", 0)
                total_stats["chapters_succeeded"] += stats.get("successful_chapters", 0)
                total_stats["chapters_failed"] += stats.get("failed_chapters", 0)

        else:
            # Default: fetch key titles (63 Revenue/Taxation, 56 Public Assistance)
            print("Fetching key titles: 63 (Revenue and Taxation), 56 (Public Assistance)")
            print("Use --titles to specify other titles, or --tax-only/--welfare-only")
            print()

            for title_num in [63, 56]:
                stats = fetch_title(converter, title_num, OUTPUT_DIR)
                total_stats["titles_processed"] += 1
                total_stats["chapters_processed"] += stats.get("chapters", 0)
                total_stats["sections_processed"] += stats.get("sections", 0)
                total_stats["chapters_succeeded"] += stats.get("successful_chapters", 0)
                total_stats["chapters_failed"] += stats.get("failed_chapters", 0)

    finally:
        converter.close()

    # Print summary
    print()
    print("=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"Titles processed:    {total_stats['titles_processed']}")
    print(f"Chapters processed:  {total_stats['chapters_processed']}")
    print(f"Chapters succeeded:  {total_stats['chapters_succeeded']}")
    print(f"Chapters failed:     {total_stats['chapters_failed']}")
    print(f"Total sections:      {total_stats['sections_processed']}")
    print(f"Output directory:    {OUTPUT_DIR}")

    # List output files
    print()
    print("OUTPUT FILES:")
    output_files = sorted(OUTPUT_DIR.glob("*.xml"))
    for f in output_files:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name} ({size_kb:.1f} KB)")

    print()
    print(f"Total AKN files created: {len(output_files)}")

    return 0 if total_stats["chapters_failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
