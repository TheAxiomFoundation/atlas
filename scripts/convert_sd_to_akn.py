#!/usr/bin/env python3
"""Convert South Dakota statutes to Akoma Ntoso XML format.

Fetches statutes from sdlegislature.gov API and converts to AKN format.

Usage:
    python scripts/convert_sd_to_akn.py

Outputs to: /tmp/rules-us-sd-akn/
"""

import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.sd import (
    SDConverter,
    SD_TITLES,
    SD_TAX_CHAPTERS,
    SD_WELFARE_CHAPTERS,
)
from atlas.models import Section as ArchSection
from atlas.models_akoma_ntoso import (
    AKN_NAMESPACE,
    Act,
    Chapter,
    DocumentType,
    FRBRAuthor,
    FRBRCountry,
    FRBRDate,
    FRBRExpression,
    FRBRLanguage,
    FRBRManifestation,
    FRBRUri,
    FRBRWork,
    HierarchicalElement,
    Identification,
    Part,
    Section,
    Subsection,
)


def section_to_akn(arch_section: ArchSection) -> Section:
    """Convert arch Section model to AKN Section element."""
    # Extract section number from citation
    section_num = arch_section.citation.section  # e.g., "SD-10-1-1"
    clean_num = section_num.replace("SD-", "")

    # Create eId from section number
    eid = f"sec_{clean_num.replace('-', '_').replace('.', '_')}"

    # Convert subsections
    children = []
    for sub in arch_section.subsections:
        sub_eid = f"{eid}__subsec_{sub.identifier}"
        akn_sub = Subsection(
            eid=sub_eid,
            num=f"({sub.identifier})",
            heading=sub.heading,
            text=sub.text,
            children=[
                Subsection(
                    eid=f"{sub_eid}__para_{child.identifier}",
                    num=f"({child.identifier})",
                    heading=child.heading,
                    text=child.text,
                    children=[],
                )
                for child in sub.children
            ],
        )
        children.append(akn_sub)

    return Section(
        eid=eid,
        num=clean_num,
        heading=arch_section.section_title,
        text=arch_section.text if not children else "",
        children=children,
    )


def create_akn_document(
    title_num: int | str,
    title_name: str,
    chapters_data: dict[str, list[Section]],
) -> Act:
    """Create an Akoma Ntoso Act document from parsed sections."""

    # Create identification
    work_uri = f"/akn/us-sd/act/code/title{title_num}"
    today = date.today()

    identification = Identification(
        source="#axiom-foundation",
        work=FRBRWork(
            uri=FRBRUri(value=work_uri),
            date=FRBRDate(value=today, name="enactment"),
            author=FRBRAuthor(href="#sdleg"),
            country=FRBRCountry(value="us-sd"),
            this=f"{work_uri}/main",
        ),
        expression=FRBRExpression(
            uri=FRBRUri(value=f"{work_uri}/eng@{today.isoformat()}"),
            date=FRBRDate(value=today, name="publication"),
            author=FRBRAuthor(href="#axiom-foundation"),
            language=FRBRLanguage(language="en"),
            this=f"{work_uri}/eng@{today.isoformat()}/main",
        ),
        manifestation=FRBRManifestation(
            uri=FRBRUri(value=f"{work_uri}/eng@{today.isoformat()}.akn"),
            date=FRBRDate(value=today, name="transformation"),
            author=FRBRAuthor(href="#axiom-foundation"),
            this=f"{work_uri}/eng@{today.isoformat()}/main.akn",
        ),
    )

    # Build body from chapters
    body = []

    # Create title element
    title_elem = Part(
        eid=f"title_{title_num}",
        num=f"Title {title_num}",
        heading=title_name,
        children=[],
    )

    # Add chapters sorted by chapter number
    for chapter_id in sorted(
        chapters_data.keys(), key=lambda x: (x.split("-")[0], x.split("-")[1] if "-" in x else "0")
    ):
        sections = chapters_data[chapter_id]
        if not sections:
            continue

        # Get chapter name from mappings
        chapter_name = SD_TAX_CHAPTERS.get(chapter_id) or SD_WELFARE_CHAPTERS.get(chapter_id, "")

        chapter_elem = Chapter(
            eid=f"chapter_{chapter_id.replace('-', '_')}",
            num=f"Chapter {chapter_id}",
            heading=chapter_name,
            children=sections,
        )

        title_elem.children.append(chapter_elem)

    body.append(title_elem)

    return Act(
        document_type=DocumentType.ACT,
        identification=identification,
        body=body,
        source_url="https://sdlegislature.gov/",
    )


def prettify_xml(elem: ET.Element, indent: str = "  ") -> str:
    """Pretty-print XML with indentation."""
    ET.indent(elem, space=indent)
    return ET.tostring(elem, encoding="unicode")


def fetch_and_convert_chapters(
    converter: SDConverter,
    chapters: list[str],
    output_dir: Path,
    title_num: int | str,
    title_name: str,
) -> dict:
    """Fetch chapters and convert to AKN.

    Returns dict with conversion stats.
    """
    stats = {
        "title": title_num,
        "chapters": 0,
        "sections": 0,
        "success": False,
        "error": None,
    }

    try:
        chapters_data: dict[str, list[Section]] = {}

        for chapter in chapters:
            print(f"  Fetching chapter {chapter}...", end=" ", flush=True)
            try:
                chapter_sections = []
                section_count = 0

                for arch_section in converter.iter_chapter(chapter):
                    akn_section = section_to_akn(arch_section)
                    chapter_sections.append(akn_section)
                    section_count += 1

                chapters_data[chapter] = chapter_sections
                stats["sections"] += section_count
                stats["chapters"] += 1
                print(f"{section_count} sections")

            except Exception as e:
                print(f"FAILED: {e}")
                continue

        if not chapters_data:
            stats["error"] = "No sections fetched"
            return stats

        # Create AKN document
        doc = create_akn_document(title_num, title_name, chapters_data)

        # Generate XML
        xml_elem = doc.to_xml_element()
        xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_str += prettify_xml(xml_elem)

        # Write output
        output_file = output_dir / f"us-sd-title-{title_num}.akn.xml"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(xml_str)

        stats["success"] = True
        stats["output_file"] = str(output_file)

    except Exception as e:
        stats["error"] = str(e)
        import traceback
        traceback.print_exc()

    return stats


def main():
    """Main conversion function."""
    # Set up paths
    output_dir = Path("/tmp/rules-us-sd-akn")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("South Dakota Statute to Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output directory: {output_dir}")
    print()

    # Initialize converter with rate limiting
    converter = SDConverter(rate_limit_delay=0.5)

    total_sections = 0
    total_chapters = 0
    results = []

    try:
        # Convert Title 10 (Taxation)
        print("Converting Title 10: Taxation")
        print("-" * 40)
        stats = fetch_and_convert_chapters(
            converter,
            list(SD_TAX_CHAPTERS.keys()),
            output_dir,
            10,
            "Taxation",
        )
        results.append(stats)
        if stats["success"]:
            total_sections += stats["sections"]
            total_chapters += stats["chapters"]
        print()

        # Convert Title 28 (Public Welfare and Assistance)
        print("Converting Title 28: Public Welfare and Assistance")
        print("-" * 40)
        stats = fetch_and_convert_chapters(
            converter,
            list(SD_WELFARE_CHAPTERS.keys()),
            output_dir,
            28,
            "Public Welfare and Assistance",
        )
        results.append(stats)
        if stats["success"]:
            total_sections += stats["sections"]
            total_chapters += stats["chapters"]

    finally:
        converter.close()

    # Print summary
    print()
    print("=" * 60)
    print("Conversion Summary")
    print("-" * 40)
    successful = sum(1 for r in results if r["success"])
    failed = sum(1 for r in results if not r["success"])
    print(f"  Titles: {successful} successful, {failed} failed")
    print(f"  Total chapters: {total_chapters}")
    print(f"  Total sections: {total_sections}")
    print()

    # List output files
    output_files = sorted(output_dir.glob("*.akn.xml"))
    if output_files:
        print("Generated files:")
        for f in output_files:
            size_kb = f.stat().st_size / 1024
            print(f"  {f.name} ({size_kb:.1f} KB)")
    else:
        print("No files generated.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
