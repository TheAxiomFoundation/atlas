#!/usr/bin/env python3
"""Convert Rhode Island General Laws HTML files to Akoma Ntoso XML.

This script reads local HTML files from data/statutes/us-ri/ and converts
them to Akoma Ntoso XML format, outputting to /tmp/rules-us-ri-akn/.

Each HTML file represents a Title index containing chapters.

Usage:
    python scripts/ri_to_akn.py
"""

import re
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def parse_title_html(html_path: Path) -> dict:
    """Parse a Rhode Island title index HTML file.

    Args:
        html_path: Path to the HTML file

    Returns:
        Dictionary with title info and chapters
    """
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")

    # Extract title number and name from h1
    # Format: "Title 44<br>Taxation"
    h1 = soup.find("h1")
    title_number = None
    title_name = ""

    if h1:
        h1_text = h1.get_text(separator="\n", strip=True)
        lines = h1_text.split("\n")
        for line in lines:
            line = line.strip()
            # Match "Title X" or "Title X.Y"
            match = re.match(r"Title\s+([\d.A-Za-z]+)", line, re.IGNORECASE)
            if match:
                title_number = match.group(1)
            elif line and not line.lower().startswith("title"):
                title_name = line

    # Fallback: extract from filename
    if not title_number:
        filename_match = re.search(r"TITLE([\d.A-Za-z]+)_INDEX", html_path.name, re.IGNORECASE)
        if filename_match:
            title_number = filename_match.group(1)

    # Extract chapters from paragraph links
    # Format: <p><a href="44-1/INDEX.htm">Chapter 44-1 State Tax Officials</a></p>
    chapters = []
    for p in soup.find_all("p"):
        link = p.find("a")
        if link and link.get("href"):
            href = link.get("href", "")
            link_text = link.get_text(strip=True)

            # Extract chapter number and name
            # Pattern: "Chapter X-Y Chapter Name" or "Chapter X-Y.Z Chapter Name"
            chapter_match = re.match(
                r"Chapter\s+([\d.A-Za-z-]+)\s*[—\-–]?\s*(.*)",
                link_text,
                re.IGNORECASE,
            )
            if chapter_match:
                chapter_num = chapter_match.group(1)
                chapter_name = chapter_match.group(2).strip()

                # Clean up non-breaking spaces
                chapter_name = chapter_name.replace("\xa0", " ").strip()

                chapters.append(
                    {
                        "number": chapter_num,
                        "name": chapter_name,
                        "href": href,
                    }
                )

    return {
        "title_number": title_number,
        "title_name": title_name,
        "chapters": chapters,
        "source_file": html_path.name,
    }


def create_akn_xml(title_data: dict) -> str:
    """Create Akoma Ntoso XML from parsed title data.

    Args:
        title_data: Dictionary from parse_title_html()

    Returns:
        XML string in Akoma Ntoso format
    """
    # Register namespace
    ET.register_namespace("akn", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container (Rhode Island General Laws is enacted legislation)
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "title")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification block
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom-foundation")

    title_num = title_data["title_number"] or "unknown"
    today = date.today().isoformat()

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")

    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-ri/act/rigl/title-{title_num}")

    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-ri/act/rigl/title-{title_num}")

    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")

    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#rigl")

    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ri")

    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", str(title_num))

    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "rigl")

    # FRBRExpression
    expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")

    expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/akn/us-ri/act/rigl/title-{title_num}/eng@{today}")

    expr_uri = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-ri/act/rigl/title-{title_num}/eng@{today}")

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
    manif_this.set("value", f"/akn/us-ri/act/rigl/title-{title_num}/eng@{today}/main.xml")

    manif_uri = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/akn/us-ri/act/rigl/title-{title_num}/eng@{today}/main.xml")

    manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")

    manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    # TLC Organization for Rhode Island
    tlc_org = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_org.set("eId", "rigl")
    tlc_org.set("href", "/ontology/organization/ri/general-assembly")
    tlc_org.set("showAs", "Rhode Island General Assembly")

    tlc_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_rf.set("eId", "axiom-foundation")
    tlc_rf.set("href", "https://axiom-foundation.org")
    tlc_rf.set("showAs", "The Axiom Foundation")

    # Body section
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Title as a hcontainer
    title_elem = ET.SubElement(body, f"{{{AKN_NS}}}hcontainer")
    title_elem.set("name", "title")
    title_elem.set("eId", f"title_{title_num}")

    # Title number
    num_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
    num_elem.text = f"Title {title_num}"

    # Title heading
    heading_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}heading")
    heading_elem.text = title_data["title_name"] or f"Title {title_num}"

    # Add chapters
    for chapter in title_data["chapters"]:
        chapter_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}chapter")
        chapter_num = chapter["number"].replace("-", "_").replace(".", "_")
        chapter_elem.set("eId", f"chp_{chapter_num}")

        chp_num = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}num")
        chp_num.text = f"Chapter {chapter['number']}"

        chp_heading = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}heading")
        chp_heading.text = chapter["name"]

    # Convert to string with declaration
    ET.indent(root, space="  ")
    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def convert_all_titles(source_dir: Path, output_dir: Path) -> dict:
    """Convert all Rhode Island title HTML files to Akoma Ntoso XML.

    Args:
        source_dir: Directory containing HTML files
        output_dir: Directory to write AKN XML files

    Returns:
        Dictionary with conversion statistics
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    stats = {
        "files_processed": 0,
        "files_succeeded": 0,
        "files_failed": 0,
        "total_chapters": 0,
        "errors": [],
    }

    # Find all HTML files
    html_files = sorted(source_dir.glob("*.html"))
    print(f"Found {len(html_files)} HTML files in {source_dir}")

    for html_path in html_files:
        try:
            print(f"Processing: {html_path.name}")
            stats["files_processed"] += 1

            # Parse HTML
            title_data = parse_title_html(html_path)

            if not title_data["title_number"]:
                print(f"  WARNING: Could not extract title number from {html_path.name}")
                stats["files_failed"] += 1
                stats["errors"].append(f"No title number: {html_path.name}")
                continue

            # Create AKN XML
            xml_content = create_akn_xml(title_data)

            # Write output file
            title_num = title_data["title_number"]
            output_filename = f"rigl-title-{title_num}.xml"
            output_path = output_dir / output_filename

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(xml_content)

            num_chapters = len(title_data["chapters"])
            stats["total_chapters"] += num_chapters
            stats["files_succeeded"] += 1
            print(f"  -> {output_filename} ({num_chapters} chapters)")

        except Exception as e:
            stats["files_failed"] += 1
            stats["errors"].append(f"{html_path.name}: {str(e)}")
            print(f"  ERROR: {e}")

    return stats


def main():
    """Main entry point."""
    # Paths
    source_dir = Path("/Users/maxghenis/TheAxiomFoundation/axiom-corpus/data/statutes/us-ri")
    output_dir = Path("/tmp/rules-us-ri-akn")

    print("=" * 60)
    print("Rhode Island General Laws -> Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Source: {source_dir}")
    print(f"Output: {output_dir}")
    print()

    # Check source directory
    if not source_dir.exists():
        print(f"ERROR: Source directory not found: {source_dir}")
        return 1

    # Convert
    stats = convert_all_titles(source_dir, output_dir)

    # Print summary
    print()
    print("=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"Files processed: {stats['files_processed']}")
    print(f"Files succeeded: {stats['files_succeeded']}")
    print(f"Files failed:    {stats['files_failed']}")
    print(f"Total chapters:  {stats['total_chapters']}")
    print(f"Output dir:      {output_dir}")

    if stats["errors"]:
        print()
        print("ERRORS:")
        for err in stats["errors"]:
            print(f"  - {err}")

    # List output files
    print()
    print("OUTPUT FILES:")
    output_files = sorted(output_dir.glob("*.xml"))
    for f in output_files:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name} ({size_kb:.1f} KB)")

    print()
    print(f"Total AKN files created: {len(output_files)}")

    return 0 if stats["files_failed"] == 0 else 1


if __name__ == "__main__":
    exit(main())
