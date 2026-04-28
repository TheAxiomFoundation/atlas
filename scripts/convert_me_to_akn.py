#!/usr/bin/env python3
"""Convert Maine statute HTML files to Akoma Ntoso XML format.

This script parses the Maine Revised Statutes HTML table of contents files
and converts them to Akoma Ntoso XML format following the OASIS standard.

Usage:
    python scripts/convert_me_to_akn.py

Output:
    Creates AKN XML files in /tmp/rules-us-me-akn/
"""

import os
import re
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from bs4 import BeautifulSoup

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Source and output directories
SOURCE_DIR = Path("/Users/maxghenis/TheAxiomFoundation/axiom-corpus/data/statutes/us-me")
OUTPUT_DIR = Path("/tmp/rules-us-me-akn")


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
    # Remove special characters, replace spaces with underscores
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"-+", "-", text)
    return text.lower()[:50]


def parse_title_number(filename: str) -> str:
    """Extract title number from filename."""
    # Pattern: statutes_36_title36ch0sec0.html.html
    match = re.search(r"statutes_([^_]+)_", filename)
    if match:
        return match.group(1)
    return ""


def parse_html_file(filepath: Path) -> dict:
    """Parse a Maine statute HTML file and extract structure.

    Returns:
        dict with keys: title_num, title_name, parts, chapters
        Each chapter has: num, name, section_range, is_repealed, href
    """
    with open(filepath, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    result = {
        "title_num": "",
        "title_name": "",
        "parts": [],
        "chapters": [],
        "source_url": f"https://legislature.maine.gov/statutes/{filepath.name}",
    }

    # Get title from page title
    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text()
        # Parse "Title 36: TAXATION"
        match = re.match(r"Title\s+([^:]+):\s*(.+)", title_text)
        if match:
            result["title_num"] = match.group(1).strip()
            result["title_name"] = match.group(2).strip()

    # Also try the heading div
    heading_div = soup.find("div", class_="title_heading")
    if heading_div:
        heading_text = heading_div.get_text().strip()
        match = re.match(r"Title\s+([^:]+):\s*(.+)", heading_text)
        if match:
            result["title_num"] = match.group(1).strip()
            result["title_name"] = match.group(2).strip()

    # Parse parts (MRSPart_toclist)
    current_part = None
    for part_div in soup.find_all("div", class_="MRSPart_toclist"):
        part_heading = part_div.find(["h2", "h3"], class_="heading_part")
        if part_heading:
            part_text = part_heading.get_text().strip()
            # Parse "Part 1: GENERAL PROVISIONS"
            match = re.match(r"Part\s+(\d+):\s*(.+)", part_text)
            if match:
                current_part = {
                    "num": match.group(1),
                    "name": match.group(2).strip(),
                    "chapters": [],
                }
                result["parts"].append(current_part)

        # Parse chapters within this part
        for ch_div in part_div.find_all("div", class_="MRSChapter_toclist"):
            chapter = parse_chapter_div(ch_div)
            if chapter:
                if current_part:
                    current_part["chapters"].append(chapter)
                result["chapters"].append(chapter)

    # If no parts found, just get all chapters
    if not result["parts"]:
        for ch_div in soup.find_all("div", class_="MRSChapter_toclist"):
            chapter = parse_chapter_div(ch_div)
            if chapter:
                result["chapters"].append(chapter)

    return result


def parse_chapter_div(ch_div) -> dict | None:
    """Parse a chapter div element."""
    link = ch_div.find("a")
    if not link:
        return None

    text = ch_div.get_text().strip()
    href = link.get("href", "")

    # Check if repealed
    is_repealed = "right_nav_repealed" in ch_div.get("class", [])

    # Parse chapter info: "Chapter 7: UNIFORM ADMINISTRATIVE PROVISIONS" or similar
    # Full text might be: "Chapter 7: UNIFORM ADMINISTRATIVE PROVISIONS §111 - §194-E"
    chapter_match = re.match(
        r"Chapter\s+([^:]+):\s*(.+?)(?:\s+§(\S+)\s*-\s*§(\S+))?$", text, re.DOTALL
    )

    if chapter_match:
        chapter_name = chapter_match.group(2).strip()
        # Clean up the name (remove REPEALED suffix if present)
        chapter_name = re.sub(r"\s*\(REPEALED\)\s*$", "", chapter_name, flags=re.IGNORECASE)

        return {
            "num": chapter_match.group(1).strip(),
            "name": chapter_name,
            "section_start": chapter_match.group(3),
            "section_end": chapter_match.group(4),
            "is_repealed": is_repealed or "(REPEALED)" in text.upper(),
            "href": href,
        }

    return None


def create_akn_document(data: dict) -> ET.Element:
    """Create an Akoma Ntoso document from parsed data."""
    # Root element
    root = make_element("akomaNtoso")

    # Act container
    act = make_subelement(root, "act", {"name": f"title-{data['title_num']}"})

    # Meta section
    meta = make_subelement(act, "meta")

    # Identification
    today = date.today().isoformat()
    work_uri = f"/akn/us-me/act/mrs/title-{data['title_num']}"
    expr_uri = f"{work_uri}/eng@{today}"
    manif_uri = f"{expr_uri}/main"

    identification = make_subelement(meta, "identification", {"source": "#axiom-foundation"})

    # FRBRWork
    frbr_work = make_subelement(identification, "FRBRWork")
    make_subelement(frbr_work, "FRBRthis", {"value": work_uri})
    make_subelement(frbr_work, "FRBRuri", {"value": work_uri})
    make_subelement(frbr_work, "FRBRdate", {"date": today, "name": "generation"})
    make_subelement(frbr_work, "FRBRauthor", {"href": "#maine-legislature"})
    make_subelement(frbr_work, "FRBRcountry", {"value": "us-me"})
    make_subelement(frbr_work, "FRBRnumber", {"value": data["title_num"]})
    make_subelement(frbr_work, "FRBRname", {"value": "Maine Revised Statutes"})

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
            "eId": "maine-legislature",
            "href": "http://legislature.maine.gov",
            "showAs": "Maine Legislature",
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

    # Title as top-level container
    title_id = f"title_{sanitize_id(data['title_num'])}"

    if data["parts"]:
        # If we have parts, use them as the primary structure
        for part in data["parts"]:
            part_id = f"part_{sanitize_id(part['num'])}"
            part_elem = make_subelement(body, "part", {"eId": part_id})
            make_subelement(part_elem, "num", text=f"Part {part['num']}")
            make_subelement(part_elem, "heading", text=part["name"])

            # Add chapters to this part
            for ch in part["chapters"]:
                add_chapter_element(part_elem, ch, data["title_num"])
    else:
        # No parts, just add chapters directly
        for ch in data["chapters"]:
            add_chapter_element(body, ch, data["title_num"])

    return root


def add_chapter_element(parent: ET.Element, chapter: dict, title_num: str):
    """Add a chapter element to the parent."""
    ch_id = f"chap_{sanitize_id(chapter['num'])}"

    attribs = {"eId": ch_id}
    if chapter["is_repealed"]:
        attribs["status"] = "repealed"

    ch_elem = make_subelement(parent, "chapter", attribs)
    make_subelement(ch_elem, "num", text=f"Chapter {chapter['num']}")
    make_subelement(ch_elem, "heading", text=chapter["name"])

    # Add section range as content note
    if chapter.get("section_start") and chapter.get("section_end"):
        content = make_subelement(ch_elem, "content")
        p = make_subelement(content, "p")
        p.text = f"Sections {chapter['section_start']} through {chapter['section_end']}"


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


def main():
    """Main conversion function."""
    register_namespace()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Find all HTML files
    html_files = list(SOURCE_DIR.glob("*.html*"))
    print(f"Found {len(html_files)} HTML files in {SOURCE_DIR}")

    # Track statistics
    stats = {
        "total_files": len(html_files),
        "converted": 0,
        "chapters": 0,
        "parts": 0,
        "repealed_chapters": 0,
        "errors": 0,
    }

    for filepath in sorted(html_files):
        try:
            print(f"Processing: {filepath.name}")

            # Parse HTML
            data = parse_html_file(filepath)

            if not data["title_num"]:
                print(f"  Warning: Could not extract title number from {filepath.name}")
                stats["errors"] += 1
                continue

            # Create AKN document
            akn_root = create_akn_document(data)

            # Write output
            output_filename = f"mrs-title-{data['title_num']}.akn.xml"
            output_path = OUTPUT_DIR / output_filename
            write_akn_file(akn_root, output_path)

            # Update stats
            stats["converted"] += 1
            stats["chapters"] += len(data["chapters"])
            stats["parts"] += len(data["parts"])
            stats["repealed_chapters"] += sum(1 for ch in data["chapters"] if ch.get("is_repealed"))

            print(
                f"  -> {output_filename} ({len(data['parts'])} parts, {len(data['chapters'])} chapters)"
            )

        except Exception as e:
            print(f"  Error processing {filepath.name}: {e}")
            stats["errors"] += 1

    # Print summary
    print("\n" + "=" * 60)
    print("Conversion Summary")
    print("=" * 60)
    print(f"Total HTML files:      {stats['total_files']}")
    print(f"Successfully converted: {stats['converted']}")
    print(f"Errors:                {stats['errors']}")
    print(f"Total parts:           {stats['parts']}")
    print(f"Total chapters:        {stats['chapters']}")
    print(f"Repealed chapters:     {stats['repealed_chapters']}")
    print(f"Output directory:      {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
