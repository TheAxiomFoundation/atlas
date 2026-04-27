#!/usr/bin/env python3
"""Convert Michigan Compiled Laws XML to Akoma Ntoso format.

This script parses Michigan statute XML files (MCL format) and converts
them to Akoma Ntoso XML format following the OASIS standard.

Usage:
    python scripts/convert_mi_to_akn.py

Output:
    Creates AKN XML files in /tmp/rules-us-mi-akn/
"""

import html
import re
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Source and output directories
SOURCE_DIR = Path("/Users/maxghenis/TheAxiomFoundation/atlas/data/michigan")
OUTPUT_DIR = Path("/tmp/rules-us-mi-akn")


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
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"-+", "-", text)
    return text.lower()[:50]


def clean_body_text(body_text: str) -> str:
    """Clean HTML-encoded body text and extract plain text."""
    if not body_text:
        return ""

    # Unescape HTML entities
    text = html.unescape(body_text)

    # Remove HTML tags but preserve structure
    # Replace paragraph tags with newlines
    text = re.sub(r"</?P>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</?Paragraph>", "\n", text, flags=re.IGNORECASE)

    # Remove section number tags
    text = re.sub(r"<Section-Number>.*?</Section-Number>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<Section-Body>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</Section-Body>", "", text, flags=re.IGNORECASE)

    # Remove emphasis tags but keep content
    text = re.sub(r"<Emph[^>]*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</Emph>", "", text, flags=re.IGNORECASE)

    # Remove table HTML (simplified - tables need special handling)
    text = re.sub(r"<table[^>]*>.*?</table>", "[TABLE]", text, flags=re.IGNORECASE | re.DOTALL)

    # Remove remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)

    # Clean up whitespace
    text = re.sub(r"\n\s*\n", "\n\n", text)
    text = text.strip()

    return text


def parse_mcl_section(section_elem) -> dict:
    """Parse an MCL section element."""
    section = {
        "mcl_number": "",
        "label": "",
        "catchline": "",
        "body_text": "",
        "is_repealed": False,
        "effective_date": None,
        "history": [],
    }

    # Get basic fields
    mcl_num = section_elem.find("MCLNumber")
    if mcl_num is not None and mcl_num.text:
        section["mcl_number"] = mcl_num.text

    label = section_elem.find("Label")
    if label is not None and label.text:
        section["label"] = label.text

    catchline = section_elem.find("CatchLine")
    if catchline is not None and catchline.text:
        section["catchline"] = catchline.text

    body = section_elem.find("BodyText")
    if body is not None and body.text:
        section["body_text"] = clean_body_text(body.text)

    repealed = section_elem.find("Repealed")
    if repealed is not None and repealed.text:
        section["is_repealed"] = repealed.text.lower() == "true"

    # Parse history
    history = section_elem.find("History")
    if history is not None:
        for hist_info in history.findall("HistoryInfo"):
            eff_date = hist_info.find("EffectiveDate")
            action = hist_info.find("Action")
            leg = hist_info.find("Legislation")

            hist_entry = {
                "effective_date": eff_date.text if eff_date is not None else None,
                "action": action.text if action is not None else None,
            }

            if leg is not None:
                leg_type = leg.find("Type")
                leg_num = leg.find("Number")
                leg_year = leg.find("Year")
                if leg_type is not None and leg_num is not None and leg_year is not None:
                    hist_entry["legislation"] = f"{leg_type.text} {leg_num.text} of {leg_year.text}"

            section["history"].append(hist_entry)

            # Get most recent effective date
            if eff_date is not None and eff_date.text and not eff_date.text.startswith("0001"):
                section["effective_date"] = eff_date.text

    return section


def parse_mcl_division(div_elem) -> dict:
    """Parse an MCL division element (Part, Chapter, etc.)."""
    division = {"type": "", "number": "", "title": "", "sections": [], "subdivisions": []}

    div_type = div_elem.find("DivisionType")
    if div_type is not None and div_type.text:
        division["type"] = div_type.text

    div_num = div_elem.find("DivisionNumber")
    if div_num is not None and div_num.text:
        division["number"] = div_num.text

    div_title = div_elem.find("DivisionTitle")
    if div_title is not None and div_title.text:
        division["title"] = div_title.text

    # Parse nested content
    collection = div_elem.find("MCLDocumentInfoCollection")
    if collection is not None:
        for child in collection:
            tag = child.tag
            if tag == "MCLSectionInfo":
                section = parse_mcl_section(child)
                if section["mcl_number"]:
                    division["sections"].append(section)
            elif tag == "MCLDivisionInfo":
                subdiv = parse_mcl_division(child)
                division["subdivisions"].append(subdiv)

    return division


def parse_mcl_chapter(filepath: Path) -> dict:
    """Parse an MCL chapter XML file."""
    # Read file content and handle encoding
    content = filepath.read_bytes()

    # Check if it's UTF-16 encoded with BOM
    if content.startswith(b"\xff\xfe") or content.startswith(b"\xfe\xff"):
        # UTF-16 with BOM
        text = content.decode("utf-16")
    else:
        # File declares UTF-16 but may actually be UTF-8 (common issue)
        # Try UTF-8 first
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            # Try UTF-16 as fallback
            try:
                text = content.decode("utf-16")
            except UnicodeDecodeError:
                text = content.decode("latin-1")

    # Remove or fix XML declaration with wrong encoding
    text = re.sub(
        r'<\?xml[^>]*encoding=["\']utf-16["\'][^>]*\?>',
        '<?xml version="1.0" encoding="UTF-8"?>',
        text,
        flags=re.IGNORECASE,
    )

    root = ET.fromstring(text)

    result = {
        "chapter_num": "",
        "title": "",
        "short_title": "",
        "long_title": "",
        "divisions": [],
        "sections": [],
    }

    # Get chapter info
    name = root.find("Name")
    if name is not None and name.text:
        result["chapter_num"] = name.text

    title = root.find("Title")
    if title is not None and title.text:
        result["title"] = title.text

    # Look for statute info
    collection = root.find("MCLDocumentInfoCollection")
    if collection is not None:
        statute = collection.find("MCLStatuteInfo")
        if statute is not None:
            short_title = statute.find("ShortTitle")
            if short_title is not None and short_title.text:
                result["short_title"] = short_title.text

            long_title = statute.find("LongTitle")
            if long_title is not None and long_title.text:
                result["long_title"] = long_title.text

            # Parse divisions and sections within statute
            stat_collection = statute.find("MCLDocumentInfoCollection")
            if stat_collection is not None:
                for child in stat_collection:
                    tag = child.tag
                    if tag == "MCLDivisionInfo":
                        division = parse_mcl_division(child)
                        result["divisions"].append(division)
                    elif tag == "MCLSectionInfo":
                        section = parse_mcl_section(child)
                        if section["mcl_number"]:
                            result["sections"].append(section)

    return result


def add_section_element(parent: ET.Element, section: dict, chapter_num: str):
    """Add a section element to the parent."""
    sec_id = f"sec_{sanitize_id(section['mcl_number'])}"

    attribs = {"eId": sec_id}
    if section["is_repealed"]:
        attribs["status"] = "repealed"

    sec_elem = make_subelement(parent, "section", attribs)
    make_subelement(sec_elem, "num", text=f"Sec. {section['label']}")

    if section["catchline"]:
        make_subelement(sec_elem, "heading", text=section["catchline"])

    if section["body_text"]:
        content = make_subelement(sec_elem, "content")
        # Split into paragraphs
        paragraphs = section["body_text"].split("\n\n")
        for para in paragraphs:
            para = para.strip()
            if para:
                make_subelement(content, "p", text=para)


def add_division_element(parent: ET.Element, division: dict, chapter_num: str):
    """Add a division element (part, chapter, etc.) to the parent."""
    div_type = division["type"].lower()
    if div_type not in ["part", "chapter", "subchapter", "article"]:
        div_type = "part"

    div_id = f"{div_type}_{sanitize_id(division['number'])}"
    div_elem = make_subelement(parent, div_type, {"eId": div_id})

    make_subelement(div_elem, "num", text=f"{division['type']} {division['number']}")

    if division["title"]:
        make_subelement(div_elem, "heading", text=division["title"])

    # Add subdivisions
    for subdiv in division["subdivisions"]:
        add_division_element(div_elem, subdiv, chapter_num)

    # Add sections
    for section in division["sections"]:
        add_section_element(div_elem, section, chapter_num)


def create_akn_document(data: dict) -> ET.Element:
    """Create an Akoma Ntoso document from parsed MCL data."""
    # Root element
    root = make_element("akomaNtoso")

    # Act container
    act = make_subelement(root, "act", {"name": f"mcl-{data['chapter_num']}"})

    # Meta section
    meta = make_subelement(act, "meta")

    # Identification
    today = date.today().isoformat()
    work_uri = f"/akn/us-mi/act/mcl/{data['chapter_num']}"
    expr_uri = f"{work_uri}/eng@{today}"
    manif_uri = f"{expr_uri}/main"

    identification = make_subelement(meta, "identification", {"source": "#axiom-foundation"})

    # FRBRWork
    frbr_work = make_subelement(identification, "FRBRWork")
    make_subelement(frbr_work, "FRBRthis", {"value": work_uri})
    make_subelement(frbr_work, "FRBRuri", {"value": work_uri})
    make_subelement(frbr_work, "FRBRdate", {"date": today, "name": "generation"})
    make_subelement(frbr_work, "FRBRauthor", {"href": "#michigan-legislature"})
    make_subelement(frbr_work, "FRBRcountry", {"value": "us-mi"})
    make_subelement(frbr_work, "FRBRnumber", {"value": data["chapter_num"]})
    make_subelement(frbr_work, "FRBRname", {"value": "Michigan Compiled Laws"})

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
            "eId": "michigan-legislature",
            "href": "http://legislature.mi.gov",
            "showAs": "Michigan Legislature",
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

    # Preface with title info
    preface = make_subelement(act, "preface")

    if data["title"]:
        make_subelement(preface, "docTitle", text=data["title"])

    if data["short_title"]:
        make_subelement(preface, "shortTitle", text=data["short_title"])

    if data["long_title"]:
        long_title = make_subelement(preface, "longTitle")
        make_subelement(long_title, "p", text=data["long_title"])

    # Body
    body = make_subelement(act, "body")

    # Add divisions
    for division in data["divisions"]:
        add_division_element(body, division, data["chapter_num"])

    # Add standalone sections (if any)
    for section in data["sections"]:
        add_section_element(body, section, data["chapter_num"])

    return root


def count_sections(data: dict) -> int:
    """Count total sections in the parsed data."""
    count = len(data["sections"])
    for div in data["divisions"]:
        count += count_sections_in_division(div)
    return count


def count_sections_in_division(division: dict) -> int:
    """Count sections within a division."""
    count = len(division["sections"])
    for subdiv in division["subdivisions"]:
        count += count_sections_in_division(subdiv)
    return count


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

    # Find all XML files
    xml_files = list(SOURCE_DIR.glob("*.xml"))
    print(f"Found {len(xml_files)} XML files in {SOURCE_DIR}")

    # Track statistics
    stats = {
        "total_files": len(xml_files),
        "converted": 0,
        "sections": 0,
        "divisions": 0,
        "errors": 0,
    }

    for filepath in sorted(xml_files):
        try:
            print(f"Processing: {filepath.name}")

            # Parse XML
            data = parse_mcl_chapter(filepath)

            if not data["chapter_num"]:
                print(f"  Warning: Could not extract chapter number from {filepath.name}")
                stats["errors"] += 1
                continue

            # Create AKN document
            akn_root = create_akn_document(data)

            # Write output
            output_filename = f"mcl-{data['chapter_num']}.akn.xml"
            output_path = OUTPUT_DIR / output_filename
            write_akn_file(akn_root, output_path)

            # Count sections
            section_count = count_sections(data)

            # Update stats
            stats["converted"] += 1
            stats["sections"] += section_count
            stats["divisions"] += len(data["divisions"])

            print(
                f"  -> {output_filename} ({len(data['divisions'])} divisions, {section_count} sections)"
            )

        except Exception as e:
            print(f"  Error processing {filepath.name}: {e}")
            import traceback

            traceback.print_exc()
            stats["errors"] += 1

    # Print summary
    print("\n" + "=" * 60)
    print("Conversion Summary")
    print("=" * 60)
    print(f"Total XML files:        {stats['total_files']}")
    print(f"Successfully converted: {stats['converted']}")
    print(f"Errors:                 {stats['errors']}")
    print(f"Total divisions:        {stats['divisions']}")
    print(f"Total sections:         {stats['sections']}")
    print(f"Output directory:       {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
