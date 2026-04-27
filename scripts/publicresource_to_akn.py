#!/usr/bin/env python3
"""Convert Public.Resource.Org HTML files to Akoma Ntoso XML.

This script converts state statute HTML files from Public.Resource.Org format
(found in Internet Archive downloads) to Akoma Ntoso XML format.

Supports: GA (Georgia), KY (Kentucky), TN (Tennessee)

HTML Structure (Public.Resource.Org format):
- Title header: <h1><b>TITLE 48</b></h1>
- Chapter nav: <li id="t48c01-cnav01">
- Section nav: <li id="t48c01s48-1-1-snav01">
- Section header: <h3 id="t48c01s48-1-1">
- Section title: <h4 class="lalign">48-1-1. Short title.</h4>
- Section text: <p>...</p>

Usage:
    python scripts/publicresource_to_akn.py ga    # Convert Georgia
    python scripts/publicresource_to_akn.py ky    # Convert Kentucky
    python scripts/publicresource_to_akn.py tn    # Convert Tennessee
    python scripts/publicresource_to_akn.py all   # Convert all
"""

import re
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

from bs4 import BeautifulSoup, NavigableString


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# State configuration
STATE_CONFIG = {
    "ga": {
        "name": "Georgia",
        "code_name": "Official Code of Georgia Annotated",
        "abbrev": "OCGA",
        "section_pattern": r"t(\d+)c([0-9A-Za-z]+)s(\d+-\d+[A-Za-z]?-\d+[A-Za-z]?)",
        "title_file_pattern": r"gov\.ga\.ocga\.title\.(\d+)\.html",
    },
    "ky": {
        "name": "Kentucky",
        "code_name": "Kentucky Revised Statutes",
        "abbrev": "KRS",
        "section_pattern": r"t([IVXLCDM]+)c([0-9A-Za-z]+)s(\d+\.\d+[A-Za-z]?)",
        "title_file_pattern": r"gov\.ky\.krs\.title\.(\d+[A-Za-z]?)\.html",
    },
    "tn": {
        "name": "Tennessee",
        "code_name": "Tennessee Code Annotated",
        "abbrev": "TCA",
        "section_pattern": r"t(\d+)c([0-9A-Za-z]+)s(\d+-\d+-\d+[A-Za-z]?)",
        "title_file_pattern": r"gov\.tn\.tca\.title\.(\d+)\.html",
    },
}


def get_data_dir(state: str) -> Path:
    """Get the data directory for a state that has HTML files."""
    base = Path(__file__).parent.parent / "data" / "statutes" / f"us-{state}"
    # Find the latest release directory that has HTML files
    releases = sorted(base.glob("release*"), reverse=True)
    for release in releases:
        html_files = list(release.glob("*.html"))
        if html_files:
            return release
    return base


def get_output_dir(state: str) -> Path:
    """Get the output directory for a state."""
    return Path(f"/tmp/rules-us-{state}-akn")


def extract_text(elem) -> str:
    """Extract text from an element, handling nested tags."""
    if elem is None:
        return ""
    texts = []
    for child in elem.children:
        if isinstance(child, NavigableString):
            texts.append(str(child))
        elif child.name in ("b", "i", "em", "strong", "span", "a"):
            texts.append(extract_text(child))
        elif child.name == "br":
            texts.append("\n")
    return "".join(texts).strip()


def parse_sections_from_html(soup: BeautifulSoup, config: dict) -> list[dict]:
    """Parse sections from Public.Resource.Org HTML.

    Returns list of section dicts with keys:
    - section_id: Full section ID (e.g., "48-1-1")
    - title_num: Title number
    - chapter: Chapter number/letter
    - title: Section title text
    - text: Section body text
    - subsections: List of subsection dicts
    """
    sections = []
    section_pattern = re.compile(config["section_pattern"])

    # Find all section headers (h3 with section ID)
    for h3 in soup.find_all("h3", id=section_pattern):
        section_id_match = section_pattern.match(h3.get("id", ""))
        if not section_id_match:
            continue

        title_num = section_id_match.group(1)
        chapter = section_id_match.group(2)
        section_num = section_id_match.group(3)

        # Find section title (next h4 with class lalign)
        title_elem = h3.find_next("h4", class_="lalign")
        if title_elem:
            # Parse "48-1-1. Short title." format
            title_text = extract_text(title_elem)
            # Remove section number prefix if present
            title_text = re.sub(r"^\d+[-\d.A-Za-z]+\.\s*", "", title_text)
        else:
            title_text = ""

        # Collect body paragraphs until next section
        body_parts = []
        subsections = []
        current = h3.find_next_sibling()

        while current:
            # Stop at next section header
            if current.name == "h3" and section_pattern.match(current.get("id", "") or ""):
                break
            # Stop at chapter/title headers
            if current.name in ("h1", "h2"):
                break

            if current.name == "p":
                # Check for subsection markers
                text = extract_text(current)
                subsec_match = re.match(r"^\(([a-z0-9]+)\)\s*", text)
                if subsec_match:
                    subsec_id = subsec_match.group(1)
                    subsec_text = text[subsec_match.end() :]
                    subsections.append(
                        {
                            "id": subsec_id,
                            "text": subsec_text,
                        }
                    )
                else:
                    body_parts.append(text)

            current = current.find_next_sibling()

        sections.append(
            {
                "section_id": section_num,
                "title_num": title_num,
                "chapter": chapter,
                "title": title_text,
                "text": "\n\n".join(body_parts),
                "subsections": subsections,
            }
        )

    return sections


def create_akn_xml(section: dict, state: str, config: dict) -> str:
    """Create Akoma Ntoso XML for a section."""
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#publicresource")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/us-{state}/act/{config['abbrev'].lower()}/{section['section_id']}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/us-{state}/act/{config['abbrev'].lower()}/{section['section_id']}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", str(date.today()))
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", f"#{state}-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", f"us-{state}")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/us-{state}/act/{config['abbrev'].lower()}/{section['section_id']}/eng"
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/us-{state}/act/{config['abbrev'].lower()}/{section['section_id']}/eng")
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", str(date.today()))
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#publicresource")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set(
        "value", f"/us-{state}/act/{config['abbrev'].lower()}/{section['section_id']}/eng/akn"
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value", f"/us-{state}/act/{config['abbrev'].lower()}/{section['section_id']}/eng/akn"
    )
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", str(date.today()))
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    org_legislature = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_legislature.set("eId", f"{state}-legislature")
    org_legislature.set("href", f"/ontology/organization/us-{state}/legislature")
    org_legislature.set("showAs", f"{config['name']} General Assembly")

    org_publicresource = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_publicresource.set("eId", "publicresource")
    org_publicresource.set("href", "https://public.resource.org")
    org_publicresource.set("showAs", "Public.Resource.Org")

    org_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_rf.set("eId", "axiom-foundation")
    org_rf.set("href", "https://axiom-foundation.org")
    org_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Section article
    article = ET.SubElement(body, f"{{{AKN_NS}}}section")
    article.set("eId", f"sec_{section['section_id'].replace('-', '_').replace('.', '_')}")

    # Section number
    num = ET.SubElement(article, f"{{{AKN_NS}}}num")
    num.text = section["section_id"]

    # Section heading
    if section["title"]:
        heading = ET.SubElement(article, f"{{{AKN_NS}}}heading")
        heading.text = section["title"]

    # Content
    content = ET.SubElement(article, f"{{{AKN_NS}}}content")

    # Main text
    if section["text"]:
        for para in section["text"].split("\n\n"):
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para.strip()

    # Subsections
    for subsec in section["subsections"]:
        subsection = ET.SubElement(article, f"{{{AKN_NS}}}subsection")
        subsection.set(
            "eId",
            f"sec_{section['section_id'].replace('-', '_').replace('.', '_')}__subsec_{subsec['id']}",
        )

        subsec_num = ET.SubElement(subsection, f"{{{AKN_NS}}}num")
        subsec_num.text = f"({subsec['id']})"

        subsec_content = ET.SubElement(subsection, f"{{{AKN_NS}}}content")
        subsec_p = ET.SubElement(subsec_content, f"{{{AKN_NS}}}p")
        subsec_p.text = subsec["text"]

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ")


def convert_state(state: str) -> tuple[int, int]:
    """Convert all HTML files for a state to AKN.

    Returns (files_processed, sections_converted).
    """
    if state not in STATE_CONFIG:
        print(f"Unknown state: {state}")
        return 0, 0

    config = STATE_CONFIG[state]
    data_dir = get_data_dir(state)
    output_dir = get_output_dir(state)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nConverting {config['name']} ({state.upper()})")
    print(f"  Source: {data_dir}")
    print(f"  Output: {output_dir}")

    # Find HTML files matching pattern
    title_pattern = re.compile(config["title_file_pattern"])
    html_files = sorted([f for f in data_dir.glob("*.html") if title_pattern.match(f.name)])

    if not html_files:
        print(f"  No matching HTML files found!")
        return 0, 0

    print(f"  Found {len(html_files)} title files")

    total_sections = 0

    for html_file in html_files:
        title_match = title_pattern.match(html_file.name)
        if not title_match:
            continue

        title_num = title_match.group(1)
        print(f"  Processing Title {title_num}...", end=" ", flush=True)

        with open(html_file, "r", encoding="utf-8", errors="replace") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        sections = parse_sections_from_html(soup, config)
        print(f"{len(sections)} sections")

        # Create title directory
        title_dir = output_dir / f"title-{title_num}"
        title_dir.mkdir(exist_ok=True)

        for section in sections:
            # Create section file
            section_filename = f"{section['section_id'].replace('.', '-')}.xml"
            section_path = title_dir / section_filename

            xml_content = create_akn_xml(section, state, config)

            with open(section_path, "w", encoding="utf-8") as f:
                f.write(xml_content)

            total_sections += 1

    print(f"  Total: {total_sections} sections converted")
    return len(html_files), total_sections


def main():
    if len(sys.argv) < 2:
        print("Usage: python publicresource_to_akn.py <state|all>")
        print("States: ga, ky, tn")
        sys.exit(1)

    target = sys.argv[1].lower()

    if target == "all":
        states = list(STATE_CONFIG.keys())
    else:
        states = [target]

    total_files = 0
    total_sections = 0

    for state in states:
        files, sections = convert_state(state)
        total_files += files
        total_sections += sections

    print(f"\n{'=' * 50}")
    print(f"Total: {total_files} files, {total_sections} sections converted")


if __name__ == "__main__":
    main()
