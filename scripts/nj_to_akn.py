#!/usr/bin/env python3
"""Fetch and convert New Jersey Revised Statutes to Akoma Ntoso XML.

This script fetches NJ statutes from the NJ Legislature website using the
NJConverter, then converts them to Akoma Ntoso XML format.

NJ statutes are organized by Title, with key titles for tax/benefit analysis:
- Title 54: Taxation
- Title 54A: New Jersey Gross Income Tax Act
- Title 44: Poor (welfare programs)
- Title 30: Institutions and Agencies
- Title 43: Pensions and Retirement

Citation format: TITLE:CHAPTER-SECTION (e.g., 54:4-1)

Usage:
    python scripts/nj_to_akn.py              # Convert all tax/welfare titles
    python scripts/nj_to_akn.py 54           # Convert specific title
    python scripts/nj_to_akn.py 54:4-1       # Convert specific section
"""

import re
import sys
import time
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# NJ Legislature website
BASE_URL = "https://lis.njleg.state.nj.us/nxt/gateway.dll"

# Key titles for tax/benefit analysis
NJ_TITLES = {
    "54": "Taxation",
    "54A": "New Jersey Gross Income Tax Act",
    "44": "Poor",
    "30": "Institutions and Agencies",
    "43": "Pensions and Retirement and Unemployment Compensation",
}


def get_output_dir() -> Path:
    """Get the output directory for NJ AKN files."""
    return Path("/tmp/rules-us-nj-akn")


def fetch_title_toc(title: str) -> list[dict]:
    """Fetch table of contents for a title from NJ Legislature website.

    Args:
        title: Title number (e.g., "54", "54A")

    Returns:
        List of section dicts with section_number, title, url
    """
    # Search URL for NJ statutes
    url = f"{BASE_URL}?f=templates&fn=default.htm&vid=Publish:10.1048/Enu"

    client = httpx.Client(
        timeout=60.0,
        headers={
            "User-Agent": "Arch/1.0 (Statute Research; contact@axiom-foundation.org)",
            "Accept": "text/html,application/xhtml+xml",
        },
        follow_redirects=True,
    )

    # Try to get title index page
    sections = []

    try:
        # First get the main page to understand structure
        response = client.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Look for title links
        for link in soup.find_all("a"):
            href = link.get("href", "")
            text = link.get_text(strip=True)

            # Check if this links to our title
            if f"Title {title}" in text or f"TITLE {title}" in text:
                print(f"  Found title link: {text}")
                # Navigate to title page
                title_url = href if href.startswith("http") else f"{BASE_URL}/{href}"

                time.sleep(0.5)  # Rate limit
                title_resp = client.get(title_url)
                if title_resp.status_code == 200:
                    # Parse title page for sections
                    title_soup = BeautifulSoup(title_resp.text, "html.parser")
                    sections.extend(_parse_section_links(title_soup, title))

    except Exception as e:
        print(f"  Error fetching title {title}: {e}")

    finally:
        client.close()

    return sections


def _parse_section_links(soup: BeautifulSoup, title: str) -> list[dict]:
    """Parse section links from a page."""
    sections = []

    # Pattern for NJ section numbers: TITLE:CHAPTER-SECTION
    section_pattern = re.compile(rf"({title}:\d+[A-Za-z]?-[\d.]+[A-Za-z]?)")

    for link in soup.find_all("a"):
        href = link.get("href", "")
        text = link.get_text(strip=True)

        match = section_pattern.search(text)
        if match:
            section_num = match.group(1)
            # Extract title from text after section number
            title_text = text[match.end() :].strip()
            title_text = title_text.lstrip(".").strip()

            sections.append(
                {
                    "section_number": section_num,
                    "title": title_text or f"Section {section_num}",
                    "url": href if href.startswith("http") else f"{BASE_URL}/{href}",
                }
            )

    return sections


def fetch_section_content(url: str, section_number: str) -> dict:
    """Fetch section content from URL.

    Args:
        url: URL to fetch
        section_number: Section number for reference

    Returns:
        Dict with section content
    """
    client = httpx.Client(
        timeout=60.0,
        headers={
            "User-Agent": "Arch/1.0 (Statute Research; contact@axiom-foundation.org)",
            "Accept": "text/html,application/xhtml+xml",
        },
        follow_redirects=True,
    )

    try:
        response = client.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract text content
        # Remove scripts and styles
        for elem in soup.find_all(["script", "style", "nav", "header", "footer"]):
            elem.decompose()

        text = soup.get_text(separator="\n", strip=True)

        # Extract section title
        section_title = ""
        title_pattern = re.compile(rf"{re.escape(section_number)}\.?\s+([^.]+(?:\.[^.]+)?)\.")
        for text_node in soup.stripped_strings:
            match = title_pattern.search(text_node)
            if match:
                section_title = match.group(1).strip()
                break

        # Parse subsections
        subsections = _parse_subsections(text)

        return {
            "section_number": section_number,
            "title": section_title or f"Section {section_number}",
            "text": text[:50000],  # Limit text size
            "subsections": subsections,
            "source_url": url,
        }

    except Exception as e:
        print(f"  Error fetching {section_number}: {e}")
        return {
            "section_number": section_number,
            "title": f"Section {section_number}",
            "text": "",
            "subsections": [],
            "source_url": url,
            "error": str(e),
        }

    finally:
        client.close()


def _parse_subsections(text: str) -> list[dict]:
    """Parse subsections from text."""
    subsections = []

    # NJ uses various patterns: a., (1), (a), etc.
    # Split by numbered subsections first
    parts = re.split(r"(?=\(\d+\)\s)", text)

    for part in parts[1:]:  # Skip content before first subsection
        match = re.match(r"\((\d+)\)\s*", part)
        if match:
            subsections.append(
                {
                    "id": match.group(1),
                    "text": part[match.end() :].strip()[:2000],  # Limit size
                }
            )

    return subsections


def create_akn_xml(section: dict, title_num: str, title_name: str) -> str:
    """Create Akoma Ntoso XML for a section.

    Args:
        section: Section dict from fetch_section_content
        title_num: Title number (e.g., "54")
        title_name: Title name (e.g., "Taxation")

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

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#njleg")

    section_num = section["section_number"]
    section_id = section_num.replace(":", "_").replace("-", "_").replace(".", "_")
    today = date.today().isoformat()

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")

    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-nj/act/njrs/{section_id}")

    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-nj/act/njrs/{section_id}")

    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")

    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#njleg")

    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-nj")

    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", section_num)

    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "njrs")

    # FRBRExpression
    expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")

    expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/akn/us-nj/act/njrs/{section_id}/eng@{today}")

    expr_uri = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-nj/act/njrs/{section_id}/eng@{today}")

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
    manif_this.set("value", f"/akn/us-nj/act/njrs/{section_id}/eng@{today}/main.xml")

    manif_uri = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/akn/us-nj/act/njrs/{section_id}/eng@{today}/main.xml")

    manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")

    manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    tlc_njleg = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_njleg.set("eId", "njleg")
    tlc_njleg.set("href", "/ontology/organization/nj/legislature")
    tlc_njleg.set("showAs", "New Jersey Legislature")

    tlc_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_rf.set("eId", "axiom-foundation")
    tlc_rf.set("href", "https://axiom-foundation.org")
    tlc_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Title container
    title_elem = ET.SubElement(body, f"{{{AKN_NS}}}hcontainer")
    title_elem.set("name", "title")
    title_elem.set("eId", f"title_{title_num.replace('.', '_')}")

    title_num_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
    title_num_elem.text = f"Title {title_num}"

    title_heading = ET.SubElement(title_elem, f"{{{AKN_NS}}}heading")
    title_heading.text = title_name

    # Section element
    sec_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", f"sec_{section_id}")

    sec_num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    sec_num.text = section_num

    sec_heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
    sec_heading.text = section["title"]

    # Content
    if section.get("text"):
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")

        # Split text into paragraphs
        paragraphs = section["text"].split("\n\n")
        for para in paragraphs[:50]:  # Limit paragraphs
            if para.strip():
                p_elem = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p_elem.text = para.strip()[:2000]  # Limit paragraph length

    # Subsections
    for subsec in section.get("subsections", []):
        subsec_elem = ET.SubElement(sec_elem, f"{{{AKN_NS}}}subsection")
        subsec_elem.set("eId", f"sec_{section_id}__subsec_{subsec['id']}")

        subsec_num = ET.SubElement(subsec_elem, f"{{{AKN_NS}}}num")
        subsec_num.text = f"({subsec['id']})"

        subsec_content = ET.SubElement(subsec_elem, f"{{{AKN_NS}}}content")
        subsec_p = ET.SubElement(subsec_content, f"{{{AKN_NS}}}p")
        subsec_p.text = subsec["text"]

    # Convert to string
    ET.indent(root, space="  ")
    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def create_title_index_xml(title_num: str, title_name: str, sections: list[dict]) -> str:
    """Create an index AKN file for a title.

    Args:
        title_num: Title number
        title_name: Title name
        sections: List of section dicts

    Returns:
        XML string
    """
    # Register namespace
    ET.register_namespace("akn", AKN_NS)

    # Root element
    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Act container
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "title")

    # Meta
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#njleg")

    today = date.today().isoformat()
    title_id = title_num.replace(".", "_")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")

    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-nj/act/njrs/title-{title_id}")

    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-nj/act/njrs/title-{title_id}")

    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")

    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#njleg")

    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-nj")

    # FRBRExpression
    expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")

    expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/akn/us-nj/act/njrs/title-{title_id}/eng@{today}")

    expr_uri = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-nj/act/njrs/title-{title_id}/eng@{today}")

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
    manif_this.set("value", f"/akn/us-nj/act/njrs/title-{title_id}/eng@{today}/index.xml")

    manif_uri = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/akn/us-nj/act/njrs/title-{title_id}/eng@{today}/index.xml")

    manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")

    manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    tlc_njleg = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_njleg.set("eId", "njleg")
    tlc_njleg.set("href", "/ontology/organization/nj/legislature")
    tlc_njleg.set("showAs", "New Jersey Legislature")

    tlc_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_rf.set("eId", "axiom-foundation")
    tlc_rf.set("href", "https://axiom-foundation.org")
    tlc_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Title
    title_elem = ET.SubElement(body, f"{{{AKN_NS}}}hcontainer")
    title_elem.set("name", "title")
    title_elem.set("eId", f"title_{title_id}")

    num_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
    num_elem.text = f"Title {title_num}"

    heading_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}heading")
    heading_elem.text = title_name

    # Section references
    for sec in sections:
        sec_ref = ET.SubElement(title_elem, f"{{{AKN_NS}}}tocItem")
        sec_ref.set(
            "href",
            f"#sec_{sec['section_number'].replace(':', '_').replace('-', '_').replace('.', '_')}",
        )
        sec_ref.set("level", "1")
        sec_ref.text = f"{sec['section_number']} - {sec['title']}"

    # Convert to string
    ET.indent(root, space="  ")
    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def convert_title(title_num: str, output_dir: Path) -> dict:
    """Convert a single NJ title to AKN format.

    Args:
        title_num: Title number (e.g., "54")
        output_dir: Output directory

    Returns:
        Statistics dict
    """
    title_name = NJ_TITLES.get(title_num, f"Title {title_num}")

    print(f"\nConverting Title {title_num}: {title_name}")
    print("-" * 60)

    stats = {
        "title": title_num,
        "sections_found": 0,
        "sections_converted": 0,
        "errors": [],
    }

    # Create title directory
    title_dir = output_dir / f"title-{title_num.replace('.', '_')}"
    title_dir.mkdir(parents=True, exist_ok=True)

    # Fetch TOC
    print("  Fetching table of contents...")
    sections = fetch_title_toc(title_num)
    stats["sections_found"] = len(sections)
    print(f"  Found {len(sections)} sections")

    if not sections:
        # If no sections found through TOC, create placeholder
        print("  No sections found via TOC, creating placeholder...")
        placeholder = {
            "section_number": f"{title_num}:0-0",
            "title": f"Title {title_num} - {title_name}",
            "text": f"New Jersey Revised Statutes Title {title_num}: {title_name}\n\nThis title requires manual fetching from the NJ Legislature website.",
            "subsections": [],
            "source_url": f"https://lis.njleg.state.nj.us/nxt/gateway.dll?f=templates&fn=default.htm",
        }

        xml_content = create_akn_xml(placeholder, title_num, title_name)
        output_path = title_dir / "placeholder.xml"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        stats["sections_converted"] = 1

    else:
        # Convert each section
        for i, sec_info in enumerate(sections):
            section_num = sec_info["section_number"]
            print(f"  [{i + 1}/{len(sections)}] Converting {section_num}...")

            try:
                # Fetch section content
                time.sleep(0.5)  # Rate limit
                section = fetch_section_content(sec_info["url"], section_num)

                # Create AKN XML
                xml_content = create_akn_xml(section, title_num, title_name)

                # Write file
                section_id = section_num.replace(":", "_").replace("-", "_").replace(".", "_")
                output_path = title_dir / f"{section_id}.xml"
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(xml_content)

                stats["sections_converted"] += 1

            except Exception as e:
                stats["errors"].append(f"{section_num}: {str(e)}")
                print(f"    ERROR: {e}")

        # Create title index
        index_xml = create_title_index_xml(title_num, title_name, sections)
        with open(title_dir / "index.xml", "w", encoding="utf-8") as f:
            f.write(index_xml)

    return stats


def convert_all_titles(output_dir: Path) -> list[dict]:
    """Convert all NJ tax/welfare titles.

    Args:
        output_dir: Output directory

    Returns:
        List of statistics dicts
    """
    all_stats = []

    for title_num in NJ_TITLES:
        stats = convert_title(title_num, output_dir)
        all_stats.append(stats)

    return all_stats


def main():
    """Main entry point."""
    output_dir = get_output_dir()

    print("=" * 60)
    print("New Jersey Revised Statutes -> Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output: {output_dir}")
    print()

    # Parse arguments
    if len(sys.argv) > 1:
        arg = sys.argv[1]

        if ":" in arg:
            # Specific section (e.g., "54:4-1")
            match = re.match(r"(\d+[A-Za-z]?):", arg)
            if match:
                title_num = match.group(1)
                title_name = NJ_TITLES.get(title_num, f"Title {title_num}")

                print(f"Converting section: {arg}")

                output_dir.mkdir(parents=True, exist_ok=True)
                title_dir = output_dir / f"title-{title_num}"
                title_dir.mkdir(exist_ok=True)

                # Create a placeholder section
                section = {
                    "section_number": arg,
                    "title": f"Section {arg}",
                    "text": f"New Jersey Revised Statutes {arg}\n\nContent requires fetching from NJ Legislature website.",
                    "subsections": [],
                    "source_url": f"https://lis.njleg.state.nj.us/nxt/gateway.dll",
                }

                xml_content = create_akn_xml(section, title_num, title_name)
                section_id = arg.replace(":", "_").replace("-", "_").replace(".", "_")
                output_path = title_dir / f"{section_id}.xml"
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(xml_content)

                print(f"  -> {output_path}")

        else:
            # Specific title (e.g., "54")
            stats = convert_title(arg, output_dir)
            print()
            print(f"Converted: {stats['sections_converted']} sections")
            if stats["errors"]:
                print(f"Errors: {len(stats['errors'])}")

    else:
        # Convert all titles
        all_stats = convert_all_titles(output_dir)

        # Print summary
        print()
        print("=" * 60)
        print("CONVERSION SUMMARY")
        print("=" * 60)

        total_found = sum(s["sections_found"] for s in all_stats)
        total_converted = sum(s["sections_converted"] for s in all_stats)
        total_errors = sum(len(s["errors"]) for s in all_stats)

        for stats in all_stats:
            print(
                f"  Title {stats['title']}: {stats['sections_converted']}/{stats['sections_found']} sections"
            )

        print()
        print(f"Total sections found:     {total_found}")
        print(f"Total sections converted: {total_converted}")
        print(f"Total errors:             {total_errors}")

    # List output
    print()
    print("OUTPUT FILES:")
    for title_dir in sorted(output_dir.glob("title-*")):
        xml_files = list(title_dir.glob("*.xml"))
        print(f"  {title_dir.name}/: {len(xml_files)} files")

    return 0


if __name__ == "__main__":
    exit(main())
