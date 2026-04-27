#!/usr/bin/env python3
"""Convert Mississippi Code to Akoma Ntoso XML format.

This script fetches Mississippi statutes from UniCourt's GitHub Pages
(unicourt.github.io/cic-code-ms) and converts them to Akoma Ntoso XML.

Usage:
    python scripts/ms_to_akn.py                    # Convert all titles
    python scripts/ms_to_akn.py --title 27         # Convert only Title 27 (Tax)
    python scripts/ms_to_akn.py --title 43         # Convert only Title 43 (Welfare)
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from xml.dom import minidom
from xml.etree import ElementTree as ET

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.ms import MSConverter, MS_TITLES
from atlas.models import Section

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def create_akn_xml(section: Section) -> str:
    """Create Akoma Ntoso XML for a Mississippi statute section."""
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Parse section info from citation (format: MS-27-7-1)
    section_id = section.citation.section.replace("MS-", "")
    parts = section_id.split("-")
    title_num = parts[0] if len(parts) >= 1 else "0"
    chapter_num = parts[1] if len(parts) >= 2 else "0"

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#unicourt")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/us-ms/act/msc/{section_id}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/us-ms/act/msc/{section_id}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", str(date.today()))
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#ms-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ms")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/us-ms/act/msc/{section_id}/eng")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/us-ms/act/msc/{section_id}/eng")
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", str(date.today()))
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#unicourt")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", f"/us-ms/act/msc/{section_id}/eng/akn")
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/us-ms/act/msc/{section_id}/eng/akn")
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", str(date.today()))
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    org_legislature = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_legislature.set("eId", "ms-legislature")
    org_legislature.set("href", "/ontology/organization/us-ms/legislature")
    org_legislature.set("showAs", "Mississippi Legislature")

    org_unicourt = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_unicourt.set("eId", "unicourt")
    org_unicourt.set("href", "https://unicourt.github.io/cic-code-ms")
    org_unicourt.set("showAs", "UniCourt CIC Project")

    org_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_rf.set("eId", "axiom-foundation")
    org_rf.set("href", "https://axiom-foundation.org")
    org_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Section article
    article = ET.SubElement(body, f"{{{AKN_NS}}}section")
    article.set("eId", f"sec_{section_id.replace('-', '_')}")

    # Section number
    num = ET.SubElement(article, f"{{{AKN_NS}}}num")
    num.text = section_id

    # Section heading
    if section.section_title:
        heading = ET.SubElement(article, f"{{{AKN_NS}}}heading")
        heading.text = section.section_title

    # Content
    content = ET.SubElement(article, f"{{{AKN_NS}}}content")

    # Main text
    if section.text:
        for para in section.text.split("\n\n"):
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para.strip()

    # Subsections
    for subsec in section.subsections:
        subsection = ET.SubElement(article, f"{{{AKN_NS}}}subsection")
        subsection.set("eId", f"sec_{section_id.replace('-', '_')}__subsec_{subsec.identifier}")

        subsec_num = ET.SubElement(subsection, f"{{{AKN_NS}}}num")
        subsec_num.text = f"({subsec.identifier})"

        subsec_content = ET.SubElement(subsection, f"{{{AKN_NS}}}content")
        if subsec.text:
            subsec_p = ET.SubElement(subsec_content, f"{{{AKN_NS}}}p")
            subsec_p.text = subsec.text

        # Nested subsections
        for child in subsec.children:
            child_subsec = ET.SubElement(subsection, f"{{{AKN_NS}}}subsection")
            child_subsec.set(
                "eId",
                f"sec_{section_id.replace('-', '_')}__subsec_{subsec.identifier}_{child.identifier}",
            )

            child_num = ET.SubElement(child_subsec, f"{{{AKN_NS}}}num")
            child_num.text = f"({child.identifier})"

            child_content = ET.SubElement(child_subsec, f"{{{AKN_NS}}}content")
            if child.text:
                child_p = ET.SubElement(child_content, f"{{{AKN_NS}}}p")
                child_p.text = child.text

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ")


def convert_title(converter: MSConverter, title: int, output_dir: Path) -> int:
    """Convert a single title to AKN format.

    Returns number of sections converted.
    """
    title_name = MS_TITLES.get(title, f"Title {title}")
    print(f"  Converting Title {title}: {title_name}")

    # Create title directory
    title_dir = output_dir / f"title-{title:02d}"
    title_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    try:
        for section in converter.iter_title(title):
            # Extract section number from citation
            section_id = section.citation.section.replace("MS-", "")

            # Create filename
            filename = f"{section_id}.xml"
            filepath = title_dir / filename

            # Generate AKN XML
            xml_content = create_akn_xml(section)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(xml_content)

            count += 1
            if count % 50 == 0:
                print(f"    ... {count} sections")

    except Exception as e:
        print(f"    Error processing title {title}: {e}")

    print(f"    Completed: {count} sections")
    return count


def main():
    parser = argparse.ArgumentParser(description="Convert Mississippi Code to Akoma Ntoso XML")
    parser.add_argument(
        "--title",
        type=int,
        help="Convert only this title number (e.g., 27 for Tax)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="/tmp/rules-us-ms-akn",
        help="Output directory (default: /tmp/rules-us-ms-akn)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Converting Mississippi Code to Akoma Ntoso XML")
    print(f"Output directory: {output_dir}")
    print()

    total_sections = 0

    with MSConverter(rate_limit_delay=0.3) as converter:
        if args.title:
            # Convert single title
            titles = [args.title]
        else:
            # Convert all titles
            titles = sorted(MS_TITLES.keys())

        for title in titles:
            count = convert_title(converter, title, output_dir)
            total_sections += count

    print()
    print(f"{'=' * 50}")
    print(f"Total: {len(titles)} titles, {total_sections} sections converted")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
