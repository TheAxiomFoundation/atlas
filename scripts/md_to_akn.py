#!/usr/bin/env python3
"""Convert Maryland state statutes to Akoma Ntoso XML format.

This script fetches Maryland Code from mgaleg.maryland.gov using the MDConverter
and converts sections to Akoma Ntoso XML format.

Maryland Code Structure:
- Articles (e.g., Tax - General (gtg), Human Services (ghu))
- Titles (e.g., Title 10 - Income Tax)
- Subtitles (e.g., Subtitle 1 - Definitions; General Provisions)
- Sections (e.g., 10-105 - State income tax rates)

Usage:
    python scripts/md_to_akn.py              # Convert all tax/benefit articles
    python scripts/md_to_akn.py gtg          # Convert Tax-General article only
    python scripts/md_to_akn.py ghu          # Convert Human Services article only
    python scripts/md_to_akn.py --all        # Convert ALL articles (slow!)
"""

import re
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.md import (
    MDConverter,
    MD_ARTICLES,
    MD_TAX_ARTICLES,
    MD_WELFARE_ARTICLES,
)
from atlas.models import Section


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-md-akn")

# State configuration
STATE = "md"
STATE_NAME = "Maryland"
CODE_NAME = "Annotated Code of Maryland"
ABBREV = "MDCODE"


def sanitize_id(text: str) -> str:
    """Convert text to a valid XML ID."""
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"-+", "_", text)
    return text.lower()[:50]


def create_akn_xml(section: Section, article_code: str, article_name: str) -> str:
    """Create Akoma Ntoso XML for a Maryland section."""
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Parse section number from citation (e.g., "MD-gtg-10-105" -> "10-105")
    section_num = section.citation.section.replace(f"MD-{article_code}-", "")

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#maryland-ga")

    # Build URIs
    base_uri = f"/us-{STATE}/act/{article_code}/{section_num}"

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", base_uri)
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", base_uri)
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", str(section.retrieved_at or date.today()))
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#maryland-ga")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", f"us-{STATE}")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", section_num)
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", CODE_NAME)

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"{base_uri}/eng@{date.today().isoformat()}")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"{base_uri}/eng@{date.today().isoformat()}")
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", str(date.today()))
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#axiom-foundation")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", f"{base_uri}/eng@{date.today().isoformat()}/akn")
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"{base_uri}/eng@{date.today().isoformat()}/akn")
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", str(date.today()))
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    org_legislature = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_legislature.set("eId", "maryland-ga")
    org_legislature.set("href", "https://mgaleg.maryland.gov")
    org_legislature.set("showAs", "Maryland General Assembly")

    org_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_rf.set("eId", "axiom-foundation")
    org_rf.set("href", "https://axiom-foundation.org")
    org_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Section article
    section_elem = ET.SubElement(body, f"{{{AKN_NS}}}section")
    section_elem.set("eId", f"sec_{sanitize_id(section_num)}")

    # Section number
    num = ET.SubElement(section_elem, f"{{{AKN_NS}}}num")
    num.text = f"{article_code.upper()} {section_num}"

    # Section heading
    if section.section_title:
        heading = ET.SubElement(section_elem, f"{{{AKN_NS}}}heading")
        heading.text = section.section_title

    # Content wrapper for intro text
    content = ET.SubElement(section_elem, f"{{{AKN_NS}}}content")

    # Add intro text (text before first subsection)
    intro_text = section.text
    if section.subsections:
        # Try to extract intro text before first subsection
        first_subsec_marker = f"({section.subsections[0].identifier})"
        if first_subsec_marker in intro_text:
            intro_idx = intro_text.find(first_subsec_marker)
            intro_text = intro_text[:intro_idx].strip()

    if intro_text and len(intro_text) < len(section.text):
        # Only add intro if it's genuinely intro text, not the whole thing
        for para in intro_text.split("\n\n"):
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para.strip()

    # Subsections
    for subsec in section.subsections:
        subsection = ET.SubElement(section_elem, f"{{{AKN_NS}}}subsection")
        subsection.set("eId", f"sec_{sanitize_id(section_num)}__subsec_{subsec.identifier}")

        subsec_num = ET.SubElement(subsection, f"{{{AKN_NS}}}num")
        subsec_num.text = f"({subsec.identifier})"

        subsec_content = ET.SubElement(subsection, f"{{{AKN_NS}}}content")
        subsec_p = ET.SubElement(subsec_content, f"{{{AKN_NS}}}p")
        subsec_p.text = subsec.text

        # Add nested children (level 2)
        for child in subsec.children:
            child_elem = ET.SubElement(subsection, f"{{{AKN_NS}}}paragraph")
            child_elem.set(
                "eId",
                f"sec_{sanitize_id(section_num)}__subsec_{subsec.identifier}__para_{child.identifier}",
            )

            child_num = ET.SubElement(child_elem, f"{{{AKN_NS}}}num")
            child_num.text = f"({child.identifier})"

            child_content = ET.SubElement(child_elem, f"{{{AKN_NS}}}content")
            child_p = ET.SubElement(child_content, f"{{{AKN_NS}}}p")
            child_p.text = child.text

            # Add level 3 children (subparagraphs)
            for grandchild in child.children:
                gc_elem = ET.SubElement(child_elem, f"{{{AKN_NS}}}subparagraph")
                gc_elem.set(
                    "eId",
                    f"sec_{sanitize_id(section_num)}__subsec_{subsec.identifier}__para_{child.identifier}__subpara_{grandchild.identifier}",
                )

                gc_num = ET.SubElement(gc_elem, f"{{{AKN_NS}}}num")
                gc_num.text = f"({grandchild.identifier})"

                gc_content = ET.SubElement(gc_elem, f"{{{AKN_NS}}}content")
                gc_p = ET.SubElement(gc_content, f"{{{AKN_NS}}}p")
                gc_p.text = grandchild.text

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ")


def convert_article(converter: MDConverter, article_code: str) -> tuple[int, int]:
    """Convert all sections in a Maryland article to AKN.

    Returns (sections_fetched, sections_converted).
    """
    article_name = MD_ARTICLES.get(article_code, f"Article {article_code}")
    print(f"\nConverting {article_name} ({article_code.upper()})")

    # Create article directory
    article_dir = OUTPUT_DIR / article_code
    article_dir.mkdir(parents=True, exist_ok=True)

    sections_fetched = 0
    sections_converted = 0
    errors = 0

    try:
        section_numbers = converter.get_article_section_numbers(article_code)
        print(f"  Found {len(section_numbers)} sections")

        for section_num in section_numbers:
            try:
                section = converter.fetch_section(article_code, section_num)
                sections_fetched += 1

                # Create AKN XML
                xml_content = create_akn_xml(section, article_code, article_name)

                # Write to file
                filename = f"{section_num.replace('.', '-')}.xml"
                filepath = article_dir / filename
                filepath.write_text(xml_content, encoding="utf-8")
                sections_converted += 1

                # Progress indicator
                if sections_converted % 50 == 0:
                    print(f"    Converted {sections_converted} sections...")

            except Exception as e:
                errors += 1
                print(f"    Error on {section_num}: {e}")

    except Exception as e:
        print(f"  Error fetching article: {e}")

    print(f"  Completed: {sections_converted} converted, {errors} errors")
    return sections_fetched, sections_converted


def main():
    """Main entry point."""
    # Parse arguments
    articles_to_convert = []

    if len(sys.argv) < 2:
        # Default: convert tax and welfare articles
        articles_to_convert = list(MD_TAX_ARTICLES.keys()) + list(MD_WELFARE_ARTICLES.keys())
        print("Converting default articles (tax + human services)")
    elif sys.argv[1] == "--all":
        articles_to_convert = list(MD_ARTICLES.keys())
        print("Converting ALL articles (this will take a while!)")
    else:
        for arg in sys.argv[1:]:
            if arg.lower() in MD_ARTICLES:
                articles_to_convert.append(arg.lower())
            else:
                print(f"Unknown article code: {arg}")
                print(f"Valid codes: {', '.join(sorted(MD_ARTICLES.keys()))}")
                sys.exit(1)

    print(f"Output directory: {OUTPUT_DIR}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_fetched = 0
    total_converted = 0

    with MDConverter(rate_limit_delay=0.3) as converter:
        for article_code in articles_to_convert:
            fetched, converted = convert_article(converter, article_code)
            total_fetched += fetched
            total_converted += converted

    print(f"\n{'=' * 60}")
    print("Conversion Summary")
    print("=" * 60)
    print(f"Articles processed:    {len(articles_to_convert)}")
    print(f"Sections fetched:      {total_fetched}")
    print(f"Sections converted:    {total_converted}")
    print(f"Output directory:      {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
