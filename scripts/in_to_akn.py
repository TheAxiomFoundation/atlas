#!/usr/bin/env python3
"""Convert Indiana Code structure to Akoma Ntoso XML.

Creates Akoma Ntoso XML structure for Indiana Code based on known
Title/Article organization. Creates one AKN file per title containing
the article structure.

Indiana Code Structure:
- Titles (e.g., Title 6: Taxation)
- Articles (e.g., Article 3: State Income Taxes)
- Chapters (e.g., Chapter 1: Definitions)
- Sections (e.g., 6-3-1-3.5: "Adjusted Gross Income")

Citation Format: IC Title-Article-Chapter-Section (e.g., IC 6-3-1-3.5)

Usage:
    python scripts/in_to_akn.py               # Create structure for tax articles
    python scripts/in_to_akn.py --all         # Create structure for all known titles
"""

import argparse
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Indiana Code titles
IN_TITLES: dict[int, str] = {
    1: "General Provisions",
    2: "Definitions; General Provisions",
    3: "Property",
    4: "State Officers and Administration",
    5: "State and Local Administration",
    6: "Taxation",
    7: "Natural and Cultural Resources",
    8: "Utilities and Transportation",
    9: "Motor Vehicles",
    10: "Public Safety",
    11: "Criminal Law and Procedure",
    12: "Human Services",
    13: "Environment",
    14: "Education",
    15: "Agriculture and Animals",
    16: "Health",
    17: "Alcohol and Tobacco",
    20: "Elections",
    21: "Civil Rights",
    22: "Labor and Safety",
    23: "Business and Other Associations",
    24: "Trade Regulation",
    25: "Professions and Occupations",
    26: "Commercial Law",
    27: "Financial Institutions",
    28: "Insurance",
    29: "Trusts and Fiduciaries",
    30: "Trusts and Trust Companies",
    31: "Family and Juvenile Law",
    32: "Property",
    33: "Courts and Court Officers",
    34: "Civil Procedure",
    35: "Criminal Law and Procedure",
    36: "Local Government",
}

# Key articles for tax analysis (Title 6: Taxation)
IN_TAX_ARTICLES: dict[str, str] = {
    "6-1": "General Provisions",
    "6-1.1": "Property Taxes",
    "6-1.5": "General Tax Administration",
    "6-2.5": "Sales and Use Tax",
    "6-3": "State Income Taxes",
    "6-3.1": "Adjusted Gross Income Tax Credits",
    "6-3.5": "County Income Taxes (Expired)",
    "6-3.6": "Local Income Taxes",
    "6-4.1": "Inheritance Tax (Expired)",
    "6-5.5": "Financial Institutions Tax",
    "6-6": "Motor Fuel and Vehicle Excise Tax",
    "6-7": "Tobacco Taxes",
    "6-8": "Commercial Licensing",
    "6-8.1": "Uniform Revenue Procedures",
    "6-9": "Innkeepers Tax; Food and Beverage Tax",
}

# Key articles for human services (Title 12: Human Services)
IN_WELFARE_ARTICLES: dict[str, str] = {
    "12-7": "General Provisions and Definitions",
    "12-8": "Secretary of Family and Social Services",
    "12-10": "Medicaid",
    "12-13": "Division of Family Resources",
    "12-14": "Family Assistance Services",
    "12-15": "Medicaid",
    "12-17": "Children's Services",
    "12-20": "Public Assistance",
    "12-21": "State Institutions",
    "12-22": "State Operated Facilities",
    "12-24": "Other State Health Institutions",
    "12-26": "Mental Health Law",
    "12-27": "Mental Health Services",
    "12-28": "Developmental Disabilities",
}

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-in-akn")

# State configuration
STATE_CONFIG = {
    "name": "Indiana",
    "code_name": "Indiana Code",
    "abbrev": "IC",
}


def create_title_akn(title_num: int, articles: dict[str, str]) -> str:
    """Create Akoma Ntoso XML for a title with its articles.

    Args:
        title_num: Indiana Code title number
        articles: Dict of article codes to article names for this title

    Returns:
        Pretty-printed AKN XML string
    """
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", f"title-{title_num}")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom-foundation")

    today = date.today().isoformat()
    work_uri = f"/akn/us-in/act/ic/title-{title_num}"
    expr_uri = f"{work_uri}/eng@{today}"
    manif_uri = f"{expr_uri}/main"

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", work_uri)
    work_uri_elem = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri_elem.set("value", work_uri)
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#in-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-in")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", str(title_num))
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", STATE_CONFIG["code_name"])

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", expr_uri)
    expr_uri_elem = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri_elem.set("value", expr_uri)
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", today)
    expr_date.set("name", "generation")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#axiom-foundation")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", manif_uri)
    manif_uri_elem = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri_elem.set("value", manif_uri)
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    org_legislature = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_legislature.set("eId", "in-legislature")
    org_legislature.set("href", "https://iga.in.gov")
    org_legislature.set("showAs", "Indiana General Assembly")

    org_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_rf.set("eId", "axiom-foundation")
    org_rf.set("href", "https://axiom-foundation.org")
    org_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Title element
    title_name = IN_TITLES.get(title_num, f"Title {title_num}")
    title_elem = ET.SubElement(body, f"{{{AKN_NS}}}title")
    title_elem.set("eId", f"title_{title_num}")

    title_num_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
    title_num_elem.text = f"Title {title_num}"

    title_heading = ET.SubElement(title_elem, f"{{{AKN_NS}}}heading")
    title_heading.text = title_name

    # Add articles for this title
    for article_code, article_name in sorted(articles.items()):
        parts = article_code.split("-")
        article_num = parts[1] if len(parts) > 1 else "1"

        article_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}article")
        article_elem.set("eId", f"art_{article_num.replace('.', '_')}")

        art_num_elem = ET.SubElement(article_elem, f"{{{AKN_NS}}}num")
        art_num_elem.text = f"Article {article_num}"

        art_heading = ET.SubElement(article_elem, f"{{{AKN_NS}}}heading")
        art_heading.text = article_name

        # Add content placeholder with source reference
        content = ET.SubElement(article_elem, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = f"See IC {article_code} at https://iga.in.gov/laws/2024/ic/titles/{title_num}/articles/{article_num}"

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ")


def main():
    parser = argparse.ArgumentParser(description="Create Indiana Code Akoma Ntoso XML structure")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Create structure for all known titles",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_DIR),
        help=f"Output directory (default: {OUTPUT_DIR})",
    )

    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Indiana Code to Akoma Ntoso Converter")
    print(f"Output directory: {output_dir}")

    # Organize articles by title
    articles_by_title: dict[int, dict[str, str]] = {}

    if args.all:
        # Include all known titles
        for title_num in IN_TITLES:
            articles_by_title[title_num] = {}
    else:
        # Default: just tax (6) and welfare (12) articles
        articles_by_title[6] = {}
        articles_by_title[12] = {}

    # Add known articles to their titles
    for article_code, article_name in IN_TAX_ARTICLES.items():
        parts = article_code.split("-")
        title_num = int(parts[0])
        if title_num in articles_by_title:
            articles_by_title[title_num][article_code] = article_name

    for article_code, article_name in IN_WELFARE_ARTICLES.items():
        parts = article_code.split("-")
        title_num = int(parts[0])
        if title_num in articles_by_title:
            articles_by_title[title_num][article_code] = article_name

    print(f"\nTitles to process: {len(articles_by_title)}")

    total_articles = 0
    files_created = 0

    for title_num, articles in sorted(articles_by_title.items()):
        title_name = IN_TITLES.get(title_num, f"Title {title_num}")
        print(f"\nTitle {title_num}: {title_name}")

        if not articles:
            # Create placeholder for titles without known article structure
            articles = {f"{title_num}-1": f"Article 1 (structure pending)"}

        print(f"  Articles: {len(articles)}")

        # Create AKN XML
        xml_content = create_title_akn(title_num, articles)

        # Write to file
        filename = f"ic-title-{title_num}.akn.xml"
        filepath = output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(xml_content)

        print(f"  -> {filename}")
        files_created += 1
        total_articles += len(articles)

    print(f"\n{'=' * 60}")
    print("Summary")
    print(f"{'=' * 60}")
    print(f"Titles processed: {len(articles_by_title)}")
    print(f"Articles included: {total_articles}")
    print(f"Files created: {files_created}")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()
