#!/usr/bin/env python3
"""Create Akoma Ntoso XML index structure for Arkansas Code.

This script creates the title/chapter structure for Arkansas Code without
requiring network access. Use this as a placeholder until full section
content can be obtained.

Usage:
    python scripts/ar_create_akn_index.py
"""

from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Arkansas Code Title information
AR_TITLES = {
    1: "General Provisions",
    2: "Legislative Department",
    3: "State Highway Commission and State Aid Roads",
    4: "General Assembly",
    5: "State Departments",
    6: "Local Government",
    7: "Corporations and Associations",
    8: "Adoption and Legitimation",
    9: "Family Law",
    10: "Criminal Law",
    11: "Civil Procedure",
    12: "Courts and Court Officers",
    13: "Administrative Procedure",
    14: "Agriculture",
    15: "Natural Resources and Economic Development",
    16: "Practice, Procedure, and Courts",
    17: "Professions, Occupations, and Businesses",
    18: "Transportation",
    19: "Revenue and Taxation",
    20: "Public Health and Welfare",
    21: "Public Lands",
    22: "Mechanics' and Laborers' Liens",
    23: "Public Utilities and Regulated Industries",
    24: "Property",
    25: "Domestic Relations",
    26: "Taxation",
    27: "Transportation",
    28: "Wills, Estates, and Fiduciary Relationships",
}

# Chapters in Title 26 (Taxation)
TITLE_26_CHAPTERS = {
    18: "Administration",
    26: "Arkansas Tax Procedure Act",
    51: "Income Taxes",
    52: "Gross Receipts Tax (Sales and Use Tax)",
    53: "Compensating (Use) Tax",
    54: "Soft Drink Tax",
    55: "Cigarette Tax",
    56: "Tobacco Products Tax",
    57: "Motor Fuel and Special Motor Fuel Tax",
    58: "Distilled Spirits Tax",
    59: "Beer Tax",
    60: "Mixed Drink Tax",
    61: "Estate Tax",
    62: "Inheritance and Estate Tax",
    63: "Special Taxes",
    64: "Property Taxes",
    65: "Miscellaneous Taxes",
    70: "Property Tax Relief",
    72: "Property Valuation and Assessment",
    74: "County Tax Collectors",
    75: "Real Property Assessment Coordination",
    76: "Personal Property Tax",
    80: "Corporate Franchise Tax",
    81: "Premium Taxes on Insurance Companies",
    82: "Banking and Financial Institution Taxes",
}

# Chapters in Title 20 (Public Health and Welfare)
TITLE_20_CHAPTERS = {
    76: "Department of Human Services",
    77: "General Provisions",
    78: "Public Assistance and Social Services",
    81: "Medical Assistance Programs",
    82: "Programs for the Aged, Blind, and Disabled",
    83: "Child Welfare Services",
    86: "Food Stamp Program / SNAP",
}


def create_title_xml(title_num: int, title_name: str, chapters: dict[int, str]) -> str:
    """Create Akoma Ntoso XML for a title index."""
    ET.register_namespace("akn", AKN_NS)

    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "title")

    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom-foundation")

    today = date.today().isoformat()

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-ar/act/aca/title-{title_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-ar/act/aca/title-{title_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#ar-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ar")
    work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
    work_number.set("value", str(title_num))
    work_name = ET.SubElement(work, f"{{{AKN_NS}}}FRBRname")
    work_name.set("value", "aca")

    # FRBRExpression
    expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/akn/us-ar/act/aca/title-{title_num}/eng@{today}")
    expr_uri = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-ar/act/aca/title-{title_num}/eng@{today}")
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
    manif_this.set("value", f"/akn/us-ar/act/aca/title-{title_num}/eng@{today}/main.xml")
    manif_uri = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/akn/us-ar/act/aca/title-{title_num}/eng@{today}/main.xml")
    manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    tlc_ar = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_ar.set("eId", "ar-legislature")
    tlc_ar.set("href", "/ontology/organization/us-ar/general-assembly")
    tlc_ar.set("showAs", "Arkansas General Assembly")

    tlc_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    tlc_rf.set("eId", "axiom-foundation")
    tlc_rf.set("href", "https://axiom-foundation.org")
    tlc_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")
    title_elem = ET.SubElement(body, f"{{{AKN_NS}}}hcontainer")
    title_elem.set("name", "title")
    title_elem.set("eId", f"title_{title_num}")

    num_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
    num_elem.text = f"Title {title_num}"

    heading_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}heading")
    heading_elem.text = title_name

    # Add chapters
    for chapter_num in sorted(chapters.keys()):
        chapter_name = chapters[chapter_num]
        chapter_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}chapter")
        chapter_elem.set("eId", f"chp_{title_num}_{chapter_num}")

        chp_num = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}num")
        chp_num.text = f"Chapter {chapter_num}"

        chp_heading = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}heading")
        chp_heading.text = chapter_name

        # Add componentRef to indicate where sections would be
        ref = ET.SubElement(chapter_elem, f"{{{AKN_NS}}}componentRef")
        ref.set("src", f"./chapter-{chapter_num}/")
        ref.set("showAs", f"Chapter {chapter_num} sections")

    ET.indent(root, space="  ")
    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def create_chapter_placeholder(title_num: int, chapter_num: int, chapter_name: str) -> str:
    """Create a placeholder chapter index."""
    ET.register_namespace("akn", AKN_NS)

    root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")
    act = ET.SubElement(root, f"{{{AKN_NS}}}act")
    act.set("name", "chapter")

    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#axiom-foundation")

    today = date.today().isoformat()

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", today)
    work_date.set("name", "generation")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#ar-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-ar")

    # FRBRExpression
    expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}/eng@{today}"
    )
    expr_uri = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}/eng@{today}")
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
        "value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}/eng@{today}/main.xml"
    )
    manif_uri = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value", f"/akn/us-ar/act/aca/title-{title_num}/chapter-{chapter_num}/eng@{today}/main.xml"
    )
    manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{title_num}_{chapter_num}")

    num_elem = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    num_elem.text = f"Chapter {chapter_num}"

    heading_elem = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    heading_elem.text = chapter_name

    # Add a note about placeholder status
    intro = ET.SubElement(chapter, f"{{{AKN_NS}}}intro")
    p = ET.SubElement(intro, f"{{{AKN_NS}}}p")
    p.text = f"This is a placeholder for Arkansas Code Title {title_num}, Chapter {chapter_num} - {chapter_name}. Section content to be added from official sources."

    ET.indent(root, space="  ")
    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def main():
    """Main entry point."""
    output_dir = Path("/tmp/rules-us-ar-akn")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Arkansas Code -> Akoma Ntoso Structure Generator")
    print("=" * 60)
    print(f"Output: {output_dir}")
    print()

    stats = {
        "titles_created": 0,
        "chapters_created": 0,
    }

    # Create Title 26 (Taxation) - primary focus
    print("Creating Title 26: Taxation")
    title_26_xml = create_title_xml(26, "Taxation", TITLE_26_CHAPTERS)
    title_26_dir = output_dir / "title-26"
    title_26_dir.mkdir(exist_ok=True)

    with open(title_26_dir / "_index.xml", "w", encoding="utf-8") as f:
        f.write(title_26_xml)
    stats["titles_created"] += 1
    print(f"  Created title-26/_index.xml")

    # Create chapter placeholders for Title 26
    for chapter_num, chapter_name in TITLE_26_CHAPTERS.items():
        chapter_dir = title_26_dir / f"chapter-{chapter_num}"
        chapter_dir.mkdir(exist_ok=True)

        chapter_xml = create_chapter_placeholder(26, chapter_num, chapter_name)
        with open(chapter_dir / "_index.xml", "w", encoding="utf-8") as f:
            f.write(chapter_xml)
        stats["chapters_created"] += 1
        print(f"    Created chapter-{chapter_num}/_index.xml: {chapter_name}")

    # Create Title 20 (Public Health and Welfare)
    print("\nCreating Title 20: Public Health and Welfare")
    title_20_xml = create_title_xml(20, "Public Health and Welfare", TITLE_20_CHAPTERS)
    title_20_dir = output_dir / "title-20"
    title_20_dir.mkdir(exist_ok=True)

    with open(title_20_dir / "_index.xml", "w", encoding="utf-8") as f:
        f.write(title_20_xml)
    stats["titles_created"] += 1
    print(f"  Created title-20/_index.xml")

    # Create chapter placeholders for Title 20
    for chapter_num, chapter_name in TITLE_20_CHAPTERS.items():
        chapter_dir = title_20_dir / f"chapter-{chapter_num}"
        chapter_dir.mkdir(exist_ok=True)

        chapter_xml = create_chapter_placeholder(20, chapter_num, chapter_name)
        with open(chapter_dir / "_index.xml", "w", encoding="utf-8") as f:
            f.write(chapter_xml)
        stats["chapters_created"] += 1
        print(f"    Created chapter-{chapter_num}/_index.xml: {chapter_name}")

    # Print summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Titles created:   {stats['titles_created']}")
    print(f"Chapters created: {stats['chapters_created']}")
    print(f"Output directory: {output_dir}")

    # List structure
    print()
    print("OUTPUT STRUCTURE:")
    for title_dir in sorted(output_dir.glob("title-*")):
        print(f"  {title_dir.name}/")
        print(f"    _index.xml")
        for chapter_dir in sorted(title_dir.glob("chapter-*")):
            print(f"    {chapter_dir.name}/")
            print(f"      _index.xml")

    return 0


if __name__ == "__main__":
    exit(main())
