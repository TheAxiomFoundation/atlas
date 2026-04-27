#!/usr/bin/env python3
"""Convert New Mexico Statutes Annotated (NMSA 1978) to Akoma Ntoso XML.

This script creates Akoma Ntoso XML files for New Mexico statutes.

NM Statute Structure:
- Chapters (e.g., Chapter 7: Taxation)
- Articles (e.g., Article 2: Income Tax General Provisions)
- Sections (e.g., Section 7-2-2: Definitions)

Data Source:
NMOneSource (https://nmonesource.com) is the official source for NM statutes,
powered by Lexum Norma. It uses dynamic JavaScript and item IDs that make
bulk scraping difficult. This script provides:

1. Chapter structure generation based on the official chapter listing
2. Stub AKN files for all known chapters
3. Section-level conversion when HTML content is provided

Usage:
    python scripts/nm_to_akn.py --chapters      # Generate chapter structure
    python scripts/nm_to_akn.py --chapter 7     # Generate Chapter 7 (Taxation)
    python scripts/nm_to_akn.py --chapter 27    # Generate Chapter 27 (Public Assistance)
    python scripts/nm_to_akn.py --all           # Generate all chapters
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.nm import (
    NM_CHAPTERS,
    NM_TAX_CHAPTERS,
    NM_TAX_ARTICLES,
    NM_WELFARE_CHAPTERS,
    NM_WELFARE_ARTICLES,
)

# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def create_frbr_identification(chapter_num: int | str, chapter_name: str) -> ET.Element:
    """Create FRBR identification section for AKN document."""
    # Register namespace
    ET.register_namespace("", AKN_NS)

    identification = ET.Element(f"{{{AKN_NS}}}identification")
    identification.set("source", "#nmcc")

    chapter_id = str(chapter_num).lower()
    today = date.today().isoformat()

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/us-nm/act/nmsa/chapter-{chapter_id}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/us-nm/act/nmsa/chapter-{chapter_id}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", "1978-01-01")
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#nm-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-nm")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/us-nm/act/nmsa/chapter-{chapter_id}/eng@{today}")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/us-nm/act/nmsa/chapter-{chapter_id}/eng@{today}")
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", today)
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#axiom-foundation")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", f"/us-nm/act/nmsa/chapter-{chapter_id}/eng@{today}.akn")
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/us-nm/act/nmsa/chapter-{chapter_id}/eng@{today}.akn")
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", today)
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    return identification


def create_references() -> ET.Element:
    """Create references section with TLC entries."""
    references = ET.Element(f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    # NM Legislature
    org_leg = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_leg.set("eId", "nm-legislature")
    org_leg.set("href", "/ontology/organization/us-nm/legislature")
    org_leg.set("showAs", "New Mexico Legislature")

    # NM Compilation Commission
    org_nmcc = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_nmcc.set("eId", "nmcc")
    org_nmcc.set("href", "https://www.nmcompcomm.us")
    org_nmcc.set("showAs", "New Mexico Compilation Commission")

    # The Axiom Foundation
    org_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_rf.set("eId", "axiom-foundation")
    org_rf.set("href", "https://axiom-foundation.org")
    org_rf.set("showAs", "The Axiom Foundation")

    return references


def create_chapter_akn(
    chapter_num: int | str,
    chapter_name: str,
    articles: dict[int | str, str] | None = None,
) -> str:
    """Create Akoma Ntoso XML for a chapter.

    Args:
        chapter_num: Chapter number (e.g., 7, "32A")
        chapter_name: Chapter name (e.g., "Taxation")
        articles: Optional dict of article numbers to names

    Returns:
        XML string in Akoma Ntoso format
    """
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "chapter")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")
    meta.append(create_frbr_identification(chapter_num, chapter_name))
    meta.append(create_references())

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Chapter element
    chapter_id = str(chapter_num).lower().replace(" ", "_")
    chapter = ET.SubElement(body, f"{{{AKN_NS}}}chapter")
    chapter.set("eId", f"chp_{chapter_id}")

    # Chapter number
    num = ET.SubElement(chapter, f"{{{AKN_NS}}}num")
    num.text = f"Chapter {chapter_num}"

    # Chapter heading
    heading = ET.SubElement(chapter, f"{{{AKN_NS}}}heading")
    heading.text = chapter_name

    # Add articles if provided
    if articles:
        for article_num, article_name in sorted(articles.items(), key=lambda x: str(x[0])):
            article_id = str(article_num).lower().replace(" ", "_")
            article = ET.SubElement(chapter, f"{{{AKN_NS}}}article")
            article.set("eId", f"chp_{chapter_id}__art_{article_id}")

            art_num = ET.SubElement(article, f"{{{AKN_NS}}}num")
            art_num.text = f"Article {article_num}"

            art_heading = ET.SubElement(article, f"{{{AKN_NS}}}heading")
            art_heading.text = article_name

            # Add placeholder content
            content = ET.SubElement(article, f"{{{AKN_NS}}}content")
            p = ET.SubElement(content, f"{{{AKN_NS}}}p")
            p.text = f"[Content for Article {article_num} - {article_name}]"
    else:
        # Add placeholder content for chapters without known articles
        content = ET.SubElement(chapter, f"{{{AKN_NS}}}content")
        p = ET.SubElement(content, f"{{{AKN_NS}}}p")
        p.text = f"[Content for Chapter {chapter_num} - {chapter_name}]"

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ")


def get_chapter_articles(chapter_num: int | str) -> dict[int | str, str] | None:
    """Get articles for a chapter if known."""
    if chapter_num == 7:
        return NM_TAX_ARTICLES
    elif chapter_num == 27:
        return NM_WELFARE_ARTICLES
    return None


def generate_chapter(chapter_num: int | str, output_dir: Path) -> bool:
    """Generate AKN file for a chapter.

    Args:
        chapter_num: Chapter number
        output_dir: Output directory

    Returns:
        True if successful
    """
    # Get chapter info
    if chapter_num in NM_TAX_CHAPTERS:
        chapter_name = NM_TAX_CHAPTERS[chapter_num]
    elif chapter_num in NM_WELFARE_CHAPTERS:
        chapter_name = NM_WELFARE_CHAPTERS[chapter_num]
    elif chapter_num in NM_CHAPTERS:
        chapter_name = NM_CHAPTERS[chapter_num]
    else:
        print(f"  Unknown chapter: {chapter_num}")
        return False

    # Get articles if known
    articles = get_chapter_articles(chapter_num)

    # Create chapter directory
    chapter_id = str(chapter_num).lower().replace(" ", "_")
    chapter_dir = output_dir / f"chapter-{chapter_id}"
    chapter_dir.mkdir(parents=True, exist_ok=True)

    # Generate XML
    xml_content = create_chapter_akn(chapter_num, chapter_name, articles)

    # Write file
    output_file = chapter_dir / f"chapter-{chapter_id}.akn.xml"
    output_file.write_text(xml_content, encoding="utf-8")

    article_count = len(articles) if articles else 0
    print(
        f"  Chapter {chapter_num}: {chapter_name} ({article_count} articles) -> {output_file.name}"
    )

    return True


def main():
    parser = argparse.ArgumentParser(description="Convert New Mexico Statutes to Akoma Ntoso XML")
    parser.add_argument(
        "--chapters",
        action="store_true",
        help="List all available chapters",
    )
    parser.add_argument(
        "--chapter",
        type=str,
        help="Generate a specific chapter (e.g., 7, 27, 32A)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate all chapters",
    )
    parser.add_argument(
        "--tax",
        action="store_true",
        help="Generate tax-related chapters (7)",
    )
    parser.add_argument(
        "--welfare",
        action="store_true",
        help="Generate welfare-related chapters (27)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="/tmp/rules-us-nm-akn",
        help="Output directory (default: /tmp/rules-us-nm-akn)",
    )

    args = parser.parse_args()

    if args.chapters:
        print("New Mexico Statutes Annotated 1978 - Chapters")
        print("=" * 60)
        for ch_num, ch_name in sorted(NM_CHAPTERS.items(), key=lambda x: str(x[0])):
            marker = ""
            if ch_num in NM_TAX_CHAPTERS:
                marker = " [TAX]"
            elif ch_num in NM_WELFARE_CHAPTERS:
                marker = " [WELFARE]"
            print(f"  Chapter {ch_num}: {ch_name}{marker}")
        print()
        print(f"Total: {len(NM_CHAPTERS)} chapters")
        return

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("New Mexico Statutes to Akoma Ntoso Converter")
    print(f"  Output: {output_dir}")
    print("=" * 60)

    chapters_to_generate = []

    if args.chapter:
        # Try to parse as int, otherwise use as string
        try:
            ch = int(args.chapter)
        except ValueError:
            ch = args.chapter
        chapters_to_generate = [ch]
    elif args.tax:
        chapters_to_generate = list(NM_TAX_CHAPTERS.keys())
    elif args.welfare:
        chapters_to_generate = list(NM_WELFARE_CHAPTERS.keys())
    elif args.all:
        chapters_to_generate = list(NM_CHAPTERS.keys())
    else:
        # Default: tax and welfare chapters
        chapters_to_generate = list(NM_TAX_CHAPTERS.keys()) + list(NM_WELFARE_CHAPTERS.keys())

    total_success = 0
    total_failed = 0

    for ch in chapters_to_generate:
        if generate_chapter(ch, output_dir):
            total_success += 1
        else:
            total_failed += 1

    print("=" * 60)
    print(f"Complete: {total_success} chapters generated, {total_failed} failed")
    print(f"Output: {output_dir}")

    # List output files
    output_files = sorted(output_dir.glob("**/*.akn.xml"))
    print(f"\nGenerated {len(output_files)} AKN files:")
    for f in output_files[:10]:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.relative_to(output_dir)} ({size_kb:.1f} KB)")
    if len(output_files) > 10:
        print(f"  ... and {len(output_files) - 10} more")


if __name__ == "__main__":
    main()
