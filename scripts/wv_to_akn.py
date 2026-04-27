#!/usr/bin/env python3
"""Convert West Virginia statutes to Akoma Ntoso XML.

This script fetches West Virginia Code statutes from code.wvlegislature.gov
and converts them to Akoma Ntoso XML format, outputting to /tmp/rules-us-wv-akn/.

Usage:
    python scripts/wv_to_akn.py                    # Convert key tax/benefit chapters
    python scripts/wv_to_akn.py --chapter 11       # Convert Chapter 11 (Taxation)
    python scripts/wv_to_akn.py --article 11-21    # Convert single article
    python scripts/wv_to_akn.py --all              # Convert all chapters (slow!)
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.dom import minidom

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.wv import (
    WVConverter,
    WV_CHAPTERS,
    WV_TAX_CHAPTERS,
    WV_WELFARE_CHAPTERS,
    WV_TAX_ARTICLES,
    WV_WELFARE_ARTICLES,
)
from atlas.models import Section, Subsection


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"

# Output directory
OUTPUT_DIR = Path("/tmp/rules-us-wv-akn")


def section_to_akn_xml(section: Section) -> str:
    """Convert an arch Section to Akoma Ntoso XML.

    Args:
        section: Section model from WVConverter

    Returns:
        XML string in Akoma Ntoso format
    """
    # Register namespace
    ET.register_namespace("", AKN_NS)

    # Create root element
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

    # Create act element
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Parse section number from citation (e.g., "WV-11-21-1" -> "11-21-1")
    section_num = section.citation.section.replace("WV-", "")
    section_id_safe = section_num.replace("-", "_")

    # Meta section
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

    # Identification
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", "#wv-legislature")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/us-wv/act/wvc/{section_num}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/us-wv/act/wvc/{section_num}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", str(date.today()))
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", "#wv-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", "us-wv")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set("value", f"/us-wv/act/wvc/{section_num}/eng")
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set("value", f"/us-wv/act/wvc/{section_num}/eng")
    expr_date = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRdate")
    expr_date.set("date", str(section.retrieved_at or date.today()))
    expr_date.set("name", "publication")
    expr_author = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRauthor")
    expr_author.set("href", "#wv-legislature")
    expr_lang = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRlanguage")
    expr_lang.set("language", "eng")

    # FRBRManifestation
    manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")
    manif_this = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRthis")
    manif_this.set("value", f"/us-wv/act/wvc/{section_num}/eng/akn")
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set("value", f"/us-wv/act/wvc/{section_num}/eng/akn")
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", str(date.today()))
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")

    org_legislature = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_legislature.set("eId", "wv-legislature")
    org_legislature.set("href", "/ontology/organization/us-wv/legislature")
    org_legislature.set("showAs", "West Virginia Legislature")

    org_rf = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org_rf.set("eId", "axiom-foundation")
    org_rf.set("href", "https://axiom-foundation.org")
    org_rf.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")

    # Section element
    section_elem = ET.SubElement(body, f"{{{AKN_NS}}}section")
    section_elem.set("eId", f"sec_{section_id_safe}")

    # Section number
    num = ET.SubElement(section_elem, f"{{{AKN_NS}}}num")
    num.text = f"WVC {section_num}"

    # Section heading
    if section.section_title:
        heading = ET.SubElement(section_elem, f"{{{AKN_NS}}}heading")
        heading.text = section.section_title

    # Add subsections recursively
    def add_subsection(parent_elem: ET.Element, subsec: Subsection, parent_id: str):
        """Add a subsection and its children to the XML."""
        subsec_id = f"{parent_id}__subsec_{subsec.identifier}"
        subsection_elem = ET.SubElement(parent_elem, f"{{{AKN_NS}}}subsection")
        subsection_elem.set("eId", subsec_id)

        subsec_num = ET.SubElement(subsection_elem, f"{{{AKN_NS}}}num")
        subsec_num.text = f"({subsec.identifier})"

        if subsec.text:
            content = ET.SubElement(subsection_elem, f"{{{AKN_NS}}}content")
            p = ET.SubElement(content, f"{{{AKN_NS}}}p")
            # Truncate very long text to keep XML manageable
            text = subsec.text[:5000] if len(subsec.text) > 5000 else subsec.text
            p.text = text

        # Add children recursively
        for child in subsec.children:
            add_subsection(subsection_elem, child, subsec_id)

    # Add all top-level subsections
    for subsec in section.subsections:
        add_subsection(section_elem, subsec, f"sec_{section_id_safe}")

    # If no subsections, add full text as content
    if not section.subsections and section.text:
        content = ET.SubElement(section_elem, f"{{{AKN_NS}}}content")
        # Split text into paragraphs
        paragraphs = section.text.split("\n\n")
        for para in paragraphs[:50]:  # Limit paragraphs
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                text = para.strip()[:5000]  # Truncate long paragraphs
                p.text = text

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ")


def get_article_from_section(section_num: str) -> str:
    """Extract article from section number.

    Args:
        section_num: e.g., "11-21-1"

    Returns:
        Article identifier, e.g., "11-21"
    """
    parts = section_num.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return parts[0]


def get_chapter_from_section(section_num: str) -> str:
    """Extract chapter from section number.

    Args:
        section_num: e.g., "11-21-1"

    Returns:
        Chapter number, e.g., "11"
    """
    return section_num.split("-")[0]


def convert_article(converter: WVConverter, chapter: int, article: str, output_dir: Path) -> int:
    """Convert a single article to AKN.

    Args:
        converter: WVConverter instance
        chapter: Chapter number (e.g., 11)
        article: Article number (e.g., "21")
        output_dir: Output directory

    Returns:
        Number of sections converted
    """
    chapter_dir = output_dir / f"chapter-{chapter}"
    article_dir = chapter_dir / f"article-{chapter}-{article}"
    article_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for section in converter.iter_article(chapter, article):
        section_num = section.citation.section.replace("WV-", "")
        filename = f"{section_num}.xml"
        filepath = article_dir / filename

        xml_content = section_to_akn_xml(section)
        filepath.write_text(xml_content, encoding="utf-8")
        count += 1
        title_display = (
            section.section_title[:50] + "..."
            if len(section.section_title) > 50
            else section.section_title
        )
        print(f"    {section_num}: {title_display}")

    return count


def convert_chapter(converter: WVConverter, chapter: int, output_dir: Path) -> tuple[int, int]:
    """Convert a single chapter to AKN.

    Args:
        converter: WVConverter instance
        chapter: Chapter number (e.g., 11)
        output_dir: Output directory

    Returns:
        Tuple of (articles_converted, sections_converted)
    """
    articles = converter.get_chapter_articles(chapter)
    print(f"  Found {len(articles)} articles in Chapter {chapter}")

    total_sections = 0
    for article in articles:
        article_name = WV_TAX_ARTICLES.get(f"{chapter}-{article}") or WV_WELFARE_ARTICLES.get(
            f"{chapter}-{article}", ""
        )
        print(f"  Article {chapter}-{article}: {article_name}")
        try:
            count = convert_article(converter, chapter, article, output_dir)
            total_sections += count
        except Exception as e:
            print(f"    ERROR: {e}")

    return len(articles), total_sections


def convert_key_chapters(output_dir: Path) -> dict:
    """Convert key tax and benefit chapters.

    Args:
        output_dir: Output directory

    Returns:
        Statistics dict
    """
    stats = {
        "chapters_converted": 0,
        "articles_converted": 0,
        "sections_converted": 0,
        "errors": [],
    }

    # Combine tax and welfare chapters
    chapters_to_convert = list(WV_TAX_CHAPTERS.keys()) + list(WV_WELFARE_CHAPTERS.keys())

    print(f"Converting {len(chapters_to_convert)} key chapters...")

    with WVConverter(rate_limit_delay=0.5) as converter:
        for chapter in chapters_to_convert:
            chapter_name = WV_TAX_CHAPTERS.get(chapter) or WV_WELFARE_CHAPTERS.get(chapter, "")
            print(f"\nChapter {chapter}: {chapter_name}")

            try:
                articles, sections = convert_chapter(converter, chapter, output_dir)
                stats["chapters_converted"] += 1
                stats["articles_converted"] += articles
                stats["sections_converted"] += sections
            except Exception as e:
                error_msg = f"Chapter {chapter}: {str(e)}"
                print(f"  ERROR: {e}")
                stats["errors"].append(error_msg)

    return stats


def convert_all_chapters(output_dir: Path) -> dict:
    """Convert all West Virginia chapters.

    Args:
        output_dir: Output directory

    Returns:
        Statistics dict
    """
    stats = {
        "chapters_converted": 0,
        "articles_converted": 0,
        "sections_converted": 0,
        "errors": [],
    }

    print(f"Converting all {len(WV_CHAPTERS)} chapters...")

    with WVConverter(rate_limit_delay=0.5) as converter:
        for chapter, chapter_name in sorted(WV_CHAPTERS.items()):
            print(f"\nChapter {chapter}: {chapter_name}")

            try:
                articles, sections = convert_chapter(converter, chapter, output_dir)
                stats["chapters_converted"] += 1
                stats["articles_converted"] += articles
                stats["sections_converted"] += sections
            except Exception as e:
                error_msg = f"Chapter {chapter}: {str(e)}"
                print(f"  ERROR: {e}")
                stats["errors"].append(error_msg)

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Convert West Virginia statutes to Akoma Ntoso XML"
    )
    parser.add_argument(
        "--chapter",
        type=int,
        help="Convert all articles in a specific chapter (e.g., 11)",
    )
    parser.add_argument(
        "--article",
        type=str,
        help="Convert a specific article (e.g., 11-21)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Convert all chapters (very slow!)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )

    args = parser.parse_args()
    output_dir = args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("West Virginia Code -> Akoma Ntoso Converter")
    print("=" * 60)
    print(f"Output: {output_dir}")
    print()

    if args.article:
        # Convert single article
        parts = args.article.split("-")
        if len(parts) < 2:
            print(f"ERROR: Invalid article format '{args.article}'. Use format: 11-21")
            return 1

        chapter = int(parts[0])
        article = parts[1]

        print(f"Converting article {args.article}...")
        with WVConverter(rate_limit_delay=0.3) as converter:
            count = convert_article(converter, chapter, article, output_dir)
        print(f"\nConverted {count} sections")

    elif args.chapter:
        # Convert single chapter
        print(f"Converting Chapter {args.chapter}...")
        with WVConverter(rate_limit_delay=0.3) as converter:
            articles, sections = convert_chapter(converter, args.chapter, output_dir)
        print(f"\nConverted {articles} articles, {sections} sections")

    elif args.all:
        # Convert all chapters
        stats = convert_all_chapters(output_dir)

        print()
        print("=" * 60)
        print("CONVERSION SUMMARY")
        print("=" * 60)
        print(f"Chapters converted: {stats['chapters_converted']}")
        print(f"Articles converted: {stats['articles_converted']}")
        print(f"Sections converted: {stats['sections_converted']}")
        print(f"Output directory:   {output_dir}")

        if stats["errors"]:
            print()
            print("ERRORS:")
            for err in stats["errors"]:
                print(f"  - {err}")

    else:
        # Default: convert key chapters
        stats = convert_key_chapters(output_dir)

        print()
        print("=" * 60)
        print("CONVERSION SUMMARY")
        print("=" * 60)
        print(f"Chapters converted: {stats['chapters_converted']}")
        print(f"Articles converted: {stats['articles_converted']}")
        print(f"Sections converted: {stats['sections_converted']}")
        print(f"Output directory:   {output_dir}")

        if stats["errors"]:
            print()
            print("ERRORS:")
            for err in stats["errors"]:
                print(f"  - {err}")

    # List output structure
    print()
    print("OUTPUT STRUCTURE:")
    for chapter_dir in sorted(output_dir.glob("chapter-*")):
        article_count = len(list(chapter_dir.glob("article-*")))
        section_count = len(list(chapter_dir.glob("*/*.xml")))
        print(f"  {chapter_dir.name}: {article_count} articles, {section_count} sections")

    return 0


if __name__ == "__main__":
    sys.exit(main())
