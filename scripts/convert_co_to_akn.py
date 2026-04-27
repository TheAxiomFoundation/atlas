#!/usr/bin/env python3
"""Convert Colorado statutes to Akoma Ntoso XML format.

Fetches statutes from colorado.public.law and converts them to AKN XML.

Usage:
    python scripts/convert_co_to_akn.py                    # All tax articles
    python scripts/convert_co_to_akn.py --title 39         # Title 39 (Taxation)
    python scripts/convert_co_to_akn.py --article 39 22    # Article 22 of Title 39
    python scripts/convert_co_to_akn.py --section 39-22-104  # Single section
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.converters.us_states.co import COConverter, CO_TAX_ARTICLES, CO_HUMAN_SERVICES_ARTICLES
from atlas.models import Section as ArchSection
from atlas.models_akoma_ntoso import (
    AKN_NAMESPACE,
    Act,
    Chapter,
    DocumentType,
    FRBRAuthor,
    FRBRCountry,
    FRBRDate,
    FRBRExpression,
    FRBRLanguage,
    FRBRManifestation,
    FRBRUri,
    FRBRWork,
    Identification,
    Part,
    Section,
    Subsection,
)


def prettify_xml(elem: ET.Element, indent: str = "  ") -> str:
    """Pretty-print XML with indentation."""
    ET.indent(elem, space=indent)
    return ET.tostring(elem, encoding="unicode")


def section_to_akn(arch_section: ArchSection, title_num: int, article_num: int) -> Section:
    """Convert an arch Section to an Akoma Ntoso Section."""
    # Parse section number from citation (e.g., "CO-39-22-104" -> "39-22-104")
    section_id = arch_section.citation.section
    if section_id.startswith("CO-"):
        section_id = section_id[3:]

    # Create safe eId
    eid = f"sec_{section_id.replace('-', '_').replace('.', '_')}"

    # Build subsections
    akn_subsections = []
    for subsec in arch_section.subsections:
        subsec_eid = f"{eid}__subsec_{subsec.identifier}"
        akn_subsec = Subsection(
            eid=subsec_eid,
            num=f"({subsec.identifier})",
            text=subsec.text[:2000] if subsec.text else "",  # Limit text size
        )
        akn_subsections.append(akn_subsec)

    return Section(
        eid=eid,
        num=section_id,
        heading=arch_section.section_title,
        text=arch_section.text[:5000] if arch_section.text else "",  # Limit text size
        children=akn_subsections,
    )


def create_akn_document(
    title_num: int,
    title_name: str,
    articles: dict[int, list[Section]],
) -> Act:
    """Create an Akoma Ntoso Act document from sections grouped by article."""

    work_uri = f"/akn/us-co/act/crs/title{title_num}"
    today = date.today()

    identification = Identification(
        source="#axiom-foundation",
        work=FRBRWork(
            uri=FRBRUri(value=work_uri),
            date=FRBRDate(value=today, name="enactment"),
            author=FRBRAuthor(href="#coleg"),
            country=FRBRCountry(value="us-co"),
            this=f"{work_uri}/main",
        ),
        expression=FRBRExpression(
            uri=FRBRUri(value=f"{work_uri}/eng@{today.isoformat()}"),
            date=FRBRDate(value=today, name="publication"),
            author=FRBRAuthor(href="#axiom-foundation"),
            language=FRBRLanguage(language="en"),
            this=f"{work_uri}/eng@{today.isoformat()}/main",
        ),
        manifestation=FRBRManifestation(
            uri=FRBRUri(value=f"{work_uri}/eng@{today.isoformat()}.akn"),
            date=FRBRDate(value=today, name="transformation"),
            author=FRBRAuthor(href="#axiom-foundation"),
            this=f"{work_uri}/eng@{today.isoformat()}/main.akn",
        ),
    )

    # Create title element
    title_elem = Part(
        eid=f"title_{title_num}",
        num=f"Title {title_num}",
        heading=title_name,
        children=[],
    )

    # Get article names based on title
    if title_num == 39:
        article_names = CO_TAX_ARTICLES
    elif title_num == 26:
        article_names = CO_HUMAN_SERVICES_ARTICLES
    else:
        article_names = {}

    # Add articles
    for article_num in sorted(articles.keys()):
        article_sections = articles[article_num]
        article_name = article_names.get(article_num, f"Article {article_num}")

        chapter_elem = Chapter(
            eid=f"article_{article_num}",
            num=f"Article {article_num}",
            heading=article_name,
            children=article_sections,
        )

        title_elem.children.append(chapter_elem)

    return Act(
        document_type=DocumentType.ACT,
        identification=identification,
        body=[title_elem],
        source_url="https://colorado.public.law",
    )


def write_akn_file(doc: Act, output_path: Path):
    """Write an Akoma Ntoso document to a file."""
    xml_elem = doc.to_xml_element()
    xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml_str += prettify_xml(xml_elem)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)


def fetch_article(converter: COConverter, title: int, article: int) -> list[Section]:
    """Fetch all sections in an article and convert to AKN Sections."""
    sections = []
    print(f"    Fetching Article {article}...", end=" ", flush=True)

    try:
        section_count = 0
        for arch_section in converter.iter_article(title, article):
            akn_section = section_to_akn(arch_section, title, article)
            sections.append(akn_section)
            section_count += 1

        print(f"{section_count} sections")
    except Exception as e:
        print(f"Error: {e}")

    return sections


def fetch_title(converter: COConverter, title: int) -> dict[int, list[Section]]:
    """Fetch all articles in a title and return grouped sections."""
    articles = {}

    # Get article list based on title
    if title == 39:
        article_nums = list(CO_TAX_ARTICLES.keys())
    elif title == 26:
        article_nums = list(CO_HUMAN_SERVICES_ARTICLES.keys())
    else:
        # Discover articles from title page
        article_nums = converter._discover_articles(title)

    for article_num in article_nums:
        sections = fetch_article(converter, title, article_num)
        if sections:
            articles[article_num] = sections

    return articles


def main():
    parser = argparse.ArgumentParser(description="Convert Colorado statutes to Akoma Ntoso XML")
    parser.add_argument("--title", type=int, help="Title number to fetch (e.g., 39)")
    parser.add_argument("--article", nargs=2, type=int, metavar=("TITLE", "ARTICLE"),
                        help="Fetch a specific article (e.g., --article 39 22)")
    parser.add_argument("--section", type=str, help="Fetch a single section (e.g., 39-22-104)")
    parser.add_argument("--output", type=str, default="/tmp/rules-us-co-akn",
                        help="Output directory (default: /tmp/rules-us-co-akn)")
    parser.add_argument("--rate-limit", type=float, default=0.5,
                        help="Delay between requests in seconds (default: 0.5)")

    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Colorado Statute to Akoma Ntoso Converter")
    print(f"Output directory: {output_dir}")
    print("-" * 60)

    total_sections = 0
    total_articles = 0

    with COConverter(rate_limit_delay=args.rate_limit) as converter:
        if args.section:
            # Fetch single section
            print(f"Fetching section {args.section}...")
            arch_section = converter.fetch_section(args.section)

            # Parse title and article from section number
            parts = args.section.split("-")
            title_num = int(parts[0])
            article_num = int(parts[1])

            akn_section = section_to_akn(arch_section, title_num, article_num)

            # Create document with single section
            doc = create_akn_document(
                title_num=title_num,
                title_name=f"Title {title_num}",
                articles={article_num: [akn_section]},
            )

            output_file = output_dir / f"us-co-{args.section.replace('-', '_')}.akn.xml"
            write_akn_file(doc, output_file)

            print(f"Written: {output_file}")
            total_sections = 1
            total_articles = 1

        elif args.article:
            # Fetch specific article
            title_num, article_num = args.article
            print(f"Fetching Title {title_num}, Article {article_num}...")

            sections = fetch_article(converter, title_num, article_num)

            if sections:
                # Get title name
                from atlas.converters.us_states.co import CO_TITLES
                title_name = CO_TITLES.get(title_num, f"Title {title_num}")

                doc = create_akn_document(
                    title_num=title_num,
                    title_name=title_name,
                    articles={article_num: sections},
                )

                output_file = output_dir / f"us-co-title-{title_num}-article-{article_num}.akn.xml"
                write_akn_file(doc, output_file)

                print(f"Written: {output_file}")
                total_sections = len(sections)
                total_articles = 1
            else:
                print(f"No sections found for Title {title_num}, Article {article_num}")

        elif args.title:
            # Fetch entire title
            print(f"Fetching Title {args.title}...")

            articles = fetch_title(converter, args.title)

            if articles:
                from atlas.converters.us_states.co import CO_TITLES
                title_name = CO_TITLES.get(args.title, f"Title {args.title}")

                doc = create_akn_document(
                    title_num=args.title,
                    title_name=title_name,
                    articles=articles,
                )

                output_file = output_dir / f"us-co-title-{args.title}.akn.xml"
                write_akn_file(doc, output_file)

                print(f"Written: {output_file}")
                total_sections = sum(len(secs) for secs in articles.values())
                total_articles = len(articles)
            else:
                print(f"No articles found for Title {args.title}")

        else:
            # Default: fetch key tax articles (Title 39, Article 22 - Income Tax)
            print("Fetching key tax articles (Title 39 - Taxation)...")
            print("  (Use --title, --article, or --section for more specific fetching)")

            # Focus on income tax (Article 22) as it's most relevant
            key_articles = [22]  # Income Tax

            articles = {}
            for article_num in key_articles:
                print(f"  Processing Article {article_num}...")
                sections = fetch_article(converter, 39, article_num)
                if sections:
                    articles[article_num] = sections

            if articles:
                from atlas.converters.us_states.co import CO_TITLES
                title_name = CO_TITLES.get(39, "Taxation")

                doc = create_akn_document(
                    title_num=39,
                    title_name=title_name,
                    articles=articles,
                )

                output_file = output_dir / "us-co-title-39-income-tax.akn.xml"
                write_akn_file(doc, output_file)

                print(f"Written: {output_file}")
                total_sections = sum(len(secs) for secs in articles.values())
                total_articles = len(articles)

    print("-" * 60)
    print(f"Conversion complete!")
    print(f"  Articles: {total_articles}")
    print(f"  Sections: {total_sections}")
    print(f"  Output: {output_dir}")

    # List output files
    output_files = sorted(output_dir.glob("*.akn.xml"))
    print(f"\nGenerated {len(output_files)} AKN files:")
    for f in output_files:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
