#!/usr/bin/env python3
"""Convert Wyoming RTF statutes to Akoma Ntoso XML format.

Usage:
    python scripts/convert_wy_to_akn.py

Reads from: data/statutes/us-wy/release84.2022.10/
Outputs to: /tmp/rules-us-wy-akn/
"""

import re
import sys
from datetime import date
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atlas.models_akoma_ntoso import (
    AKN_NAMESPACE,
    Act,
    Article,
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
    HierarchicalElement,
    Identification,
    Part,
    Section,
    Subsection,
)


def strip_rtf_control_words(text: str) -> str:
    """Remove RTF control words and formatting, extracting plain text."""
    # First pass: handle special RTF sequences
    text = text.replace("\\par }", " ")
    text = text.replace("\\par}", " ")
    text = text.replace("\\par", "\n")
    text = text.replace("\\line", "\n")
    text = text.replace("\\tab", " ")
    text = text.replace("\\~", " ")  # Non-breaking space
    text = text.replace("\\-", "")  # Optional hyphen
    text = text.replace("\\_", "-")  # Non-breaking hyphen

    # Handle RTF hex escapes like \'a7 (section symbol)
    def hex_replace(match):
        try:
            return chr(int(match.group(1), 16))
        except ValueError:
            return ""
    text = re.sub(r"\\'([0-9a-fA-F]{2})", hex_replace, text)

    # Remove bookmark markers
    text = re.sub(r"\{\\\*\\bkmkstart[^}]*\}", "", text)
    text = re.sub(r"\{\\\*\\bkmkend[^}]*\}", "", text)

    # Remove nested groups with formatting info
    text = re.sub(r"\{\\[^{}]+\}", "", text)

    # Remove control words with parameters like \fs24, \cf0, etc.
    text = re.sub(r"\\[a-z]+[-]?\d+", "", text)

    # Remove remaining control words
    text = re.sub(r"\\[a-z]+\*?", "", text)

    # Clean up remaining braces and whitespace
    text = text.replace("{", "").replace("}", "")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def parse_rtf_to_sections(rtf_content: str) -> list[dict]:
    """Parse RTF content into a list of section dictionaries.

    Returns a list of dicts with keys:
    - type: 'title', 'chapter', 'article', 'section'
    - number: e.g., '39', '11', '39-11-101'
    - heading: e.g., 'Taxation and Revenue', 'Administration'
    - text: body text content
    - children: nested elements
    """
    sections = []

    # Pattern to find title headers
    title_pattern = re.compile(
        r"\\s2[^}]*\}[^{]*\{[^}]*Title\s+(\d+(?:\.\d+)?)\s+([^\\]+?)\\par",
        re.IGNORECASE | re.DOTALL
    )

    # Pattern to find chapter headers
    chapter_pattern = re.compile(
        r"\\s3[^}]*\}[^{]*\{[^}]*Chapter\s+(\d+)\s+([^\\]+?)\\par",
        re.IGNORECASE | re.DOTALL
    )

    # Pattern to find article headers
    article_pattern = re.compile(
        r"\\s4[^}]*\}[^{]*\{[^}]*Article\s+(\d+)[.\s]+([^\\]+?)\\par",
        re.IGNORECASE | re.DOTALL
    )

    # Pattern to find section headers (e.g., 39-11-101)
    section_pattern = re.compile(
        r"\{\\[*]\\bkmkstart\s+(\d+-\d+-\d+(?:\.\d+)?)\}.*?"
        r"\\s14[^}]*\}[^{]*\{[^}]*"
        r"[^\d]*(\d+-\d+-\d+(?:\.\d+)?)[.\s]+([^\\]*?)\\par",
        re.IGNORECASE | re.DOTALL
    )

    # Alternative section pattern for different RTF formatting
    section_pattern_alt = re.compile(
        r"\{\\[*]\\bkmkstart\s+(\d+-\d+-\d+(?:\.\d+)?)\}"
        r".*?"
        r"(\d+-\d+-\d+(?:\.\d+)?)[.\s]+([^\\{]+)",
        re.DOTALL
    )

    # Find sections using bookmarks as primary identifier
    bookmark_pattern = re.compile(
        r"\{\\\*\\bkmkstart\s+(\d+-\d+-\d+(?:\.\d+)?)\}\{\\\*\\bkmkend[^}]+\}"
    )

    # Get section numbers from bookmarks
    section_nums = bookmark_pattern.findall(rtf_content)

    for section_num in section_nums:
        # Build pattern to find section content
        # Look for the bookmark and then extract heading and text
        escaped_num = re.escape(section_num)

        # Pattern to find section heading after bookmark
        heading_pattern = re.compile(
            rf"\{{\\\*\\bkmkstart\s+{escaped_num}\}}\{{\\\*\\bkmkend\s+{escaped_num}\}}"
            rf".*?"
            rf"(?:{escaped_num}[.\s]+)?([^\\{{}}]+?)\\par",
            re.DOTALL
        )

        match = heading_pattern.search(rtf_content)
        heading = ""
        if match:
            heading = strip_rtf_control_words(match.group(1))

        # Find the text content for this section
        # Look for content between this section and the next
        start_pattern = re.compile(
            rf"\{{\\\*\\bkmkstart\s+{escaped_num}\}}",
            re.DOTALL
        )
        start_match = start_pattern.search(rtf_content)

        if start_match:
            start_pos = start_match.end()

            # Find next section bookmark
            next_section = re.search(
                r"\{\\\*\\bkmkstart\s+\d+-\d+-\d+",
                rtf_content[start_pos:]
            )

            if next_section:
                end_pos = start_pos + next_section.start()
            else:
                # Find next chapter or end of content
                next_chapter = re.search(
                    r"\\s3\s+\\qc",
                    rtf_content[start_pos:]
                )
                if next_chapter:
                    end_pos = start_pos + next_chapter.start()
                else:
                    end_pos = len(rtf_content)

            section_content = rtf_content[start_match.start():end_pos]
            text = extract_section_text(section_content)

            sections.append({
                "type": "section",
                "number": section_num,
                "heading": heading,
                "text": text,
            })

    return sections


def extract_section_text(rtf_content: str) -> str:
    """Extract the text content from a section's RTF content."""
    # Remove style definitions and formatting info
    text = rtf_content

    # Remove hidden text markers
    text = re.sub(r"\\v\\fs8[^}]*\}", "", text)
    text = re.sub(r"\{\\v[^}]*\}", "", text)

    # Remove annotation blocks (style s38)
    text = re.sub(r"\\s38[^\\]*", "", text)

    # Remove history blocks (style s47)
    text = re.sub(r"\\s47[^\\]*", "", text)

    # Strip RTF control codes
    text = strip_rtf_control_words(text)

    # Clean up multiple newlines and whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" +", " ", text)

    return text.strip()


def parse_title_info(rtf_content: str) -> tuple[str, str]:
    """Extract title number and name from RTF content."""
    # Look for title header (style s2)
    title_pattern = re.compile(
        r"\\s2\s*\\[^{]*\{[^}]*Title\s+(\d+(?:\.\d+)?)\s+([^\n\\]+)",
        re.IGNORECASE | re.DOTALL
    )

    match = title_pattern.search(rtf_content)
    if match:
        return match.group(1), strip_rtf_control_words(match.group(2))

    return "", ""


def create_akn_document(
    title_num: str,
    title_name: str,
    sections: list[dict],
) -> Act:
    """Create an Akoma Ntoso Act document from parsed sections."""

    # Create identification
    work_uri = f"/akn/us-wy/act/code/title{title_num}"
    today = date.today()

    identification = Identification(
        source="#axiom-foundation",
        work=FRBRWork(
            uri=FRBRUri(value=work_uri),
            date=FRBRDate(value=date(2022, 10, 1), name="enactment"),
            author=FRBRAuthor(href="#wyleg"),
            country=FRBRCountry(value="us-wy"),
            this=f"{work_uri}/main",
        ),
        expression=FRBRExpression(
            uri=FRBRUri(value=f"{work_uri}/eng@2022-10-01"),
            date=FRBRDate(value=date(2022, 10, 1), name="publication"),
            author=FRBRAuthor(href="#axiom-foundation"),
            language=FRBRLanguage(language="en"),
            this=f"{work_uri}/eng@2022-10-01/main",
        ),
        manifestation=FRBRManifestation(
            uri=FRBRUri(value=f"{work_uri}/eng@2022-10-01.akn"),
            date=FRBRDate(value=today, name="transformation"),
            author=FRBRAuthor(href="#axiom-foundation"),
            this=f"{work_uri}/eng@2022-10-01/main.akn",
        ),
    )

    # Build body from sections
    # Group sections by chapter
    chapters = {}
    for section in sections:
        num = section["number"]
        # Parse section number to get chapter (e.g., 39-11-101 -> chapter 11)
        parts = num.split("-")
        if len(parts) >= 2:
            chapter_num = parts[1]
            if chapter_num not in chapters:
                chapters[chapter_num] = []
            chapters[chapter_num].append(section)

    # Create hierarchical body
    body = []

    # Create title element
    title_elem = Part(
        eid=f"title_{title_num}",
        num=f"Title {title_num}",
        heading=title_name,
        children=[],
    )

    # Add chapters
    for chapter_num in sorted(chapters.keys(), key=lambda x: int(x) if x.isdigit() else 0):
        chapter_sections = chapters[chapter_num]

        chapter_elem = Chapter(
            eid=f"chapter_{chapter_num}",
            num=f"Chapter {chapter_num}",
            heading="",  # Would need to extract from RTF
            children=[],
        )

        # Add sections to chapter
        for section in chapter_sections:
            section_elem = Section(
                eid=f"sec_{section['number'].replace('-', '_').replace('.', '_')}",
                num=section['number'],
                heading=section['heading'],
                text=section['text'],
            )
            chapter_elem.children.append(section_elem)

        title_elem.children.append(chapter_elem)

    body.append(title_elem)

    return Act(
        document_type=DocumentType.ACT,
        identification=identification,
        body=body,
        source_url="https://wyoleg.gov/",
    )


def prettify_xml(elem: ET.Element, indent: str = "  ") -> str:
    """Pretty-print XML with indentation."""
    ET.indent(elem, space=indent)
    return ET.tostring(elem, encoding="unicode")


def convert_rtf_file(rtf_path: Path, output_dir: Path) -> dict:
    """Convert a single RTF file to Akoma Ntoso XML.

    Returns dict with conversion stats.
    """
    stats = {
        "file": rtf_path.name,
        "sections": 0,
        "chapters": 0,
        "success": False,
        "error": None,
    }

    try:
        # Read RTF content
        with open(rtf_path, "r", encoding="latin-1") as f:
            rtf_content = f.read()

        # Extract title info from filename
        # e.g., gov.wy.code.title.39.rtf -> title 39
        filename = rtf_path.stem
        title_match = re.search(r"title\.(\d+(?:\.\d+)?)", filename)
        if title_match:
            title_num = title_match.group(1)
        else:
            # Handle constitution files
            if "constitution" in filename:
                title_num = "constitution"
            else:
                stats["error"] = "Could not determine title number"
                return stats

        # Parse title name from content
        title_num_parsed, title_name = parse_title_info(rtf_content)
        if not title_name:
            title_name = f"Title {title_num}"

        # Parse sections
        sections = parse_rtf_to_sections(rtf_content)
        stats["sections"] = len(sections)

        # Group by chapter for counting
        chapters = set()
        for section in sections:
            parts = section["number"].split("-")
            if len(parts) >= 2:
                chapters.add(parts[1])
        stats["chapters"] = len(chapters)

        # Create AKN document
        doc = create_akn_document(title_num, title_name, sections)

        # Generate XML
        xml_elem = doc.to_xml_element()
        xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_str += prettify_xml(xml_elem)

        # Write output
        output_file = output_dir / f"us-wy-title-{title_num}.akn.xml"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(xml_str)

        stats["success"] = True
        stats["output_file"] = str(output_file)

    except Exception as e:
        stats["error"] = str(e)

    return stats


def main():
    """Main conversion function."""
    # Set up paths
    arch_root = Path(__file__).parent.parent
    input_dir = arch_root / "data/statutes/us-wy/release84.2022.10"
    output_dir = Path("/tmp/rules-us-wy-akn")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find RTF files
    rtf_files = sorted(input_dir.glob("*.rtf"))

    print(f"Found {len(rtf_files)} RTF files in {input_dir}")
    print(f"Output directory: {output_dir}")
    print("-" * 60)

    # Convert each file
    total_sections = 0
    total_chapters = 0
    successful = 0
    failed = 0

    for rtf_file in rtf_files:
        print(f"Converting {rtf_file.name}...", end=" ")
        stats = convert_rtf_file(rtf_file, output_dir)

        if stats["success"]:
            successful += 1
            total_sections += stats["sections"]
            total_chapters += stats["chapters"]
            print(f"OK ({stats['sections']} sections, {stats['chapters']} chapters)")
        else:
            failed += 1
            print(f"FAILED: {stats['error']}")

    # Print summary
    print("-" * 60)
    print(f"Conversion complete!")
    print(f"  Files: {successful} successful, {failed} failed")
    print(f"  Total sections: {total_sections}")
    print(f"  Total chapters: {total_chapters}")
    print(f"  Output: {output_dir}")

    # List output files
    output_files = sorted(output_dir.glob("*.akn.xml"))
    print(f"\nGenerated {len(output_files)} AKN files:")
    for f in output_files:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
