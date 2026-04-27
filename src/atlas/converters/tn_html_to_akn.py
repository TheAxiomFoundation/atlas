"""Convert Tennessee Code Annotated HTML to Akoma Ntoso XML.

Tennessee statute HTML files are provided by Public.Resource.Org and have a
well-structured format:
- Title at the top (h1)
- Chapters (h2 with id like `t67c01`)
- Parts (h2 with class `parth2` and id like `t67c01p01`)
- Sections (h3 with id like `t67c01s67-1-101`)
- Subsections in ordered lists (`ol.alpha`, nested `ol`)

Output format is Akoma Ntoso 3.0 XML.

Usage:
    from atlas.converters.tn_html_to_akn import TennesseeToAKN

    converter = TennesseeToAKN()
    converter.convert_directory(
        "/path/to/tn/release76.2021.05.21",
        "/tmp/rules-us-tn-akn"
    )
"""

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterator
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup, Tag, NavigableString


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


@dataclass
class ParsedSubsection:
    """A parsed subsection from HTML."""

    identifier: str  # e.g., "a", "1", "A"
    eId: str  # Full eId path
    text: str
    children: list["ParsedSubsection"] = field(default_factory=list)


@dataclass
class ParsedSection:
    """A parsed section from HTML."""

    section_num: str  # e.g., "67-1-101"
    heading: str  # e.g., "Liberal construction of title..."
    eId: str  # e.g., "sec_67-1-101"
    text: str = ""
    subsections: list[ParsedSubsection] = field(default_factory=list)
    history: str = ""  # Acts history line


@dataclass
class ParsedPart:
    """A parsed part from HTML."""

    part_num: str  # e.g., "1"
    heading: str  # e.g., "Miscellaneous Provisions"
    eId: str  # e.g., "chp_1__part_1"
    sections: list[ParsedSection] = field(default_factory=list)


@dataclass
class ParsedChapter:
    """A parsed chapter from HTML."""

    chapter_num: str  # e.g., "1"
    heading: str  # e.g., "General Provisions"
    eId: str  # e.g., "chp_1"
    parts: list[ParsedPart] = field(default_factory=list)
    sections: list[ParsedSection] = field(default_factory=list)  # Sections directly under chapter


@dataclass
class ParsedTitle:
    """A parsed title from HTML."""

    title_num: str  # e.g., "67"
    heading: str  # e.g., "Taxes And Licenses"
    chapters: list[ParsedChapter] = field(default_factory=list)
    source_file: str = ""


class TennesseeToAKN:
    """Convert Tennessee Code Annotated HTML to Akoma Ntoso XML."""

    def __init__(self):
        self.jurisdiction = "us-tn"
        self.source_format = "html"

    def convert_directory(
        self,
        input_dir: Path | str,
        output_dir: Path | str,
    ) -> dict:
        """Convert all title HTML files in a directory to AKN XML.

        Args:
            input_dir: Directory containing gov.tn.tca.title.XX.html files
            output_dir: Output directory for AKN XML files

        Returns:
            Dictionary with success/failure counts and details
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        results = {
            "success": 0,
            "failed": 0,
            "titles": [],
            "errors": [],
        }

        # Find all title HTML files
        title_files = sorted(input_dir.glob("gov.tn.tca.title.*.html"))

        for title_file in title_files:
            try:
                title_num = self._extract_title_num(title_file.name)
                if not title_num:
                    continue  # pragma: no cover

                output_file = output_dir / f"title-{title_num}.akn.xml"

                # Parse and convert
                html = title_file.read_text(encoding="utf-8")
                parsed = self.parse_html(html, str(title_file))
                xml_str = self.to_akn_xml(parsed)

                # Write output
                output_file.write_text(xml_str, encoding="utf-8")

                results["success"] += 1
                results["titles"].append(
                    {
                        "title": title_num,
                        "heading": parsed.heading,
                        "chapters": len(parsed.chapters),
                        "output": str(output_file),
                    }
                )

            except Exception as e:  # pragma: no cover
                results["failed"] += 1  # pragma: no cover
                results["errors"].append(
                    {  # pragma: no cover
                        "file": str(title_file),
                        "error": str(e),
                    }
                )

        return results

    def _extract_title_num(self, filename: str) -> str | None:
        """Extract title number from filename like gov.tn.tca.title.67.html"""
        match = re.search(r"gov\.tn\.tca\.title\.(\d+)\.html", filename)
        return match.group(1) if match else None

    def parse_html(self, html: str, source_file: str = "") -> ParsedTitle:
        """Parse Tennessee statute HTML into structured data."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract title info from h1
        h1 = soup.find("h1")
        title_num = ""
        title_heading = ""
        if h1:
            h1_text = h1.get_text(strip=True)
            # Format: "Title 67 Taxes And Licenses"
            match = re.match(r"Title\s+(\d+)\s+(.+)", h1_text)
            if match:
                title_num = match.group(1)
                title_heading = match.group(2)

        parsed_title = ParsedTitle(
            title_num=title_num,
            heading=title_heading,
            source_file=source_file,
        )

        # Find main content
        main = soup.find("main")
        if not main:
            return parsed_title

        # Parse chapters - they are h2 elements with id starting with 't{title_num}c'
        current_chapter = None
        current_part = None

        for elem in main.find_all(["h2", "h3", "div"]):
            if elem.name == "h2":
                elem_id = elem.get("id", "")
                elem_class = elem.get("class", [])

                # Check if this is a part heading (has parth2 class)
                if "parth2" in elem_class:
                    # Part heading
                    part_text = elem.get_text(strip=True)
                    match = re.match(r"Part\s+(\d+)\s+(.+)", part_text)
                    if match and current_chapter:
                        part_num = match.group(1)
                        part_heading = match.group(2)
                        current_part = ParsedPart(
                            part_num=part_num,
                            heading=part_heading,
                            eId=f"chp_{current_chapter.chapter_num}__part_{part_num}",
                        )
                        current_chapter.parts.append(current_part)
                else:
                    # Chapter heading
                    chapter_text = elem.get_text(strip=True)
                    match = re.match(r"Chapter\s+(\d+)\s+(.+)", chapter_text)
                    if match:
                        chapter_num = match.group(1)
                        chapter_heading = match.group(2)
                        current_chapter = ParsedChapter(
                            chapter_num=chapter_num,
                            heading=chapter_heading,
                            eId=f"chp_{chapter_num}",
                        )
                        parsed_title.chapters.append(current_chapter)
                        current_part = None  # Reset part when entering new chapter

            elif elem.name == "h3":
                # Section heading
                elem_id = elem.get("id", "")
                section_text = elem.get_text(strip=True)

                # Extract section number and heading
                # Format: "67-1-101. Liberal construction of title..."
                match = re.match(r"([\d\-]+)\.\s*(.+)", section_text)
                if match:
                    section_num = match.group(1)
                    section_heading = match.group(2)

                    # Get the containing div to extract content
                    parent_div = elem.find_parent("div")
                    section_text, subsections, history = self._parse_section_content(
                        parent_div, elem
                    )

                    section = ParsedSection(
                        section_num=section_num,
                        heading=section_heading,
                        eId=f"sec_{section_num}",
                        text=section_text,
                        subsections=subsections,
                        history=history,
                    )

                    # Add to current part or chapter
                    if current_part:
                        current_part.sections.append(section)
                    elif current_chapter:
                        current_chapter.sections.append(section)

        return parsed_title

    def _parse_section_content(
        self, div: Tag | None, h3: Tag
    ) -> tuple[str, list[ParsedSubsection], str]:
        """Parse section content from containing div.

        Returns:
            Tuple of (text, subsections, history)
        """
        if not div:
            return "", [], ""  # pragma: no cover

        text_parts = []
        subsections = []
        history = ""

        # Find the section number for eId construction
        section_text = h3.get_text(strip=True)
        match = re.match(r"([\d\-]+)\.", section_text)
        section_num = match.group(1) if match else ""
        base_eId = f"sec_{section_num}"

        # Process elements after h3
        for sibling in h3.find_next_siblings():
            if sibling.name == "h3":
                # Hit next section
                break  # pragma: no cover

            if sibling.name == "ol" and "alpha" in sibling.get("class", []):
                # Main subsection list
                subsections = self._parse_subsection_list(sibling, base_eId, 0)

            elif sibling.name == "p":
                # Check if this is history/acts line
                p_text = sibling.get_text(strip=True)
                if re.match(r"Acts\s+\d{4}", p_text):
                    history = p_text
                else:
                    # Regular paragraph text
                    if not subsections:  # Only add if no subsections yet
                        text_parts.append(p_text)

        return "\n".join(text_parts), subsections, history

    def _parse_subsection_list(
        self, ol: Tag, parent_eId: str, level: int
    ) -> list[ParsedSubsection]:
        """Recursively parse subsection lists."""
        subsections = []

        for li in ol.find_all("li", recursive=False):
            li_id = li.get("id", "")

            # Extract identifier from id like "t67c01s67-1-101ol1a"
            identifier = self._extract_subsection_identifier(li_id, level)
            if not identifier:
                continue  # pragma: no cover

            # Build eId
            level_name = self._level_name(level)
            eId = f"{parent_eId}__{level_name}_{identifier}"

            # Get text content (excluding nested lists)
            text_parts = []
            children = []

            for child in li.children:
                if isinstance(child, NavigableString):
                    text = str(child).strip()
                    if text:
                        text_parts.append(text)
                elif isinstance(child, Tag):
                    if child.name == "ol":
                        # Nested subsections
                        children = self._parse_subsection_list(child, eId, level + 1)
                    else:
                        # Get text from other elements
                        text = child.get_text(strip=True)  # pragma: no cover
                        if text:  # pragma: no cover
                            text_parts.append(text)  # pragma: no cover

            subsections.append(
                ParsedSubsection(
                    identifier=identifier,
                    eId=eId,
                    text=" ".join(text_parts),
                    children=children,
                )
            )

        return subsections

    def _extract_subsection_identifier(self, li_id: str, level: int) -> str:
        """Extract subsection identifier from li id.

        Examples:
            t67c01s67-1-101ol1a -> a
            t67c01s67-1-101ol1b1 -> 1
            t67c01s67-1-101ol1c2 -> 2
        """
        # Look for pattern at end: ol1a, ol1b1, etc.
        match = re.search(r"ol\d+([a-z])(\d+)?$", li_id)
        if match:
            if level == 0:
                return match.group(1)  # a, b, c
            elif match.group(2):
                return match.group(2)  # 1, 2, 3 for nested
        return ""  # pragma: no cover

    def _level_name(self, level: int) -> str:
        """Map level to Akoma Ntoso element name."""
        names = ["subsec", "para", "subpara", "clause", "subclause"]
        return names[min(level, len(names) - 1)]

    def to_akn_xml(self, parsed: ParsedTitle) -> str:
        """Convert ParsedTitle to Akoma Ntoso XML string."""
        # Create root akomaNtoso element
        root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")
        root.set("xmlns", AKN_NS)

        # Create act element
        act = ET.SubElement(root, f"{{{AKN_NS}}}act")
        act.set("name", "act")

        # Meta section
        meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")

        # Identification
        identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
        identification.set("source", "#public-resource-org")

        # FRBRWork
        frbr_work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")

        work_this = ET.SubElement(frbr_work, f"{{{AKN_NS}}}FRBRthis")
        work_this.set("value", f"/us-tn/act/tca/title-{parsed.title_num}")

        work_uri = ET.SubElement(frbr_work, f"{{{AKN_NS}}}FRBRuri")
        work_uri.set("value", f"/us-tn/act/tca/title-{parsed.title_num}")

        work_date = ET.SubElement(frbr_work, f"{{{AKN_NS}}}FRBRdate")
        work_date.set("date", "2021-05-21")
        work_date.set("name", "release")

        work_author = ET.SubElement(frbr_work, f"{{{AKN_NS}}}FRBRauthor")
        work_author.set("href", "#tennessee-legislature")

        work_country = ET.SubElement(frbr_work, f"{{{AKN_NS}}}FRBRcountry")
        work_country.set("value", "us-tn")

        # FRBRExpression
        frbr_expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")

        expr_this = ET.SubElement(frbr_expr, f"{{{AKN_NS}}}FRBRthis")
        expr_this.set("value", f"/us-tn/act/tca/title-{parsed.title_num}/eng@2021-05-21")

        expr_uri = ET.SubElement(frbr_expr, f"{{{AKN_NS}}}FRBRuri")
        expr_uri.set("value", f"/us-tn/act/tca/title-{parsed.title_num}/eng@2021-05-21")

        expr_date = ET.SubElement(frbr_expr, f"{{{AKN_NS}}}FRBRdate")
        expr_date.set("date", "2021-05-21")
        expr_date.set("name", "release")

        expr_author = ET.SubElement(frbr_expr, f"{{{AKN_NS}}}FRBRauthor")
        expr_author.set("href", "#public-resource-org")

        expr_lang = ET.SubElement(frbr_expr, f"{{{AKN_NS}}}FRBRlanguage")
        expr_lang.set("language", "eng")

        # FRBRManifestation
        frbr_manif = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")

        manif_this = ET.SubElement(frbr_manif, f"{{{AKN_NS}}}FRBRthis")
        manif_this.set("value", f"/us-tn/act/tca/title-{parsed.title_num}/eng@2021-05-21.akn")

        manif_uri = ET.SubElement(frbr_manif, f"{{{AKN_NS}}}FRBRuri")
        manif_uri.set("value", f"/us-tn/act/tca/title-{parsed.title_num}/eng@2021-05-21.akn")

        manif_date = ET.SubElement(frbr_manif, f"{{{AKN_NS}}}FRBRdate")
        manif_date.set("date", date.today().isoformat())
        manif_date.set("name", "generation")

        manif_author = ET.SubElement(frbr_manif, f"{{{AKN_NS}}}FRBRauthor")
        manif_author.set("href", "#axiom-foundation")

        # References
        references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
        references.set("source", "#axiom-foundation")

        org1 = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
        org1.set("eId", "tennessee-legislature")
        org1.set("href", "/ontology/organization/us-tn/tennessee-legislature")
        org1.set("showAs", "Tennessee General Assembly")

        org2 = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
        org2.set("eId", "public-resource-org")
        org2.set("href", "https://public.resource.org")
        org2.set("showAs", "Public.Resource.Org")

        org3 = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
        org3.set("eId", "axiom-foundation")
        org3.set("href", "https://axiom-foundation.org")
        org3.set("showAs", "The Axiom Foundation")

        # Body
        body = ET.SubElement(act, f"{{{AKN_NS}}}body")

        # Add title as top-level element
        title_elem = ET.SubElement(body, f"{{{AKN_NS}}}title")
        title_elem.set("eId", f"title_{parsed.title_num}")

        title_num_elem = ET.SubElement(title_elem, f"{{{AKN_NS}}}num")
        title_num_elem.text = f"Title {parsed.title_num}"

        title_heading = ET.SubElement(title_elem, f"{{{AKN_NS}}}heading")
        title_heading.text = parsed.heading

        # Add chapters
        for chapter in parsed.chapters:
            self._add_chapter(title_elem, chapter)

        # Format and return
        ET.indent(root)

        # Convert to string with XML declaration
        xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_str += ET.tostring(root, encoding="unicode")

        return xml_str

    def _add_chapter(self, parent: ET.Element, chapter: ParsedChapter) -> None:
        """Add chapter element to parent."""
        chap_elem = ET.SubElement(parent, f"{{{AKN_NS}}}chapter")
        chap_elem.set("eId", chapter.eId)

        num_elem = ET.SubElement(chap_elem, f"{{{AKN_NS}}}num")
        num_elem.text = f"Chapter {chapter.chapter_num}"

        heading = ET.SubElement(chap_elem, f"{{{AKN_NS}}}heading")
        heading.text = chapter.heading

        # Add parts if present
        for part in chapter.parts:
            self._add_part(chap_elem, part)

        # Add direct sections (if no parts)
        for section in chapter.sections:
            self._add_section(chap_elem, section)

    def _add_part(self, parent: ET.Element, part: ParsedPart) -> None:
        """Add part element to parent."""
        part_elem = ET.SubElement(parent, f"{{{AKN_NS}}}part")
        part_elem.set("eId", part.eId)

        num_elem = ET.SubElement(part_elem, f"{{{AKN_NS}}}num")
        num_elem.text = f"Part {part.part_num}"

        heading = ET.SubElement(part_elem, f"{{{AKN_NS}}}heading")
        heading.text = part.heading

        # Add sections
        for section in part.sections:
            self._add_section(part_elem, section)

    def _add_section(self, parent: ET.Element, section: ParsedSection) -> None:
        """Add section element to parent."""
        sec_elem = ET.SubElement(parent, f"{{{AKN_NS}}}section")
        sec_elem.set("eId", section.eId)

        num_elem = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
        num_elem.text = section.section_num

        heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
        heading.text = section.heading

        # Add content if present
        if section.text:
            content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
            p = ET.SubElement(content, f"{{{AKN_NS}}}p")
            p.text = section.text

        # Add subsections
        for sub in section.subsections:
            self._add_subsection(sec_elem, sub, 0)

        # Add history as notes
        if section.history:
            notes = ET.SubElement(sec_elem, f"{{{AKN_NS}}}notes")
            note = ET.SubElement(notes, f"{{{AKN_NS}}}note")
            note.set("type", "history")
            p = ET.SubElement(note, f"{{{AKN_NS}}}p")
            p.text = section.history

    def _add_subsection(self, parent: ET.Element, sub: ParsedSubsection, level: int) -> None:
        """Add subsection element recursively."""
        # Element names by level
        level_names = ["subsection", "paragraph", "subparagraph", "clause", "subclause"]
        elem_name = level_names[min(level, len(level_names) - 1)]

        sub_elem = ET.SubElement(parent, f"{{{AKN_NS}}}{elem_name}")
        sub_elem.set("eId", sub.eId)

        num_elem = ET.SubElement(sub_elem, f"{{{AKN_NS}}}num")
        num_elem.text = f"({sub.identifier})"

        # Add content
        if sub.text:
            content = ET.SubElement(sub_elem, f"{{{AKN_NS}}}content")
            p = ET.SubElement(content, f"{{{AKN_NS}}}p")
            p.text = sub.text

        # Add children
        for child in sub.children:
            self._add_subsection(sub_elem, child, level + 1)


def main():
    """CLI entry point for Tennessee converter."""
    import sys  # pragma: no cover

    if len(sys.argv) < 3:  # pragma: no cover
        print("Usage: python tn_html_to_akn.py <input_dir> <output_dir>")  # pragma: no cover
        print(
            "Example: python tn_html_to_akn.py data/statutes/us-tn/release76.2021.05.21 /tmp/rules-us-tn-akn"
        )  # pragma: no cover
        sys.exit(1)  # pragma: no cover

    input_dir = sys.argv[1]  # pragma: no cover
    output_dir = sys.argv[2]  # pragma: no cover

    converter = TennesseeToAKN()  # pragma: no cover
    results = converter.convert_directory(input_dir, output_dir)  # pragma: no cover

    print(f"\nConversion complete:")  # pragma: no cover
    print(f"  Success: {results['success']}")  # pragma: no cover
    print(f"  Failed: {results['failed']}")  # pragma: no cover

    if results["titles"]:  # pragma: no cover
        print(f"\nConverted titles:")  # pragma: no cover
        for title in results["titles"]:  # pragma: no cover
            print(
                f"  Title {title['title']}: {title['heading']} ({title['chapters']} chapters)"
            )  # pragma: no cover

    if results["errors"]:  # pragma: no cover
        print(f"\nErrors:")  # pragma: no cover
        for error in results["errors"]:  # pragma: no cover
            print(f"  {error['file']}: {error['error']}")  # pragma: no cover


if __name__ == "__main__":
    main()
