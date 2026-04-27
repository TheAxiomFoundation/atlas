"""Convert Minnesota Statutes HTML to Akoma Ntoso XML.

This module converts Minnesota Revisor of Statutes HTML pages into
Akoma Ntoso XML format for standardized legal document processing.

Source: https://www.revisor.mn.gov/statutes/
Format: HTML pages with semantic class-based structure

Usage:
    from atlas.converters.mn_statutes import MNStatutesToAKN

    converter = MNStatutesToAKN()
    akn_xml = converter.convert_file("statutes_cite_609.75.html")
"""

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup, Tag


# Akoma Ntoso namespace
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


@dataclass
class MNSubsection:
    """A parsed subdivision/subsection from MN HTML."""

    identifier: str  # e.g., "1", "1a", "2"
    headnote: str  # The subdivision heading
    paragraphs: list[str] = field(default_factory=list)  # Text paragraphs
    status: str = ""  # e.g., "Repealed"


@dataclass
class MNSection:
    """A parsed section from MN statute HTML."""

    citation: str  # e.g., "105.63", "609.75"
    chapter: str  # e.g., "105", "609"
    title: str  # Section heading, e.g., "GAMBLING; DEFINITIONS."
    part_name: str  # e.g., "CRIMES; EXPUNGEMENT; VICTIMS"
    year: str  # e.g., "2025"
    is_repealed: bool = False
    repealed_by: str = ""  # e.g., "1990 c 391 art 10 s 4"
    text: str = ""  # Direct text if no subdivisions
    subdivisions: list[MNSubsection] = field(default_factory=list)
    history: str = ""  # Legislative history
    source_url: str = ""


class MNStatutesToAKN:
    """Convert Minnesota Statutes HTML to Akoma Ntoso XML."""

    def __init__(self):
        self.country = "us-mn"  # FRBRcountry value

    def parse_citation_from_filename(self, filename: str) -> str:
        """Extract citation from filename like 'statutes_cite_609.75.html'."""
        match = re.search(r"statutes_cite_([0-9A-Za-z.-]+)\.html", filename)
        if match:
            return match.group(1)
        return ""

    def parse_html(self, html: str, source_url: str = "", filename: str = "") -> MNSection:
        """Parse Minnesota statute HTML into structured data."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract citation from filename or content
        citation = self.parse_citation_from_filename(filename)

        # Extract chapter from citation
        chapter = citation.split(".")[0] if "." in citation else citation

        # Extract year from breadcrumb "2025 Minnesota Statutes"
        year = "2025"
        breadcrumb = soup.find("div", id="breadcrumb")
        if breadcrumb:
            year_link = breadcrumb.find("a", href="/statutes/")
            if year_link:
                year_match = re.match(r"(\d{4})", year_link.get_text(strip=True))
                if year_match:
                    year = year_match.group(1)

        # Extract part name from breadcrumb
        part_name = ""
        if breadcrumb:
            breadcrumb_links = breadcrumb.find_all("a")
            for link in breadcrumb_links:
                href = link.get("href", "")
                if "/statutes/part/" in href:
                    part_name = link.get_text(strip=True)
                    break

        # Find the statute content div
        xtend = soup.find("div", id="xtend", class_="statute")

        # Default values
        title = ""
        is_repealed = False
        repealed_by = ""
        text = ""
        subdivisions = []
        history = ""

        if xtend:
            # Check for repealed section (simple format)
            sr_div = xtend.find("div", class_="sr")
            if sr_div:
                sr_text = sr_div.get_text(strip=True)
                is_repealed = True
                # Parse: "105.63 [Repealed, 1990 c 391 art 10 s 4]"
                repeal_match = re.search(r"\[Repealed,\s*([^\]]+)\]", sr_text)
                if repeal_match:
                    repealed_by = repeal_match.group(1).strip()

            # Check for full section with heading
            section_div = xtend.find("div", class_="section")
            if section_div:
                # Get section title
                h1 = section_div.find("h1", class_="shn")
                if h1:
                    title = h1.get_text(strip=True)
                    # Remove section number prefix if present
                    title_match = re.match(r"[\d.]+\s+(.+)", title)
                    if title_match:
                        title = title_match.group(1)

                # Get direct text paragraphs (not in subdivisions)
                direct_paragraphs = []
                for child in section_div.children:
                    if isinstance(child, Tag):
                        if child.name == "p" and "subd" not in child.get("class", []):
                            direct_paragraphs.append(child.get_text(strip=True))

                if direct_paragraphs:
                    text = "\n\n".join(direct_paragraphs)

                # Parse subdivisions
                for subd_div in section_div.find_all("div", class_="subd"):
                    subd = self._parse_subdivision(subd_div)
                    if subd:
                        subdivisions.append(subd)

            # Parse history section
            history_div = xtend.find("div", class_="history")
            if history_div:
                history_p = history_div.find("p", class_="first")
                if history_p:
                    history = history_p.get_text(strip=True)

        return MNSection(
            citation=citation,
            chapter=chapter,
            title=title,
            part_name=part_name,
            year=year,
            is_repealed=is_repealed,
            repealed_by=repealed_by,
            text=text,
            subdivisions=subdivisions,
            history=history,
            source_url=source_url,
        )

    def _parse_subdivision(self, subd_div: Tag) -> Optional[MNSubsection]:
        """Parse a subdivision div element."""
        subd_id = subd_div.get("id", "")
        # Extract subdivision number from id like "stat.609.75.1" -> "1"
        id_match = re.search(r"\.(\d+[a-z]?)$", subd_id)
        identifier = id_match.group(1) if id_match else ""

        # Get headnote from h2
        headnote = ""
        h2 = subd_div.find("h2", class_="subd_no")
        if h2:
            # Get just the headnote span
            headnote_span = h2.find("span", class_="headnote")
            if headnote_span:
                headnote = headnote_span.get_text(strip=True)

        # Get paragraphs
        paragraphs = []
        for p in subd_div.find_all("p"):
            p_text = p.get_text(strip=True)
            if p_text:
                paragraphs.append(p_text)

        # Check if repealed
        status = ""
        if paragraphs and "[Repealed," in paragraphs[0]:
            status = "repealed"  # pragma: no cover

        return MNSubsection(
            identifier=identifier,
            headnote=headnote,
            paragraphs=paragraphs,
            status=status,
        )

    def to_akn_xml(self, section: MNSection) -> str:
        """Convert parsed MN section to Akoma Ntoso XML."""
        # Register namespace
        ET.register_namespace("akn", AKN_NS)
        ET.register_namespace("", AKN_NS)

        # Root element
        root = ET.Element(f"{{{AKN_NS}}}akomaNtoso")

        # Document type - act for statutes
        act = ET.SubElement(root, f"{{{AKN_NS}}}act")

        # Meta section
        meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")
        self._add_identification(meta, section)
        self._add_lifecycle(meta, section)

        # Preface (optional - for part context)
        if section.part_name:
            preface = ET.SubElement(act, f"{{{AKN_NS}}}preface")
            container = ET.SubElement(preface, f"{{{AKN_NS}}}container")
            container.set("name", "part")
            p = ET.SubElement(container, f"{{{AKN_NS}}}p")
            p.text = section.part_name

        # Body
        body = ET.SubElement(act, f"{{{AKN_NS}}}body")

        # Section element
        section_elem = ET.SubElement(body, f"{{{AKN_NS}}}section")
        section_elem.set("eId", f"sec_{section.citation.replace('.', '_')}")

        # Add num
        num = ET.SubElement(section_elem, f"{{{AKN_NS}}}num")
        num.text = section.citation

        # Add heading
        if section.title:
            heading = ET.SubElement(section_elem, f"{{{AKN_NS}}}heading")
            heading.text = section.title

        # Handle repealed sections
        if section.is_repealed and not section.subdivisions:
            section_elem.set("status", "repealed")
            content = ET.SubElement(section_elem, f"{{{AKN_NS}}}content")
            p = ET.SubElement(content, f"{{{AKN_NS}}}p")
            p.text = f"[Repealed, {section.repealed_by}]" if section.repealed_by else "[Repealed]"

        # Add direct text content
        elif section.text and not section.subdivisions:
            content = ET.SubElement(section_elem, f"{{{AKN_NS}}}content")
            for para in section.text.split("\n\n"):
                if para.strip():
                    p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                    p.text = para.strip()

        # Add subdivisions
        for subd in section.subdivisions:
            self._add_subdivision(section_elem, subd, section.citation)

        # Convert to string with declaration
        ET.indent(root)
        xml_str = ET.tostring(root, encoding="unicode")
        return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'

    def _add_identification(self, meta: ET.Element, section: MNSection) -> None:
        """Add FRBR identification block."""
        identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
        identification.set("source", "#revisor")

        # FRBRWork
        work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")

        work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
        work_uri = f"/akn/{self.country}/act/statute/{section.citation}"
        work_this.set("value", work_uri)

        work_uri_elem = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
        work_uri_elem.set("value", work_uri)

        work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
        work_date.set("date", f"{section.year}-01-01")
        work_date.set("name", "enactment")

        work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
        work_author.set("href", "#mnleg")

        work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
        work_country.set("value", self.country)

        work_number = ET.SubElement(work, f"{{{AKN_NS}}}FRBRnumber")
        work_number.set("value", section.citation)

        # FRBRExpression
        expression = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")

        expr_this = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRthis")
        expr_uri = f"{work_uri}/eng@{section.year}"
        expr_this.set("value", expr_uri)

        expr_uri_elem = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRuri")
        expr_uri_elem.set("value", expr_uri)

        expr_date = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRdate")
        expr_date.set("date", f"{section.year}-01-01")
        expr_date.set("name", "publication")

        expr_author = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRauthor")
        expr_author.set("href", "#revisor")

        expr_lang = ET.SubElement(expression, f"{{{AKN_NS}}}FRBRlanguage")
        expr_lang.set("language", "eng")

        # FRBRManifestation
        manifestation = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRManifestation")

        manif_this = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRthis")
        manif_uri = f"{expr_uri}/main.xml"
        manif_this.set("value", manif_uri)

        manif_uri_elem = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRuri")
        manif_uri_elem.set("value", manif_uri)

        manif_date = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRdate")
        manif_date.set("date", date.today().isoformat())
        manif_date.set("name", "generation")

        manif_author = ET.SubElement(manifestation, f"{{{AKN_NS}}}FRBRauthor")
        manif_author.set("href", "#axiom-foundation")

    def _add_lifecycle(self, meta: ET.Element, section: MNSection) -> None:
        """Add lifecycle events."""
        lifecycle = ET.SubElement(meta, f"{{{AKN_NS}}}lifecycle")
        lifecycle.set("source", "#revisor")

        # Add repeal event if repealed
        if section.is_repealed and section.repealed_by:
            event = ET.SubElement(lifecycle, f"{{{AKN_NS}}}eventRef")
            event.set("eId", "evt_repeal")
            event.set("type", "repeal")
            event.set("source", f"#{section.repealed_by.replace(' ', '_')}")

    def _add_subdivision(self, parent: ET.Element, subd: MNSubsection, citation: str) -> None:
        """Add a subdivision element."""
        subd_elem = ET.SubElement(parent, f"{{{AKN_NS}}}subsection")
        subd_elem.set("eId", f"sec_{citation.replace('.', '_')}__subd_{subd.identifier}")

        if subd.status == "repealed":
            subd_elem.set("status", "repealed")

        # Add num
        num = ET.SubElement(subd_elem, f"{{{AKN_NS}}}num")
        num.text = f"Subd. {subd.identifier}."

        # Add heading if present
        if subd.headnote:
            heading = ET.SubElement(subd_elem, f"{{{AKN_NS}}}heading")
            heading.text = subd.headnote

        # Add content
        if subd.paragraphs:
            content = ET.SubElement(subd_elem, f"{{{AKN_NS}}}content")
            for para_text in subd.paragraphs:
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para_text

    def convert_html(self, html: str, source_url: str = "", filename: str = "") -> str:
        """Convert HTML string to Akoma Ntoso XML string."""
        section = self.parse_html(html, source_url, filename)
        return self.to_akn_xml(section)

    def convert_file(self, input_path: Path | str, output_path: Path | str | None = None) -> str:
        """Convert an HTML file to Akoma Ntoso XML.

        Args:
            input_path: Path to input HTML file
            output_path: Optional path for output XML file

        Returns:
            Akoma Ntoso XML string
        """
        input_path = Path(input_path)
        html = input_path.read_text()

        source_url = f"https://www.revisor.mn.gov/statutes/cite/{self.parse_citation_from_filename(input_path.name)}"
        xml_str = self.convert_html(html, source_url, input_path.name)

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(xml_str)

        return xml_str

    def convert_directory(
        self,
        input_dir: Path | str,
        output_dir: Path | str,
    ) -> dict[str, int]:
        """Convert all HTML files in a directory to Akoma Ntoso XML.

        Args:
            input_dir: Directory containing HTML files
            output_dir: Directory for output XML files

        Returns:
            Dictionary with counts: {"total": N, "success": N, "failed": N}
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        stats = {"total": 0, "success": 0, "failed": 0}

        for html_file in input_dir.glob("statutes_cite_*.html"):
            stats["total"] += 1

            try:
                citation = self.parse_citation_from_filename(html_file.name)
                output_file = output_dir / f"mn_statute_{citation}.xml"

                self.convert_file(html_file, output_file)
                stats["success"] += 1

            except Exception as e:  # pragma: no cover
                stats["failed"] += 1  # pragma: no cover
                print(f"Error converting {html_file.name}: {e}")  # pragma: no cover

        return stats


if __name__ == "__main__":
    import sys

    # Default paths
    input_dir = Path("/Users/maxghenis/TheAxiomFoundation/atlas/data/statutes/us-mn")
    output_dir = Path("/tmp/rules-us-mn-akn")

    converter = MNStatutesToAKN()
    stats = converter.convert_directory(input_dir, output_dir)

    print(f"\nConversion complete:")
    print(f"  Total files: {stats['total']}")
    print(f"  Success: {stats['success']}")
    print(f"  Failed: {stats['failed']}")
    print(f"\nOutput directory: {output_dir}")
