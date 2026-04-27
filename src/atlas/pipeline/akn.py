"""Akoma Ntoso XML conversion for statute sections."""

from datetime import date
from xml.dom import minidom
from xml.etree import ElementTree as ET

from atlas.models import Section

AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def section_to_akn_xml(section: Section, state: str) -> str:
    """Convert a Section model to Akoma Ntoso XML.

    Args:
        section: The Section object to convert
        state: Two-letter state code (e.g., 'ak', 'ny')

    Returns:
        Pretty-printed XML string in Akoma Ntoso 3.0 format
    """
    ET.register_namespace("", AKN_NS)

    section_id = (
        section.citation.section if hasattr(section.citation, "section") else str(section.citation)
    )

    # Create root
    akomaNtoso = ET.Element(f"{{{AKN_NS}}}akomaNtoso")
    act = ET.SubElement(akomaNtoso, f"{{{AKN_NS}}}act")
    act.set("name", "section")

    # Meta
    meta = ET.SubElement(act, f"{{{AKN_NS}}}meta")
    identification = ET.SubElement(meta, f"{{{AKN_NS}}}identification")
    identification.set("source", f"#{state}-legislature")

    # FRBRWork
    work = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRWork")
    work_this = ET.SubElement(work, f"{{{AKN_NS}}}FRBRthis")
    work_this.set("value", f"/akn/us-{state}/act/statute/sec-{section_id}")
    work_uri = ET.SubElement(work, f"{{{AKN_NS}}}FRBRuri")
    work_uri.set("value", f"/akn/us-{state}/act/statute/sec-{section_id}")
    work_date = ET.SubElement(work, f"{{{AKN_NS}}}FRBRdate")
    work_date.set("date", str(date.today()))
    work_date.set("name", "enacted")
    work_author = ET.SubElement(work, f"{{{AKN_NS}}}FRBRauthor")
    work_author.set("href", f"#{state}-legislature")
    work_country = ET.SubElement(work, f"{{{AKN_NS}}}FRBRcountry")
    work_country.set("value", f"us-{state}")

    # FRBRExpression
    expr = ET.SubElement(identification, f"{{{AKN_NS}}}FRBRExpression")
    expr_this = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRthis")
    expr_this.set(
        "value", f"/akn/us-{state}/act/statute/sec-{section_id}/eng@{date.today().isoformat()}"
    )
    expr_uri = ET.SubElement(expr, f"{{{AKN_NS}}}FRBRuri")
    expr_uri.set(
        "value", f"/akn/us-{state}/act/statute/sec-{section_id}/eng@{date.today().isoformat()}"
    )
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
    manif_this.set(
        "value",
        f"/akn/us-{state}/act/statute/sec-{section_id}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_uri = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRuri")
    manif_uri.set(
        "value",
        f"/akn/us-{state}/act/statute/sec-{section_id}/eng@{date.today().isoformat()}/main.xml",
    )
    manif_date = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRdate")
    manif_date.set("date", str(date.today()))
    manif_date.set("name", "generation")
    manif_author = ET.SubElement(manif, f"{{{AKN_NS}}}FRBRauthor")
    manif_author.set("href", "#axiom-foundation")

    # References
    references = ET.SubElement(meta, f"{{{AKN_NS}}}references")
    references.set("source", "#axiom-foundation")
    org = ET.SubElement(references, f"{{{AKN_NS}}}TLCOrganization")
    org.set("eId", "axiom-foundation")
    org.set("href", "https://axiom-foundation.org")
    org.set("showAs", "The Axiom Foundation")

    # Body
    body = ET.SubElement(act, f"{{{AKN_NS}}}body")
    sec_elem = ET.SubElement(body, f"{{{AKN_NS}}}section")
    sec_elem.set("eId", f"sec_{section_id.replace('.', '_').replace('-', '_')}")

    # Section number
    num = ET.SubElement(sec_elem, f"{{{AKN_NS}}}num")
    num.text = section_id

    # Section heading
    if section.section_title:
        heading = ET.SubElement(sec_elem, f"{{{AKN_NS}}}heading")
        heading.text = section.section_title

    # Content
    if section.text:
        content = ET.SubElement(sec_elem, f"{{{AKN_NS}}}content")
        for para in section.text.split("\n\n"):
            if para.strip():
                p = ET.SubElement(content, f"{{{AKN_NS}}}p")
                p.text = para.strip()[:10000]

    # Pretty print
    xml_str = ET.tostring(akomaNtoso, encoding="unicode")
    try:
        dom = minidom.parseString(xml_str)
        pretty = dom.toprettyxml(indent="  ", encoding="UTF-8")
        lines = pretty.decode("utf-8").split("\n")
        return "\n".join(line for line in lines if line.strip())
    except Exception:  # pragma: no cover
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str  # pragma: no cover
