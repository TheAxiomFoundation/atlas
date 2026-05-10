from datetime import date

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.utah import (
    UTAH_CODE_HTML_SOURCE_FORMAT,
    UTAH_CODE_XML_SOURCE_FORMAT,
    UtahSource,
    _SectionTarget,
    extract_utah_code,
    parse_utah_child_links,
    parse_utah_section_xml,
)

ROOT_HTML = """
true
<html><body><table id="childtbl">
<tr><td><a href="Title59/59.html?v=C59_1800010118000101">Title 59</a></td><td>Revenue and Taxation</td></tr>
<tr><td><a href="Title60/60.html?v=C60_2026050620261001">Title 60</a></td><td>Future Title<i><b>(Effective 10/1/2026)</b></i></td></tr>
</table></body></html>
"""

TITLE_HTML = """
true
<html><body><table id="childtbl">
<tr><td><a href="../Title59/Chapter10/59-10.html?v=C59-10_1800010118000101">Chapter 10</a></td><td>Individual Income Tax Act</td></tr>
</table></body></html>
"""

CHAPTER_HTML = """
true
<html><body><table id="childtbl">
<tr><td><a href="../../Title59/Chapter10/59-10-P1.html?v=C59-10-P1_1800010118000101">Part 1</a></td><td>Determination and Reporting of Tax Liability and Information</td></tr>
</table></body></html>
"""

PART_HTML = """
true
<html><body><table id="childtbl">
<tr><td><a href="../../Title59/Chapter10/59-10-S104.html?v=C59-10-S104_2026050620260506">Section 104</a></td><td>Tax basis -- Tax rate -- Exemption.</td></tr>
<tr><td><a href="../../Title59/Chapter10/59-10-S104.1.html?v=C59-10-S104.1_2026050620261001">Section 104.1</a></td><td>Future section.<i><b>(Effective 10/1/2026)</b></i></td></tr>
</table></body></html>
"""

SECTION_XML = """<section number="59-10-104">
<effdate>1/1/2026</effdate>
<histories><history>Amended by Chapter <modchap sess="2026GS">250</modchap>, 2026 General Session</history><modyear>2026</modyear></histories>
<catchline>Tax basis -- Tax rate -- Exemption.</catchline>
<subsection number="59-10-104(1)">A tax is imposed on the state taxable income of a resident individual.</subsection>
<subsection number="59-10-104(2)">For purposes of Subsection <xref refnumber="59-10-104(1)">(1)</xref>, the tax is:
  <subsection number="59-10-104(2)(a)">state taxable income; multiplied by</subsection>
  <subsection number="59-10-104(2)(b)">4.45%.</subsection>
</subsection>
<subsection number="59-10-104(3)">This section does not apply under Section <xref refnumber="59-10-104.1">59-10-104.1</xref>.</subsection>
</section>"""

SAMPLE_SOURCE = UtahSource(
    source_url="https://le.utah.gov/xcode/Title59/Chapter10/C59-10-S104_2026050620260506.xml",
    source_path="sources/us-ut/statute/test/utah-code-xml/Title59/Chapter10/C59-10-S104_2026050620260506.xml",
    source_format=UTAH_CODE_XML_SOURCE_FORMAT,
    sha256="abc",
    source_document_id="C59-10-S104_2026050620260506",
)


def test_parse_utah_child_links_filters_future_effective_rows():
    links = parse_utah_child_links(
        ROOT_HTML,
        source_url="https://le.utah.gov/xcode/C_1800010118000101.html",
        expression_date=date(2026, 5, 10),
    )

    assert [(link.kind, link.label, link.heading) for link in links] == [
        ("title", "59", "Revenue and Taxation")
    ]
    assert links[0].source_url == "https://le.utah.gov/xcode/Title59/C59_1800010118000101.html"
    assert links[0].relative_path == (
        f"{UTAH_CODE_HTML_SOURCE_FORMAT}/Title59/C59_1800010118000101.html"
    )


def test_parse_utah_section_xml_preserves_current_section_version():
    title_link = parse_utah_child_links(
        ROOT_HTML,
        source_url="https://le.utah.gov/xcode/C_1800010118000101.html",
        expression_date=date(2026, 5, 10),
    )[0]
    chapter_link = parse_utah_child_links(
        TITLE_HTML,
        source_url=title_link.source_url,
        expression_date=date(2026, 5, 10),
    )[0]
    part_link = parse_utah_child_links(
        CHAPTER_HTML,
        source_url=chapter_link.source_url,
        expression_date=date(2026, 5, 10),
    )[0]
    section_link = parse_utah_child_links(
        PART_HTML,
        source_url=part_link.source_url,
        expression_date=date(2026, 5, 10),
    )[0]

    target = _SectionTarget(
        link=section_link,
        title="59",
        chapter="59-10",
        part="59-10-1",
        parent_citation_path="us-ut/statute/title-59/chapter-59-10/part-59-10-1",
        level=3,
    )
    section = parse_utah_section_xml(
        SECTION_XML,
        target=target,
        source=SAMPLE_SOURCE,
        expression_date=date(2026, 5, 10),
    )

    assert section is not None
    assert section.citation_path == "us-ut/statute/59-10-104"
    assert "(2)(b) 4.45%." in (section.body or "")
    assert section.references_to == ("us-ut/statute/59-10-104.1",)
    assert section.source_history == (
        "Amended by Chapter 250, 2026 General Session",
    )


def test_parse_utah_section_xml_handles_duplicate_official_section_fragment():
    section_link = parse_utah_child_links(
        PART_HTML,
        source_url="https://le.utah.gov/xcode/Title59/Chapter10/C59-10-P1_1800010118000101.html",
        expression_date=date(2026, 5, 10),
    )[0]
    target = _SectionTarget(
        link=section_link,
        title="59",
        chapter="59-10",
        part="59-10-1",
        parent_citation_path="us-ut/statute/title-59/chapter-59-10/part-59-10-1",
        level=3,
    )
    duplicate_xml = (
        '<section number="59-10-104"><catchline>Old copy.</catchline></section>'
        + SECTION_XML
    )

    section = parse_utah_section_xml(
        duplicate_xml,
        target=target,
        source=SAMPLE_SOURCE,
        expression_date=date(2026, 5, 10),
    )

    assert section is not None
    assert section.heading == "Tax basis -- Tax rate -- Exemption."
    assert section.source_history == (
        "Amended by Chapter 250, 2026 General Session",
    )


def test_extract_utah_code_from_source_dir(tmp_path):
    source_dir = tmp_path / "source"
    files = {
        f"{UTAH_CODE_HTML_SOURCE_FORMAT}/C_1800010118000101.html": ROOT_HTML,
        f"{UTAH_CODE_XML_SOURCE_FORMAT}/C_1800010118000101.xml": "<code><catchline>Utah Code</catchline></code>",
        f"{UTAH_CODE_HTML_SOURCE_FORMAT}/Title59/C59_1800010118000101.html": TITLE_HTML,
        f"{UTAH_CODE_HTML_SOURCE_FORMAT}/Title59/Chapter10/C59-10_1800010118000101.html": CHAPTER_HTML,
        f"{UTAH_CODE_HTML_SOURCE_FORMAT}/Title59/Chapter10/C59-10-P1_1800010118000101.html": PART_HTML,
        f"{UTAH_CODE_XML_SOURCE_FORMAT}/Title59/Chapter10/C59-10-S104_2026050620260506.xml": SECTION_XML,
    }
    for relative_path, text in files.items():
        path = source_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    store = CorpusArtifactStore(tmp_path / "corpus")
    report = extract_utah_code(
        store,
        version="2026-05-10",
        source_dir=source_dir,
        source_as_of="2026-05-10",
        expression_date="2026-05-10",
        source_url="https://le.utah.gov/xcode/C_1800010118000101.html",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    assert report.provisions_written == 4
    assert len(load_source_inventory(report.inventory_path)) == 4
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-ut/statute/title-59",
        "us-ut/statute/title-59/chapter-59-10",
        "us-ut/statute/title-59/chapter-59-10/part-59-10-1",
        "us-ut/statute/59-10-104",
    ]
    assert records[-1].metadata is not None
    assert records[-1].metadata["effective_date"] == "1/1/2026"
