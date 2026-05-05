import zipfile
from datetime import date
from xml.sax.saxutils import escape

import pytest
import requests

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.states import (
    _date_text,
    _load_nebraska_html,
    _nebraska_chapter_filter,
    _nebraska_chapter_from_href,
    _nebraska_chapter_link_heading,
    _nebraska_href_to_citation_path,
    _nebraska_section_from_href,
    _nebraska_section_heading_parts,
    _non_file_url,
    _parse_nebraska_chapter_sections,
    _parse_nebraska_chapters,
    _parse_state_html_sections,
    _parse_washington_chapter_sections,
    _parse_washington_chapters,
    _parse_washington_titles,
    _split_state_html_last_hyphen,
    _split_state_html_prefixed_section,
    _split_state_html_triplet,
    _state_html_converter,
    _state_html_parse_args,
    _state_html_parse_context,
    _state_html_section_identity,
    _state_html_section_metadata,
    _state_html_section_number,
    _state_html_to_section_args,
    _state_html_to_sections,
    extract_california_codes_bulk,
    extract_cic_html_release,
    extract_cic_odt_release,
    extract_colorado_docx_release,
    extract_dc_code,
    extract_minnesota_statutes,
    extract_nebraska_revised_statutes,
    extract_ohio_revised_code,
    extract_state_html_directory,
    extract_texas_tcas,
    extract_washington_rcw,
    state_run_id,
)
from axiom_corpus.models import Citation as LegacyCitation
from axiom_corpus.models import Section as LegacySection

SAMPLE_DC_INDEX = """<?xml version="1.0" encoding="utf-8"?>
<container xmlns="https://code.dccouncil.us/schemas/dc-library"
           xmlns:xi="http://www.w3.org/2001/XInclude"
           enacted="true">
  <prefix>Title</prefix>
  <num>1</num>
  <heading>Government Organization.</heading>
  <container>
    <prefix>Chapter</prefix>
    <num>1</num>
    <heading>General Provisions.</heading>
    <xi:include href="./sections/1-101.xml"/>
  </container>
</container>
"""


SAMPLE_DC_SECTION = """<?xml version="1.0" encoding="utf-8"?>
<section xmlns="https://code.dccouncil.us/schemas/dc-library">
  <num>1-101</num>
  <heading>Short title.</heading>
  <text>This chapter may be cited as the District Charter.</text>
  <para>
    <num>(a)</num>
    <text>See <cite path="§1-102">section 1-102</cite>.</text>
  </para>
  <annotations>
    <text type="History">Law 1-1.</text>
  </annotations>
</section>
"""


SAMPLE_CIC_HTML = """<!DOCTYPE html>
<html>
<body>
  <nav><h1><span>Title 67<br/>Taxes and Licenses</span></h1></nav>
  <main>
    <div>
      <h2 id="t67c01"><span>Chapter 1<br/>General Provisions</span></h2>
      <div>
        <h3 id="t67c01s67-1-101"><span>67-1-101. Short title.</span></h3>
        <p>This part may be cited as the Revenue Act.</p>
        <p>Cross-References. See <cite><a href="#t67c01s67-1-102">67-1-102</a></cite>.</p>
      </div>
    </div>
  </main>
</body>
</html>
"""

SAMPLE_LOCAL_AL_HTML = """<!DOCTYPE html>
<html>
<head><title>Code of Alabama 40-18-1</title></head>
<body>
<h1>Code of Alabama 1975</h1>
<h2>Title 40 - Revenue and Taxation</h2>
<h3>Chapter 18 - Income Tax</h3>
<div class="content">
<p><b>Section 40-18-1 Definitions.</b></p>
<p>For the purpose of this chapter, the following terms shall have the meanings
respectively ascribed to them by this section:</p>
<p>(1) PERSON. Includes an individual, trust, estate, partnership, corporation,
or other entity.</p>
<p>(Acts 1935, No. 194, p. 256.)</p>
</div>
</body>
</html>
"""

SAMPLE_CIC_ODT_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<office:document-content
    xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
    xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0">
  <office:body>
    <office:text>
      <text:p text:style-name="P1">Title 8.01. Civil Remedies and Procedure.</text:p>
      <text:p text:style-name="P2">Chapter 1. General Provisions.</text:p>
      <text:p text:style-name="P2">§ 8.01-1. Short title.</text:p>
      <text:p text:style-name="P3">Chapter 1. General Provisions.</text:p>
      <text:p text:style-name="P4">§ 8.01-1. Short title.</text:p>
      <text:p text:style-name="P5">Text</text:p>
      <text:p text:style-name="P6">This title may be cited as the Civil Remedies Act.</text:p>
      <text:p text:style-name="P5">History</text:p>
      <text:p text:style-name="P6">Acts 2026, c. 1.</text:p>
    </office:text>
  </office:body>
</office:document-content>
"""


SAMPLE_COLORADO_PARAGRAPHS = [
    "Colorado Revised Statutes 2025",
    "TITLE 26",
    "HUMAN SERVICES CODE",
    "ARTICLE 1",
    "Department of Human Services",
    "PART 1",
    "GENERAL PROVISIONS",
    "26-1-101. Short title. This title shall be known as the human services code.",
    "Source: L. 2025: Entire section added.",
    "26-1-102. Legislative declaration. (1) The general assembly declares the policy.",
]


SAMPLE_COLORADO_SUPPLEMENT_PARAGRAPHS = [
    "Colorado Revised Statutes 2025",
    "TITLE 39",
    "Taxation",
    "ARTICLE 22",
    "Income Tax",
    "39-22-516.8. Credit. (1) The credit is allowed.",
    "(2) [Insert 39-22-516.8(2)(b).pdf here]",
]


SAMPLE_COLORADO_VARIANT_PARAGRAPHS = [
    "Colorado Revised Statutes 2025",
    "TITLE 1",
    "ELECTIONS",
    "ARTICLE 7",
    "General Election",
    "1-7-1001.3. Definitions. [Editor's note: This version of this section is effective until March 1, 2026.] Current version.",
    "1-7-1001.3. Definitions. [Editor's note: This version of this section is effective March 1, 2026.] Future version.",
    "1-7-1002. Fees. A table row follows.",
    "1-7-1002.5 (4) 12.00",
    "Final paragraph.",
]


SAMPLE_OHIO_INDEX = """<!DOCTYPE html>
<html>
<body>
  <main>
    <a href="/ohio-revised-code/general-provisions">General Provisions</a>
    <a href="/ohio-revised-code/title-51">Title 51 | Public Welfare</a>
  </main>
</body>
</html>
"""


SAMPLE_OHIO_TITLE = """<!DOCTYPE html>
<html>
<body>
  <main>
    <h1>Title 51 | Public Welfare</h1>
    <a href="/ohio-revised-code/chapter-5160">Chapter 5160 | Medical Assistance Programs</a>
  </main>
</body>
</html>
"""


SAMPLE_OHIO_CHAPTER = """<!DOCTYPE html>
<html>
<body>
  <main>
    <h1>Chapter 5160 | Medical Assistance Programs</h1>
    <div class="list-content">
      <span class="content-head">
        <span class="content-head-text">
          <a href="section-5160.01">Section 5160.01 <span>|</span> Definitions.</a>
        </span>
      </span>
      <div class="content-body">
        <div class="laws-section-info">
          <div class="laws-section-info-module">
            <div class="label">Effective:</div>
            <div class="value">September 29, 2013</div>
          </div>
          <div class="laws-section-info-module">
            <div class="label">Latest Legislation: </div>
            <div class="value">House Bill 59 - 130th General Assembly</div>
          </div>
          <div class="laws-section-info-module no-print">
            <div class="label">PDF:</div>
            <div class="value"><a href="/assets/laws/revised-code/authenticated/51/5160/5160.01.pdf">Download Authenticated PDF</a></div>
          </div>
        </div>
        <section class="laws-body">
          <span>
            <p>As used in this chapter:</p>
            <p>(A) "Medicaid managed care organization" has the same meaning as in section <a class="section-link" href="/ohio-revised-code/section-5167.01">5167.01</a> of the Revised Code.</p>
          </span>
        </section>
      </div>
    </div>
    <div class="list-content">
      <span class="content-head">
        <span class="content-head-text">
          <a href="section-5160.011">Section 5160.011 <span>|</span> References to department.</a>
        </span>
      </span>
      <div class="content-body">
        <div class="laws-section-info">
          <div class="laws-section-info-module">
            <div class="label">Effective:</div>
            <div class="value">January 1, 2025</div>
          </div>
        </div>
        <section class="laws-body">
          <span>
            <p>References are deemed to refer to the department of medicaid.</p>
          </span>
          <div class="laws-notice"><p>Last updated January 1, 2025 at 5:35 AM</p></div>
        </section>
      </div>
    </div>
  </main>
</body>
</html>
"""


SAMPLE_MINNESOTA_INDEX = """<!DOCTYPE html>
<html>
<body>
  <table>
    <tr>
      <td><a href="/statutes/part/PUBLIC+WELFARE+AND+RELATED+ACTIVITIES">245 - 256B</a></td>
      <td>PUBLIC WELFARE AND RELATED ACTIVITIES</td>
    </tr>
  </table>
</body>
</html>
"""


SAMPLE_MINNESOTA_PART = """<!DOCTYPE html>
<html>
<body>
  <table>
    <tr>
      <td><a href="https://www.revisor.mn.gov/statutes/cite/256B">256B</a></td>
      <td>Medical Assistance</td>
    </tr>
  </table>
</body>
</html>
"""


SAMPLE_MINNESOTA_CHAPTER = """<!DOCTYPE html>
<html>
<body>
  <div id="xtend" class="statute">
    <h2 class="chapter_title">CHAPTER 256B. MEDICAL ASSISTANCE</h2>
    <div id="chapter_analysis">
      <table>
        <tr><td><a href="#stat.256B.01">256B.01</a></td><td>DEFINITIONS.</td></tr>
        <tr><td><a href="#stat.256B.02">256B.02</a></td><td class="inactive">[Repealed]</td></tr>
      </table>
    </div>
    <div class="section" id="stat.256B.01">
      <h1 class="shn">256B.01 DEFINITIONS.</h1>
      <div class="subd" id="stat.256B.01.1">
        <a class="permalink" href="#stat.256B.01.1">§</a>
        <h2 class="subd_no">Subdivision 1.<span class="headnote">Scope.</span></h2>
        <p>Terms used in this chapter have the meanings given them.</p>
        <p>See section <a href="/statutes/cite/256B.056">256B.056</a>.</p>
      </div>
    </div>
    <div class="sr" id="stat.256B.02"><b>256B.02</b> [Repealed, 1974 c 1 s 1]</div>
  </div>
</body>
</html>
"""


SAMPLE_NEBRASKA_INDEX = """<!DOCTYPE html>
<html>
<body>
  <main>
    <a href="/laws/browse-chapters.php?chapter=01">Chapter 1 ACCOUNTANTS</a>
  </main>
</body>
</html>
"""


SAMPLE_NEBRASKA_CHAPTER = """<!DOCTYPE html>
<html>
<body>
  <div class="printwidth">
    <strong>1-101. Repealed. Laws 1957, c. 1, § 65.</strong><br/>
  </div>
  <div class="printwidth">
    <strong>1-105. Act, how cited.</strong><br/>
    <p style="text-indent: 1.5em;">Sections <a href="/laws/statutes.php?statute=1-105">1-105</a> to <a href="/laws/statutes.php?statute=1-171">1-171</a> shall be known as the Public Accountancy Act.</p>
    <div class="source"><span><strong>Source:</strong></span>Laws 1957, c. 1, § 64, p. 78; <a href="/FloorDocs/104/PDF/Slip/LB159.pdf">Laws 2015, LB159, § 1.</a></div>
  </div>
</body>
</html>
"""


SAMPLE_WASHINGTON_INDEX = """<!DOCTYPE html>
<html>
<body>
  <div id="contentWrapper">
    <table>
      <tr>
        <td><a href="default.aspx?Cite=42">Title 42</a></td>
        <td>PUBLIC OFFICERS AND AGENCIES</td>
      </tr>
    </table>
  </div>
</body>
</html>
"""


SAMPLE_WASHINGTON_TITLE = """<!DOCTYPE html>
<html>
<body>
  <h1>Title 42 RCW</h1>
  <h2>PUBLIC OFFICERS AND AGENCIES</h2>
  <div id="contentWrapper" class="title-page">
    <h3 class="list-heading">Chapters</h3>
    <table>
      <tr>
        <td><a href="http://app.leg.wa.gov/RCW/default.aspx?cite=42.56">42.56</a></td>
        <td>Public records act.</td>
      </tr>
    </table>
  </div>
</body>
</html>
"""


SAMPLE_WASHINGTON_CHAPTER = """<!DOCTYPE html>
<html>
<body>
  <h1>Chapter 42.56 RCW</h1>
  <h2>PUBLIC RECORDS ACT</h2>
  <div id="ContentPlaceHolder1_pnlExpanded">
    <span>
      <a name="42.56.010"></a>
      <div><h3><a href="default.aspx?cite=42.56.010&amp;pdf=true">PDF</a>RCW <a href="default.aspx?cite=42.56.010">42.56.010</a></h3></div>
      <div><h3>Definitions.</h3></div>
      <div>
        <div style="text-indent:0.5in;">The definitions in this section apply throughout this chapter.</div>
        <div style="text-indent:0.5in;">See RCW <a href="default.aspx?cite=42.56.020">42.56.020</a>.</div>
      </div>
      <div style="margin-top:15pt;margin-bottom:0pt;">[ 2022 c 71 s 6 .]</div>
      <div style="margin-top:0.25in;margin-bottom:0.25in;"><h3>Notes:</h3></div>
      <div style="margin-bottom:0.2in;"><div>Findings: See note following RCW <a href="default.aspx?cite=28B.10.930">28B.10.930</a>.</div></div>
    </span>
    <span>
      <a name="42.56.020"></a>
      <div><h3>RCW <a href="default.aspx?cite=42.56.020">42.56.020</a></h3></div>
      <div><h3>Short title.</h3></div>
      <div><div>This chapter may be known and cited as the public records act.</div></div>
      <div style="margin-top:15pt;margin-bottom:0pt;">[ 2005 c 274 s 102 .]</div>
    </span>
  </div>
</body>
</html>
"""


SAMPLE_CALIFORNIA_SECTION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<section>
  <heading>17052. Personal exemption credit</heading>
  <content>
    <p>(a) A credit is allowed.</p>
    <p>See <a href="https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=RTC&amp;sectionNum=17053">Section 17053</a>.</p>
  </content>
</section>
"""


SAMPLE_CALIFORNIA_INACTIVE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<section>
  <heading>17053. Repealed credit</heading>
  <content><p>This section is inactive.</p></content>
</section>
"""


SAMPLE_TEXAS_HTML = """<html><body><pre xml:space="preserve">
<p class="center" style="font-weight:bold;">TAX CODE</p>
<p class="center" style="font-weight:bold;">TITLE 1. PROPERTY TAX CODE</p>
<p class="center" style="font-weight:bold;">SUBTITLE A. GENERAL PROVISIONS</p>
<p class="center" style="font-weight:bold;">CHAPTER 1. GENERAL PROVISIONS</p>
<p class="left"><a name="1.01"></a><a name="65126.55941"></a></p>
<p style="text-indent:7ex;" class="left"><a target="_blank"
 href="https://statutes.capitol.texas.gov/Docs/TX/htm/TX.1.htm#1.01"
 style="color:inherit;font-weight:bold;">Sec. 1.01.  SHORT TITLE.</a>
 This title may be cited as the Property Tax Code.</p>
<p class="left">Acts 1979, 66th Leg., ch. 841.</p>
<p class="center" style="font-weight:bold;">SUBCHAPTER A. DEFINITIONS</p>
<p class="left"><a name="1.03"></a><a name="65128.55943"></a></p>
<p style="text-indent:7ex;" class="left"><a target="_blank"
 href="https://statutes.capitol.texas.gov/Docs/TX/htm/TX.1.htm#1.03"
 style="color:inherit;font-weight:bold;">Sec. 1.03.  CONSTRUCTION OF TITLE.</a>
 The Code Construction Act (Chapter <a target="_blank"
 href="https://statutes.capitol.texas.gov/GetStatute.aspx?Code=GV&amp;Value=311">311</a>,
 Government Code) applies.</p>
</pre></body></html>
"""


def _write_odt(path, content_xml):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("mimetype", "application/vnd.oasis.opendocument.text")
        archive.writestr("content.xml", content_xml)


def _write_docx(path, paragraphs):
    body = "".join(
        f"<w:p><w:r><w:t>{escape(paragraph)}</w:t></w:r></w:p>" for paragraph in paragraphs
    )
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{body}</w:body>"
        "</w:document>"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("word/document.xml", document_xml)


def _write_pdf(path, text):
    import fitz

    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 72), text)
    document.save(path)
    document.close()


def _write_ohio_fixture(source_dir):
    title_dir = source_dir / "ohio-revised-code" / "titles"
    chapter_dir = source_dir / "ohio-revised-code" / "chapters"
    title_dir.mkdir(parents=True)
    chapter_dir.mkdir()
    (source_dir / "ohio-revised-code" / "index.html").write_text(SAMPLE_OHIO_INDEX)
    (title_dir / "title-51.html").write_text(SAMPLE_OHIO_TITLE)
    (chapter_dir / "chapter-5160.html").write_text(SAMPLE_OHIO_CHAPTER)


def _write_minnesota_fixture(source_dir):
    part_dir = source_dir / "minnesota-statutes-html" / "parts"
    chapter_dir = source_dir / "minnesota-statutes-html" / "chapters"
    part_dir.mkdir(parents=True)
    chapter_dir.mkdir()
    (source_dir / "minnesota-statutes-html" / "index.html").write_text(SAMPLE_MINNESOTA_INDEX)
    (part_dir / "public-welfare-and-related-activities.html").write_text(
        SAMPLE_MINNESOTA_PART
    )
    (chapter_dir / "chapter-256B.html").write_text(SAMPLE_MINNESOTA_CHAPTER)


def _write_nebraska_fixture(source_dir):
    chapter_dir = source_dir / "nebraska-revised-statutes-html" / "chapters"
    chapter_dir.mkdir(parents=True)
    (source_dir / "nebraska-revised-statutes-html" / "index.html").write_text(
        SAMPLE_NEBRASKA_INDEX
    )
    (chapter_dir / "chapter-1-full.html").write_text(SAMPLE_NEBRASKA_CHAPTER)


def _write_washington_fixture(source_dir):
    title_dir = source_dir / "washington-rcw-html" / "titles"
    chapter_dir = source_dir / "washington-rcw-html" / "chapters"
    title_dir.mkdir(parents=True)
    chapter_dir.mkdir()
    (source_dir / "washington-rcw-html" / "index.html").write_text(SAMPLE_WASHINGTON_INDEX)
    (title_dir / "title-42.html").write_text(SAMPLE_WASHINGTON_TITLE)
    (chapter_dir / "chapter-42.56-full.html").write_text(SAMPLE_WASHINGTON_CHAPTER)


def _write_california_bulk_fixture(path):
    def row(*values):
        return "\t".join(values) + "\n"

    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "CODES_TBL.dat",
            row("RTC", "Revenue and Taxation Code")
            + row("WIC", "Welfare and Institutions Code"),
        )
        archive.writestr(
            "LAW_TOC_TBL.dat",
            row(
                "RTC",
                "2",
                "10",
                "10.2",
                "3",
                "2",
                "Personal Income Tax Credits",
                "Y",
                "",
                "",
                "1",
                "1",
                "1",
                "1.1",
                "Y",
                "",
                "",
                "",
                "",
            )
        )
        archive.writestr(
            "LAW_TOC_SECTIONS_TBL.dat",
            row(
                "1",
                "RTC",
                "1.1",
                "17052",
                "1",
                "Personal exemption credit",
                "",
                "",
                "",
                "",
                "",
                "1001",
                "1",
            )
        )
        archive.writestr(
            "LAW_SECTION_TBL.dat",
            row(
                "1",
                "RTC",
                "17052",
                "",
                "",
                "",
                "2025-01-01",
                "1001",
                "2",
                "10",
                "10.2",
                "3",
                "2",
                "Added by Stats. 2025.",
                "rtc-17052.xml",
                "Y",
                "",
                "",
            )
            + row(
                "2",
                "RTC",
                "17053",
                "",
                "",
                "",
                "2025-01-01",
                "1002",
                "2",
                "10",
                "10.2",
                "3",
                "2",
                "Repealed by Stats. 2025.",
                "rtc-17053.xml",
                "N",
                "",
                "",
            ),
        )
        archive.writestr("rtc-17052.xml", SAMPLE_CALIFORNIA_SECTION_XML)
        archive.writestr("rtc-17053.xml", SAMPLE_CALIFORNIA_INACTIVE_XML)


def _write_texas_fixture(source_dir):
    (source_dir / "assets").mkdir(parents=True)
    (source_dir / "trees").mkdir()
    html_dir = source_dir / "html" / "TX" / "htm"
    html_dir.mkdir(parents=True)
    (source_dir / "assets" / "StatuteCodeTree.json").write_text(
        """
{
  "StatuteCode": [
    {"codeID": "28", "code": "TX", "CodeName": "Tax Code"}
  ]
}
"""
    )
    (source_dir / "trees" / "TX.json").write_text(
        """
[
  {
    "name": "TITLE 1. PROPERTY TAX CODE",
    "value": "65122.55940",
    "valuePath": "S/28/65122.55940",
    "children": [
      {
        "name": "SUBTITLE A. GENERAL PROVISIONS",
        "value": "65123.55940",
        "valuePath": "S/28/65122.55940/65123.55940",
        "children": [
          {
            "name": "CHAPTER 1. GENERAL PROVISIONS",
            "value": "65124.55940",
            "valuePath": "S/28/65122.55940/65123.55940/65124.55940",
            "htmLink": "/TX/htm/TX.1.htm",
            "children": null
          }
        ]
      }
    ]
  }
]
"""
    )
    (html_dir / "TX.1.htm").write_text(SAMPLE_TEXAS_HTML)


def test_state_run_id_scopes_title_and_limit():
    assert (
        state_run_id("2026-04-29", jurisdiction="us-tn", only_title="067", limit=2)
        == "2026-04-29-us-tn-title-67-limit-2"
    )


def test_extract_dc_code_writes_inventory_provisions_and_coverage(tmp_path):
    source_dir = tmp_path / "dc" / "titles"
    title_dir = source_dir / "1"
    sections_dir = title_dir / "sections"
    sections_dir.mkdir(parents=True)
    (title_dir / "index.xml").write_text(SAMPLE_DC_INDEX)
    (sections_dir / "1-101.xml").write_text(SAMPLE_DC_SECTION)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_dc_code(
        store,
        version="2026-04-29",
        source_dir=source_dir,
        source_as_of="2026-04-01",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    assert report.provisions_written == 3
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert [item.citation_path for item in inventory] == [
        "us-dc/statute/1",
        "us-dc/statute/1/chapter-1",
        "us-dc/statute/1/1-101",
    ]
    assert records[-1].heading == "Short title."
    assert records[-1].parent_citation_path == "us-dc/statute/1/chapter-1"
    assert records[-1].metadata["references_to"] == ["us-dc/statute/1/1-102"]
    assert "District Charter" in records[-1].body


def test_extract_cic_html_release_writes_state_records(tmp_path):
    release_dir = tmp_path / "release76.2021.05.21"
    release_dir.mkdir()
    (release_dir / "gov.tn.tca.title.67.html").write_text(SAMPLE_CIC_HTML)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_cic_html_release(
        store,
        jurisdiction="us-tn",
        version="2021-05-21",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-tn/statute/67",
        "us-tn/statute/67/chapter-1",
        "us-tn/statute/67/67-1-101",
    ]
    assert records[-1].heading == "Short title."
    assert records[-1].source_as_of == "2021-05-21"
    assert "Revenue Act" in records[-1].body


def test_extract_cic_odt_release_writes_state_records(tmp_path):
    release_dir = tmp_path / "release90.2023.03"
    release_dir.mkdir()
    _write_odt(release_dir / "gov.va.code.title.08.01.odt", SAMPLE_CIC_ODT_CONTENT)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_cic_odt_release(
        store,
        jurisdiction="us-va",
        version="2023-03-01",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-va/statute/8.01",
        "us-va/statute/8.01/chapter-1",
        "us-va/statute/8.01/8.01-1",
    ]
    assert records[-1].heading == "Short title."
    assert records[-1].source_format == "cic-state-code-odt"
    assert records[-1].source_as_of == "2023-03-01"
    assert "Civil Remedies Act" in records[-1].body
    assert "History" not in records[-1].body
    assert "Acts 2026" in records[-1].body


def test_extract_texas_tcas_writes_state_records(tmp_path):
    source_dir = tmp_path / "texas"
    _write_texas_fixture(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_texas_tcas(
        store,
        version="2026-05-01",
        source_dir=source_dir,
        source_as_of="2026-05-01",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 5
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-tx/statute/tx",
        "us-tx/statute/tx/title-1",
        "us-tx/statute/tx/title-1/subtitle-a",
        "us-tx/statute/tx/title-1/subtitle-a/chapter-1",
        "us-tx/statute/tx/title-1/subtitle-a/chapter-1/subchapter-a",
        "us-tx/statute/tx/1.01",
        "us-tx/statute/tx/1.03",
    ]
    assert records[-1].parent_citation_path.endswith("/subchapter-a")
    assert records[-1].source_format == "texas-tcas-html"
    assert records[-1].metadata["references_to"] == ["us-tx/statute/gv/311"]
    assert "Code Construction Act" in records[-1].body


def test_extract_ohio_revised_code_writes_state_records(tmp_path):
    source_dir = tmp_path / "ohio"
    _write_ohio_fixture(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_ohio_revised_code(
        store,
        version="2026-05-01",
        source_dir=source_dir,
        source_as_of="2026-05-01",
        only_title="51",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-oh/statute/title-51",
        "us-oh/statute/chapter-5160",
        "us-oh/statute/5160.01",
        "us-oh/statute/5160.011",
    ]
    assert records[2].metadata["references_to"] == ["us-oh/statute/5167.01"]
    assert records[2].metadata["effective_date"] == "September 29, 2013"
    assert records[3].metadata["last_updated"] == "Last updated January 1, 2025 at 5:35 AM"
    assert "department of medicaid" in (records[3].body or "")


def test_extract_minnesota_statutes_writes_state_records(tmp_path):
    source_dir = tmp_path / "minnesota"
    _write_minnesota_fixture(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_minnesota_statutes(
        store,
        version="2026-05-01",
        source_dir=source_dir,
        source_as_of="2026-05-01",
        only_title="256B",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-mn/statute/part-public-welfare-and-related-activities",
        "us-mn/statute/256B",
        "us-mn/statute/256B.01",
        "us-mn/statute/256B.02",
    ]
    assert records[1].heading == "MEDICAL ASSISTANCE"
    assert records[2].heading == "DEFINITIONS"
    assert records[2].metadata["references_to"] == ["us-mn/statute/256B.056"]
    assert "Terms used in this chapter" in (records[2].body or "")
    assert records[3].metadata["status"] == "inactive"
    assert "[Repealed, 1974 c 1 s 1]" in (records[3].body or "")


def test_extract_nebraska_revised_statutes_writes_state_records(tmp_path):
    source_dir = tmp_path / "nebraska"
    _write_nebraska_fixture(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_nebraska_revised_statutes(
        store,
        version="2026-05-04",
        source_dir=source_dir,
        source_as_of="2026-05-04",
        only_title="1",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-ne/statute/1",
        "us-ne/statute/1/1-101",
        "us-ne/statute/1/1-105",
    ]
    assert records[0].heading == "ACCOUNTANTS"
    assert records[1].metadata["status"] == "repealed"
    assert records[1].body is None
    assert records[2].heading == "Act, how cited"
    assert records[2].metadata["references_to"] == ["us-ne/statute/1/1-171"]
    assert records[2].metadata["source_history"] == [
        "Laws 1957, c. 1, § 64, p. 78;",
        "Laws 2015, LB159, § 1.",
    ]
    assert "Public Accountancy Act" in (records[2].body or "")
    inventory = load_source_inventory(report.inventory_path)
    assert inventory[-1].source_format == "nebraska-revised-statutes-html"
    assert inventory[-1].source_path.endswith(
        "/nebraska-revised-statutes-html/chapters/chapter-1-full.html"
    )


def test_extract_washington_rcw_writes_state_records(tmp_path):
    source_dir = tmp_path / "washington"
    _write_washington_fixture(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_washington_rcw(
        store,
        version="2026-05-04",
        source_dir=source_dir,
        source_as_of="2026-05-04",
        only_title="42",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-wa/statute/42",
        "us-wa/statute/42/42.56",
        "us-wa/statute/42/42.56/42.56.010",
        "us-wa/statute/42/42.56/42.56.020",
    ]
    assert records[0].heading == "PUBLIC OFFICERS AND AGENCIES"
    assert records[1].heading == "PUBLIC RECORDS ACT"
    assert records[2].heading == "Definitions"
    assert records[2].metadata["references_to"] == [
        "us-wa/statute/28B/28B.10/28B.10.930",
        "us-wa/statute/42/42.56/42.56.020",
    ]
    assert records[2].metadata["source_history"] == ["[ 2022 c 71 s 6 .]"]
    assert records[2].metadata["notes"] == [
        "Findings: See note following RCW 28B.10.930 ."
    ]
    assert "definitions in this section" in (records[2].body or "")
    inventory = load_source_inventory(report.inventory_path)
    assert inventory[-1].source_format == "washington-rcw-html"
    assert inventory[-1].source_path.endswith(
        "/washington-rcw-html/chapters/chapter-42.56-full.html"
    )


def test_nebraska_helpers_cover_defensive_paths():
    chapters = _parse_nebraska_chapters(
        b"""
        <a href="/laws/browse-chapters.php?chapter=01">Chapter 1 ACCOUNTANTS</a>
        <a href="/laws/browse-chapters.php?chapter=bad">Bad</a>
        <a href="/laws/browse-chapters.php?chapter=01">Duplicate</a>
        """
    )
    assert [chapter.num for chapter in chapters] == ["1"]
    sibling_heading = _parse_nebraska_chapters(
        b"""
        <span><a href="/laws/browse-chapters.php?chapter=02">Chapter 2</a></span>
        <span>AGRICULTURE</span>
        """
    )
    assert sibling_heading[0].heading == "AGRICULTURE"
    assert _nebraska_chapter_from_href("/laws/browse-chapters.php") is None
    assert _nebraska_chapter_link_heading("Standalone heading", "1") == "Standalone heading"
    assert _nebraska_chapter_filter(None) is None
    assert _nebraska_chapter_filter("Chapter 01") == "1"
    with pytest.raises(ValueError, match="invalid Nebraska chapter filter"):
        _nebraska_chapter_filter("chapter one")

    sections = _parse_nebraska_chapter_sections(SAMPLE_NEBRASKA_CHAPTER.encode(), chapter=chapters[0])
    assert [section.section for section in sections] == ["1-101", "1-105"]
    assert sections[0].status == "repealed"
    assert sections[1].heading == "Act, how cited"
    assert _nebraska_section_from_href("/laws/statutes.php?statute=01-105.01") == "1-105.01"
    assert _nebraska_section_from_href("/laws/statutes.php?statute=77-27,187") == "77-27,187"
    assert _nebraska_section_from_href("/laws/statutes.php") is None
    assert _nebraska_href_to_citation_path("/laws/statutes.php?statute=1-171") == (
        "us-ne/statute/1/1-171"
    )
    assert _nebraska_href_to_citation_path("/laws/not-statutes.php") is None
    assert _nebraska_section_heading_parts("not a section") is None


def test_washington_helpers_cover_defensive_paths():
    titles = _parse_washington_titles(
        b"""
        <a href="default.aspx?Cite=042">Title 42</a>
        <a href="default.aspx?Cite=bad">Bad</a>
        <a href="default.aspx?Cite=42">Duplicate</a>
        """
    )
    assert [title.num for title in titles] == ["42"]
    chapters = _parse_washington_chapters(SAMPLE_WASHINGTON_TITLE.encode(), title=titles[0])
    assert [chapter.num for chapter in chapters] == ["42.56"]
    sections = _parse_washington_chapter_sections(
        SAMPLE_WASHINGTON_CHAPTER.encode(),
        chapter=chapters[0],
    )
    assert [section.section for section in sections] == ["42.56.010", "42.56.020"]
    assert sections[0].references_to == (
        "us-wa/statute/28B/28B.10/28B.10.930",
        "us-wa/statute/42/42.56/42.56.020",
    )
    ucc_titles = _parse_washington_titles(b'<a href="default.aspx?cite=62A">Title 62A</a>')
    ucc_chapters = _parse_washington_chapters(
        b"""
        <div id="contentWrapper">
          <a href="default.aspx?cite=62A.1">62A.1</a>
        </div>
        """,
        title=ucc_titles[0],
    )
    ucc_sections = _parse_washington_chapter_sections(
        b"""
        <div id="ContentPlaceHolder1_pnlExpanded">
          <span>
            <a name="62A.1-101"></a>
            <div><h3>RCW <a href="default.aspx?cite=62A.1-101">62A.1-101</a></h3></div>
            <div><h3>Short titles.</h3></div>
            <div><div>This title may be cited as the Uniform Commercial Code.</div></div>
          </span>
        </div>
        """,
        chapter=ucc_chapters[0],
    )
    assert ucc_sections[0].citation_path == "us-wa/statute/62A/62A.1/62A.1-101"


def test_nebraska_extraction_reports_source_errors(tmp_path):
    source_dir = tmp_path / "missing-chapter"
    root = source_dir / "nebraska-revised-statutes-html"
    root.mkdir(parents=True)
    (root / "index.html").write_text(SAMPLE_NEBRASKA_INDEX)

    with pytest.raises(ValueError, match="no Nebraska Revised Statutes provisions extracted"):
        extract_nebraska_revised_statutes(
            CorpusArtifactStore(tmp_path / "corpus-missing-chapter"),
            version="2026-05-04",
            source_dir=source_dir,
            only_title="1",
        )

    bad_section_dir = tmp_path / "bad-section"
    _write_nebraska_fixture(bad_section_dir)
    chapter_path = (
        bad_section_dir / "nebraska-revised-statutes-html" / "chapters" / "chapter-1-full.html"
    )
    chapter_path.write_text("<html><body><div>No statutory sections.</div></body></html>")

    report = extract_nebraska_revised_statutes(
        CorpusArtifactStore(tmp_path / "corpus-bad-section"),
        version="2026-05-04",
        source_dir=bad_section_dir,
        source_as_of="2026-05-04",
        only_title="1",
        limit=3,
    )

    assert report.section_count == 0
    assert report.errors == ("chapter 1: no sections parsed",)


def test_nebraska_live_source_helpers_cover_success_and_exhausted_retry(tmp_path, monkeypatch):
    chapters = _parse_nebraska_chapters(SAMPLE_NEBRASKA_INDEX.encode())
    source_dir = tmp_path / "source"
    _write_nebraska_fixture(source_dir)
    assert _load_nebraska_html(
        object(),
        source_dir,
        None,
        relative_name="nebraska-revised-statutes-html/chapters/chapter-1-full.html",
        url=chapters[0].full_source_url,
    ) == SAMPLE_NEBRASKA_CHAPTER.encode()

    class Response:
        status_code = 429
        content = b"retry exhausted"
        headers = {"Retry-After": "0"}

        def raise_for_status(self):
            return None

    class Session:
        def get(self, url, timeout):
            del url, timeout
            return Response()

    content = _load_nebraska_html(
        Session(),
        None,
        None,
        relative_name="nebraska-revised-statutes-html/index.html",
        url="https://example.test",
    )
    assert content == b"retry exhausted"


def test_load_nebraska_html_downloads_and_retries(tmp_path, monkeypatch):
    cached = tmp_path / "cached" / "nebraska-revised-statutes-html" / "index.html"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"<html>cached</html>")

    assert _load_nebraska_html(
        object(),
        None,
        tmp_path / "cached",
        relative_name="nebraska-revised-statutes-html/index.html",
        url="https://example.test",
    ) == b"<html>cached</html>"

    class Response:
        def __init__(self, status_code, content=b"", headers=None):
            self.status_code = status_code
            self.content = content
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400 and self.status_code != 429:
                raise requests.HTTPError(str(self.status_code))

    class Session:
        def __init__(self):
            self.responses = [
                Response(429, headers={"Retry-After": "0"}),
                Response(200, b"<html>ok</html>"),
            ]

        def get(self, url, timeout):
            del url, timeout
            return self.responses.pop(0)

    monkeypatch.setattr("axiom_corpus.corpus.states.time.sleep", lambda seconds: None)
    content = _load_nebraska_html(
        Session(),
        None,
        tmp_path,
        relative_name="nebraska-revised-statutes-html/index.html",
        url="https://example.test",
    )

    assert content == b"<html>ok</html>"
    assert (tmp_path / "nebraska-revised-statutes-html" / "index.html").read_bytes() == content


def test_extract_california_codes_bulk_writes_state_records(tmp_path):
    source_zip = tmp_path / "pubinfo_2025.zip"
    _write_california_bulk_fixture(source_zip)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_california_codes_bulk(
        store,
        version="2026-05-01",
        source_zip=source_zip,
        source_as_of="2026-04-26",
        only_title="RTC",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    assert report.skipped_source_count == 1
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-ca/statute/rtc",
        "us-ca/statute/rtc/node-1.1",
        "us-ca/statute/rtc/17052",
    ]
    assert [item.citation_path for item in inventory] == [
        "us-ca/statute/rtc",
        "us-ca/statute/rtc/node-1.1",
        "us-ca/statute/rtc/17052",
    ]
    assert records[-1].heading == "Personal exemption credit"
    assert records[-1].parent_citation_path == "us-ca/statute/rtc/node-1.1"
    assert records[-1].source_format == "california-leginfo-bulk"
    assert records[-1].source_as_of == "2026-04-26"
    assert records[-1].metadata["references_to"] == ["us-ca/statute/rtc/17053"]
    assert "A credit is allowed" in (records[-1].body or "")


def test_extract_colorado_docx_release_writes_state_records(tmp_path):
    release_dir = tmp_path / "release2025"
    docx_dir = release_dir / "docx"
    docx_dir.mkdir(parents=True)
    _write_docx(docx_dir / "crs2025-title-26.docx", SAMPLE_COLORADO_PARAGRAPHS)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_colorado_docx_release(
        store,
        version="2026-04-29",
        release_dir=release_dir,
        source_as_of="2025-09-16",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 3
    assert report.section_count == 2
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-co/statute/26",
        "us-co/statute/26/article-1",
        "us-co/statute/26/article-1/part-1",
        "us-co/statute/26/26-1-101",
        "us-co/statute/26/26-1-102",
    ]
    assert records[0].heading == "HUMAN SERVICES CODE"
    assert records[-2].heading == "Short title."
    assert records[-2].parent_citation_path == "us-co/statute/26/article-1/part-1"
    assert records[-2].source_format == "colorado-crs-docx"
    assert records[-2].source_as_of == "2025-09-16"
    assert "human services code" in records[-2].body
    assert "Source: L. 2025" in records[-2].body


def test_extract_colorado_docx_release_inlines_supplement_pdfs(tmp_path):
    release_dir = tmp_path / "release2025"
    docx_dir = release_dir / "docx"
    supplement_dir = release_dir / "supplement-pdfs" / "Title 39"
    supplement_dir.mkdir(parents=True)
    docx_dir.mkdir(parents=True)
    _write_docx(docx_dir / "crs2025-title-39.docx", SAMPLE_COLORADO_SUPPLEMENT_PARAGRAPHS)
    _write_pdf(supplement_dir / "39-22-516.8(2)(b).pdf", "Supplement table text")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_colorado_docx_release(
        store,
        version="2026-04-29",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    assert len(report.source_paths) == 2
    records = load_provisions(report.provisions_path)
    section = records[-1]
    assert section.citation_path == "us-co/statute/39/39-22-516.8"
    assert "Supplement table text" in section.body
    assert section.metadata["supplement_pdf_files"] == ["39-22-516.8(2)(b).pdf"]
    inventory = load_source_inventory(report.inventory_path)
    assert inventory[-1].metadata["supplement_pdf_files"] == ["39-22-516.8(2)(b).pdf"]


def test_extract_colorado_docx_release_keeps_effective_date_variants(tmp_path):
    release_dir = tmp_path / "release2025"
    docx_dir = release_dir / "docx"
    docx_dir.mkdir(parents=True)
    _write_docx(docx_dir / "crs2025-title-01.docx", SAMPLE_COLORADO_VARIANT_PARAGRAPHS)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_colorado_docx_release(
        store,
        version="2026-04-29",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    records = load_provisions(report.provisions_path)
    section_paths = [record.citation_path for record in records if record.kind == "section"]
    assert section_paths == [
        "us-co/statute/1/1-7-1001.3",
        "us-co/statute/1/1-7-1001.3@this-version-of-this-section-is-effective-march-1-2026",
        "us-co/statute/1/1-7-1002",
    ]
    assert "Final paragraph." in records[-1].body
    assert "1-7-1002.5 (4) 12.00" in records[-1].body
    assert records[-2].metadata["variant"] == (
        "this-version-of-this-section-is-effective-march-1-2026"
    )


def test_extract_dc_code_cli(tmp_path, capsys):
    source_dir = tmp_path / "dc" / "titles"
    title_dir = source_dir / "1"
    sections_dir = title_dir / "sections"
    sections_dir.mkdir(parents=True)
    (title_dir / "index.xml").write_text(SAMPLE_DC_INDEX)
    (sections_dir / "1-101.xml").write_text(SAMPLE_DC_SECTION)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-dc-code",
            "--base",
            str(base),
            "--version",
            "2026-04-29",
            "--source-dir",
            str(source_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-dc"' in output
    assert '"provisions_written": 3' in output


def test_extract_colorado_docx_cli(tmp_path, capsys):
    release_dir = tmp_path / "release2025"
    docx_dir = release_dir / "docx"
    docx_dir.mkdir(parents=True)
    _write_docx(docx_dir / "crs2025-title-26.docx", SAMPLE_COLORADO_PARAGRAPHS)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-colorado-docx",
            "--base",
            str(base),
            "--version",
            "2026-04-29",
            "--release-dir",
            str(release_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-co"' in output
    assert '"provisions_written": 5' in output


def test_extract_texas_tcas_cli_local_source(tmp_path, capsys):
    source_dir = tmp_path / "texas"
    _write_texas_fixture(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-texas-tcas",
            "--base",
            str(base),
            "--version",
            "2026-05-01",
            "--source-dir",
            str(source_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-tx"' in output
    assert '"provisions_written": 7' in output


def test_extract_ohio_revised_code_cli_local_source(tmp_path, capsys):
    source_dir = tmp_path / "ohio"
    _write_ohio_fixture(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-ohio-revised-code",
            "--base",
            str(base),
            "--version",
            "2026-05-01",
            "--source-dir",
            str(source_dir),
            "--only-title",
            "51",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-oh"' in output
    assert '"provisions_written": 4' in output


def test_extract_minnesota_statutes_cli_local_source(tmp_path, capsys):
    source_dir = tmp_path / "minnesota"
    _write_minnesota_fixture(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-minnesota-statutes",
            "--base",
            str(base),
            "--version",
            "2026-05-01",
            "--source-dir",
            str(source_dir),
            "--only-title",
            "256B",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-mn"' in output
    assert '"provisions_written": 4' in output


def test_extract_nebraska_revised_statutes_cli_local_source(tmp_path, capsys):
    source_dir = tmp_path / "nebraska"
    _write_nebraska_fixture(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-nebraska-revised-statutes",
            "--base",
            str(base),
            "--version",
            "2026-05-04",
            "--source-dir",
            str(source_dir),
            "--only-title",
            "1",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-ne"' in output
    assert '"provisions_written": 3' in output


def test_extract_washington_rcw_cli_local_source(tmp_path, capsys):
    source_dir = tmp_path / "washington"
    _write_washington_fixture(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-washington-rcw",
            "--base",
            str(base),
            "--version",
            "2026-05-04",
            "--source-dir",
            str(source_dir),
            "--only-title",
            "42",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-wa"' in output
    assert '"provisions_written": 4' in output


def test_extract_california_codes_cli_local_source(tmp_path, capsys):
    source_zip = tmp_path / "pubinfo_2025.zip"
    _write_california_bulk_fixture(source_zip)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-california-codes",
            "--base",
            str(base),
            "--version",
            "2026-05-01",
            "--source-zip",
            str(source_zip),
            "--only-title",
            "RTC",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-ca"' in output
    assert '"provisions_written": 3' in output


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        (
            "mgawebsite_Laws_StatuteText_article-GTG_section-10-105_enactments-false.html",
            "gtg/10-105",
        ),
        ("code-of-alabama_section-1-1-1.1.html", "1-1-1.1"),
        ("statutes.asp_01-05-006.html", "01.05.006"),
        ("viewdocument_docName-www.azleg.gov_ars_20_00259.htm.html", "20-00259"),
        ("statutes_cite_105.63.html", "105.63"),
        ("RCW_default.aspx_cite-10.89.html", "10.89"),
        ("document_statutes_100.52.html", "100.52"),
        ("laws_statutes.php_statute-44-911_print-true.html", "44-911"),
        ("Docs_AG_htm_AG.1.htm_1-001.html", "AG/1.001"),
        ("statutes_13-A_title13-Ach0sec0.html.html", "13-A-0"),
        ("Laws_GeneralLaws_PartI_TitleIX_Chapter62_Section2.html", "62-2"),
        ("legislation_ilcs_fulltext.asp_DocName-003500050K201.html", "35-5-201"),
        ("statutes_section_32_151_05828b.html", "32-151-5828b"),
        ("xcode_Title59_Chapter10_C59-10-S104_1800010118000101.html", "59-10-104"),
        ("Statutes_TITLE1_INDEX.HTM.html", "TITLE1"),
        ("code_t02c003.php.html", "2-3"),
        ("Legis_Laws_Toc.aspx_folder-1.html", "1"),
        ("rsa_html_NHTOC_NHTOC-I.htm.html", "I"),
        ("NRS_NRS-000.html.html", "000"),
        ("statutes_consolidated_view-statute_txtType-HTM_ttl-10.html", "10"),
        ("sec_custom.html", "custom"),
    ],
)
def test_state_html_section_number_patterns(filename, expected):
    assert _state_html_section_number(filename, "xx") == expected


def test_state_html_parse_context_patterns():
    base = {"html": "<html></html>", "source_url": "file:///tmp/source.html"}

    tx = _state_html_parse_context("AG/1.001", state_code="tx", filename="", **base)
    assert tx["code"] == "AG"
    assert tx["section_number"] == "1.001"

    me = _state_html_parse_context("13-A-0", state_code="me", filename="", **base)
    assert me["title"] == "13-A"
    assert me["section"] == "0"

    ma = _state_html_parse_context("62-2", state_code="ma", filename="", **base)
    assert ma["chapter"] == "62"
    assert ma["section_number"] == "2"

    md = _state_html_parse_context("gtg/10-105", state_code="md", filename="", **base)
    assert md["article_code"] == "gtg"
    assert md["section"] == "10-105"

    il = _state_html_parse_context("35-5-201", state_code="il", filename="", **base)
    assert il["chapter"] == 35
    assert il["act"] == 5
    assert il["section_number"] == "201"

    vt = _state_html_parse_context("32-151-5828b", state_code="vt", filename="", **base)
    assert vt["title"] == 32
    assert vt["chapter"] == 151
    assert vt["section"] == "5828b"

    la = _state_html_parse_context("1", state_code="la", filename="", **base)
    assert la["doc_id"] == 1

    de = _state_html_parse_context(
        "18-64",
        state_code="de",
        filename="title18_c064_index.html.html",
        **base,
    )
    assert de["title"] == 18
    assert de["chapter"] == 64

    sc = _state_html_parse_context(
        "2-3",
        state_code="sc",
        filename="code_t02c003.php.html",
        **base,
    )
    assert sc["title"] == 2
    assert sc["chapter"] == 3

    or_context = _state_html_parse_context(
        "316",
        state_code="or",
        filename="ors_316.html",
        **base,
    )
    assert or_context["chapter"] == 316

    ct = _state_html_parse_context(
        "12A",
        state_code="ct",
        filename="chapter_12A.html",
        **base,
    )
    assert ct["chapter"] == "12A"


def _legacy_section(**overrides):
    values = {
        "citation": LegacyCitation(title=0, section="AL-40-18-1"),
        "title_name": "Code of Alabama - Revenue and Taxation",
        "section_title": "Definitions",
        "text": "Definitions text.",
        "source_url": "file:///tmp/source.html",
        "retrieved_at": date(2026, 5, 4),
        "uslm_id": "al/40/18/40-18-1",
    }
    values.update(overrides)
    return LegacySection(**values)


def test_state_html_helper_defensive_paths():
    assert _date_text(date(2026, 5, 4), "fallback") == "2026-05-04"
    assert _non_file_url("file:///tmp/source.html") is None
    assert _non_file_url("https://example.test/source") == "https://example.test/source"

    with pytest.raises(ValueError, match="unsupported local state HTML converter"):
        _state_html_converter("zz")

    with pytest.raises(ValueError, match="no supported local parse method"):
        _parse_state_html_sections(
            b"<html></html>",
            filename="sec_1.html",
            state_code="al",
            converter=object(),
            source_url="file:///tmp/source.html",
        )

    def parser(soup, html, optional="fallback"):
        return soup, html, optional

    parsed_args = _state_html_parse_args(parser, {"html": "<p>Text</p>"})
    assert parsed_args[0].get_text() == "Text"
    assert parsed_args[1:] == ["<p>Text</p>", "fallback"]

    def bad_parser(required):
        return required

    with pytest.raises(ValueError, match="unsupported parser argument"):
        _state_html_parse_args(bad_parser, {"html": ""})

    section = _legacy_section()
    assert _state_html_to_sections(object(), None, {}) == ()
    assert _state_html_to_sections(object(), section, {}) == (section,)

    class Converter:
        def _to_section(self, parsed, title="ignored"):
            return parsed

    assert _state_html_to_sections(Converter(), {"one": section}, {}) == (section,)
    assert _state_html_to_sections(Converter(), [section], {}) == (section,)

    class NoToSection:
        pass

    with pytest.raises(ValueError, match="has no _to_section"):
        _state_html_to_sections(NoToSection(), object(), {})

    def converter_needs_missing(parsed, missing):
        return parsed, missing

    with pytest.raises(ValueError, match="unsupported converter argument"):
        _state_html_to_section_args(converter_needs_missing, section, {})

    for splitter, value in [
        (_split_state_html_last_hyphen, "missing"),
        (_split_state_html_prefixed_section, "missing"),
        (_split_state_html_triplet, "missing"),
    ]:
        with pytest.raises(ValueError):
            splitter(value, "xx")


def test_state_html_identity_and_metadata_fallbacks():
    identity = _state_html_section_identity(
        _legacy_section(
            citation=LegacyCitation(title=7, section="AL-Custom-1"),
            uslm_id=None,
        ),
        "us-al",
    )
    assert identity.citation_path == "us-al/statute/7/custom-1"
    assert identity.parent_citation_path == "us-al/statute/7"

    no_title = _state_html_section_identity(
        _legacy_section(
            citation=LegacyCitation(title=0, section="??"),
            uslm_id=None,
        ),
        "us-al",
    )
    assert no_title.citation_path == "us-al/statute/0"
    assert no_title.parent_citation_path is None

    metadata = _state_html_section_metadata(
        _legacy_section(
            effective_date=date(2026, 1, 1),
            public_laws=["Act 1"],
            references_to=["us-al/statute/40/40-18-2"],
        ),
        kind="section",
    )
    assert metadata["effective_date"] == "2026-01-01"
    assert metadata["public_laws"] == ["Act 1"]
    assert metadata["references_to"] == ["us-al/statute/40/40-18-2"]


def test_extract_local_state_html_writes_source_first_records(tmp_path):
    source_dir = tmp_path / "us-al"
    source_dir.mkdir()
    (source_dir / "code-of-alabama_section-40-18-1.html").write_text(SAMPLE_LOCAL_AL_HTML)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_state_html_directory(
        store,
        jurisdiction="us-al",
        version="2026-05-04-us-al-local-html-probe",
        source_dir=source_dir,
        source_as_of="2026-05-04",
        expression_date="2026-05-04",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.section_count == 1
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-al/statute/40",
        "us-al/statute/40/40-18-1",
    ]
    assert records[1].source_path.endswith(
        "/state-html/us-al/code-of-alabama_section-40-18-1.html"
    )
    assert records[1].source_format == "local-state-html-snapshot"
    inventory = load_source_inventory(report.inventory_path)
    assert [item.citation_path for item in inventory] == [
        "us-al/statute/40",
        "us-al/statute/40/40-18-1",
    ]


def test_extract_state_statutes_manifest_supports_local_state_html(tmp_path, capsys):
    source_dir = tmp_path / "us-al"
    source_dir.mkdir()
    (source_dir / "code-of-alabama_section-40-18-1.html").write_text(SAMPLE_LOCAL_AL_HTML)
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        f"""
version: "2026-05-04-local-html-smoke"
sources:
  - source_id: us-al-local-html
    jurisdiction: us-al
    document_class: statute
    adapter: local-state-html
    options:
      source_dir: {source_dir}
      source_as_of: "2026-05-04"
      expression_date: "2026-05-04"
"""
    )
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-state-statutes",
            "--base",
            str(base),
            "--manifest",
            str(manifest),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"adapter": "local-state-html"' in output
    assert '"coverage_complete": true' in output
    records = load_provisions(
        base / "provisions/us-al/statute/2026-05-04-local-html-smoke.jsonl"
    )
    assert [record.citation_path for record in records] == [
        "us-al/statute/40",
        "us-al/statute/40/40-18-1",
    ]


def test_extract_cic_state_html_cli(tmp_path, capsys):
    release_dir = tmp_path / "release76.2021.05.21"
    release_dir.mkdir()
    (release_dir / "gov.tn.tca.title.67.html").write_text(SAMPLE_CIC_HTML)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-cic-state-html",
            "--base",
            str(base),
            "--version",
            "2021-05-21",
            "--jurisdiction",
            "us-tn",
            "--release-dir",
            str(release_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-tn"' in output
    assert '"provisions_written": 3' in output


def test_extract_cic_state_odt_cli(tmp_path, capsys):
    release_dir = tmp_path / "release90.2023.03"
    release_dir.mkdir()
    _write_odt(release_dir / "gov.va.code.title.08.01.odt", SAMPLE_CIC_ODT_CONTENT)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-cic-state-odt",
            "--base",
            str(base),
            "--version",
            "2023-03-01",
            "--jurisdiction",
            "us-va",
            "--release-dir",
            str(release_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-va"' in output
    assert '"provisions_written": 3' in output
