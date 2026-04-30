import zipfile
from xml.sax.saxutils import escape

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.states import (
    extract_cic_html_release,
    extract_cic_odt_release,
    extract_colorado_docx_release,
    extract_dc_code,
    state_run_id,
)

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
