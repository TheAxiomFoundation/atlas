import fitz

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.iowa import (
    IOWA_CHAPTER_INDEX_SOURCE_FORMAT,
    IOWA_SECTION_INDEX_SOURCE_FORMAT,
    IOWA_SECTION_PDF_SOURCE_FORMAT,
    IOWA_TITLE_INDEX_SOURCE_FORMAT,
    IowaTitle,
    extract_iowa_code,
    parse_iowa_chapter_index,
    parse_iowa_section_index,
    parse_iowa_section_pdf,
    parse_iowa_title_index,
)

SAMPLE_TITLE_INDEX_HTML = """
<html><body>
  <table id="iacList"><tbody>
    <tr>
      <td>Title X - FINANCIAL RESOURCES (Ch. 421 - 454)</td>
      <td><a href="/law/iowaCode/chapters?title=X&amp;year=2026">Chapters</a></td>
    </tr>
  </tbody></table>
</body></html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """
<html><body>
  <table id="iacList"><tbody>
    <tr>
      <td>Chapter 422 - INDIVIDUAL INCOME, CORPORATE, AND FRANCHISE TAXES</td>
      <td><a href="/law/iowaCode/sections?codeChapter=422&amp;year=2026">Sections</a></td>
      <td><a href="/docs/code/2026/422.pdf">PDF</a></td>
      <td><a href="/docs/code/2026/422.rtf">RTF</a></td>
    </tr>
    <tr class="reservedChapterRow">
      <td>Chapter 423 - RESERVED</td>
      <td><a href="/law/iowaCode/sections?codeChapter=423&amp;year=2026">Sections</a></td>
    </tr>
  </tbody></table>
</body></html>
"""

SAMPLE_SECTION_INDEX_HTML = """
<html><body>
  <table id="iacList"><tbody>
    <tr>
      <td>&#167;422.1 - Classification of chapter.</td>
      <td><a href="/docs/code/2026/422.1.pdf">PDF</a></td>
      <td><a href="/docs/code/2026/422.1.rtf">RTF</a></td>
    </tr>
  </tbody></table>
</body></html>
"""


def _sample_pdf() -> bytes:
    document = fitz.open()
    page = document.new_page()
    page.insert_text(
        (72, 72),
        "\n".join(
            [
                "1",
                "INDIVIDUAL INCOME, CORPORATE, AND FRANCHISE TAXES, §422.1",
                "422.1 Classification of chapter.",
                "1.",
                "This chapter shall be known as the state revenue chapter.",
                "2.",
                "The department shall administer this chapter with section 422.2.",
                "[C35, §6943-f1; C39, §6943.001]",
                "Referred to in §422.2, 422.3",
                "Fri Dec 12 20:18:38 2025",
                "Iowa Code 2026, Section 422.1 (89, 2)",
            ]
        ),
        fontsize=10,
    )
    return document.tobytes()


SAMPLE_TITLE = IowaTitle(
    roman="X",
    heading="FINANCIAL RESOURCES",
    chapter_range="421 - 454",
    source_url="https://www.legis.iowa.gov/law/iowaCode/chapters?title=X&year=2026",
    ordinal=1,
)


def test_parse_iowa_title_index_extracts_titles():
    titles = parse_iowa_title_index(SAMPLE_TITLE_INDEX_HTML)

    assert [title.roman for title in titles] == ["X"]
    assert titles[0].heading == "FINANCIAL RESOURCES"
    assert titles[0].chapter_range == "421 - 454"
    assert titles[0].citation_path == "us-ia/statute/title-x"


def test_parse_iowa_chapter_and_section_indexes():
    chapters = parse_iowa_chapter_index(SAMPLE_CHAPTER_INDEX_HTML, title=SAMPLE_TITLE)

    assert [chapter.chapter for chapter in chapters] == ["422"]
    assert chapters[0].heading == "INDIVIDUAL INCOME, CORPORATE, AND FRANCHISE TAXES"
    sections = parse_iowa_section_index(SAMPLE_SECTION_INDEX_HTML, chapter=chapters[0])
    assert [section.section for section in sections] == ["422.1"]
    assert sections[0].pdf_url == "https://www.legis.iowa.gov/docs/code/2026/422.1.pdf"
    assert sections[0].citation_path == "us-ia/statute/422.1"


def test_parse_iowa_section_pdf_removes_running_text_and_extracts_notes():
    parsed = parse_iowa_section_pdf(
        _sample_pdf(),
        section="422.1",
        heading="Classification of chapter.",
        source_year=2026,
    )

    assert parsed.body is not None
    assert "state revenue chapter" in parsed.body
    assert "Iowa Code 2026" not in parsed.body
    assert parsed.source_history == ("[C35, §6943-f1; C39, §6943.001]",)
    assert parsed.source_notes == ("Referred to in §422.2, 422.3",)
    assert parsed.references_to == ("us-ia/statute/422.2", "us-ia/statute/422.3")


def test_extract_iowa_code_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / IOWA_TITLE_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / IOWA_CHAPTER_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / IOWA_SECTION_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / IOWA_SECTION_PDF_SOURCE_FORMAT / "chapter-422").mkdir(parents=True)
    (source_dir / IOWA_TITLE_INDEX_SOURCE_FORMAT / "2026.html").write_text(
        SAMPLE_TITLE_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / IOWA_CHAPTER_INDEX_SOURCE_FORMAT / "title-X.html").write_text(
        SAMPLE_CHAPTER_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / IOWA_SECTION_INDEX_SOURCE_FORMAT / "chapter-422.html").write_text(
        SAMPLE_SECTION_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / IOWA_SECTION_PDF_SOURCE_FORMAT / "chapter-422" / "422.1.pdf").write_bytes(
        _sample_pdf()
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_iowa_code(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2026-05-09",
        expression_date="2026-05-09",
        only_title="X",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 1
    assert report.provisions_written == 3
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert len(inventory) == 3
    assert [record.citation_path for record in records] == [
        "us-ia/statute/title-x",
        "us-ia/statute/chapter-422",
        "us-ia/statute/422.1",
    ]
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == [
        "us-ia/statute/422.2",
        "us-ia/statute/422.3",
    ]
