from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.idaho import (
    IDAHO_CHAPTER_SOURCE_FORMAT,
    IDAHO_SECTION_SOURCE_FORMAT,
    IDAHO_TITLE_INDEX_SOURCE_FORMAT,
    IDAHO_TITLE_SOURCE_FORMAT,
    _RecordedSource,
    extract_idaho_statutes,
    parse_idaho_chapter_page,
    parse_idaho_section_page,
    parse_idaho_title_index,
    parse_idaho_title_page,
)

SAMPLE_TITLE_INDEX_HTML = """
<html><body>
  <table>
    <tr>
      <td><a href="/statutesrules/idstat/Title63">TITLE 63</a></td>
      <td>&#160;&#160;</td>
      <td> REVENUE AND TAXATION </td>
    </tr>
  </table>
</body></html>
"""

SAMPLE_TITLE_HTML = """
<html><body>
  <h1 class="lso-toc"><center>TITLE 63 REVENUE AND TAXATION</center></h1>
  <table>
    <tr>
      <td><a href="/statutesrules/idstat/Title63/T63CH30">CHAPTER 30</a></td>
      <td>&#160;&#160;</td>
      <td> INCOME TAX </td>
      <td>&#160;&#160;</td>
      <td><a href="/wp-content/uploads/statutesrules/idstat/Title63/T63CH30.pdf">Download Entire Chapter (PDF)</a></td>
    </tr>
    <tr>
      <td>CHAPTER 31</td>
      <td>&#160;&#160;</td>
      <td> OLD TAX ACT [REPEALED] </td>
      <td>&#160;&#160;</td>
      <td>&#160;&#160;</td>
    </tr>
  </table>
</body></html>
"""

SAMPLE_CHAPTER_HTML = """
<html><body>
  <h1 class="lso-toc"><center>TITLE 63 REVENUE AND TAXATION</center></h1>
  <h2 class="lso-toc"><center>CHAPTER 30 INCOME TAX</center></h2>
  <table>
    <tr>
      <td><a href="/statutesrules/idstat/Title63/T63CH30/SECT63-3002">63-3002</a></td>
      <td>&#160;&#160;</td>
      <td> DECLARATION OF INTENT. </td>
    </tr>
  </table>
</body></html>
"""

SAMPLE_SECTION_HTML = """
<html><body>
  <div class="pgbrk">
    <div style="line-height: 12pt; text-align: center"><span style="font-family: Courier New;">TITLE 63</span></div>
    <div style="line-height: 12pt; text-align: center"><span style="font-family: Courier New;">REVENUE AND TAXATION</span></div>
    <div style="line-height: 12pt; text-align: center"><span style="font-family: Courier New;">CHAPTER 30</span></div>
    <div style="line-height: 12pt; text-align: center"><span style="font-family: Courier New;">INCOME TAX</span></div>
    <div style="line-height: 12pt; text-align: justify"><span style="font-family: Courier New;">63-3002. <span style="text-transform: uppercase">Declaration of intent.</span> The income tax act applies with section 63-3003.</span></div>
    <div style="line-height: 12pt; text-align: justify"><span style="font-family: Courier New;">History:</span></div>
    <div style="line-height: 12pt; text-align: justify"><span style="font-family: Courier New;">[63-3002, added 1959, ch. 299, sec. 2, p. 613.]</span></div>
  </div>
</body></html>
"""

SAMPLE_RECORDED = _RecordedSource(
    source_url="https://legislature.idaho.gov/statutesrules/idstat/",
    source_path="sources/us-id/statute/test/index.html",
    source_format=IDAHO_TITLE_INDEX_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_idaho_indexes_and_section_page():
    titles = parse_idaho_title_index(SAMPLE_TITLE_INDEX_HTML, source=SAMPLE_RECORDED)

    assert [title.number for title in titles] == ["63"]
    assert titles[0].heading == "REVENUE AND TAXATION"
    title_source = _RecordedSource(
        source_url=titles[0].source_url,
        source_path="sources/us-id/statute/test/title.html",
        source_format=IDAHO_TITLE_SOURCE_FORMAT,
        sha256="def",
    )
    chapters = parse_idaho_title_page(SAMPLE_TITLE_HTML, title=titles[0], source=title_source)

    assert [chapter.chapter for chapter in chapters] == ["30", "31"]
    assert chapters[0].pdf_url == (
        "https://legislature.idaho.gov/wp-content/uploads/statutesrules/idstat/"
        "Title63/T63CH30.pdf"
    )
    assert chapters[1].active is False
    assert chapters[1].status == "repealed"

    sections = parse_idaho_chapter_page(SAMPLE_CHAPTER_HTML, chapter=chapters[0])
    assert [section.section for section in sections] == ["63-3002"]
    section_source = _RecordedSource(
        source_url=sections[0].source_url,
        source_path="sources/us-id/statute/test/63-3002.html",
        source_format=IDAHO_SECTION_SOURCE_FORMAT,
        sha256="ghi",
    )
    parsed = parse_idaho_section_page(
        SAMPLE_SECTION_HTML,
        listing=sections[0],
        source=section_source,
    )

    assert parsed.heading == "Declaration of intent"
    assert parsed.body == "The income tax act applies with section 63-3003."
    assert parsed.source_history == ("[63-3002, added 1959, ch. 299, sec. 2, p. 613.]",)
    assert parsed.references_to == ("us-id/statute/63-3003",)


def test_extract_idaho_statutes_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / IDAHO_TITLE_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / IDAHO_TITLE_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / IDAHO_CHAPTER_SOURCE_FORMAT / "title-63").mkdir(parents=True)
    (source_dir / IDAHO_SECTION_SOURCE_FORMAT / "title-63" / "chapter-30").mkdir(parents=True)
    (source_dir / IDAHO_TITLE_INDEX_SOURCE_FORMAT / "index.html").write_text(
        SAMPLE_TITLE_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / IDAHO_TITLE_SOURCE_FORMAT / "title-63.html").write_text(
        SAMPLE_TITLE_HTML,
        encoding="utf-8",
    )
    (source_dir / IDAHO_CHAPTER_SOURCE_FORMAT / "title-63" / "chapter-30.html").write_text(
        SAMPLE_CHAPTER_HTML,
        encoding="utf-8",
    )
    (
        source_dir
        / IDAHO_SECTION_SOURCE_FORMAT
        / "title-63"
        / "chapter-30"
        / "63-3002.html"
    ).write_text(SAMPLE_SECTION_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_idaho_statutes(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2025-07-01",
        expression_date="2025-07-01",
        only_title="63",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 3
    assert report.section_count == 1
    assert report.provisions_written == 4
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert len(inventory) == 4
    assert [record.citation_path for record in records] == [
        "us-id/statute/title-63",
        "us-id/statute/title-63/chapter-30",
        "us-id/statute/63-3002",
        "us-id/statute/title-63/chapter-31",
    ]
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == ["us-id/statute/63-3003"]
    assert records[3].metadata is not None
    assert records[3].metadata["status"] == "repealed"
