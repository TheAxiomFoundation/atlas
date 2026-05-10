from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.arizona import (
    ARIZONA_SECTION_SOURCE_FORMAT,
    ARIZONA_TITLE_DETAIL_SOURCE_FORMAT,
    ARIZONA_TITLE_INDEX_SOURCE_FORMAT,
    ArizonaTitle,
    extract_arizona_revised_statutes,
    parse_arizona_section,
    parse_arizona_title_detail,
    parse_arizona_title_index,
)

SAMPLE_TITLE_INDEX_HTML = """
<table id="arsTable">
  <tr>
    <td><input value="1"></td>
    <td><a href="https://www.azleg.gov/arsDetail?title=1">Title 1</a></td>
    <td>General Provision</td>
  </tr>
  <tr>
    <td><input value="2"></td>
    <td>Title 2</td>
    <td>THIS TITLE HAS BEEN REPEALED</td>
  </tr>
</table>
"""

SAMPLE_TITLE_DETAIL_HTML = """
<div id="chapter2" class="accordion">
  <h5>
    <a class="one-sixth first" href="">Chapter 2</a>
    <div class="two-thirds">COMMON LAW, STATUTES AND RULES OF CONSTRUCTION</div>
    <div class="one-sixth">Sec: 1-201-1-219</div>
  </h5>
  <div>
    <div class="article">
      <a class="one-sixth first" href="">Article 1</a>
      <span class="five-sixths">Common Law</span>
      <div>
        <ul>
          <li class="colleft">
            <a class="stat" href="/viewdocument/?docName=https://www.azleg.gov/ars/1/00201.htm">1-201</a>
          </li>
          <li class="colright">Adoption of common law; exceptions</li>
        </ul>
      </div>
    </div>
  </div>
</div>
"""

SAMPLE_SECTION_HTML = """
<HTML>
<HEAD><TITLE>1-201 - Adoption of common law; exceptions</TITLE></HEAD>
<BODY>
<p><font color=GREEN>1-201</font>. <font color=PURPLE><u>Adoption of common law; exceptions</u></font></p>
<p>The common law is adopted. See section 1-202.</p>
</BODY>
</HTML>
"""

SAMPLE_TITLE = ArizonaTitle(
    number="1",
    heading="General Provision",
    source_url="https://www.azleg.gov/arsDetail?title=1",
    ordinal=1,
)


def test_parse_arizona_title_index_extracts_titles():
    titles = parse_arizona_title_index(SAMPLE_TITLE_INDEX_HTML)

    assert [title.number for title in titles] == ["1", "2"]
    assert titles[0].heading == "General Provision"
    assert titles[0].citation_path == "us-az/statute/title-1"
    assert titles[1].repealed is True
    assert titles[1].source_url is None


def test_parse_arizona_title_detail_and_section():
    document = parse_arizona_title_detail(SAMPLE_TITLE_DETAIL_HTML, title=SAMPLE_TITLE)

    assert [chapter.chapter for chapter in document.chapters] == ["2"]
    assert document.chapters[0].section_range == "1-201-1-219"
    assert [article.article for article in document.articles] == ["1"]
    assert [section.section for section in document.sections] == ["1-201"]
    assert document.sections[0].source_url == "https://www.azleg.gov/ars/1/00201.htm"

    parsed = parse_arizona_section(SAMPLE_SECTION_HTML)

    assert parsed.heading == "Adoption of common law; exceptions"
    assert parsed.body == "The common law is adopted. See section 1-202."
    assert parsed.references_to == ("us-az/statute/1-202",)


def test_extract_arizona_revised_statutes_from_source_dir_writes_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / ARIZONA_TITLE_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / ARIZONA_TITLE_DETAIL_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / ARIZONA_SECTION_SOURCE_FORMAT / "title-1").mkdir(parents=True)
    (source_dir / ARIZONA_TITLE_INDEX_SOURCE_FORMAT / "index.html").write_text(
        SAMPLE_TITLE_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / ARIZONA_TITLE_DETAIL_SOURCE_FORMAT / "title-1.html").write_text(
        SAMPLE_TITLE_DETAIL_HTML,
        encoding="utf-8",
    )
    (source_dir / ARIZONA_SECTION_SOURCE_FORMAT / "title-1" / "1-201.html").write_text(
        SAMPLE_SECTION_HTML,
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_arizona_revised_statutes(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2026-05-09",
        expression_date="2026-05-09",
        only_title="1",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    assert report.provisions_written == 4
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert len(inventory) == 4
    assert [record.citation_path for record in records] == [
        "us-az/statute/title-1",
        "us-az/statute/title-1/chapter-2",
        "us-az/statute/title-1/chapter-2/article-1",
        "us-az/statute/1-201",
    ]
    assert records[3].metadata is not None
    assert records[3].metadata["references_to"] == ["us-az/statute/1-202"]
