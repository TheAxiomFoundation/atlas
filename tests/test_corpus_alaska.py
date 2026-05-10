from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.alaska import (
    ALASKA_CHAPTER_PRINT_SOURCE_FORMAT,
    ALASKA_TITLE_INDEX_SOURCE_FORMAT,
    ALASKA_TITLE_TOC_SOURCE_FORMAT,
    AlaskaTitle,
    extract_alaska_statutes,
    parse_alaska_chapter_print,
    parse_alaska_title_index,
    parse_alaska_title_toc,
)

SAMPLE_TITLE_INDEX_HTML = """
<html><body>
  <ul id="TitleToc">
    <li><a onclick="loadTOC( 1);" href="javascript:void(0);">
      Title 1. General Provisions. <BR>
    </a></li>
  </ul>
</body></html>
"""

SAMPLE_TITLE_TOC_HTML = """
<a onclick=loadTOC("01"); href=javascript:void(0)>Title 1. General Provisions.</a>
<li><a onclick=loadTOC("01.05"); href=javascript:void(0)>
  <b>Chapter 05. Alaska Statutes.<BR></h6></b>
</a></li>
"""

SAMPLE_CHAPTER_PRINT_HTML = """
<div class="statute">
  <b><a name="01.05"> </a><h6>Chapter 05. Alaska Statutes.<BR></h6></b>
  <b><h7>Article 1. General Rules.</h7><BR></b>
  <b><a name="01.05.006"> </a>Sec. 01.05.006.
    Adoption of Alaska Statutes. <BR></b>
  The published laws are adopted. See AS 01.05.011.<BR><BR>
  <b><a name="01.05.011"> </a>Sec. 01.05.011.
    Designation and citation. <BR></b>
  This section may be cited as AS 01.05.011.<BR><BR>
</div>
"""

SAMPLE_TITLE = AlaskaTitle(
    number="01",
    heading="General Provisions",
    source_url="https://www.akleg.gov/basis/statutes.asp#01",
    ordinal=1,
)


def test_parse_alaska_title_index_extracts_titles():
    titles = parse_alaska_title_index(SAMPLE_TITLE_INDEX_HTML)

    assert [title.number for title in titles] == ["01"]
    assert titles[0].heading == "General Provisions"
    assert titles[0].citation_path == "us-ak/statute/title-01"


def test_parse_alaska_title_toc_and_chapter_print():
    chapters = parse_alaska_title_toc(SAMPLE_TITLE_TOC_HTML, title=SAMPLE_TITLE)

    assert [chapter.number for chapter in chapters] == ["01.05"]
    assert chapters[0].heading == "Alaska Statutes"

    document = parse_alaska_chapter_print(SAMPLE_CHAPTER_PRINT_HTML, chapter=chapters[0])

    assert len(document.articles) == 1
    assert document.articles[0].citation_path == "us-ak/statute/01.05/article-1"
    assert [section.section for section in document.sections] == [
        "01.05.006",
        "01.05.011",
    ]
    assert document.sections[0].body is not None
    assert "published laws are adopted" in document.sections[0].body
    assert document.sections[0].references_to == ("us-ak/statute/01.05.011",)


def test_extract_alaska_statutes_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / ALASKA_TITLE_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / ALASKA_TITLE_TOC_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / ALASKA_CHAPTER_PRINT_SOURCE_FORMAT / "title-01").mkdir(parents=True)
    (source_dir / ALASKA_TITLE_INDEX_SOURCE_FORMAT / "index.html").write_text(
        SAMPLE_TITLE_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / ALASKA_TITLE_TOC_SOURCE_FORMAT / "title-01.html").write_text(
        SAMPLE_TITLE_TOC_HTML,
        encoding="utf-8",
    )
    (
        source_dir
        / ALASKA_CHAPTER_PRINT_SOURCE_FORMAT
        / "title-01"
        / "chapter-01.05.html"
    ).write_text(SAMPLE_CHAPTER_PRINT_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_alaska_statutes(
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
    assert report.section_count == 2
    assert report.provisions_written == 5
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert len(inventory) == 5
    assert [record.citation_path for record in records] == [
        "us-ak/statute/title-01",
        "us-ak/statute/01.05",
        "us-ak/statute/01.05/article-1",
        "us-ak/statute/01.05.006",
        "us-ak/statute/01.05.011",
    ]
    assert records[3].metadata is not None
    assert records[3].metadata["references_to"] == ["us-ak/statute/01.05.011"]
