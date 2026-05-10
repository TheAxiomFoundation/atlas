import json

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.west_virginia import (
    WEST_VIRGINIA_ARTICLE_SOURCE_FORMAT,
    WEST_VIRGINIA_INDEX_SOURCE_FORMAT,
    extract_west_virginia_code,
    parse_west_virginia_article_sections_json,
    parse_west_virginia_code_index,
)

SAMPLE_INDEX_HTML = """<!doctype html>
<html><body><div id="wrapper">
  <h1><a href="https://code.wvlegislature.gov/2/">CHAPTER 2. COMMON LAW, STATUTES, LEGAL HOLIDAYS, DEFINITIONS AND LEGAL CAPACITY.</a></h1>
  <h2><a href="https://code.wvlegislature.gov/2-2/">CHAPTER 2, ARTICLE 2. LEGAL HOLIDAYS; SPECIAL MEMORIAL DAYS; CONSTRUCTION OF STATUTES; DEFINITIONS.</a></h2>
  <h3><a href="https://code.wvlegislature.gov/2-2-1/">§2-2-1. Legal holidays; official acts or court proceedings.</a></h3>
  <h3><a href="https://code.wvlegislature.gov/2-2-1A/">§2-2-1a. Special memorial days.</a></h3>
  <h3><a href="https://code.wvlegislature.gov/2-2-1B/">§2-2-1b. Repealed. Acts, 1982 Reg. Sess., Ch. 76.</a></h3>
</div></body></html>
"""

SAMPLE_ARTICLE_JSON = {
    "html": """
<h4>§2-2-1. Legal holidays; official acts or court proceedings.</h4>
<p>(a) The following days are legal holidays.</p>
<p>(b) See §2-2-1a for special memorial days.</p>
<h4>§2-2-1a. Special memorial days.</h4>
<p>June 20 is West Virginia Day.</p>
<h4>§2-2-1b. Repealed. Acts, 1982 Reg. Sess., Ch. 76.</h4>
"""
}


def test_parse_west_virginia_code_index_extracts_hierarchy():
    index = parse_west_virginia_code_index(SAMPLE_INDEX_HTML)

    assert [chapter.chapter for chapter in index.chapters] == ["2"]
    assert index.chapters[0].citation_path == "us-wv/statute/chapter-2"
    assert [article.article for article in index.articles] == ["2"]
    assert index.articles[0].citation_path == "us-wv/statute/chapter-2/article-2"
    assert [section.section for section in index.sections] == ["2-2-1", "2-2-1A", "2-2-1B"]
    assert index.sections[1].heading == "Special memorial days"


def test_parse_west_virginia_article_sections_json_extracts_bodies_refs_and_status():
    sections = parse_west_virginia_article_sections_json(json.dumps(SAMPLE_ARTICLE_JSON))

    assert [section.section for section in sections] == ["2-2-1", "2-2-1A", "2-2-1B"]
    assert sections[0].body is not None
    assert "legal holidays" in sections[0].body
    assert sections[0].references_to == ("us-wv/statute/2-2-1A",)
    assert sections[2].status == "repealed"


def test_parse_west_virginia_article_sections_json_handles_repealed_paragraphs_and_ranges():
    payload = {
        "html": """
<p>§4-4-1 to 4-4-3.</p><p>Repealed.</p><p>Acts, 1991 Reg. Sess., Ch. 71.</p>
<p>§5-1C-1.</p><p>Repealed.</p><p>Acts, 2003 Reg. Sess., Ch. 197.</p>
"""
    }

    sections = parse_west_virginia_article_sections_json(json.dumps(payload))

    assert [section.section for section in sections] == [
        "4-4-1",
        "4-4-2",
        "4-4-3",
        "5-1C-1",
    ]
    assert sections[0].body == "Repealed.\nActs, 1991 Reg. Sess., Ch. 71."
    assert sections[0].status == "repealed"
    assert sections[3].body == "Repealed.\nActs, 2003 Reg. Sess., Ch. 197."


def test_extract_west_virginia_code_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / WEST_VIRGINIA_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / WEST_VIRGINIA_ARTICLE_SOURCE_FORMAT / "chapter-2").mkdir(parents=True)
    (source_dir / WEST_VIRGINIA_INDEX_SOURCE_FORMAT / "wvcodeentire.html").write_text(
        SAMPLE_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / WEST_VIRGINIA_ARTICLE_SOURCE_FORMAT / "chapter-2" / "article-2.json").write_text(
        json.dumps(SAMPLE_ARTICLE_JSON),
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_west_virginia_code(
        store,
        version="2026-05-08",
        source_dir=source_dir,
        source_as_of="2026-05-08",
        expression_date="2026-05-08",
        only_chapter=2,
        only_article=2,
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 3
    assert report.provisions_written == 5
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[0].source_format == WEST_VIRGINIA_INDEX_SOURCE_FORMAT
    assert records[0].citation_path == "us-wv/statute/chapter-2"
    assert records[1].citation_path == "us-wv/statute/chapter-2/article-2"
    assert records[2].citation_path == "us-wv/statute/2-2-1"
    assert records[2].source_path is not None
    assert records[2].source_path.endswith(
        "/west-virginia-code-article-json/chapter-2/article-2.json"
    )
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == ["us-wv/statute/2-2-1A"]
