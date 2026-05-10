from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.new_mexico import (
    NEW_MEXICO_EPUB_SOURCE_FORMAT,
    NEW_MEXICO_NAV_SOURCE_FORMAT,
    NewMexicoChapterLink,
    extract_new_mexico_statutes,
    parse_new_mexico_epub,
    parse_new_mexico_navigation_page,
)

SAMPLE_NAV_HTML = """
<html><body>
  <ul class="listing">
    <li><span class="title"><a href="/nmos/nmsa/en/item/4340/index.do">Chapter 7 - Taxation</a></span></li>
    <li><span class="title"><a href="/nmos/nmsa/en/item/4358/index.do">Chapter 27 - Public Assistance</a></span></li>
  </ul>
</body></html>
"""

SAMPLE_CHAPTER = NewMexicoChapterLink(
    chapter="7",
    heading="Taxation",
    item_id="4340",
    source_url="https://nmonesource.com/nmos/nmsa/en/item/4340/index.do",
    ordinal=1,
)


def _sample_epub() -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("mimetype", "application/epub+zip")
        archive.writestr(
            "OEBPS/toc.ncx",
            """<?xml version="1.0" encoding="UTF-8"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/">
  <navMap>
    <navPoint id="navPoint-1" playOrder="1">
      <navLabel><text>ARTICLE 1 Administration</text></navLabel>
      <content src="chunk-0.html#zoupio-a1" />
      <navPoint id="navPoint-2" playOrder="2">
        <navLabel><text>7-1-1. Short title.</text></navLabel>
        <content src="chunk-0.html#zoupio-s1" />
        <navPoint id="navPoint-3" playOrder="3">
          <navLabel><text>ANNOTATIONS</text></navLabel>
          <content src="chunk-0.html#zoupio-ann1" />
        </navPoint>
      </navPoint>
      <navPoint id="navPoint-4" playOrder="4">
        <navLabel><text>7-1-2. Repealed.</text></navLabel>
        <content src="chunk-0.html#zoupio-s2" />
      </navPoint>
      <navPoint id="navPoint-5" playOrder="5">
        <navLabel><text>7-1-1. Short title. (Effective July 1, 2026.)</text></navLabel>
        <content src="chunk-0.html#zoupio-s1future" />
      </navPoint>
      <navPoint id="navPoint-6" playOrder="6">
        <navLabel><text>7-1-2.1, 7-1-2.2. Repealed.</text></navLabel>
        <content src="chunk-0.html#zoupio-s21" />
      </navPoint>
      <navPoint id="navPoint-7" playOrder="7">
        <navLabel><text>PART 2</text></navLabel>
        <content src="chunk-0.html#zoupio-part2" />
        <navPoint id="navPoint-8" playOrder="8">
          <navLabel><text>7-1-3. Nested section.</text></navLabel>
          <content src="chunk-0.html#zoupio-s3" />
        </navPoint>
      </navPoint>
    </navPoint>
  </navMap>
</ncx>
""",
        )
        archive.writestr(
            "OEBPS/chunk-0.html",
            """<!DOCTYPE html>
<html><body>
<p class="Title1">CHAPTER 7<br />Taxation</p>
<h1><a id="zoupio-a1" name="zoupio-a1">ARTICLE 1<br />Administration</a></h1>
<h5><a id="zoupio-s1" name="zoupio-s1"></a><a id="7-1-1" name="7-1-1">7-1-1. Short title.</a></h5>
<p class="text"><span class="statutes">Chapter 7, Article 1 NMSA 1978 may be cited as the Tax Administration Act.</span></p>
<p class="text"><span class="statutes">See <span data-qweri-anchor="7-1-2"><a href="/nmos/nmsa/en/item/4340/index.do#!b/7-1-2">7-1-2</a></span> NMSA 1978.</span></p>
<p class="history"><span><b>History:</b> Laws 1965, ch. 248, s. 1.</span></p>
<h6><a id="zoupio-ann1" name="zoupio-ann1">ANNOTATIONS</a></h6>
<p class="annotations">This annotation should not enter body text.</p>
<h5><a id="zoupio-s2" name="zoupio-s2"></a><a id="7-1-2" name="7-1-2">7-1-2. Repealed.</a></h5>
<h6>ANNOTATIONS</h6>
<p class="annotations">Repeals note.</p>
<h5><a id="zoupio-s1future" name="zoupio-s1future">7-1-1. Short title. (Effective July 1, 2026.)</a></h5>
<p class="text"><span class="statutes">Chapter 7, Article 1 NMSA 1978 will be cited as the Tax Administration Act.</span></p>
<h5><a id="zoupio-s21" name="zoupio-s21">7-1-2.1, 7-1-2.2. Repealed.</a></h5>
<h6>ANNOTATIONS</h6>
<h2><a id="zoupio-part2" name="zoupio-part2">PART 2</a></h2>
<h5><a id="zoupio-s3" name="zoupio-s3">7-1-3. Nested section.</a></h5>
<p class="text"><span class="statutes">Nested sections under ePub part headings are extracted.</span></p>
</body></html>
""",
        )
    return buffer.getvalue()


def test_parse_new_mexico_navigation_page_extracts_chapters():
    chapters = parse_new_mexico_navigation_page(SAMPLE_NAV_HTML)

    assert [chapter.chapter for chapter in chapters] == ["7", "27"]
    assert chapters[0].heading == "Taxation"
    assert chapters[0].item_id == "4340"
    assert chapters[0].citation_path == "us-nm/statute/chapter-7"


def test_parse_new_mexico_epub_extracts_articles_sections_and_excludes_annotations():
    parsed = parse_new_mexico_epub(_sample_epub(), chapter=SAMPLE_CHAPTER)

    assert [article.article for article in parsed.articles] == ["1"]
    assert parsed.articles[0].citation_path == "us-nm/statute/chapter-7/article-1"
    assert [section.source_id for section in parsed.sections] == [
        "7-1-1",
        "7-1-2",
        "7-1-1@effective-2026-07-01",
        "7-1-2.1",
        "7-1-2.2",
        "7-1-3",
    ]
    assert parsed.sections[0].body is not None
    assert "Tax Administration Act" in parsed.sections[0].body
    assert "annotation should not enter" not in parsed.sections[0].body
    assert parsed.sections[0].references_to == ("us-nm/statute/7-1-2",)
    assert parsed.sections[0].source_history == ("History: Laws 1965, ch. 248, s. 1.",)
    assert parsed.sections[1].status == "repealed"
    assert parsed.sections[2].citation_path == "us-nm/statute/7-1-1@effective-2026-07-01"
    assert parsed.sections[2].canonical_citation_path == "us-nm/statute/7-1-1"
    assert parsed.sections[2].effective_note == "Effective July 1, 2026."
    assert parsed.sections[2].status == "future_or_conditional"
    assert parsed.sections[3].section_group == ("7-1-2.1", "7-1-2.2")


def test_extract_new_mexico_statutes_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / NEW_MEXICO_NAV_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / NEW_MEXICO_EPUB_SOURCE_FORMAT / "chapter-7").mkdir(parents=True)
    (source_dir / NEW_MEXICO_NAV_SOURCE_FORMAT / "nav_date-page-1.html").write_text(
        SAMPLE_NAV_HTML,
        encoding="utf-8",
    )
    (source_dir / NEW_MEXICO_EPUB_SOURCE_FORMAT / "chapter-7" / "n-4340-en.epub").write_bytes(
        _sample_epub()
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_new_mexico_statutes(
        store,
        version="2026-05-08",
        source_dir=source_dir,
        source_as_of="2026-05-08",
        expression_date="2026-05-08",
        only_title="7",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 6
    assert report.provisions_written == 8
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[0].source_format == NEW_MEXICO_EPUB_SOURCE_FORMAT
    assert records[0].citation_path == "us-nm/statute/chapter-7"
    assert records[1].citation_path == "us-nm/statute/chapter-7/article-1"
    assert records[2].citation_path == "us-nm/statute/7-1-1"
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == ["us-nm/statute/7-1-2"]
    assert records[4].citation_path == "us-nm/statute/7-1-1@effective-2026-07-01"
    assert records[4].metadata is not None
    assert records[4].metadata["canonical_citation_path"] == "us-nm/statute/7-1-1"
    assert records[5].citation_path == "us-nm/statute/7-1-2.1"
    assert records[5].metadata is not None
    assert records[5].metadata["section_group"] == ["7-1-2.1", "7-1-2.2"]
