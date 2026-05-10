from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.wisconsin import (
    WISCONSIN_STATUTES_SOURCE_FORMAT,
    WisconsinSource,
    extract_wisconsin_publication_note,
    extract_wisconsin_statutes,
    parse_wisconsin_chapter_links,
    parse_wisconsin_chapter_page,
)

TOC_HTML = """
<html><body>
<div class="qssubhead"><span class="qstr">Updated through 2025 Wisconsin Act 103 and through all Orders of the Controlled Substances Board affecting Chapter 961 and Supreme Court Orders filed before and in effect on April 3, 2026.</span></div>
<div class="qstoc_entry"><span class="qstr"><a rel="statutes/ch. 71" href="/document/statutes/ch.%2071" title="Statutes ch. 71">71. Income and franchise taxes for state and local revenues.</a></span></div>
<div class="qstoc_entry"><span class="qstr"><a rel="statutes/ch. 72" href="/document/statutes/ch.%2072" title="Statutes ch. 72">72. Estate tax.</a></span></div>
</body></html>
"""

CHAPTER_HTML = """
<html><body><div id="document">
<div class="qsnum_chap" data-path="/statutes/statutes/71/title"><span class="qstr">CHAPTER 71</span></div>
<div class="qstitle_chap"><span class="qstr">INCOME AND FRANCHISE TAXES FOR STATE AND LOCAL REVENUES</span></div>
<div class="qsnum_subchap level2" data-path="/statutes/statutes/71/i" data-cites='["statutes/subch. I of ch. 71"]'><a class="reference" href="/document/statutes/subch. I of ch. 71">subch. I of ch. 71</a><span class="qstr">SUBCHAPTER I</span></div>
<div class="qstitle_subchap"><span class="qstr">TAXATION OF INDIVIDUALS AND FIDUCIARIES</span></div>
<div class="qsatxt_1sect level3" data-path="/statutes/statutes/71/i/01" data-section="71.01" data-cites='["statutes/71.01","statutes/71.01(intro.)"]'><a class="reference" href="/document/statutes/71.01">71.01</a><span class="qsnum_sect"><span class="qstr">71.01</span></span><span class="qstitle_sect"><span class="qstr">Definitions.</span></span><span class="qstr"> In this chapter in regard to natural persons:</span></div>
<div class="qsatxt_2subsect level4" data-path="/statutes/statutes/71/i/01/1" data-section="71.01" data-cites='["statutes/71.01(1)"]'><a class="reference" href="/document/statutes/71.01(1)">71.01(1)</a><span class="qsnum_subsect"><span class="qstr">(1)</span></span><span class="qstr"> &ldquo;Adjusted gross income&rdquo; has the meaning given in s. <a rel="statutes/71.02" href="/document/statutes/71.02">71.02</a>.</span></div>
<div class="qsnote_history" data-path="/statutes/statutes/71/i/01/_1" data-section="71.01" data-cites="[]"><span class="reference">71.01 History</span><span class="qstr">History: 1973 c. 147.</span></div>
</div></body></html>
"""


def test_parse_wisconsin_chapter_links_from_official_toc():
    chapters = parse_wisconsin_chapter_links(TOC_HTML)

    assert [(chapter.label, chapter.heading) for chapter in chapters] == [
        ("71", "Income and franchise taxes for state and local revenues."),
        ("72", "Estate tax."),
    ]
    assert chapters[0].source_url == "https://docs.legis.wisconsin.gov/statutes/statutes/71?view=section"
    assert chapters[0].relative_path == (
        f"{WISCONSIN_STATUTES_SOURCE_FORMAT}/statutes/statutes/71.html"
    )


def test_parse_wisconsin_publication_note():
    note = extract_wisconsin_publication_note(TOC_HTML)

    assert note is not None
    assert "2025 Wisconsin Act 103" in note
    assert "April 3, 2026" in note


def test_parse_wisconsin_chapter_page_extracts_sections_and_metadata():
    chapter = parse_wisconsin_chapter_links(TOC_HTML)[0]
    source = WisconsinSource(
        source_url=chapter.source_url,
        source_path="sources/us-wi/statute/test/wisconsin-statutes-html/statutes/statutes/71.html",
        source_format=WISCONSIN_STATUTES_SOURCE_FORMAT,
        sha256="abc",
        source_document_id="chapter-71",
    )

    subchapters, sections = parse_wisconsin_chapter_page(
        CHAPTER_HTML,
        chapter=chapter,
        source=source,
    )

    assert [(subchapter.label, subchapter.heading) for subchapter in subchapters] == [
        ("I", "TAXATION OF INDIVIDUALS AND FIDUCIARIES")
    ]
    assert len(sections) == 1
    section = sections[0]
    assert section.label == "71.01"
    assert section.heading == "Definitions."
    assert section.parent_citation_path == "us-wi/statute/chapter-71/subchapter-i"
    assert section.lines == [
        "In this chapter in regard to natural persons:",
        "(1) \u201cAdjusted gross income\u201d has the meaning given in s. 71.02.",
    ]
    assert section.history == ["History: 1973 c. 147."]
    assert section.references_to == ["us-wi/statute/71.02"]


def test_extract_wisconsin_statutes_from_source_dir(tmp_path):
    source_dir = tmp_path / "source"
    files = {
        f"{WISCONSIN_STATUTES_SOURCE_FORMAT}/statutes/prefaces/toc.html": TOC_HTML,
        f"{WISCONSIN_STATUTES_SOURCE_FORMAT}/statutes/statutes/71.html": CHAPTER_HTML,
    }
    for relative_path, text in files.items():
        path = source_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    store = CorpusArtifactStore(tmp_path / "corpus")
    report = extract_wisconsin_statutes(
        store,
        version="2026-05-10",
        source_dir=source_dir,
        source_as_of="2026-04-03",
        expression_date="2026-04-03",
        only_title="71",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 1
    assert report.provisions_written == 3
    assert len(load_source_inventory(report.inventory_path)) == 3
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-wi/statute/chapter-71",
        "us-wi/statute/chapter-71/subchapter-i",
        "us-wi/statute/71.01",
    ]
    assert records[-1].body == (
        "In this chapter in regard to natural persons:\n"
        "(1) \u201cAdjusted gross income\u201d has the meaning given in s. 71.02."
    )
    assert records[-1].metadata is not None
    assert records[-1].metadata["history"] == ["History: 1973 c. 147."]
    assert records[-1].metadata["references_to"] == ["us-wi/statute/71.02"]
