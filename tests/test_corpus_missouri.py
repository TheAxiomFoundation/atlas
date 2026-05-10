from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.missouri import (
    MISSOURI_CHAPTER_SOURCE_FORMAT,
    MISSOURI_ROOT_SOURCE_FORMAT,
    MISSOURI_SECTION_SOURCE_FORMAT,
    MISSOURI_VIEW_CHAPTER_SOURCE_FORMAT,
    _RecordedSource,
    extract_missouri_revised_statutes,
    parse_missouri_chapter_page,
    parse_missouri_root,
    parse_missouri_section_page,
    parse_missouri_view_chapter_page,
)

SAMPLE_ROOT_HTML = """
<html><body>
<details>
  <summary>
    <span class="lr-font-emph">Chs. 135-155</span>
    <span class="lr-font-emph">X TAXATION AND REVENUE</span>
  </summary>
  <div><a href="/main/OneChapter.aspx?chapter=143">143 Income Tax</a></div>
</details>
</body></html>
"""

SAMPLE_CHAPTER_HTML = """
<html><body>
<p>Title X TAXATION AND REVENUE Chapter 143 Income Tax</p>
<table>
  <tr>
    <td><a href="/main/PageSelect.aspx?section=143.011&amp;bid=51511&amp;hl=">143.011</a></td>
    <td>Resident individuals -- tax rates -- rate reductions, when. <span>(1/2/2023)</span></td>
  </tr>
</table>
</body></html>
"""

SAMPLE_SECTION_HTML = """
<html><body>
<span id="effdt">Effective - 02 Jan 2023, 6 histories</span>
<div class="norm">
  <p class="norm">
    <span class="bold">143.011. Resident individuals -- tax rates -- rate reductions, when. -- </span>
    1. A tax is hereby imposed on Missouri taxable income under section
    <a href="/main/OneSection.aspx?section=143.021">143.021</a>.
  </p>
  <p class="norm">2. The director shall publish adjusted tables.</p>
  <div class="foot">
    <p title="Footnotes follow">--------</p>
    <p class="norm">(L. 1972 S.B. 549, A.L. 2022 1st Ex. Sess. S.B. 3 &amp; 5)</p>
    <p class="norm">Effective 1-02-23</p>
  </div>
</div>
</body></html>
"""

SAMPLE_VIEW_CHAPTER_HTML = """
<html><body>
<div class="norm">
  <p class="norm">
    <span class="bold">143.011. Resident individuals -- tax rates -- rate reductions, when. -- </span>
    1. A tax is hereby imposed on Missouri taxable income under section
    <a href="/main/OneSection.aspx?section=143.021">143.021</a>.
  </p>
  <div class="foot">
    <p title="Footnotes follow">--------</p>
    <p class="norm">(L. 1972 S.B. 549, A.L. 2022 1st Ex. Sess. S.B. 3 &amp; 5)</p>
    <p class="norm">Effective 1-02-23</p>
  </div>
</div>
<p>----------------- 143.011 1/2/2023 -----------------</p>
</body></html>
"""

SAMPLE_ROOT_SOURCE = _RecordedSource(
    source_url="https://revisor.mo.gov/main/Home.aspx",
    source_path="sources/us-mo/statute/test/Home.aspx.html",
    source_format=MISSOURI_ROOT_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_missouri_root_chapter_and_section():
    titles, chapters = parse_missouri_root(SAMPLE_ROOT_HTML, source=SAMPLE_ROOT_SOURCE)
    assert [title.roman for title in titles] == ["X"]
    assert titles[0].heading == "Taxation And Revenue"
    assert [chapter.chapter for chapter in chapters] == ["143"]
    assert chapters[0].heading == "Income Tax"

    chapter_source = _RecordedSource(
        source_url="https://revisor.mo.gov/main/OneChapter.aspx?chapter=143",
        source_path="sources/us-mo/statute/test/chapter-143.html",
        source_format=MISSOURI_CHAPTER_SOURCE_FORMAT,
        sha256="def",
    )
    chapter, listings = parse_missouri_chapter_page(
        SAMPLE_CHAPTER_HTML,
        listing=chapters[0],
        source=chapter_source,
    )
    assert chapter.heading == "Income Tax"
    assert [listing.section_label for listing in listings] == ["143.011"]
    assert listings[0].effective_date == "2023-01-02"
    assert listings[0].source_url.endswith("section=143.011&bid=51511")

    section_source = _RecordedSource(
        source_url=listings[0].source_url,
        source_path="sources/us-mo/statute/test/143.011-51511.html",
        source_format=MISSOURI_SECTION_SOURCE_FORMAT,
        sha256="ghi",
    )
    section = parse_missouri_section_page(
        SAMPLE_SECTION_HTML,
        listing=listings[0],
        source=section_source,
    )
    assert section.section_label == "143.011"
    assert section.heading == "Resident individuals -- tax rates -- rate reductions, when"
    assert "Missouri taxable income" in (section.body or "")
    assert "L. 1972" in section.source_history[0]
    assert section.effective_date == "2023-01-02"
    assert section.references_to == ("us-mo/statute/143.021",)

    view_source = _RecordedSource(
        source_url="https://revisor.mo.gov/main/ViewChapter.aspx?chapter=143",
        source_path="sources/us-mo/statute/test/view-chapter-143.html",
        source_format=MISSOURI_VIEW_CHAPTER_SOURCE_FORMAT,
        sha256="jkl",
    )
    view_sections = parse_missouri_view_chapter_page(
        SAMPLE_VIEW_CHAPTER_HTML,
        listings=listings,
        source=view_source,
    )
    assert len(view_sections) == 1
    assert view_sections[0].source_path == view_source.source_path
    assert view_sections[0].source_url == listings[0].source_url
    assert view_sections[0].effective_date == "2023-01-02"


def test_extract_missouri_revised_statutes_from_source_dir_writes_complete_artifacts(
    tmp_path,
):
    source_dir = tmp_path / "source"
    (source_dir / MISSOURI_ROOT_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / MISSOURI_CHAPTER_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / MISSOURI_VIEW_CHAPTER_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / MISSOURI_SECTION_SOURCE_FORMAT / "chapter-143").mkdir(parents=True)
    (source_dir / MISSOURI_ROOT_SOURCE_FORMAT / "Home.aspx.html").write_text(
        SAMPLE_ROOT_HTML,
        encoding="utf-8",
    )
    (source_dir / MISSOURI_CHAPTER_SOURCE_FORMAT / "chapter-143.html").write_text(
        SAMPLE_CHAPTER_HTML,
        encoding="utf-8",
    )
    (source_dir / MISSOURI_VIEW_CHAPTER_SOURCE_FORMAT / "chapter-143.html").write_text(
        SAMPLE_VIEW_CHAPTER_HTML,
        encoding="utf-8",
    )
    (
        source_dir
        / MISSOURI_SECTION_SOURCE_FORMAT
        / "chapter-143"
        / "143.011-51511.html"
    ).write_text(SAMPLE_SECTION_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_missouri_revised_statutes(
        store,
        version="2026-05-10",
        source_dir=source_dir,
        source_as_of="2026-05-10",
        expression_date="2026-05-10",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    assert report.provisions_written == 3
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert len(inventory) == 3
    assert [record.citation_path for record in records] == [
        "us-mo/statute/title-x",
        "us-mo/statute/chapter-143",
        "us-mo/statute/143.011",
    ]
    assert records[-1].metadata is not None
    assert records[-1].metadata["references_to"] == ["us-mo/statute/143.021"]
