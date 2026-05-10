from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.new_hampshire import (
    NEW_HAMPSHIRE_CHAPTER_SOURCE_FORMAT,
    NEW_HAMPSHIRE_MERGED_CHAPTER_SOURCE_FORMAT,
    NEW_HAMPSHIRE_ROOT_SOURCE_FORMAT,
    NEW_HAMPSHIRE_TITLE_SOURCE_FORMAT,
    _RecordedSource,
    extract_new_hampshire_rsa,
    parse_new_hampshire_chapter_toc,
    parse_new_hampshire_merged_chapter,
    parse_new_hampshire_root,
    parse_new_hampshire_title_page,
)

SAMPLE_ROOT_HTML = """
<html><body>
<ul>
  <li><a href="NHTOC/NHTOC-V.htm">TITLE V: TAXATION</a></li>
  <p class="chapter_list">Chapters 71 - 84</p>
</ul>
</body></html>
"""

SAMPLE_TITLE_HTML = """
<html><body>
<ul>
  <li><a href="NHTOC-V-77-A.htm">CHAPTER 77-A: BUSINESS PROFITS TAX</a></li>
</ul>
</body></html>
"""

SAMPLE_CHAPTER_TOC_HTML = """
<html><body>
<h2><a href="../V/77-A/77-A-mrg.htm">Entire Chapter</a></h2>
<ul>
  <li><a href="../V/77-A/77-A-1.htm">Section: 77-A:1 Definitions.</a></li>
  <li><a href="../V/77-A/77-A-2.htm">Section: 77-A:2 Tax Imposed.</a></li>
</ul>
</body></html>
"""

SAMPLE_MERGED_CHAPTER_HTML = """
<html><body>
<center><h3>Section 77-A:1</h3></center>
<codesect>
  <b>77-A:1 Definitions. &#150;</b>
  For purposes of RSA 77-A:2, "gross business profits" means gross income.
</codesect>
<sourcenote>Source. 1970, 5:1.</sourcenote>
<center><h3>Section 77-A:2</h3></center>
<codesect>
  <b>77-A:2 Tax Imposed. &#150;</b>
  A tax is imposed on taxable business profits.
</codesect>
<sourcenote>Source. 1970, 5:2.</sourcenote>
</body></html>
"""

SAMPLE_ROOT_SOURCE = _RecordedSource(
    source_url="https://gc.nh.gov/rsa/html/nhtoc.htm",
    source_path="sources/us-nh/statute/test/nhtoc.htm",
    source_format=NEW_HAMPSHIRE_ROOT_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_new_hampshire_rsa_pages():
    titles = parse_new_hampshire_root(SAMPLE_ROOT_HTML, source=SAMPLE_ROOT_SOURCE)
    assert [title.title for title in titles] == ["V"]
    assert titles[0].heading == "Taxation"
    assert titles[0].chapter_range == "Chapters 71 - 84"

    chapters = parse_new_hampshire_title_page(SAMPLE_TITLE_HTML, title=titles[0])
    assert [chapter.chapter for chapter in chapters] == ["77-A"]
    assert chapters[0].heading == "Business Profits Tax"
    assert chapters[0].source_url.endswith("/NHTOC/NHTOC-V-77-A.htm")

    chapter_source = _RecordedSource(
        source_url="https://gc.nh.gov/rsa/html/NHTOC/NHTOC-V-77-A.htm",
        source_path="sources/us-nh/statute/test/NHTOC-V-77-A.htm",
        source_format=NEW_HAMPSHIRE_CHAPTER_SOURCE_FORMAT,
        sha256="def",
    )
    chapter, listings = parse_new_hampshire_chapter_toc(
        SAMPLE_CHAPTER_TOC_HTML,
        listing=chapters[0],
        source=chapter_source,
    )
    assert chapter.citation_path == "us-nh/statute/chapter-77-a"
    assert [listing.section_label for listing in listings] == ["77-A:1", "77-A:2"]
    assert listings[0].source_url == "https://gc.nh.gov/rsa/html/V/77-A/77-A-1.htm"

    merged_source = _RecordedSource(
        source_url="https://gc.nh.gov/rsa/html/V/77-A/77-A-mrg.htm",
        source_path="sources/us-nh/statute/test/77-A-mrg.htm",
        source_format=NEW_HAMPSHIRE_MERGED_CHAPTER_SOURCE_FORMAT,
        sha256="ghi",
    )
    sections = parse_new_hampshire_merged_chapter(
        SAMPLE_MERGED_CHAPTER_HTML,
        listings=listings,
        source=merged_source,
    )
    assert sections[0].citation_path == "us-nh/statute/77-a:1"
    assert "gross business profits" in (sections[0].body or "")
    assert sections[0].source_history == ("1970, 5:1.",)
    assert sections[0].references_to == ("us-nh/statute/77-a:2",)


def test_extract_new_hampshire_rsa_from_source_dir_writes_complete_artifacts(
    tmp_path,
):
    source_dir = tmp_path / "source"
    (source_dir / NEW_HAMPSHIRE_ROOT_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / NEW_HAMPSHIRE_TITLE_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / NEW_HAMPSHIRE_CHAPTER_SOURCE_FORMAT).mkdir(parents=True)
    (
        source_dir / NEW_HAMPSHIRE_MERGED_CHAPTER_SOURCE_FORMAT / "V" / "77-A"
    ).mkdir(parents=True)
    (source_dir / NEW_HAMPSHIRE_ROOT_SOURCE_FORMAT / "nhtoc.htm").write_text(
        SAMPLE_ROOT_HTML,
        encoding="utf-8",
    )
    (source_dir / NEW_HAMPSHIRE_TITLE_SOURCE_FORMAT / "NHTOC-V.htm").write_text(
        SAMPLE_TITLE_HTML,
        encoding="utf-8",
    )
    (source_dir / NEW_HAMPSHIRE_CHAPTER_SOURCE_FORMAT / "NHTOC-V-77-A.htm").write_text(
        SAMPLE_CHAPTER_TOC_HTML,
        encoding="utf-8",
    )
    (
        source_dir
        / NEW_HAMPSHIRE_MERGED_CHAPTER_SOURCE_FORMAT
        / "V"
        / "77-A"
        / "77-A-mrg.htm"
    ).write_text(SAMPLE_MERGED_CHAPTER_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_new_hampshire_rsa(
        store,
        version="2026-05-10",
        source_dir=source_dir,
        source_as_of="2026-05-10",
        expression_date="2026-05-10",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    assert report.provisions_written == 4
    assert len(load_source_inventory(report.inventory_path)) == 4
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-nh/statute/title-v",
        "us-nh/statute/chapter-77-a",
        "us-nh/statute/77-a:1",
        "us-nh/statute/77-a:2",
    ]
    assert records[-2].metadata is not None
    assert records[-2].metadata["references_to"] == ["us-nh/statute/77-a:2"]
