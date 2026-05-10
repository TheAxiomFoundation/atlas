from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.south_carolina import (
    SOUTH_CAROLINA_SOURCE_FORMAT,
    extract_south_carolina_code,
    parse_south_carolina_chapter_html,
    parse_south_carolina_master_index_html,
    parse_south_carolina_title_html,
)

SAMPLE_MASTER_HTML = """<!doctype html>
<html>
<body>
<a href="/code/title11.php">Title 11</a> - Public Finance</span><br />
<a href="/code/title12.php">Title 12</a> - Taxation</span><br />
</body>
</html>
"""

SAMPLE_TITLE_HTML = """<!doctype html>
<html>
<body>
<table>
<tr>
  <td>CHAPTER 6 - SOUTH CAROLINA INCOME TAX ACT</td>
  <td><a href="/code/t12c006.php">HTML</a></td>
  <td><a href="/getfile.php?TYPE=CODEOFLAWS&amp;TITLE=12&amp;CHAPTER=6">Word</a></td>
</tr>
<tr>
  <td>CHAPTER 8 - INCOME TAX WITHHOLDING</td>
  <td><a href="/code/t12c008.php">HTML</a></td>
  <td><a href="/getfile.php?TYPE=CODEOFLAWS&amp;TITLE=12&amp;CHAPTER=8">Word</a></td>
</tr>
</table>
</body>
</html>
"""

SAMPLE_CHAPTER_HTML = """<!doctype html>
<html>
<body>
<div style="text-align: center;">CHAPTER 6</div>
<div style="text-align: center;">South Carolina Income Tax Act</div><br />
<div style="text-align: center;">ARTICLE 1</div>
<div style="text-align: center;">Adoption of Internal Revenue Code-Definitions</div><br />
<span style="font-weight: bold;"> SECTION 12-6-10.</span> Short title.<br /><br />
This chapter may be cited as the "South Carolina Income Tax Act".<br /><br />
HISTORY: 1995 Act No. 76, SECTION 1.<br /><br />
<span style="font-weight: bold;"> SECTION 12-6-20.</span> Administration and enforcement of chapter.<br /><br />
The department shall administer and enforce this chapter under Section 12-6-10.<br /><br />
HISTORY: 1995 Act No. 76, SECTION 1.<br /><br />
<div style="text-align: center;">ARTICLE 5</div>
<div style="text-align: center;">Tax Rates and Imposition</div><br />
<span style="font-weight: bold;"> SECTION 12-6-510.</span> Tax rates.<br /><br />
(A) A tax is imposed at these rates:<br /><br />
<table>
<tr><th>Bracket</th><th>Rate</th></tr>
<tr><td>Not over $2,220</td><td>2.5 percent</td></tr>
</table>
HISTORY: 1995 Act No. 76, SECTION 1.<br /><br />
</body>
</html>
"""

SAMPLE_REPEALED_CHAPTER_HTML = """<!doctype html>
<html>
<body>
<div style="text-align: center;">CHAPTER 23</div>
<div style="text-align: center;">Zoning and Planning [Repealed]</div><br />
Editor's Note<br /><br />
This Chapter, which included SECTIONS 5-23-10 to 5-23-190, was repealed.<br /><br />
South Carolina Legislative Services Agency * 223 Blatt Building
</body>
</html>
"""


def test_parse_south_carolina_master_index_html_extracts_titles():
    titles = parse_south_carolina_master_index_html(SAMPLE_MASTER_HTML)

    assert [title.number for title in titles] == [11, 12]
    assert titles[1].heading == "Taxation"
    assert titles[1].citation_path == "us-sc/statute/title-12"


def test_parse_south_carolina_title_html_extracts_chapters():
    chapters = parse_south_carolina_title_html(SAMPLE_TITLE_HTML, title=12)

    assert [chapter.number for chapter in chapters] == ["6", "8"]
    assert chapters[0].heading == "South Carolina Income Tax Act"
    assert chapters[0].citation_path == "us-sc/statute/title-12/chapter-6"


def test_parse_south_carolina_title_html_handles_probate_code_articles():
    title_html = """<!doctype html>
<html><body><table><tr>
  <td>ARTICLE 1 - GENERAL PROVISIONS, DEFINITIONS, AND PROBATE JURISDICTION OF COURT</td>
  <td><a href="/code/t62c001.php">HTML</a></td>
</tr></table></body></html>
"""

    chapters = parse_south_carolina_title_html(title_html, title=62)

    assert chapters[0].number == "1"
    assert chapters[0].heading == "General Provisions, Definitions, and Probate Jurisdiction of Court"


def test_parse_south_carolina_chapter_html_extracts_sections_articles_and_refs():
    sections = parse_south_carolina_chapter_html(SAMPLE_CHAPTER_HTML, title=12, chapter=6)

    assert [section.section for section in sections] == ["12-6-10", "12-6-20", "12-6-510"]
    assert sections[0].heading == "Short title"
    assert sections[0].article == "1"
    assert sections[1].references_to == ("us-sc/statute/12-6-10",)
    assert sections[2].article == "5"
    assert sections[2].article_heading == "Tax Rates and Imposition"
    assert sections[2].body is not None
    assert "Not over $2,220 | 2.5 percent" in sections[2].body


def test_extract_south_carolina_code_preserves_repealed_chapter_note(tmp_path):
    title_html = """<!doctype html>
<html><body><table><tr>
  <td>CHAPTER 23 - ZONING AND PLANNING [REPEALED]</td>
  <td><a href="/code/t05c023.php">HTML</a></td>
</tr></table></body></html>
"""
    source_dir = tmp_path / "source"
    (source_dir / SOUTH_CAROLINA_SOURCE_FORMAT / "title-5").mkdir(parents=True)
    (source_dir / SOUTH_CAROLINA_SOURCE_FORMAT / "statmast.html").write_text(
        '<a href="/code/title5.php">Title 5</a> - Municipal Corporations',
        encoding="utf-8",
    )
    (source_dir / SOUTH_CAROLINA_SOURCE_FORMAT / "title-5.html").write_text(
        title_html,
        encoding="utf-8",
    )
    (source_dir / SOUTH_CAROLINA_SOURCE_FORMAT / "title-5" / "chapter-23.html").write_text(
        SAMPLE_REPEALED_CHAPTER_HTML,
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_south_carolina_code(
        store,
        version="2026-05-08",
        source_dir=source_dir,
        only_title=5,
        only_chapter=23,
    )

    records = load_provisions(report.provisions_path)
    assert report.errors == ()
    assert report.section_count == 0
    assert records[1].citation_path == "us-sc/statute/title-5/chapter-23"
    assert records[1].body is not None
    assert "This Chapter" in records[1].body
    assert records[1].metadata is not None
    assert records[1].metadata["status"] == "repealed"


def test_extract_south_carolina_code_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / SOUTH_CAROLINA_SOURCE_FORMAT / "title-12").mkdir(parents=True)
    (source_dir / SOUTH_CAROLINA_SOURCE_FORMAT / "statmast.html").write_text(
        SAMPLE_MASTER_HTML,
        encoding="utf-8",
    )
    (source_dir / SOUTH_CAROLINA_SOURCE_FORMAT / "title-12.html").write_text(
        SAMPLE_TITLE_HTML,
        encoding="utf-8",
    )
    (source_dir / SOUTH_CAROLINA_SOURCE_FORMAT / "title-12" / "chapter-6.html").write_text(
        SAMPLE_CHAPTER_HTML,
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_south_carolina_code(
        store,
        version="2026-05-08",
        source_dir=source_dir,
        source_as_of="2026-05-08",
        expression_date="2026-05-08",
        only_title=12,
        only_chapter=6,
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 3
    assert report.provisions_written == 5
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[0].source_format == SOUTH_CAROLINA_SOURCE_FORMAT
    assert records[0].citation_path == "us-sc/statute/title-12"
    assert records[2].citation_path == "us-sc/statute/12-6-10"
    assert records[2].source_path is not None
    assert records[2].source_path.endswith(
        "/south-carolina-code-html/title-12/chapter-6.html"
    )
    assert records[3].metadata is not None
    assert records[3].metadata["references_to"] == ["us-sc/statute/12-6-10"]
