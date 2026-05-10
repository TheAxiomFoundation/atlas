from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.massachusetts import (
    MASSACHUSETTS_GENERAL_LAWS_SOURCE_FORMAT,
    MassachusettsChapter,
    MassachusettsTitle,
    extract_massachusetts_general_laws,
    parse_massachusetts_chapter_page,
    parse_massachusetts_chapters,
    parse_massachusetts_parts,
    parse_massachusetts_section,
    parse_massachusetts_titles,
)

SAMPLE_INDEX_HTML = """
<html>
<body>
<ul class="generalLawsList">
  <li><a href="/Laws/GeneralLaws/PartI">
    <span class="part">Part I</span>
    <span class="partTitle">ADMINISTRATION OF THE GOVERNMENT</span>
    <span class="chapters">Chapters 1-182</span>
  </a></li>
</ul>
</body>
</html>
"""

SAMPLE_PART_HTML = """
<html>
<body>
<div id="accordion" class="panel-group titlePanels">
  <div id="Ititle" class="panel panel-default">
    <div class="panel-heading">
      <div class="row">
        <div class="col-xs-2">
          <h4 class="glTitle panel-title">
            <a href="#titleIX" onclick="accordionAjaxLoad('1', '9', 'IX')">Title IX</a>
          </h4>
        </div>
        <div class="col-xs-10 col-sm-8">
          <h4 class="panel-title">
            <a href="#titleIX" onclick="accordionAjaxLoad('1', '9', 'IX')">TAXATION</a>
          </h4>
        </div>
        <div class="col-xs-12 col-sm-2">
          <span class="titleChapters">
            <a href="#titleIX" onclick="accordionAjaxLoad('1', '9', 'IX')">
              <small>Chapters</small> 58 - 65C
            </a>
          </span>
        </div>
      </div>
    </div>
    <div id="titleIX" class="panel-collapse collapse">
      <ul class="generalLawsList"></ul>
    </div>
  </div>
</div>
</body>
</html>
"""

SAMPLE_CHAPTERS_FRAGMENT = """
<div id="title" class="panel-collapse fnContentLoaded collapse">
  <ul class="generalLawsList">
    <li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62">
      <span class="chapter">Chapter 62</span>
      <span class="chapterTitle">TAXATION OF INCOMES</span>
    </a></li>
  </ul>
</div>
"""

SAMPLE_CHAPTER_HTML = """
<html>
<body>
<h2 id="skipTo" class="h3 genLawHeading hidden-print">
  Chapter 62: <small>TAXATION OF INCOMES</small>
</h2>
<ul class="generalLawsList">
  <li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section1">
    <span class="section">Section 1</span>
    <span class="sectionTitle">Definitions</span>
  </a></li>
  <li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section2">
    <span class="section">Section 2</span>
    <span class="sectionTitle">Gross income defined</span>
  </a></li>
</ul>
</body>
</html>
"""

SAMPLE_SECTION_HTML = """
<html>
<body>
<main>
  <div class="col-xs-12">
    <h2 id="skipTo" class="h3 genLawHeading hidden-print">
      Section 2: <small>Gross income defined</small>
    </h2>
    <p>Section 2. Massachusetts gross income shall mean federal gross income.</p>
    <p>A credit under section 1 of this chapter is excluded.</p>
  </div>
</main>
</body>
</html>
"""

SAMPLE_SECTION_ONE_HTML = """
<html>
<body>
<main>
  <div class="col-xs-12">
    <h2 id="skipTo" class="h3 genLawHeading hidden-print">
      Section 1: <small>Definitions</small>
    </h2>
    <p>Section 1. Terms used in section 2 of this chapter have the following meanings.</p>
  </div>
</main>
</body>
</html>
"""

SAMPLE_REPEALED_SECTION_HTML = """
<html>
<body>
<h2 id="skipTo" class="h3 genLawHeading hidden-print">
  Section 20, 21: <small>Repealed, 1966, 698, Sec. 18</small>
</h2>
<p>Repealed, 1966, 698, Sec. 18</p>
</body>
</html>
"""


def test_parse_massachusetts_index_part_title_chapter_and_section():
    parts = parse_massachusetts_parts(SAMPLE_INDEX_HTML)

    assert [part.code for part in parts] == ["I"]
    assert parts[0].citation_path == "us-ma/statute/part-i"

    titles = parse_massachusetts_titles(SAMPLE_PART_HTML, part=parts[0])

    assert [title.roman for title in titles] == ["IX"]
    assert titles[0].heading == "TAXATION"
    assert titles[0].relative_path == "ajax/GetChaptersForTitle/part-1-title-9-IX.html"

    chapters = parse_massachusetts_chapters(SAMPLE_CHAPTERS_FRAGMENT, title=titles[0])

    assert [chapter.number for chapter in chapters] == ["62"]
    assert chapters[0].citation_path == "us-ma/statute/part-i/title-ix/chapter-62"

    targets = parse_massachusetts_chapter_page(SAMPLE_CHAPTER_HTML, chapter=chapters[0])

    assert [target.section for target in targets] == ["1", "2"]
    assert targets[1].citation_path == "us-ma/statute/62/2"

    parsed = parse_massachusetts_section(SAMPLE_SECTION_HTML, target=targets[1])

    assert parsed.heading == "Gross income defined"
    assert parsed.body == (
        "Massachusetts gross income shall mean federal gross income.\n"
        "A credit under section 1 of this chapter is excluded."
    )
    assert parsed.references_to == ("us-ma/statute/62/1",)


def test_parse_massachusetts_repealed_section_status():
    title = MassachusettsTitle(
        part_code="I",
        part_heading="ADMINISTRATION OF THE GOVERNMENT",
        part_citation_path="us-ma/statute/part-i",
        roman="IX",
        title_id="9",
        part_id="1",
        heading="TAXATION",
        ordinal=1,
    )
    chapter = MassachusettsChapter(
        part_code="I",
        part_heading="ADMINISTRATION OF THE GOVERNMENT",
        title_roman="IX",
        title_heading="TAXATION",
        title_citation_path=title.citation_path,
        number="62",
        heading="TAXATION OF INCOMES",
        href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62",
        ordinal=1,
    )
    target = parse_massachusetts_chapter_page(
        """
        <ul class="generalLawsList">
          <li><a href="/Laws/GeneralLaws/PartI/TitleIX/Chapter62/Section20,%2021">
            <span class="section">Section 20, 21</span>
            <span class="sectionTitle">Repealed, 1966, 698, Sec. 18</span>
          </a></li>
        </ul>
        """,
        chapter=chapter,
    )[0]

    parsed = parse_massachusetts_section(SAMPLE_REPEALED_SECTION_HTML, target=target)

    assert target.citation_path == "us-ma/statute/62/20-21"
    assert parsed.status == "repealed"


def test_extract_massachusetts_general_laws_from_source_dir_writes_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / "pages" / "Laws" / "GeneralLaws" / "PartI" / "TitleIX" / "Chapter62").mkdir(
        parents=True
    )
    (source_dir / "ajax" / "GetChaptersForTitle").mkdir(parents=True)
    (source_dir / "pages" / "Laws" / "GeneralLaws" / "index.html").write_text(
        SAMPLE_INDEX_HTML,
        encoding="utf-8",
    )
    (source_dir / "pages" / "Laws" / "GeneralLaws" / "PartI.html").write_text(
        SAMPLE_PART_HTML,
        encoding="utf-8",
    )
    (
        source_dir / "ajax" / "GetChaptersForTitle" / "part-1-title-9-IX.html"
    ).write_text(
        SAMPLE_CHAPTERS_FRAGMENT,
        encoding="utf-8",
    )
    (
        source_dir
        / "pages"
        / "Laws"
        / "GeneralLaws"
        / "PartI"
        / "TitleIX"
        / "Chapter62.html"
    ).write_text(SAMPLE_CHAPTER_HTML, encoding="utf-8")
    (
        source_dir
        / "pages"
        / "Laws"
        / "GeneralLaws"
        / "PartI"
        / "TitleIX"
        / "Chapter62"
        / "Section1.html"
    ).write_text(SAMPLE_SECTION_ONE_HTML, encoding="utf-8")
    (
        source_dir
        / "pages"
        / "Laws"
        / "GeneralLaws"
        / "PartI"
        / "TitleIX"
        / "Chapter62"
        / "Section2.html"
    ).write_text(SAMPLE_SECTION_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_massachusetts_general_laws(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2026-01-06",
        expression_date="2026-01-06",
        only_part="I",
        only_title="IX",
        limit=1,
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    assert report.provisions_written == 4
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[0].source_format == MASSACHUSETTS_GENERAL_LAWS_SOURCE_FORMAT
    assert [record.citation_path for record in records] == [
        "us-ma/statute/part-i",
        "us-ma/statute/part-i/title-ix",
        "us-ma/statute/part-i/title-ix/chapter-62",
        "us-ma/statute/62/1",
    ]
    assert records[3].metadata is not None
    assert records[3].metadata["references_to"] == ["us-ma/statute/62/2"]
