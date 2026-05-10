from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.kansas import (
    KANSAS_CHAPTER_SOURCE_FORMAT,
    KANSAS_ROOT_SOURCE_FORMAT,
    KANSAS_SECTION_SOURCE_FORMAT,
    _RecordedSource,
    extract_kansas_statutes,
    parse_kansas_chapter_page,
    parse_kansas_root,
    parse_kansas_section_page,
)

SAMPLE_ROOT_HTML = """
<html><body>
<div class="ksa">
<ul>
  <li><a href="/statutes/ksa_ch1.html">Chapter 1.&mdash;ACCOUNTANTS; CERTIFIED PUBLIC</a></li>
</ul>
</div>
</body></html>
"""

SAMPLE_CHAPTER_HTML = """
<html><body>
<div class="ksa">
<h2>Kansas Statutes</h2>
<h2>Chapter 1.&mdash;ACCOUNTANTS; CERTIFIED PUBLIC</h2>
<ul class="handle" id="tree">
  <li>
    <a class="collapsed">Article 1.&mdash;EXAMINATION AND CERTIFICATION OF PUBLIC ACCOUNTANTS</a>
    <ul>
      <li>
        1-101 through 1-109
        <a href="/statutes/chapters/ch01/001_001_0001.html">Repealed.</a>
      </li>
    </ul>
  </li>
  <li>
    <a class="collapsed">Article 2.&mdash;STATE BOARD OF ACCOUNTANCY</a>
    <ul>
      <li>
        PUBLIC SERVICE 1-201
        <a href="/statutes/chapters/ch01/001_002_0001.html">
          Membership; appointment; qualifications; term; vacancies; removal.
        </a>
      </li>
      <li>
        1-201
        <a href="/statutes/chapters/ch01/001_002_0001.html">
          Membership; appointment; qualifications; term; vacancies; removal.
        </a>
      </li>
    </ul>
  </li>
</ul>
</div>
</body></html>
"""

SAMPLE_SECTION_HTML = """
<html><body>
<div id="print">
<p class="ksa_stat">
  <span class="stat_number">1-201.</span>
  <span class="stat_caption">Membership; appointment; qualifications; term; vacancies; removal.</span>
  (a) There is hereby created a board of accountancy under K.S.A. 74-7501.
</p>
<p class="ksa_stat">(b) Each member shall serve for a term of three years.</p>
<p class="ksa_stat_hist">
  <span class="history">History:</span>
  L. 1951, ch. 1, &sect; 1; L. 2000, ch. 81, &sect; 3; July 1.
</p>
</div>
</body></html>
"""

SAMPLE_REPEALED_RANGE_HTML = """
<html><body>
<div id="print">
<p class="ksa_stat"><span class="stat_number">1-101 through 1-109.</span></p>
<p class="ksa_stat_hist">
  <span class="history">History:</span>
  L. 1915, ch. 1, &sect;&sect; 1-9; Repealed, L. 1951, ch. 1, &sect; 24; July 1.
</p>
</div>
</body></html>
"""

SAMPLE_RESERVED_RANGE_HTML = """
<html><body>
<div id="print">
<p class="lm_stats_num_reserved">
  <span class="stat_number">79-15,147 through 79-15,200.</span>
  <span class="stat_caption">Reserved.</span>
</p>
</div>
</body></html>
"""

SAMPLE_ROOT_SOURCE = _RecordedSource(
    source_url="https://ksrevisor.gov/ksa.html",
    source_path="sources/us-ks/statute/test/ksa.html",
    source_format=KANSAS_ROOT_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_kansas_indexes_and_section_page():
    chapters = parse_kansas_root(SAMPLE_ROOT_HTML, source=SAMPLE_ROOT_SOURCE)
    assert [chapter.chapter for chapter in chapters] == ["1"]
    assert chapters[0].heading == "Accountants; Certified Public"

    chapter_source = _RecordedSource(
        source_url="https://ksrevisor.gov/statutes/ksa_ch1.html",
        source_path="sources/us-ks/statute/test/ksa_ch1.html",
        source_format=KANSAS_CHAPTER_SOURCE_FORMAT,
        sha256="def",
    )
    chapter, articles, listings = parse_kansas_chapter_page(
        SAMPLE_CHAPTER_HTML,
        root_chapter=chapters[0],
        source=chapter_source,
    )
    assert chapter.heading == "Accountants; Certified Public"
    assert [article.article for article in articles] == ["1", "2"]
    assert [listing.section_label for listing in listings] == [
        "1-101 through 1-109",
        "1-201",
    ]

    section_source = _RecordedSource(
        source_url=listings[1].source_url,
        source_path="sources/us-ks/statute/test/001_002_0001.html",
        source_format=KANSAS_SECTION_SOURCE_FORMAT,
        sha256="ghi",
    )
    section = parse_kansas_section_page(
        SAMPLE_SECTION_HTML,
        listing=listings[1],
        source=section_source,
    )
    assert section.section_label == "1-201"
    assert section.source_id == "1-201"
    assert section.heading == (
        "Membership; appointment; qualifications; term; vacancies; removal"
    )
    assert "board of accountancy" in (section.body or "")
    assert section.source_history == (
        "L. 1951, ch. 1, \u00a7 1; L. 2000, ch. 81, \u00a7 3; July 1.",
    )
    assert section.references_to == ("us-ks/statute/74-7501",)

    repealed = parse_kansas_section_page(
        SAMPLE_REPEALED_RANGE_HTML,
        listing=listings[0],
        source=section_source,
    )
    assert repealed.section_label == "1-101 through 1-109"
    assert repealed.source_id == "1-101-through-1-109"
    assert repealed.status == "repealed"

    reserved = parse_kansas_section_page(
        SAMPLE_RESERVED_RANGE_HTML,
        listing=listings[0],
        source=section_source,
    )
    assert reserved.section_label == "79-15,147 through 79-15,200"
    assert reserved.source_id == "79-15,147-through-79-15,200"
    assert reserved.heading == "Reserved"


def test_extract_kansas_statutes_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / KANSAS_ROOT_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / KANSAS_CHAPTER_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / KANSAS_SECTION_SOURCE_FORMAT / "ch01").mkdir(parents=True)
    (source_dir / KANSAS_ROOT_SOURCE_FORMAT / "ksa.html").write_text(
        SAMPLE_ROOT_HTML,
        encoding="utf-8",
    )
    (source_dir / KANSAS_CHAPTER_SOURCE_FORMAT / "ksa_ch1.html").write_text(
        SAMPLE_CHAPTER_HTML,
        encoding="utf-8",
    )
    (source_dir / KANSAS_SECTION_SOURCE_FORMAT / "ch01" / "001_001_0001.html").write_text(
        SAMPLE_REPEALED_RANGE_HTML,
        encoding="utf-8",
    )
    (source_dir / KANSAS_SECTION_SOURCE_FORMAT / "ch01" / "001_002_0001.html").write_text(
        SAMPLE_SECTION_HTML,
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_kansas_statutes(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2026-05-09",
        expression_date="2026-05-09",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 3
    assert report.section_count == 2
    assert report.provisions_written == 5
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert len(inventory) == 5
    assert [record.citation_path for record in records] == [
        "us-ks/statute/chapter-1",
        "us-ks/statute/chapter-1/article-1",
        "us-ks/statute/chapter-1/article-2",
        "us-ks/statute/1-101-through-1-109",
        "us-ks/statute/1-201",
    ]
    assert records[-1].metadata is not None
    assert records[-1].metadata["references_to"] == ["us-ks/statute/74-7501"]
