from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.hawaii import (
    HAWAII_CHAPTER_DIRECTORY_SOURCE_FORMAT,
    HAWAII_CHAPTER_INDEX_SOURCE_FORMAT,
    HAWAII_ROOT_INDEX_SOURCE_FORMAT,
    HAWAII_SECTION_SOURCE_FORMAT,
    HAWAII_VOLUME_DIRECTORY_SOURCE_FORMAT,
    _RecordedSource,
    extract_hawaii_revised_statutes,
    parse_hawaii_chapter_directory,
    parse_hawaii_chapter_index,
    parse_hawaii_root_index,
    parse_hawaii_section_page,
    parse_hawaii_volume_directory,
)

SAMPLE_ROOT_HTML = """
<html><body><pre>
  1/6/2026 3:46 PM &lt;dir&gt;
  <a href="/hrscurrent/Vol04_Ch0201-0257/">Vol04_Ch0201-0257</a>
</pre></body></html>
"""

SAMPLE_VOLUME_HTML = """
<html><body><pre>
  1/6/2026 3:46 PM &lt;dir&gt;
  <a href="/hrscurrent/Vol04_Ch0201-0257/HRS0235/">HRS0235</a>
</pre></body></html>
"""

SAMPLE_CHAPTER_DIRECTORY_HTML = """
<html><body><pre>
  <a href="/hrscurrent/Vol04_Ch0201-0257/HRS0235/HRS_0235-.htm">HRS_0235-.htm</a>
  <a href="/hrscurrent/Vol04_Ch0201-0257/HRS0235/HRS_0235-0055_0007_0005.htm">
    HRS_0235-0055_0007_0005.htm
  </a>
</pre></body></html>
"""

SAMPLE_CHAPTER_INDEX_HTML = """
<html><body>
<div class="WordSection1">
<p class="RegularParagraphs" align="center"><b>CHAPTER 235</b></p>
<p class="RegularParagraphs" align="center"><b>INCOME TAX LAW</b></p>
<p class="RegularParagraphs">Part III. Individual Income Tax</p>
<p class="RegularParagraphs">235-55.75 Refundable earned income tax credit</p>
</div>
</body></html>
"""

SAMPLE_SECTION_HTML = """
<html><body>
<div class="WordSection1">
<p class="RegularParagraphs" align="center"><b>PART III. INDIVIDUAL INCOME TAX</b></p>
<p class="RegularParagraphs">
  <b>&sect;235-55.75 Refundable earned income tax credit.</b>
  (a) Each qualifying individual taxpayer may claim a refundable earned
  income tax credit under section 235-93.
</p>
<p class="RegularParagraphs">(b) The credit shall be claimed against net income tax liability.</p>
<p class="RegularParagraphs">[L 2017, c 107, &sect;1; am L 2023, c 163, &sect;5]</p>
<p class="XNotesHeading">Cross References</p>
<p class="XNotes">Earned income tax credit, see section 235-55.75.</p>
</div>
</body></html>
"""

SAMPLE_REPEALED_RANGE_HTML = """
<html><body>
<div class="WordSection1">
<p class="RegularParagraphs">
  <b>&sect;&sect;5-1 to 3 REPEALED.</b> L 1988, c 138, &sect;&sect;2 to 4.
</p>
</div>
</body></html>
"""

SAMPLE_NONBREAKING_HYPHEN_HTML = """
<html><body>
<div class="WordSection1">
<p class="RegularParagraphs">
  <b>[&sect;6F&#8209;7] Judiciary history center trust fund.</b>
  (a) The fund is established.
</p>
</div>
</body></html>
"""

SAMPLE_SPLIT_SUFFIX_HTML = """
<html><body>
<div class="WordSection1">
<p class="RegularParagraphs">
  <b>&sect;514B-<span>10</span>3<span> Association; registration.</span></b>
  (a) Each project or association shall register.
</p>
</div>
</body></html>
"""

SAMPLE_SPLIT_DECIMAL_HTML = """
<html><body>
<div class="WordSection1">
<p class="RegularParagraphs">
  <span>&sect;667-</span><span>5</span><span>.5 Foreclosure notice.</span>
  Notice shall be mailed.
</p>
</div>
</body></html>
"""

SAMPLE_ROOT_SOURCE = _RecordedSource(
    source_url="https://data.capitol.hawaii.gov/hrscurrent/",
    source_path="sources/us-hi/statute/test/index.html",
    source_format=HAWAII_ROOT_INDEX_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_hawaii_indexes_and_section_page():
    volumes = parse_hawaii_root_index(SAMPLE_ROOT_HTML, source=SAMPLE_ROOT_SOURCE)

    assert [volume.number for volume in volumes] == ["04"]
    assert volumes[0].heading == "Chapters 0201-0257"

    chapters = parse_hawaii_volume_directory(
        SAMPLE_VOLUME_HTML,
        volume=volumes[0],
    )
    assert [chapter.chapter for chapter in chapters] == ["235"]

    listings = parse_hawaii_chapter_directory(
        SAMPLE_CHAPTER_DIRECTORY_HTML,
        directory=chapters[0],
    )
    assert [listing.fallback_section for listing in listings] == ["235-55.75"]

    chapter_source = _RecordedSource(
        source_url="https://data.capitol.hawaii.gov/hrscurrent/Vol04_Ch0201-0257/HRS0235/HRS_0235-.htm",
        source_path="sources/us-hi/statute/test/chapter.html",
        source_format=HAWAII_CHAPTER_INDEX_SOURCE_FORMAT,
        sha256="def",
    )
    chapter = parse_hawaii_chapter_index(
        SAMPLE_CHAPTER_INDEX_HTML,
        directory=chapters[0],
        source=chapter_source,
    )
    assert chapter.heading == "Income Tax Law"

    section_source = _RecordedSource(
        source_url=listings[0].source_url,
        source_path="sources/us-hi/statute/test/section.html",
        source_format=HAWAII_SECTION_SOURCE_FORMAT,
        sha256="ghi",
    )
    section = parse_hawaii_section_page(
        SAMPLE_SECTION_HTML,
        listing=listings[0],
        source=section_source,
    )
    assert section.section == "235-55.75"
    assert section.heading == "Refundable earned income tax credit"
    assert "qualifying individual taxpayer" in (section.body or "")
    assert section.part_heading == "PART III. INDIVIDUAL INCOME TAX"
    assert section.source_history == (
        "[L 2017, c 107, \u00a71; am L 2023, c 163, \u00a75]",
    )
    assert section.references_to == ("us-hi/statute/235-93", "us-hi/statute/235-55.75")

    repealed_listing = listings[0].__class__(
        volume_number="01",
        chapter="5",
        padded_chapter="0005",
        filename="HRS_0005-0001.htm",
        source_url="https://example.test/HRS_0005-0001.htm",
        ordinal=1,
        fallback_section="5-1",
    )
    repealed = parse_hawaii_section_page(
        SAMPLE_REPEALED_RANGE_HTML,
        listing=repealed_listing,
        source=section_source,
    )
    assert repealed.section == "5-1"
    assert repealed.heading == "REPEALED"
    assert repealed.status == "repealed"

    hyphen_listing = listings[0].__class__(
        volume_number="01",
        chapter="6F",
        padded_chapter="0006F",
        filename="HRS_0006F-0007.htm",
        source_url="https://example.test/HRS_0006F-0007.htm",
        ordinal=1,
        fallback_section="6F-7",
    )
    hyphenated = parse_hawaii_section_page(
        SAMPLE_NONBREAKING_HYPHEN_HTML,
        listing=hyphen_listing,
        source=section_source,
    )
    assert hyphenated.section == "6F-7"
    assert hyphenated.heading == "Judiciary history center trust fund"

    split_suffix_listing = listings[0].__class__(
        volume_number="12",
        chapter="514B",
        padded_chapter="0514B",
        filename="HRS_0514B-0103.htm",
        source_url="https://example.test/HRS_0514B-0103.htm",
        ordinal=1,
        fallback_section="514B-103",
    )
    split_suffix = parse_hawaii_section_page(
        SAMPLE_SPLIT_SUFFIX_HTML,
        listing=split_suffix_listing,
        source=section_source,
    )
    assert split_suffix.section == "514B-103"
    assert split_suffix.heading == "Association; registration"
    assert split_suffix.body == "(a) Each project or association shall register."

    split_decimal_listing = listings[0].__class__(
        volume_number="13",
        chapter="667",
        padded_chapter="0667",
        filename="HRS_0667-0005_0005.htm",
        source_url="https://example.test/HRS_0667-0005_0005.htm",
        ordinal=1,
        fallback_section="667-5.5",
    )
    split_decimal = parse_hawaii_section_page(
        SAMPLE_SPLIT_DECIMAL_HTML,
        listing=split_decimal_listing,
        source=section_source,
    )
    assert split_decimal.section == "667-5.5"
    assert split_decimal.heading == "Foreclosure notice"


def test_extract_hawaii_revised_statutes_from_source_dir_writes_complete_artifacts(
    tmp_path,
):
    source_dir = tmp_path / "source"
    (source_dir / HAWAII_ROOT_INDEX_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / HAWAII_VOLUME_DIRECTORY_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / HAWAII_CHAPTER_DIRECTORY_SOURCE_FORMAT / "volume-04").mkdir(
        parents=True
    )
    (source_dir / HAWAII_CHAPTER_INDEX_SOURCE_FORMAT / "volume-04").mkdir(parents=True)
    (source_dir / HAWAII_SECTION_SOURCE_FORMAT / "volume-04" / "HRS0235").mkdir(
        parents=True
    )
    (source_dir / HAWAII_ROOT_INDEX_SOURCE_FORMAT / "index.html").write_text(
        SAMPLE_ROOT_HTML,
        encoding="utf-8",
    )
    (source_dir / HAWAII_VOLUME_DIRECTORY_SOURCE_FORMAT / "volume-04.html").write_text(
        SAMPLE_VOLUME_HTML,
        encoding="utf-8",
    )
    (
        source_dir
        / HAWAII_CHAPTER_DIRECTORY_SOURCE_FORMAT
        / "volume-04"
        / "HRS0235.html"
    ).write_text(SAMPLE_CHAPTER_DIRECTORY_HTML, encoding="utf-8")
    (
        source_dir
        / HAWAII_CHAPTER_INDEX_SOURCE_FORMAT
        / "volume-04"
        / "HRS0235.html"
    ).write_text(SAMPLE_CHAPTER_INDEX_HTML, encoding="utf-8")
    (
        source_dir
        / HAWAII_SECTION_SOURCE_FORMAT
        / "volume-04"
        / "HRS0235"
        / "HRS_0235-0055_0007_0005.htm"
    ).write_text(SAMPLE_SECTION_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_hawaii_revised_statutes(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2026-01-06",
        expression_date="2026-01-06",
        only_title="04",
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
        "us-hi/statute/volume-04",
        "us-hi/statute/chapter-235",
        "us-hi/statute/235-55.75",
    ]
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == [
        "us-hi/statute/235-93",
        "us-hi/statute/235-55.75",
    ]
