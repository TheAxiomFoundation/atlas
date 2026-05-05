import json
from pathlib import Path

import pytest

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.nevada import (
    NEVADA_NRS_BASE_URL,
    extract_nevada_nrs,
    parse_nevada_chapter_html,
    parse_nevada_nrs_index,
)

SAMPLE_NEVADA_INDEX = """<!doctype html>
<html>
<body>
<h3 class="card-title h5"><a href="NRS-000.html">PRELIMINARY CHAPTER</a></h3>
<div class="card">
  <h3 class="card-title h5">TITLE 1 - STATE JUDICIAL DEPARTMENT</h3>
  <ul>
    <li class="list-group-item"><a href="NRS-001.html">Chapter 1</a> - Judicial Department Generally</li>
  </ul>
</div>
<div class="card">
  <h3 class="card-title h5">TITLE 32-REVENUE AND TAXATION</h3>
  <ul>
    <li class="list-group-item"><a href="NRS-361.html">Chapter 361</a> - Property Tax</li>
    <li class="list-group-item"><a href="NRS-361A.html">Chapter 361A</a> - Agricultural Real Property</li>
  </ul>
</div>
</body>
</html>
"""

SAMPLE_NEVADA_CHAPTER = """<!doctype html>
<html>
<head><title>NRS: CHAPTER 361 - PROPERTY TAX</title></head>
<body>
<p>[Rev. 4/15/2026 2:20:22 PM--2025]</p>
<p class="Chapter"><a name="NRS361"></a>CHAPTER 361 - PROPERTY TAX</p>
<p class="SectBody"><span class="Empty"><a name="NRS361Sec010"></a>NRS </span><span class="Section">361.010</span><span class="Empty"> </span><span class="Leadline">Definitions.</span><span class="Empty"> </span>As used in this chapter, see <a href="#NRS361Sec013">361.013</a> and <a href="NRS-355.html#NRS355Sec140">NRS 355.140</a>.</p>
<p class="SourceNote">(Added to NRS by 1953, 344)</p>
<p class="SectBody"><span class="Empty"><a name="NRS361Sec013"></a>NRS </span><span class="Section">361.013</span><span class="Empty"> </span><span class="Leadline">Billboard defined. [Effective through June 30, 2026.]</span><span class="Empty"> </span>Old definition.</p>
<p class="SectBody">1. First body paragraph.</p>
<p class="SourceNote">(Added to NRS by 1989, 1817)</p>
<p class="SectBody"><span class="Empty">NRS </span><span class="Section">361.013</span><span class="Empty"> </span><span class="Leadline">Billboard defined. [Effective July 1, 2026.]</span><span class="Empty"> </span>New definition references NRS 361.010.</p>
<p class="SourceNote">(Added to NRS by 2025, 100, effective July 1, 2026)</p>
</body>
</html>
"""

SAMPLE_PRELIMINARY_CHAPTER = """<!doctype html>
<html>
<body>
<p>[Rev. 4/15/2026 10:37:10 AM--2025]</p>
<p class="MsoTitle"><a name="NRS000"></a>PRELIMINARY CHAPTER</p>
<p class="SectBody"><span class="Empty"><a name="NRS0Sec010"></a>NRS </span><span class="Section">0.010</span><span class="Empty"> </span><span class="Leadline">Scope.</span><span class="Empty"> </span>This chapter applies to the Nevada Revised Statutes.</p>
<p class="SourceNote">(Added to NRS by 1957, 1)</p>
</body>
</html>
"""


def _write_source_dir(root: Path) -> Path:
    source_dir = root / "source"
    source_dir.mkdir()
    (source_dir / "index.html").write_text(SAMPLE_NEVADA_INDEX, encoding="utf-8")
    (source_dir / "NRS-361.html").write_text(SAMPLE_NEVADA_CHAPTER, encoding="utf-8")
    (source_dir / "NRS-000.html").write_text(SAMPLE_PRELIMINARY_CHAPTER, encoding="utf-8")
    return source_dir


def test_parse_nevada_nrs_index_extracts_titles_and_chapters():
    index = parse_nevada_nrs_index(SAMPLE_NEVADA_INDEX)

    assert [title.citation_path for title in index.titles] == [
        "us-nv/statute/title-1",
        "us-nv/statute/title-32",
    ]
    assert [chapter.chapter for chapter in index.chapters] == ["0", "1", "361", "361A"]
    assert index.chapters[0].legal_identifier == "NRS Preliminary Chapter"
    assert index.chapters[-1].parent_citation_path == "us-nv/statute/title-32"
    assert index.chapters[-1].heading == "Agricultural Real Property"


def test_parse_nevada_chapter_html_extracts_body_history_refs_and_variants():
    parsed = parse_nevada_chapter_html(SAMPLE_NEVADA_CHAPTER)

    assert parsed.chapter == "361"
    assert parsed.heading == "Chapter 361 - Property Tax"
    assert parsed.revision == "Rev. 4/15/2026 2:20:22 PM--2025"
    assert parsed.source_year == 2025
    assert [section.source_id for section in parsed.sections] == [
        "361.010",
        "361.013",
        "361.013@effective-2026-07-01",
    ]
    assert parsed.sections[0].heading == "Definitions"
    assert parsed.sections[0].body == "As used in this chapter, see 361.013 and NRS 355.140."
    assert parsed.sections[0].references_to == (
        "us-nv/statute/361.013",
        "us-nv/statute/355.140",
    )
    assert parsed.sections[1].body == "Old definition.\n1. First body paragraph."
    assert parsed.sections[1].effective_note == "Effective through June 30, 2026."
    assert parsed.sections[2].variant == "effective-2026-07-01"
    assert parsed.sections[2].canonical_citation_path == "us-nv/statute/361.013"
    assert parsed.sections[2].source_history == (
        "(Added to NRS by 2025, 100, effective July 1, 2026)",
    )


def test_parse_nevada_chapter_html_disambiguates_repeated_effective_variants():
    duplicate = SAMPLE_NEVADA_CHAPTER.replace(
        "</body>",
        """<p class="SectBody"><span class="Empty">NRS </span><span class="Section">361.013</span><span class="Empty"> </span><span class="Leadline">Billboard defined. [Effective July 1, 2026.]</span><span class="Empty"> </span>Another future version.</p>
<p class="SourceNote">(Added to NRS by 2025, 101, effective July 1, 2026)</p>
</body>""",
    )

    parsed = parse_nevada_chapter_html(duplicate)

    assert [section.source_id for section in parsed.sections][-2:] == [
        "361.013@effective-2026-07-01",
        "361.013@effective-2026-07-01-3",
    ]


def test_parse_nevada_preliminary_chapter_html():
    parsed = parse_nevada_chapter_html(SAMPLE_PRELIMINARY_CHAPTER)

    assert parsed.chapter == "0"
    assert parsed.heading == "Preliminary Chapter"
    assert parsed.sections[0].citation_path == "us-nv/statute/0.010"
    assert parsed.sections[0].legal_identifier == "NRS 0.010"


def test_parse_nevada_chapter_html_handles_split_alphanumeric_section_span():
    html = """<!doctype html><html><body>
    <p>[Rev. 4/15/2026 2:20:22 PM--2025]</p>
    <p class="Chapter">CHAPTER 388C - UNIVERSITY SCHOOLS</p>
    <p class="SectBody"><span class="Empty"><a name="NRS388CSec140"></a>NRS</span><span class="Section">388C</span><span class="Section">.140</span><span class="Empty"> </span><span class="Leadline">Rules.</span>Body text.</p>
    </body></html>"""

    parsed = parse_nevada_chapter_html(html)

    assert parsed.sections[0].source_id == "388C.140"
    assert parsed.sections[0].citation_path == "us-nv/statute/388C.140"
    assert parsed.sections[0].body == "Body text."


def test_extract_nevada_nrs_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = _write_source_dir(tmp_path)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_nevada_nrs(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        source_as_of="2026-04-21",
        expression_date="2026-04-21",
        only_chapter="361",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 3
    assert report.provisions_written == 5
    assert report.provisions_path.name == "2026-05-05-us-nv-chapter-361.jsonl"
    assert [path.name for path in report.source_paths] == ["index.html", "NRS-361.html"]

    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert [item.citation_path for item in inventory] == [
        "us-nv/statute/title-32",
        "us-nv/statute/361",
        "us-nv/statute/361.010",
        "us-nv/statute/361.013",
        "us-nv/statute/361.013@effective-2026-07-01",
    ]
    assert records[0].heading == "Revenue And Taxation"
    assert records[1].parent_citation_path == "us-nv/statute/title-32"
    assert records[-1].metadata is not None
    assert records[-1].metadata["status"] == "future_or_conditional"
    assert records[-1].metadata["canonical_citation_path"] == "us-nv/statute/361.013"


def test_extract_nevada_nrs_handles_preliminary_chapter_without_title(tmp_path):
    source_dir = _write_source_dir(tmp_path)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_nevada_nrs(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        only_chapter="0",
    )

    records = load_provisions(report.provisions_path)
    assert report.title_count == 0
    assert report.container_count == 1
    assert [record.citation_path for record in records] == [
        "us-nv/statute/0",
        "us-nv/statute/0.010",
    ]
    assert records[0].level == 0


def test_extract_nevada_nrs_filters_limits_and_reports_missing_sources(tmp_path):
    source_dir = _write_source_dir(tmp_path)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_nevada_nrs(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        only_title="32",
        limit=1,
    )

    assert report.provisions_written == 3
    assert report.section_count == 1
    assert report.skipped_source_count == 0
    assert report.provisions_path.name == "2026-05-05-us-nv-title-32-limit-1.jsonl"

    with pytest.raises(ValueError, match="no Nevada NRS chapters selected"):
        extract_nevada_nrs(store, version="2026-05-05", source_dir=source_dir, only_title="99")
    with pytest.raises(ValueError, match="invalid Nevada chapter filter"):
        extract_nevada_nrs(store, version="2026-05-05", source_dir=source_dir, only_chapter="x")


def test_extract_nevada_nrs_cli_local_source(tmp_path, capsys):
    source_dir = _write_source_dir(tmp_path)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-nevada-nrs",
            "--base",
            str(base),
            "--version",
            "2026-05-05",
            "--source-dir",
            str(source_dir),
            "--only-chapter",
            "361",
            "--source-as-of",
            "2026-04-21",
            "--expression-date",
            "2026-04-21",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["adapter"] == "nevada-nrs"
    assert payload["coverage_complete"] is True
    assert payload["provisions_written"] == 5


def test_extract_state_statutes_dry_run_allows_live_nevada_source(tmp_path, capsys):
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        f"""
version: "2026-05-05"
sources:
  - source_id: us-nv-nrs
    jurisdiction: us-nv
    document_class: statute
    adapter: nevada-nrs
    source_url: {NEVADA_NRS_BASE_URL}
"""
    )

    exit_code = main(
        [
            "extract-state-statutes",
            "--base",
            str(tmp_path / "corpus"),
            "--manifest",
            str(manifest),
            "--dry-run",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["rows"][0]["adapter"] == "nevada-nrs"
    assert payload["rows"][0]["source_path"] is None
    assert payload["rows"][0]["source_path_exists"] is True
