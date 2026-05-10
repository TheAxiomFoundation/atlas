import json

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.south_dakota import (
    SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT,
    SouthDakotaSource,
    extract_south_dakota_codified_laws,
    parse_south_dakota_chapter_html,
)

SAMPLE_SOURCE = SouthDakotaSource(
    source_url="https://sdlegislature.gov/api/Statutes/Statute/1-1",
    source_path="sources/us-sd/statute/test/south-dakota-statutes-json/statute-1-1.json",
    source_format=SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT,
    sha256="abc",
)

SAMPLE_CHAPTER_HTML = """
<html><body><div>
<p>CHAPTER <a href="https://sdlegislature.gov/Statutes?Statute=1-1">1-1</a></p>
<p>STATE SOVEREIGNTY AND JURISDICTION</p>
<p><a href="https://sdlegislature.gov/Statutes?Statute=1-1-1">1-1-1</a> Territorial extent of sovereignty.</p>
<p><a href="https://sdlegislature.gov/Statutes?Statute=1-1-2">1-1-2</a> 1-1-2. Repealed by SL 2006, ch 130, \u00a7 1.</p>
<p><a href="https://sdlegislature.gov/Statutes?Statute=1-1-3">1-1-3</a> Cross-reference section.</p>
<p><a href="https://sdlegislature.gov/Statutes?Statute=1-1-1">1-1-1</a>. Territorial extent of sovereignty.</p>
<p>The sovereignty of this state extends to places described in \u00a7 1-1-3.</p>
<p>Source: SDC 1939, \u00a7 55.0101.</p>
<p><a href="https://sdlegislature.gov/Statutes?Statute=1-1-3">1-1-3</a>. Cross-reference section.</p>
<p>This section points back to \u00a7 1-1-1.</p>
<p>Source: SL 2024, ch 3, \u00a7 1.</p>
</div></body></html>
"""


def test_parse_south_dakota_chapter_html_merges_toc_with_bodies():
    sections = parse_south_dakota_chapter_html(
        SAMPLE_CHAPTER_HTML,
        chapter_label="1-1",
        source=SAMPLE_SOURCE,
        parent_citation_path="us-sd/statute/title-1/chapter-1-1",
    )

    assert [section.citation_path for section in sections] == [
        "us-sd/statute/1-1-1",
        "us-sd/statute/1-1-2",
        "us-sd/statute/1-1-3",
    ]
    assert sections[0].body == (
        "The sovereignty of this state extends to places described in \u00a7 1-1-3."
    )
    assert sections[0].references_to == ("us-sd/statute/1-1-3",)
    assert sections[0].source_history == ("SDC 1939, \u00a7 55.0101.",)
    assert sections[1].status == "repealed"
    assert sections[1].body is None
    assert sections[2].references_to == ("us-sd/statute/1-1-1",)


def test_extract_south_dakota_codified_laws_from_source_dir(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source_format_dir = source_dir / SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT
    source_format_dir.mkdir()
    (source_format_dir / "effective-date.json").write_text(
        json.dumps("2026-04-16T00:00:00-05:00"),
        encoding="utf-8",
    )
    (source_format_dir / "titles.json").write_text(
        json.dumps(
            [
                {
                    "StatuteId": 1,
                    "Statute": "1",
                    "Type": "Title",
                    "CatchLine": "STATE AFFAIRS AND GOVERNMENT",
                }
            ]
        ),
        encoding="utf-8",
    )
    (source_format_dir / "statute-1.json").write_text(
        json.dumps(
            {
                "StatuteId": 1,
                "Statute": "1",
                "Type": "Title",
                "CatchLine": "STATE AFFAIRS AND GOVERNMENT",
                "Html": """
                <html><body>
                <p>TITLE 1</p>
                <p><a href="https://sdlegislature.gov/Statutes/1-1">01</a> State Sovereignty And Jurisdiction</p>
                </body></html>
                """,
            }
        ),
        encoding="utf-8",
    )
    (source_format_dir / "statute-1-1.json").write_text(
        json.dumps(
            {
                "StatuteId": 2,
                "Statute": "1-1",
                "Type": "Chapter",
                "CatchLine": "STATE SOVEREIGNTY AND JURISDICTION",
                "Html": SAMPLE_CHAPTER_HTML,
            }
        ),
        encoding="utf-8",
    )

    store = CorpusArtifactStore(tmp_path / "corpus")
    report = extract_south_dakota_codified_laws(
        store,
        version="2026-05-10",
        source_dir=source_dir,
        source_as_of="2026-05-10",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 3
    assert report.provisions_written == 5
    assert len(load_source_inventory(report.inventory_path)) == 5
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-sd/statute/title-1",
        "us-sd/statute/title-1/chapter-1-1",
        "us-sd/statute/1-1-1",
        "us-sd/statute/1-1-2",
        "us-sd/statute/1-1-3",
    ]
    assert records[0].expression_date == "2026-04-16"
    assert records[3].metadata is not None
    assert records[3].metadata["status"] == "repealed"
