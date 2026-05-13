from pathlib import Path

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.washington_wac import extract_washington_wac


def _write_wac_sources(source_dir: Path) -> None:
    (source_dir / "washington-wac-html/titles").mkdir(parents=True)
    (source_dir / "washington-wac-html/chapters").mkdir(parents=True)
    (source_dir / "washington-wac-html/index.html").write_text(
        """
        <html>
          <body>
            <div id="ContentPlaceHolder1_pnlDefaultUpdated">April 1, 2026</div>
            <div id="contentWrapper">
              <table>
                <tr>
                  <td><a href="/WAC/default.aspx?cite=3">Title 3 WAC</a></td>
                  <td>Academic Achievement and Accountability Commission</td>
                </tr>
                <tr>
                  <td><a href="/WAC/default.aspx?cite=237">Title 237 WAC</a></td>
                  <td>Geographic Names, Board on</td>
                </tr>
                <tr>
                  <td><a href="/WAC/default.aspx?cite=388">Title 388 WAC</a></td>
                  <td>Department of Social and Health Services</td>
                </tr>
              </table>
            </div>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    (source_dir / "washington-wac-html/titles/title-3.html").write_text(
        """
        <html>
          <body>
            <h1>Title 3 WAC</h1>
            <div id="contentWrapper" class="title-page">
              <div>No active sections in this title - please see "Show Dispositions" link above</div>
            </div>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    (source_dir / "washington-wac-html/titles/title-237.html").write_text(
        """
        <html>
          <body>
            <h1>Title 237 WAC Geographic Names, Board on</h1>
            <div id="contentWrapper">
              <table>
                <tr>
                  <td><a href="/WAC/default.aspx?cite=237-990">Chapter 237-990 WAC</a></td>
                  <td>Geographic names</td>
                </tr>
              </table>
            </div>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    (source_dir / "washington-wac-html/chapters/chapter-237-990-full.html").write_text(
        """
        <html>
          <body>
            <h1>Chapter 237-990 WAC Geographic names</h1>
            <div id="contentWrapper" class="chapter-page">
              <div>
                <span style="font-weight:bold;">Reviser's note:</span>
                Adoption of geographic names is included in this compilation.
              </div>
              <div><span style="font-weight:bold;">Abernethy Creek</span>: Stream.</div>
            </div>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    (source_dir / "washington-wac-html/titles/title-388.html").write_text(
        """
        <html>
          <body>
            <h1>Title 388 WAC Department of Social and Health Services</h1>
            <div id="contentWrapper">
              <table>
                <tr>
                  <td><a href="/WAC/default.aspx?cite=388-400">Chapter 388-400 WAC</a></td>
                  <td>Applications</td>
                </tr>
              </table>
            </div>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    (source_dir / "washington-wac-html/chapters/chapter-388-400-full.html").write_text(
        """
        <html>
          <body>
            <h1>Chapter 388-400 WAC Applications</h1>
            <div id="ContentPlaceHolder1_pnlExpanded">
              <span>
                <a name="388-400-0005"></a>
                <div>WAC 388-400-0005</div>
                <div>Who is eligible for cash assistance?</div>
                <div>
                  (1) You may apply for benefits.
                  See
                  <a href="/WAC/default.aspx?cite=388-408-0015">WAC 388-408-0015</a>
                  and <a href="/RCW/default.aspx?cite=74.04.050">RCW 74.04.050</a>.
                </div>
                <div style="margin-top: 15pt">
                  [Statutory Authority: RCW 74.04.050.]
                </div>
                <div>Notes:</div>
                <div>Example note.</div>
              </span>
            </div>
          </body>
        </html>
        """,
        encoding="utf-8",
    )


def test_extract_washington_wac_local_sources_writes_records(tmp_path):
    source_dir = tmp_path / "wac-source"
    _write_wac_sources(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_washington_wac(
        store,
        version="2026-05-12",
        source_dir=source_dir,
        only_title="388",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.chapter_count == 1
    assert report.section_count == 1
    assert report.provisions_written == 4

    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-wa/regulation",
        "us-wa/regulation/388",
        "us-wa/regulation/388/388-400",
        "us-wa/regulation/388/388-400/388-400-0005",
    ]
    section = records[-1]
    assert section.heading == "Who is eligible for cash assistance?"
    assert section.body is not None
    assert "You may apply for benefits" in section.body
    assert section.ordinal == 0
    assert section.source_as_of == "2026-04-01"
    assert section.expression_date == "2026-04-01"
    assert section.metadata is not None
    assert section.metadata["source_history"] == [
        "[Statutory Authority: RCW 74.04.050.]"
    ]
    assert section.metadata["notes"] == ["Example note."]
    assert section.metadata["references_to"] == [
        "us-wa/regulation/388/388-408/388-408-0015",
        "us-wa/statute/74/74.04/74.04.050",
    ]

    inventory = load_source_inventory(report.inventory_path)
    assert [item.citation_path for item in inventory] == [
        "us-wa/regulation",
        "us-wa/regulation/388",
        "us-wa/regulation/388/388-400",
        "us-wa/regulation/388/388-400/388-400-0005",
    ]
    assert inventory[-1].source_format == "washington-wac-html"


def test_extract_washington_wac_preserves_no_active_title_body(tmp_path):
    source_dir = tmp_path / "wac-source"
    _write_wac_sources(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_washington_wac(
        store,
        version="2026-05-12",
        source_dir=source_dir,
        only_title="3",
    )

    assert report.errors == ()
    assert report.coverage.complete
    assert report.title_count == 1
    assert report.chapter_count == 0
    assert report.section_count == 0
    assert report.provisions_written == 2
    records = load_provisions(report.provisions_path)
    assert records[-1].citation_path == "us-wa/regulation/3"
    assert records[-1].body is not None
    assert "No active sections in this title" in records[-1].body
    assert records[-1].metadata is not None
    assert records[-1].metadata["status"] == "no_active_sections"


def test_extract_washington_wac_preserves_non_section_chapter_body(tmp_path):
    source_dir = tmp_path / "wac-source"
    _write_wac_sources(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_washington_wac(
        store,
        version="2026-05-12",
        source_dir=source_dir,
        only_chapter="237-990",
    )

    assert report.errors == ()
    assert report.coverage.complete
    assert report.title_count == 1
    assert report.chapter_count == 1
    assert report.section_count == 0
    assert report.provisions_written == 3
    records = load_provisions(report.provisions_path)
    assert records[-1].citation_path == "us-wa/regulation/237/237-990"
    assert records[-1].body is not None
    assert "Abernethy Creek" in records[-1].body
    assert records[-1].metadata is not None
    assert records[-1].metadata["status"] == "non_section_content"


def test_extract_washington_wac_cli_local_sources(tmp_path, capsys):
    source_dir = tmp_path / "wac-source"
    _write_wac_sources(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-washington-wac",
            "--base",
            str(base),
            "--version",
            "2026-05-12",
            "--source-dir",
            str(source_dir),
            "--only-chapter",
            "388-400",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-wa"' in output
    assert '"document_class": "regulation"' in output
    assert '"section_count": 1' in output
    assert '"provisions_written": 4' in output
