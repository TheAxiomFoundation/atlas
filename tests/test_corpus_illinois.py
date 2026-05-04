import json
from pathlib import Path

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.illinois import (
    ILLINOIS_ILCS_BASE_URL,
    _remote_document_paths,
    extract_illinois_ilcs,
    parse_illinois_ilcs_doc_name,
    parse_illinois_ilcs_links,
    parse_illinois_ilcs_section,
    parse_illinois_section_sequence,
)

SAMPLE_SECTION_1 = """<!doctype html>
<html>
<body>
<h1>General Provisions Act.</h1>
<p>(5 ILCS 70/1) (from Ch. 1, par. 1001)</p>
<p>Sec. 1. Short title.</p>
<p>This Act may be cited as the General Provisions Act.</p>
<p>References to (5 ILCS 70/2) are references to the next Section.</p>
<p>(Source: P.A. 99-1.)</p>
</body>
</html>
"""


SAMPLE_SECTION_2 = """<!doctype html>
<html>
<body>
<h1>General Provisions Act.</h1>
<p>(5 ILCS 70/2) (from Ch. 1, par. 1002)</p>
<p>Sec. 2. Definitions.</p>
<p>Words and phrases have the meanings provided by law.</p>
</body>
</html>
"""


def _write_fixture_tree(root: Path) -> None:
    act_dir = root / "Ch 0005" / "Act 0070"
    act_dir.mkdir(parents=True)
    (root / "aReadMe").mkdir()
    (root / "aReadMe" / "aReadMe.txt").write_text(
        """<html><body>
        <a href="../Ch%200005/Act%200070/000500700K2.html">Section 2</a>
        <a href="../Ch%200005/Act%200070/000500700K1.html">Section 1</a>
        </body></html>
        """,
        encoding="utf-8",
    )
    (root / "aReadMe" / "Section Sequence.txt").write_text(
        "000500700K1 000500700K2\n",
        encoding="utf-8",
    )
    (act_dir / "000500700K2.html").write_text(SAMPLE_SECTION_2, encoding="utf-8")
    (act_dir / "000500700K1.html").write_text(SAMPLE_SECTION_1, encoding="utf-8")


def test_parse_illinois_ilcs_doc_name_decodes_citation_shape():
    parsed = parse_illinois_ilcs_doc_name("000500700K1-10.5a.html")

    assert parsed.chapter_int == 5
    assert parsed.act_int == 70
    assert parsed.doc_type == "K"
    assert parsed.section == "1-10.5a"
    assert parsed.citation == "5 ILCS 70/1-10.5a"
    assert parsed.citation_path == "us-il/statute/5/70/1-10.5a"


def test_parse_illinois_ilcs_links_reads_directory_and_sequence_styles():
    text = """
    <a href="Ch%200005/Act%200070/000500700K1.html">000500700K1.html</a>
    000500700K2 000500700F
    """

    assert parse_illinois_ilcs_links(text) == (
        "Ch 0005/Act 0070/000500700K1.html",
        "000500700K2.html",
        "000500700F.html",
    )
    assert parse_illinois_section_sequence(text) == {
        "000500700K1": 0,
        "000500700K2": 1,
        "000500700F": 2,
    }


def test_parse_illinois_ilcs_section_extracts_citation_heading_body_and_refs():
    document = parse_illinois_ilcs_doc_name("000500700K1.html")

    section = parse_illinois_ilcs_section(SAMPLE_SECTION_1, document=document)

    assert section.citation == "5 ILCS 70/1"
    assert section.citation_path == "us-il/statute/5/70/1"
    assert section.heading == "Short title"
    assert "General Provisions Act" in section.body
    assert section.references_to == ("5 ILCS 70/2",)


class _FakeIllinoisResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _FakeIllinoisSession:
    def __init__(self, pages: dict[str, str]) -> None:
        self.pages = pages

    def get(self, url: str, timeout: int) -> _FakeIllinoisResponse:
        del timeout
        return _FakeIllinoisResponse(self.pages[url])


def test_remote_document_paths_filters_chapter_act_before_section_limit():
    pages = {
        ILLINOIS_ILCS_BASE_URL: """
        <a href="/ftp/ILCS/Ch%200005/">Ch 0005</a>
        <a href="/ftp/ILCS/Ch%200010/">Ch 0010</a>
        """,
        f"{ILLINOIS_ILCS_BASE_URL}Ch%200005/": """
        <a href="/ftp/ILCS/Ch%200005/Act%200070/">Act 0070</a>
        <a href="/ftp/ILCS/Ch%200005/Act%200075/">Act 0075</a>
        """,
        f"{ILLINOIS_ILCS_BASE_URL}Ch%200005/Act%200070/": """
        <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700F.html">Act text</a>
        <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700K0.01.html">Sec. 0.01</a>
        <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700K1.01.html">Sec. 1.01</a>
        <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700K1.02.html">Sec. 1.02</a>
        """,
    }

    paths = _remote_document_paths(
        _FakeIllinoisSession(pages),
        ILLINOIS_ILCS_BASE_URL,
        limit=2,
        chapter_filter=5,
        act_filter=70,
    )

    assert paths == (
        "Ch 0005/Act 0070/000500700F.html",
        "Ch 0005/Act 0070/000500700K0.01.html",
        "Ch 0005/Act 0070/000500700K1.01.html",
    )


def test_extract_illinois_ilcs_local_fixture_orders_by_section_sequence(tmp_path):
    source_root = tmp_path / "ilcs"
    _write_fixture_tree(source_root)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_illinois_ilcs(
        store,
        version="2026-05-04",
        source_dir=source_root,
    )

    assert report.coverage.complete
    assert report.jurisdiction == "us-il"
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    assert report.provisions_written == 4
    assert len(report.source_paths) == 2

    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-il/statute/5",
        "us-il/statute/5/70",
        "us-il/statute/5/70/1",
        "us-il/statute/5/70/2",
    ]
    assert records[2].heading == "Short title"
    assert records[2].legal_identifier == "5 ILCS 70/1"
    assert records[2].metadata == {"references_to": ["5 ILCS 70/2"]}
    assert records[3].ordinal == 1

    inventory = load_source_inventory(report.inventory_path)
    assert [item.citation_path for item in inventory] == [
        "us-il/statute/5",
        "us-il/statute/5/70",
        "us-il/statute/5/70/1",
        "us-il/statute/5/70/2",
    ]
    assert inventory[2].metadata["source_id"] == "000500700K1"

    coverage = json.loads(report.coverage_path.read_text())
    assert coverage["complete"] is True
    assert coverage["source_count"] == 4
    assert coverage["provision_count"] == 4


def test_extract_illinois_ilcs_cli_local_source(tmp_path, capsys):
    source_root = tmp_path / "ilcs"
    _write_fixture_tree(source_root)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-illinois-ilcs",
            "--base",
            str(base),
            "--version",
            "2026-05-04",
            "--source-dir",
            str(source_root),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["adapter"] == "illinois-ilcs"
    assert payload["coverage_complete"] is True
    assert payload["provisions_written"] == 4
