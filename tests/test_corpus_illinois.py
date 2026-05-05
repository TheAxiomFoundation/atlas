import json
from datetime import date
from pathlib import Path

import pytest
import requests

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.illinois import (
    ILLINOIS_ILCS_BASE_URL,
    _discover_local_sources,
    _discover_remote_sources,
    _load_local_section_sequence,
    _load_remote_section_sequence,
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


def test_parse_illinois_ilcs_doc_name_handles_container_and_invalid_names():
    parsed = parse_illinois_ilcs_doc_name("001012345H.html")

    assert parsed.chapter_int == 10
    assert parsed.act_int == 12345
    assert parsed.section is None
    assert parsed.citation is None
    assert parsed.citation_path == "us-il/statute/10/12345"

    with pytest.raises(ValueError, match="not an ILCS document name"):
        parse_illinois_ilcs_doc_name("not-ilcs.html")


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


def test_parse_illinois_ilcs_links_does_not_invent_tokens_from_spaced_href_labels():
    text = """
    <a href="/ftp/ILCS/Ch%200005/Act%200100/000501000HArt.%201.html">000501000HArt. 1.html</a>
    <a href="/ftp/ILCS/Ch%200005/Act%200100/000501000K1-1.html">000501000K1-1.html</a>
    """

    assert parse_illinois_ilcs_links(text) == (
        "/ftp/ILCS/Ch 0005/Act 0100/000501000HArt. 1.html",
        "/ftp/ILCS/Ch 0005/Act 0100/000501000K1-1.html",
    )


def test_parse_illinois_ilcs_section_extracts_citation_heading_body_and_refs():
    document = parse_illinois_ilcs_doc_name("000500700K1.html")

    section = parse_illinois_ilcs_section(SAMPLE_SECTION_1, document=document)

    assert section.citation == "5 ILCS 70/1"
    assert section.citation_path == "us-il/statute/5/70/1"
    assert section.heading == "Short title"
    assert "General Provisions Act" in section.body
    assert section.references_to == ("5 ILCS 70/2",)


def test_parse_illinois_ilcs_section_uses_document_citation_fallback():
    document = parse_illinois_ilcs_doc_name("000500700K3.html")

    section = parse_illinois_ilcs_section(
        "<html><body><script>hidden()</script><p>Sec. 3.</p><p>Fallback body.</p></body></html>",
        document=document,
    )

    assert section.citation == "5 ILCS 70/3"
    assert section.heading is None
    assert section.body == "Sec. 3.\nFallback body."


def test_parse_illinois_ilcs_section_rejects_uncited_container_document():
    document = parse_illinois_ilcs_doc_name("000500700K.html")

    with pytest.raises(ValueError, match="has no citation"):
        parse_illinois_ilcs_section("<html><body>No citation here.</body></html>", document=document)


class _FakeIllinoisResponse:
    def __init__(self, value: str | bytes) -> None:
        self.content = value if isinstance(value, bytes) else value.encode()
        self.text = value.decode() if isinstance(value, bytes) else value

    def raise_for_status(self) -> None:
        return None


class _FakeIllinoisSession:
    def __init__(self, pages: dict[str, str | bytes]) -> None:
        self.pages = pages
        self.headers: dict[str, str] = {}

    def get(self, url: str, timeout: int) -> _FakeIllinoisResponse:
        del timeout
        if url not in self.pages:
            raise requests.HTTPError(f"404 Client Error: Not Found for url: {url}")
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


def test_remote_document_paths_prefers_official_section_sequence():
    session = _FakeIllinoisSession({})

    paths = _remote_document_paths(
        session,
        ILLINOIS_ILCS_BASE_URL,
        limit=2,
        chapter_filter=5,
        act_filter=70,
        sequence={
            "000500000A": 0,
            "000500700F": 1,
            "000500700K0.01": 2,
            "000500700K1": 3,
            "001000050K1": 4,
        },
    )

    assert paths == (
        "Ch 0005/Act 0070/000500700F.html",
        "Ch 0005/Act 0070/000500700K0.01.html",
        "Ch 0005/Act 0070/000500700K1.html",
    )


def test_remote_document_paths_limits_direct_links_by_sections():
    session = _FakeIllinoisSession(
        {
            ILLINOIS_ILCS_BASE_URL: """
            <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700F.html">Act text</a>
            <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700K1.html">Sec. 1</a>
            <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700K2.html">Sec. 2</a>
            <a href="/ftp/ILCS/Ch%200010/Act%200005/001000050K1.html">Other chapter</a>
            """
        }
    )

    paths = _remote_document_paths(
        session,
        ILLINOIS_ILCS_BASE_URL,
        limit=1,
        chapter_filter=5,
        act_filter=70,
    )

    assert paths == (
        "Ch 0005/Act 0070/000500700F.html",
        "Ch 0005/Act 0070/000500700K1.html",
    )


def test_discover_remote_sources_fetches_filtered_document_bytes(monkeypatch):
    pages: dict[str, str | bytes] = {
        ILLINOIS_ILCS_BASE_URL: """
        <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700F.html">Act text</a>
        <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700K1.html">Sec. 1</a>
        """,
        f"{ILLINOIS_ILCS_BASE_URL}Ch%200005/Act%200070/000500700F.html": b"<h1>Act.</h1>",
        f"{ILLINOIS_ILCS_BASE_URL}Ch%200005/Act%200070/000500700K1.html": SAMPLE_SECTION_1.encode(),
    }
    session = _FakeIllinoisSession(pages)
    monkeypatch.setattr(
        "axiom_corpus.corpus.state_adapters.illinois.requests.Session",
        lambda: session,
    )

    entries = _discover_remote_sources(
        ILLINOIS_ILCS_BASE_URL,
        limit=1,
        chapter_filter=5,
        act_filter=70,
    )

    assert tuple(entry[1] for entry in entries) == (
        "Ch 0005/Act 0070/000500700F.html",
        "Ch 0005/Act 0070/000500700K1.html",
    )
    assert entries[0][0].doc_type == "F"
    assert entries[1][3] == SAMPLE_SECTION_1.encode()


def test_discover_remote_sources_records_fetch_errors(monkeypatch):
    pages: dict[str, str | bytes] = {
        ILLINOIS_ILCS_BASE_URL: """
        <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700K1.html">Sec. 1</a>
        <a href="/ftp/ILCS/Ch%200005/Act%200070/000500700K2.html">Sec. 2</a>
        """,
        f"{ILLINOIS_ILCS_BASE_URL}Ch%200005/Act%200070/000500700K1.html": SAMPLE_SECTION_1.encode(),
    }
    session = _FakeIllinoisSession(pages)
    monkeypatch.setattr(
        "axiom_corpus.corpus.state_adapters.illinois.requests.Session",
        lambda: session,
    )
    errors: list[str] = []

    entries = _discover_remote_sources(
        ILLINOIS_ILCS_BASE_URL,
        limit=None,
        chapter_filter=5,
        act_filter=70,
        errors=errors,
    )

    assert tuple(entry[1] for entry in entries) == (
        "Ch 0005/Act 0070/000500700K1.html",
    )
    assert len(errors) == 1
    assert "000500700K2.html" in errors[0]


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


def test_extract_illinois_ilcs_local_filters_and_scoped_limited_run(tmp_path):
    source_root = tmp_path / "ilcs"
    _write_fixture_tree(source_root)
    other_act = source_root / "Ch 0005" / "Act 0075"
    other_act.mkdir(parents=True)
    (other_act / "000500750K1.html").write_text(
        "<p>(5 ILCS 75/1)</p><p>Sec. 1. Other act.</p>",
        encoding="utf-8",
    )
    other_chapter = source_root / "Ch 0010" / "Act 0005"
    other_chapter.mkdir(parents=True)
    (other_chapter / "001000050K1.html").write_text(
        "<p>(10 ILCS 5/1)</p><p>Sec. 1. Other chapter.</p>",
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_illinois_ilcs(
        store,
        version="2026-05-04",
        source_dir=source_root,
        source_as_of="2025-11-24",
        expression_date=date(2026, 5, 4),
        only_chapter=5,
        only_act=70,
        limit=1,
    )

    assert report.section_count == 1
    assert report.provisions_written == 3
    assert report.provisions_path.name == "2026-05-04-us-il-chapter-5-act-70-limit-1.jsonl"
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-il/statute/5",
        "us-il/statute/5/70",
        "us-il/statute/5/70/1",
    ]
    assert records[0].source_as_of == "2025-11-24"
    assert records[0].expression_date == "2026-05-04"


def test_extract_illinois_ilcs_local_counts_skips_errors_and_duplicates(tmp_path):
    source_root = tmp_path / "ilcs"
    act_dir = source_root / "Ch 0005" / "Act 0070"
    act_dir.mkdir(parents=True)
    (source_root / "not-ilcs.html").write_text("<p>ignored</p>", encoding="utf-8")
    (act_dir / "000500700F.html").write_text(
        "<html><body><h1>General Provisions Act.</h1></body></html>",
        encoding="utf-8",
    )
    (act_dir / "000500700K.html").write_text(
        "<html><body>No citation fallback.</body></html>",
        encoding="utf-8",
    )
    (act_dir / "000500700K1.html").write_text(SAMPLE_SECTION_1, encoding="utf-8")
    (act_dir / "000500700K1A.html").write_text(SAMPLE_SECTION_1, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_illinois_ilcs(store, version="2026-05-04", source_dir=source_root)

    assert report.section_count == 1
    assert report.provisions_written == 3
    assert report.skipped_source_count == 2
    assert len(report.errors) == 1
    assert "000500700K.html" in report.errors[0]
    records = load_provisions(report.provisions_path)
    assert records[1].heading == "General Provisions Act."


def test_extract_illinois_ilcs_raises_when_local_source_has_no_ilcs_documents(tmp_path):
    source_root = tmp_path / "empty"
    source_root.mkdir()
    (source_root / "readme.html").write_text("<p>not an ILCS file</p>", encoding="utf-8")

    with pytest.raises(ValueError, match="no Illinois ILCS provisions extracted"):
        extract_illinois_ilcs(
            CorpusArtifactStore(tmp_path / "corpus"),
            version="2026-05-04",
            source_dir=source_root,
        )


def test_illinois_local_source_helpers_handle_empty_and_root_sequence(tmp_path):
    source_root = tmp_path / "ilcs"
    source_root.mkdir()
    (source_root / "Section Sequence.txt").write_text("000500700K1\n", encoding="utf-8")

    assert _discover_local_sources(None) == ()
    assert _load_local_section_sequence(None) == {}
    assert _load_local_section_sequence(source_root) == {"000500700K1": 0}


def test_load_remote_section_sequence_handles_success_and_request_errors(monkeypatch):
    monkeypatch.setattr(
        "axiom_corpus.corpus.state_adapters.illinois.requests.get",
        lambda *args, **kwargs: _FakeIllinoisResponse("000500700K1"),
    )

    assert _load_remote_section_sequence(ILLINOIS_ILCS_BASE_URL) == {"000500700K1": 0}

    def _raise_request_error(*args, **kwargs):
        raise requests.RequestException("offline")

    monkeypatch.setattr(
        "axiom_corpus.corpus.state_adapters.illinois.requests.get",
        _raise_request_error,
    )

    assert _load_remote_section_sequence(ILLINOIS_ILCS_BASE_URL) == {}


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
