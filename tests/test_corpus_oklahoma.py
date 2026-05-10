from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.oklahoma import (
    OKLAHOMA_STATUTES_RTF_SOURCE_FORMAT,
    OklahomaSource,
    extract_oklahoma_statutes,
    parse_oklahoma_title_rtf,
)

SAMPLE_TITLE_RTF = r"""{\rtf1\ansi
OKLAHOMA STATUTES\par
TITLE 75. STATUTES AND REPORTS\par
§75-1.  Repealed by Laws 1961, p. 609, § 1.\tab 5\par
§75-11.  Statutes defined.\tab 5\par
§75-12.  Original acts shall govern.\tab 6\par
\par
§75-1.  Repealed by Laws 1961, p. 609, § 1.\par
\par
§75-11.  Statutes defined.\par
The Statutes of Oklahoma are defined in Section 12 of Title 75.\par
R.L. 1910, § 8147.\par
\par
§75-12.  Original acts shall govern.\par
Original acts govern conflicts with adopted statutes.\par
Laws 1983, c. 164, § 1, emerg. eff. June 6, 1983.\par
}"""

SAMPLE_RULE_RTF = r"""{\rtf1\ansi
OKLAHOMA STATUTES\par
TITLE 74, APPENDIX I, ETHICS COMMISSION RULES\par
Rule 1.1.  Purpose of Ethics Rules.\tab 6\par
\par
Rule 1.1.  Purpose of Ethics Rules.\par
The purpose of these Rules is to fulfill Ethics Commission duties.\par
Promulgated by Ethics Commission January 10, 2014.\par
}"""

SAMPLE_SOURCE = OklahomaSource(
    title="75",
    file_name="os75.rtf",
    source_url="https://www.oklegislature.gov/OK_Statutes/CompleteTitles/os75.rtf",
    source_path="sources/us-ok/statute/test/oklahoma-statutes-rtf/os75.rtf",
    source_format=OKLAHOMA_STATUTES_RTF_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_oklahoma_title_rtf_skips_toc_and_extracts_sections():
    provisions = parse_oklahoma_title_rtf(
        SAMPLE_TITLE_RTF,
        source=SAMPLE_SOURCE,
        title_heading="Title 75. Statutes And Reports",
    )

    assert [provision.citation_path for provision in provisions] == [
        "us-ok/statute/75-1",
        "us-ok/statute/75-11",
        "us-ok/statute/75-12",
    ]
    assert provisions[0].status == "repealed"
    assert provisions[1].heading == "Statutes defined"
    assert provisions[1].references_to == ("us-ok/statute/75-12",)
    assert provisions[1].source_history == ("R.L. 1910, § 8147.",)


def test_extract_oklahoma_statutes_from_source_dir_writes_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "index.html").write_text(
        """
        <html><body>
        <a href="/OK_Statutes/CompleteTitles/os75.rtf">os75.rtf</a>
        <a href="/OK_Statutes/CompleteTitles/os74E.rtf">os74E.rtf</a>
        </body></html>
        """,
        encoding="utf-8",
    )
    (source_dir / "os75.rtf").write_text(SAMPLE_TITLE_RTF, encoding="cp1252")
    (source_dir / "os74E.rtf").write_text(SAMPLE_RULE_RTF, encoding="cp1252")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_oklahoma_statutes(
        store,
        version="2026-05-10",
        source_dir=source_dir,
        source_as_of="2026-05-10",
        expression_date="2026-05-10",
    )

    assert report.coverage.complete is True
    assert report.title_count == 2
    assert report.container_count == 0
    assert report.section_count == 4
    assert report.provisions_written == 6
    assert len(load_source_inventory(report.inventory_path)) == 6
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-ok/statute/title-74e",
        "us-ok/statute/74e-rule-1-1",
        "us-ok/statute/title-75",
        "us-ok/statute/75-1",
        "us-ok/statute/75-11",
        "us-ok/statute/75-12",
    ]
    assert records[1].kind == "rule"
    assert records[1].metadata is not None
    assert records[1].metadata["source_history"] == [
        "Promulgated by Ethics Commission January 10, 2014."
    ]
