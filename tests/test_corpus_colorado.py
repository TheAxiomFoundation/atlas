import json
from pathlib import Path

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.colorado import extract_colorado_ccr
from axiom_corpus.corpus.io import load_provisions

CCR_SAMPLE_LINES = [
    "CodeofColoradoRegulations",
    "SecretaryofState",
    "StateofColorado",
    "DEPARTMENT OF HUMAN SERVICES",
    "Supplemental Nutrition Assistance Program (SNAP)",
    "RULE MANUAL VOLUME 4, SNAP",
    "10 CCR 2506-1",
    "4.000 SNAP",
    "The Supplemental Nutrition Assistance Program provides food assistance.",
    "4.000.1 SNAP DEFINITIONS",
    '"Application" means a request on a state-approved form.',
]


def _write_pdf(path: Path, lines: list[str]) -> None:
    import fitz

    document = fitz.open()
    page = document.new_page()
    y = 72
    for line in lines:
        page.insert_text((72, y), line, fontsize=10)
        y += 14
    document.save(path)
    document.close()


def _write_ccr_release(release_dir: Path) -> None:
    release_dir.mkdir(parents=True)
    _write_pdf(release_dir / "10-ccr-2506-1.pdf", CCR_SAMPLE_LINES)
    (release_dir / "Welcome.html").write_text(
        "The Code of Colorado Regulations is current with administrative rules "
        "effective on or before <b>04/13/2026.</b>"
    )
    (release_dir / "NumericalDeptList.html").write_text("<html></html>")
    (release_dir / "rule-info-10-ccr-2506-1.html").write_text(
        "<p class='pagehead5'>10 CCR 2506-1 RULE MANUAL VOLUME 4, SNAP</p>"
    )
    (release_dir / "manifest.json").write_text(
        json.dumps(
            {
                "current_through": "2026-04-13",
                "documents": [
                    {
                        "series": "10 CCR 2506-1",
                        "title": "RULE MANUAL VOLUME 4, SNAP",
                        "rule_info_url": "https://www.sos.state.co.us/CCR/DisplayRule.do?action=ruleinfo&ruleId=2818",
                        "pdf_url": "https://www.sos.state.co.us/CCR/GenerateRulePdf.do?ruleVersionId=12299&fileName=10%20CCR%202506-1",
                        "rule_version_id": "12299",
                        "effective_date": "2025-12-30",
                        "department": "Department of Human Services",
                        "agency": "Supplemental Nutrition Assistance Program (SNAP)",
                        "file_name": "10-ccr-2506-1.pdf",
                        "rule_info_file": "rule-info-10-ccr-2506-1.html",
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def test_extract_colorado_ccr_local_release_writes_records(tmp_path):
    release_dir = tmp_path / "ccr-release"
    _write_ccr_release(release_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_colorado_ccr(
        store,
        version="2026-04-29",
        release_dir=release_dir,
    )

    assert report.coverage.complete
    assert report.document_count == 1
    assert report.section_count == 2
    assert report.provisions_written == 4
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-co/regulation",
        "us-co/regulation/10-ccr-2506-1",
        "us-co/regulation/10-ccr-2506-1/4.000",
        "us-co/regulation/10-ccr-2506-1/4.000.1",
    ]
    assert records[1].metadata["document_subtype"] == "rule_manual"
    assert records[1].source_as_of == "2026-04-13"
    assert records[2].heading == "SNAP"
    assert "food assistance" in records[2].body
    assert records[3].legal_identifier == "10 CCR 2506-1 4.000.1"


def test_extract_colorado_ccr_cli_local_release(tmp_path, capsys):
    release_dir = tmp_path / "ccr-release"
    _write_ccr_release(release_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-colorado-ccr",
            "--base",
            str(base),
            "--version",
            "2026-04-29",
            "--release-dir",
            str(release_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"jurisdiction": "us-co"' in output
    assert '"document_class": "regulation"' in output
    assert '"provisions_written": 4' in output
