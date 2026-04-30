import json

from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.coverage import ProvisionCoverageReport
from axiom_corpus.corpus.ecfr import EcfrExtractReport, EcfrInventory
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.usc import UscExtractReport

SAMPLE_USLM_CLI = """
<uscDoc identifier="/us/usc/t26">
  <meta><docNumber>26</docNumber></meta>
  <title identifier="/us/usc/t26">
    <heading>Internal Revenue Code</heading>
    <section identifier="/us/usc/t26/s32">
      <num>§ 32.</num>
      <heading>Earned income</heading>
      <content><p>(a) Allowance of credit.</p></content>
    </section>
  </title>
</uscDoc>
"""


def test_validate_manifest_cli(capsys):
    exit_code = main(["validate-manifest", "manifests/corpus.example.yaml"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"ok": true' in output


def test_inventory_ecfr_cli(tmp_path, capsys, monkeypatch):
    import axiom_corpus.corpus.cli as cli

    monkeypatch.setattr(
        cli,
        "build_ecfr_inventory",
        lambda **kwargs: EcfrInventory(
            items=(SourceInventoryItem(citation_path="us/regulation/7/273/1"),),
            title_count=1,
            part_count=1,
        ),
    )
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "inventory-ecfr",
            "--base",
            str(base),
            "--run-id",
            "2026-04-29",
            "--as-of",
            "2024-04-16",
            "--only-title",
            "7",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"items_written": 1' in output
    inventory = json.loads((base / "inventory/us/regulation/2026-04-29-title-7.json").read_text())
    assert inventory["items"][0]["citation_path"] == "us/regulation/7/273/1"


def test_extract_ecfr_cli(tmp_path, capsys, monkeypatch):
    import axiom_corpus.corpus.cli as cli

    base = tmp_path / "corpus"
    coverage = ProvisionCoverageReport(
        jurisdiction="us",
        document_class="regulation",
        version="2026-04-29-title-7",
        source_count=1,
        provision_count=1,
        matched_count=1,
        missing_from_provisions=(),
        extra_provisions=(),
    )

    def fake_extract(*args, **kwargs):
        return EcfrExtractReport(
            title_count=1,
            part_count=1,
            provisions_written=1,
            inventory_path=base / "inventory/us/regulation/2026-04-29-title-7.json",
            provisions_path=base / "provisions/us/regulation/2026-04-29-title-7.jsonl",
            coverage_path=base / "coverage/us/regulation/2026-04-29-title-7.json",
            coverage=coverage,
            source_paths=(base / "sources/us/regulation/2026-04-29-title-7/ecfr/title-7.xml",),
        )

    monkeypatch.setattr(cli, "extract_ecfr", fake_extract)

    exit_code = main(
        [
            "extract-ecfr",
            "--base",
            str(base),
            "--version",
            "2026-04-29",
            "--as-of",
            "2024-04-16",
            "--only-title",
            "7",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"provisions_written": 1' in output


def test_inventory_usc_cli(tmp_path, capsys):
    base = tmp_path / "corpus"
    source_xml = tmp_path / "usc26.xml"
    source_xml.write_text(SAMPLE_USLM_CLI)

    exit_code = main(
        [
            "inventory-usc",
            "--base",
            str(base),
            "--run-id",
            "2026-04-29",
            "--source-xml",
            str(source_xml),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"items_written": 2' in output
    inventory = json.loads((base / "inventory/us/statute/2026-04-29-title-26.json").read_text())
    assert [item["citation_path"] for item in inventory["items"]] == [
        "us/statute/26",
        "us/statute/26/32",
    ]


def test_extract_usc_cli(tmp_path, capsys, monkeypatch):
    import axiom_corpus.corpus.cli as cli

    base = tmp_path / "corpus"
    source_xml = tmp_path / "usc26.xml"
    source_xml.write_text(SAMPLE_USLM_CLI)
    coverage = ProvisionCoverageReport(
        jurisdiction="us",
        document_class="statute",
        version="2026-04-29-title-26",
        source_count=2,
        provision_count=2,
        matched_count=2,
        missing_from_provisions=(),
        extra_provisions=(),
    )

    def fake_extract(*args, **kwargs):
        assert kwargs["source_xml"] == source_xml
        return UscExtractReport(
            title="26",
            title_count=1,
            section_count=1,
            provisions_written=2,
            inventory_path=base / "inventory/us/statute/2026-04-29-title-26.json",
            provisions_path=base / "provisions/us/statute/2026-04-29-title-26.jsonl",
            coverage_path=base / "coverage/us/statute/2026-04-29-title-26.json",
            coverage=coverage,
            source_paths=(base / "sources/us/statute/2026-04-29-title-26/uslm/usc26.xml",),
        )

    monkeypatch.setattr(cli, "extract_usc", fake_extract)

    exit_code = main(
        [
            "extract-usc",
            "--base",
            str(base),
            "--version",
            "2026-04-29",
            "--source-xml",
            str(source_xml),
            "--as-of",
            "2026-04-01",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"provisions_written": 2' in output


def test_extract_usc_dir_cli(tmp_path, capsys, monkeypatch):
    import axiom_corpus.corpus.cli as cli

    base = tmp_path / "corpus"
    source_dir = tmp_path / "uscode"
    source_dir.mkdir()
    coverage = ProvisionCoverageReport(
        jurisdiction="us",
        document_class="statute",
        version="2026-04-29",
        source_count=2,
        provision_count=2,
        matched_count=2,
        missing_from_provisions=(),
        extra_provisions=(),
    )

    def fake_extract_dir(*args, **kwargs):
        assert kwargs["source_dir"] == source_dir
        return UscExtractReport(
            title=None,
            title_count=53,
            section_count=1,
            provisions_written=2,
            inventory_path=base / "inventory/us/statute/2026-04-29.json",
            provisions_path=base / "provisions/us/statute/2026-04-29.jsonl",
            coverage_path=base / "coverage/us/statute/2026-04-29.json",
            coverage=coverage,
            source_paths=(base / "sources/us/statute/2026-04-29/uslm/usc26.xml",),
        )

    monkeypatch.setattr(cli, "extract_usc_directory", fake_extract_dir)

    exit_code = main(
        [
            "extract-usc-dir",
            "--base",
            str(base),
            "--version",
            "2026-04-29",
            "--source-dir",
            str(source_dir),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"title_count": 53' in output


def test_export_supabase_cli(tmp_path, capsys):
    from axiom_corpus.corpus.artifacts import CorpusArtifactStore

    store = CorpusArtifactStore(tmp_path / "corpus")
    provisions = store.provisions_path("us", "regulation", "2026-04-29")
    store.write_provisions(
        provisions,
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273",
                heading="Certification of Eligible Households",
            )
        ],
    )
    out = tmp_path / "supabase.jsonl"

    exit_code = main(["export-supabase", "--provisions", str(provisions), "--output", str(out)])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"rows_written": 1' in output
    assert json.loads(out.read_text())["doc_type"] == "regulation"


def test_load_supabase_cli_dry_run(tmp_path, capsys):
    from axiom_corpus.corpus.artifacts import CorpusArtifactStore

    store = CorpusArtifactStore(tmp_path / "corpus")
    provisions = store.provisions_path("us", "regulation", "2026-04-29")
    store.write_provisions(
        provisions,
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273",
                heading="Certification of Eligible Households",
            )
        ],
    )

    exit_code = main(["load-supabase", "--provisions", str(provisions), "--dry-run"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["rows_total"] == 1
    assert payload["rows_loaded"] == 0


def test_load_supabase_cli_replace_scope_dry_run(tmp_path, capsys):
    from axiom_corpus.corpus.artifacts import CorpusArtifactStore

    store = CorpusArtifactStore(tmp_path / "corpus")
    provisions = store.provisions_path("us-ga", "statute", "2022-11-01")
    store.write_provisions(
        provisions,
        [
            ProvisionRecord(
                jurisdiction="us-ga",
                document_class="statute",
                citation_path="us-ga/statute/1",
                heading="Title 1",
            )
        ],
    )

    exit_code = main(
        [
            "load-supabase",
            "--provisions",
            str(provisions),
            "--replace-scope",
            "--dry-run",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["replace_scope"]["dry_run"] is True
    assert payload["rows_total"] == 1
