import json

from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.coverage import ProvisionCoverageReport
from axiom_corpus.corpus.documents import OfficialDocumentExtractReport
from axiom_corpus.corpus.ecfr import EcfrExtractReport, EcfrInventory
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
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


def test_extract_official_documents_cli(tmp_path, capsys, monkeypatch):
    import axiom_corpus.corpus.cli as cli

    base = tmp_path / "corpus"
    manifest_path = tmp_path / "documents.yaml"
    manifest_path.write_text("documents: []\n")
    coverage = ProvisionCoverageReport(
        jurisdiction="us-co",
        document_class="policy",
        version="2026-04-30",
        source_count=1,
        provision_count=1,
        matched_count=1,
        missing_from_provisions=(),
        extra_provisions=(),
    )

    def fake_extract(*args, **kwargs):
        assert kwargs["manifest_path"] == manifest_path
        assert kwargs["source_as_of"] == "2026-04-30"
        return OfficialDocumentExtractReport(
            jurisdiction="us-co",
            document_class="policy",
            document_count=1,
            block_count=3,
            provisions_written=4,
            inventory_path=base / "inventory/us-co/policy/2026-04-30.json",
            provisions_path=base / "provisions/us-co/policy/2026-04-30.jsonl",
            coverage_path=base / "coverage/us-co/policy/2026-04-30.json",
            coverage=coverage,
            source_paths=(base / "sources/us-co/policy/2026-04-30/doc.pdf",),
        )

    monkeypatch.setattr(cli, "extract_official_documents", fake_extract)

    exit_code = main(
        [
            "extract-official-documents",
            "--base",
            str(base),
            "--version",
            "2026-04-30",
            "--manifest",
            str(manifest_path),
            "--as-of",
            "2026-04-30",
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"document_class": "policy"' in output
    assert '"block_count": 3' in output


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


def test_snapshot_provision_counts_cli_writes_supabase_counts(tmp_path, capsys, monkeypatch):
    import axiom_corpus.corpus.cli as cli

    out = tmp_path / "provision-counts.json"
    monkeypatch.setattr(
        cli,
        "resolve_service_key",
        lambda *args, **kwargs: "service",
    )
    monkeypatch.setattr(
        cli,
        "fetch_provision_counts",
        lambda **kwargs: (
            {
                "jurisdiction": "us-wa",
                "document_class": "statute",
                "provision_count": 54631,
            },
        ),
    )

    exit_code = main(["snapshot-provision-counts", "--output", str(out)])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["written_to"] == str(out)
    assert json.loads(out.read_text()) == {
        "rows": [
            {
                "document_class": "statute",
                "jurisdiction": "us-wa",
                "provision_count": 54631,
            }
        ]
    }


def test_extract_state_statutes_batch_cli(tmp_path, capsys, monkeypatch):
    import axiom_corpus.corpus.cli as cli

    base = tmp_path / "corpus"
    html_release = tmp_path / "release76.2021.05.21"
    odt_release = tmp_path / "release90.2023.03"
    html_release.mkdir()
    odt_release.mkdir()
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        f"""
version: "2026-04-29"
sources:
  - source_id: us-tn-tca
    jurisdiction: us-tn
    document_class: statute
    adapter: cic-html
    version: "2026-04-29"
    options:
      release_dir: {html_release.name}
      source_as_of: "2021-05-21"
  - source_id: us-va-code
    jurisdiction: us-va
    document_class: statute
    adapter: cic-odt
    version: "2026-04-29"
    options:
      release_dir: {odt_release.name}
      source_as_of: "2023-03-01"
"""
    )
    coverage = ProvisionCoverageReport(
        jurisdiction="us-tn",
        document_class="statute",
        version="2026-04-29",
        source_count=1,
        provision_count=1,
        matched_count=1,
        missing_from_provisions=(),
        extra_provisions=(),
    )

    def fake_html(*args, **kwargs):
        assert kwargs["jurisdiction"] == "us-tn"
        assert kwargs["release_dir"] == html_release
        assert kwargs["source_as_of"] == "2021-05-21"
        return StateStatuteExtractReport(
            jurisdiction="us-tn",
            title_count=1,
            container_count=0,
            section_count=1,
            provisions_written=1,
            inventory_path=base / "inventory/us-tn/statute/2026-04-29.json",
            provisions_path=base / "provisions/us-tn/statute/2026-04-29.jsonl",
            coverage_path=base / "coverage/us-tn/statute/2026-04-29.json",
            coverage=coverage,
            source_paths=(base / "sources/us-tn/statute/2026-04-29/title.html",),
        )

    def fake_odt(*args, **kwargs):
        assert kwargs["jurisdiction"] == "us-va"
        assert kwargs["release_dir"] == odt_release
        assert kwargs["source_as_of"] == "2023-03-01"
        return StateStatuteExtractReport(
            jurisdiction="us-va",
            title_count=1,
            container_count=0,
            section_count=1,
            provisions_written=2,
            inventory_path=base / "inventory/us-va/statute/2026-04-29.json",
            provisions_path=base / "provisions/us-va/statute/2026-04-29.jsonl",
            coverage_path=base / "coverage/us-va/statute/2026-04-29.json",
            coverage=ProvisionCoverageReport(
                jurisdiction="us-va",
                document_class="statute",
                version="2026-04-29",
                source_count=2,
                provision_count=2,
                matched_count=2,
                missing_from_provisions=(),
                extra_provisions=(),
            ),
            source_paths=(base / "sources/us-va/statute/2026-04-29/title.odt",),
        )

    monkeypatch.setattr(cli, "extract_cic_html_release", fake_html)
    monkeypatch.setattr(cli, "extract_cic_odt_release", fake_odt)

    exit_code = main(
        [
            "extract-state-statutes",
            "--base",
            str(base),
            "--manifest",
            str(manifest),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["source_count"] == 2
    assert payload["completed_count"] == 2
    assert payload["provisions_written"] == 3
    assert payload["coverage_complete"] is True


def test_extract_state_statutes_batch_dry_run_reports_missing_sources(tmp_path, capsys):
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        """
version: "2026-04-29"
sources:
  - source_id: us-tn-tca
    jurisdiction: us-tn
    document_class: statute
    adapter: cic-html
    options:
      release_dir: missing-release
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

    assert exit_code == 1
    assert payload["dry_run"] is True
    assert payload["rows"][0]["source_path_exists"] is False


def test_extract_state_statutes_batch_dry_run_allows_live_texas_source(tmp_path, capsys):
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        """
version: "2026-05-01"
sources:
  - source_id: us-tx-statutes
    jurisdiction: us-tx
    document_class: statute
    adapter: texas-tcas
    source_url: https://statutes.capitol.texas.gov/
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
    assert payload["rows"][0]["adapter"] == "texas-tcas"
    assert payload["rows"][0]["source_path"] is None
    assert payload["rows"][0]["source_path_exists"] is True


def test_extract_state_statutes_batch_dry_run_allows_live_ohio_source(tmp_path, capsys):
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        """
version: "2026-05-01"
sources:
  - source_id: us-oh-revised-code
    jurisdiction: us-oh
    document_class: statute
    adapter: ohio-revised-code
    source_url: https://codes.ohio.gov/ohio-revised-code
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
    assert payload["rows"][0]["adapter"] == "ohio-revised-code"
    assert payload["rows"][0]["source_path"] is None
    assert payload["rows"][0]["source_path_exists"] is True


def test_extract_state_statutes_batch_dry_run_allows_live_minnesota_source(tmp_path, capsys):
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        """
version: "2026-05-01"
sources:
  - source_id: us-mn-statutes
    jurisdiction: us-mn
    document_class: statute
    adapter: minnesota-statutes
    source_url: https://www.revisor.mn.gov/statutes/
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
    assert payload["rows"][0]["adapter"] == "minnesota-statutes"
    assert payload["rows"][0]["source_path"] is None
    assert payload["rows"][0]["source_path_exists"] is True


def test_extract_state_statutes_batch_dry_run_allows_live_nebraska_source(tmp_path, capsys):
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        """
version: "2026-05-04"
sources:
  - source_id: us-ne-revised-statutes
    jurisdiction: us-ne
    document_class: statute
    adapter: nebraska-revised-statutes
    source_url: https://nebraskalegislature.gov/laws/browse-statutes.php
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
    assert payload["rows"][0]["adapter"] == "nebraska-revised-statutes"
    assert payload["rows"][0]["source_path"] is None
    assert payload["rows"][0]["source_path_exists"] is True


def test_extract_state_statutes_batch_dry_run_allows_live_washington_source(tmp_path, capsys):
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        """
version: "2026-05-04"
sources:
  - source_id: us-wa-rcw
    jurisdiction: us-wa
    document_class: statute
    adapter: washington-rcw
    source_url: https://app.leg.wa.gov/RCW/default.aspx
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
    assert payload["rows"][0]["adapter"] == "washington-rcw"
    assert payload["rows"][0]["source_path"] is None
    assert payload["rows"][0]["source_path_exists"] is True


def test_extract_state_statutes_batch_dry_run_checks_california_source_zip(tmp_path, capsys):
    source_zip = tmp_path / "pubinfo_2025.zip"
    source_zip.write_bytes(b"zip placeholder")
    manifest = tmp_path / "state-statutes.yaml"
    manifest.write_text(
        f"""
version: "2026-05-01"
sources:
  - source_id: us-ca-codes
    jurisdiction: us-ca
    document_class: statute
    adapter: california-codes-bulk
    source_url: https://downloads.leginfo.legislature.ca.gov/pubinfo_2025.zip
    options:
      source_zip: {source_zip.name}
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
    assert payload["rows"][0]["adapter"] == "california-codes-bulk"
    assert payload["rows"][0]["source_path"] == str(source_zip)
    assert payload["rows"][0]["source_path_exists"] is True


def test_artifact_report_cli_accepts_release_name(tmp_path, capsys):
    from axiom_corpus.corpus.artifacts import CorpusArtifactStore

    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-co/policy/doc")],
    )
    store.write_inventory(
        store.inventory_path("us-ny", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-ny/policy/doc")],
    )
    release_dir = store.root / "releases"
    release_dir.mkdir(parents=True)
    (release_dir / "current.json").write_text(
        json.dumps(
            {
                "name": "current",
                "scopes": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "policy",
                        "version": "2026-04-30",
                    }
                ],
            }
        )
    )

    exit_code = main(
        [
            "artifact-report",
            "--base",
            str(store.root),
            "--prefix",
            "inventory",
            "--release",
            "current",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["release"] == "current"
    assert payload["release_scope_count"] == 1
    assert payload["scope_count"] == 1
    assert payload["local_count"] == 1
    assert payload["rows"][0]["jurisdiction"] == "us-co"


def test_artifact_report_cli_defaults_to_current_release(tmp_path, capsys):
    from axiom_corpus.corpus.artifacts import CorpusArtifactStore

    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-co/policy/doc")],
    )
    store.write_inventory(
        store.inventory_path("us-ny", "policy", "2026-04-30"),
        [SourceInventoryItem(citation_path="us-ny/policy/doc")],
    )
    release_dir = store.root / "releases"
    release_dir.mkdir(parents=True)
    (release_dir / "current.json").write_text(
        json.dumps(
            {
                "name": "current",
                "scopes": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "policy",
                        "version": "2026-04-30",
                    }
                ],
            }
        )
    )

    exit_code = main(
        [
            "artifact-report",
            "--base",
            str(store.root),
            "--prefix",
            "inventory",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["release"] == "current"
    assert payload["scope_count"] == 1
    assert payload["rows"][0]["jurisdiction"] == "us-co"

    exit_code = main(
        [
            "artifact-report",
            "--base",
            str(store.root),
            "--prefix",
            "inventory",
            "--all-scopes",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert "release" not in payload
    assert payload["scope_count"] == 2


def test_release_artifact_manifest_cli_writes_digest_manifest(tmp_path, capsys):
    from axiom_corpus.corpus.artifacts import CorpusArtifactStore

    store = CorpusArtifactStore(tmp_path / "corpus")
    source = store.source_path("us-co", "policy", "2026-04-30", "source.html")
    source_sha = store.write_text(source, "<p>Text.</p>")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [
            SourceInventoryItem(
                citation_path="us-co/policy/doc",
                source_path=source.relative_to(store.root).as_posix(),
                sha256=source_sha,
            )
        ],
    )
    store.write_provisions(
        store.provisions_path("us-co", "policy", "2026-04-30"),
        [
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="policy",
                citation_path="us-co/policy/doc",
                version="2026-04-30",
                body="Text.",
                source_as_of="2026-04-30",
                expression_date="2026-04-30",
            )
        ],
    )
    store.write_json(
        store.coverage_path("us-co", "policy", "2026-04-30"),
        {
            "complete": True,
            "source_count": 1,
            "provision_count": 1,
            "matched_count": 1,
            "missing_from_provisions": [],
            "extra_provisions": [],
        },
    )
    release_dir = store.root / "releases"
    release_dir.mkdir(parents=True)
    (release_dir / "current.json").write_text(
        json.dumps(
            {
                "name": "current",
                "scopes": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "policy",
                        "version": "2026-04-30",
                    }
                ],
            }
        )
    )
    output = store.root / "releases" / "current-artifacts.json"

    exit_code = main(
        [
            "release-artifact-manifest",
            "--base",
            str(store.root),
            "--release",
            "current",
            "--output",
            str(output),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    written = json.loads(output.read_text())

    assert exit_code == 0
    assert payload["artifact_count"] == 4
    assert payload["written_to"] == str(output)
    assert written["artifacts"][0]["sha256"]


def test_validate_release_cli_gates_release(tmp_path, capsys):
    from axiom_corpus.corpus.artifacts import CorpusArtifactStore

    store = CorpusArtifactStore(tmp_path / "corpus")
    source = store.source_path("us-co", "policy", "2026-04-30", "source.html")
    source_sha = store.write_text(source, "<p>Text.</p>")
    store.write_inventory(
        store.inventory_path("us-co", "policy", "2026-04-30"),
        [
            SourceInventoryItem(
                citation_path="us-co/policy/doc",
                source_path=source.relative_to(store.root).as_posix(),
                sha256=source_sha,
            )
        ],
    )
    store.write_provisions(
        store.provisions_path("us-co", "policy", "2026-04-30"),
        [
            ProvisionRecord(
                jurisdiction="us-co",
                document_class="policy",
                citation_path="us-co/policy/doc",
                version="2026-04-30",
                body="Text.",
                source_as_of="2026-04-30",
                expression_date="2026-04-30",
            )
        ],
    )
    store.write_json(
        store.coverage_path("us-co", "policy", "2026-04-30"),
        {
            "complete": True,
            "source_count": 1,
            "provision_count": 1,
            "matched_count": 1,
            "missing_from_provisions": [],
            "extra_provisions": [],
        },
    )
    release_dir = store.root / "releases"
    release_dir.mkdir(parents=True)
    (release_dir / "current.json").write_text(
        json.dumps(
            {
                "name": "current",
                "scopes": [
                    {
                        "jurisdiction": "us-co",
                        "document_class": "policy",
                        "version": "2026-04-30",
                    }
                ],
            }
        )
    )

    exit_code = main(["validate-release", "--base", str(store.root), "--release", "current"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["error_count"] == 0
    assert payload["warning_count"] == 0
