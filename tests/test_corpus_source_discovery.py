import json

from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope
from axiom_corpus.corpus.source_discovery import (
    DiscoveryDisposition,
    SourceStatus,
    build_source_discovery_report,
)


def test_source_discovery_classifies_static_external_urls(tmp_path):
    urls = tmp_path / "policyengine_urls.txt"
    urls.write_text(
        "\n".join(
            [
                "https://www.irs.gov/instructions/i1040gi?utm_source=test",
                "https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines",
                "https://ftb.ca.gov/forms/misc/1001.pdf#page=2",
                "https://law.cornell.edu/cfr/text/26/1.402(g)-1#old",
                "https://law.cornell.edu/cfr/text/26/1.402(g)-1#new",
                "https://advance.lexis.com/documentpage/?crid=abc",
                "https://docs.google.com/spreadsheets/d/example/edit",
                "not a url",
            ]
        )
    )
    release = ReleaseManifest(
        name="current",
        scopes=(ReleaseScope("us-ca", "form", "2026-05-11"),),
    )

    report = build_source_discovery_report(
        (urls,),
        release=release,
        generated_at="2026-05-11T12:00:00+00:00",
    )
    rows = {row.host: row for row in report.rows}

    assert report.raw_url_count == 8
    assert report.invalid_url_count == 1
    assert report.unique_url_count == 6
    assert rows["irs.gov"].source_status is SourceStatus.PRIMARY_OFFICIAL
    assert rows["irs.gov"].disposition is DiscoveryDisposition.READY_FOR_MANIFEST
    assert rows["irs.gov"].document_class == "form"
    assert rows["irs.gov"].jurisdiction == "us"
    assert rows["aspe.hhs.gov"].source_status is SourceStatus.PRIMARY_OFFICIAL
    assert rows["aspe.hhs.gov"].disposition is DiscoveryDisposition.READY_FOR_MANIFEST
    assert rows["aspe.hhs.gov"].document_class == "guidance"
    assert rows["aspe.hhs.gov"].jurisdiction == "us"
    assert rows["ftb.ca.gov"].release_scope_present is True
    assert rows["law.cornell.edu"].source_status is SourceStatus.SECONDARY_MIRROR
    assert rows["law.cornell.edu"].disposition is DiscoveryDisposition.EXCLUDED_SECONDARY
    assert rows["law.cornell.edu"].input_count == 2
    assert rows["advance.lexis.com"].disposition is DiscoveryDisposition.BLOCKED_VENDOR_ONLY
    assert rows["docs.google.com"].disposition is DiscoveryDisposition.NEEDS_REVIEW

    payload = report.to_mapping()
    assert payload["ready_for_manifest_count"] == 3
    assert payload["needs_review_count"] == 1
    assert payload["blocked_or_excluded_count"] == 2
    assert payload["release_scope_present_count"] == 1
    assert payload["source_status_counts"]["primary_official"] == 3
    assert payload["ready_group_count"] == 2
    assert [row["group_key"] for row in payload["group_rows"]] == [
        "us/form/individual_income_tax_forms",
        "us/guidance/poverty_guidelines",
    ]
    assert payload["group_rows"][0]["suggested_manifest_stem"] == (
        "us-individual-income-tax-forms"
    )


def test_source_discovery_cli_writes_report(tmp_path, capsys):
    source = tmp_path / "state_references.txt"
    source.write_text("https://leg.colorado.gov/colorado-revised-statutes\n")
    output = tmp_path / "analytics" / "source-discovery-current.json"

    exit_code = main(
        [
            "source-discovery",
            "--base",
            str(tmp_path),
            "--input",
            str(source),
            "--release",
            "",
            "--output",
            str(output),
        ]
    )
    printed = json.loads(capsys.readouterr().out)
    written = json.loads(output.read_text())

    assert exit_code == 0
    assert printed["written_to"] == str(output)
    assert written["unique_url_count"] == 1
    assert written["ready_group_count"] == 1
    assert written["group_rows"][0]["group_key"] == "us-co/statute/statute"
    assert written["rows"][0]["jurisdiction"] == "us-co"
    assert written["rows"][0]["disposition"] == "ready_for_manifest"
