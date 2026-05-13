import json

from axiom_corpus.corpus.models import CorpusManifest, ProvisionRecord
from axiom_corpus.corpus.supabase import SUPABASE_PROVISIONS_COLUMNS


def test_manifest_loads_source_first_contract(tmp_path):
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        """
version: "2026-04-29"
sources:
  - source_id: us-ecfr
    jurisdiction: us
    document_class: regulation
    adapter: ecfr
"""
    )

    manifest = CorpusManifest.load(manifest_path)

    assert manifest.version == "2026-04-29"
    assert manifest.sources[0].adapter == "ecfr"
    assert json.loads(manifest.to_json())["sources"][0]["document_class"] == "regulation"


def test_provision_record_maps_to_supabase_shape():
    record = ProvisionRecord(
        jurisdiction="us",
        document_class="regulation",
        citation_path="us/regulation/7/273/1",
        heading="Household concept",
        body="General household definition.",
        source_url="https://www.ecfr.gov/current/title-7/part-273#p-273.1",
        source_format="ecfr-xml",
        parent_citation_path="us/regulation/7/273",
        level=1,
        source_as_of="2024-04-16",
        expression_date="2024-04-16",
        legal_identifier="7 CFR 273.1",
        identifiers={"ecfr:title": "7", "ecfr:part": "273", "ecfr:section": "1"},
    )

    row = record.to_supabase_row()

    assert tuple(row) == SUPABASE_PROVISIONS_COLUMNS
    assert row["doc_type"] == "regulation"
    assert row["citation_path"] == "us/regulation/7/273/1"
    assert row["version"] is None
    assert "source_url" in row
    assert "source_id" not in row
    assert "source_format" not in row
    assert "metadata" not in row
    assert row["legal_identifier"] == "7 CFR 273.1"
    assert row["identifiers"] == {"ecfr:title": "7", "ecfr:part": "273", "ecfr:section": "1"}
