import json

from axiom_corpus.corpus.analytics import (
    build_analytics_report,
    load_provision_count_snapshot,
)
from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem


def test_analytics_report_groups_source_provision_and_supabase_counts(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    store.write_inventory(
        store.inventory_path("us", "regulation", "2026-04-29"),
        [
            SourceInventoryItem(citation_path="us/regulation/7/273/1"),
            SourceInventoryItem(citation_path="us/regulation/7/273/2"),
        ],
    )
    store.write_provisions(
        store.provisions_path("us", "regulation", "2026-04-29"),
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
                body="Text.",
            )
        ],
    )

    report = build_analytics_report(
        store,
        version="2026-04-29",
        provision_counts={("us", "regulation"): 3},
    )

    assert report.totals_by_document_class()["regulation"]["source_count"] == 2
    assert report.totals_by_document_class()["regulation"]["provision_count"] == 1
    assert report.totals_by_document_class()["regulation"]["missing_count"] == 1
    assert report.totals()["supabase_count"] == 3


def test_load_provision_count_snapshot_supports_doc_type_rows(tmp_path):
    snapshot = tmp_path / "counts.json"
    snapshot.write_text(
        json.dumps(
            {
                "rows": [
                    {"jurisdiction": "us", "doc_type": "statute", "provision_count": 3},
                    {"jurisdiction": "us", "document_class": "regulation", "count": 5},
                    {"jurisdiction": "us-tn", "count": 7},
                ]
            }
        )
    )

    counts = load_provision_count_snapshot(snapshot)

    assert counts == {
        ("us", "statute"): 3,
        ("us", "regulation"): 5,
        ("us-tn", "statute"): 7,
    }
