from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem


def test_store_writes_inventory_and_provision_jsonl(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    inventory_path = store.inventory_path("us", "regulation", "2026-04-29")
    provisions_path = store.provisions_path("us", "regulation", "2026-04-29")

    store.write_inventory(
        inventory_path,
        [SourceInventoryItem(citation_path="us/regulation/7/273/1")],
    )
    store.write_provisions(
        provisions_path,
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
                body="Text.",
            )
        ],
    )

    assert load_source_inventory(inventory_path)[0].citation_path == "us/regulation/7/273/1"
    assert load_provisions(provisions_path)[0].body == "Text."


def test_compare_provision_coverage_reports_missing_and_extra():
    report = compare_provision_coverage(
        (
            SourceInventoryItem(citation_path="us/regulation/7/273/1"),
            SourceInventoryItem(citation_path="us/regulation/7/273/2"),
        ),
        (
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/1",
                body="Text.",
            ),
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/273/9",
                body="Text.",
            ),
        ),
        jurisdiction="us",
        document_class="regulation",
        version="2026-04-29",
    )

    assert report.matched_count == 1
    assert report.missing_from_provisions == ("us/regulation/7/273/2",)
    assert report.extra_provisions == ("us/regulation/7/273/9",)
    assert not report.complete
