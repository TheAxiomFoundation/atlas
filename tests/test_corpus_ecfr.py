from datetime import date
from urllib.error import HTTPError

import pytest

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.ecfr import (
    EcfrPartTarget,
    build_ecfr_inventory,
    build_ecfr_inventory_from_structures,
    extract_ecfr,
    iter_ecfr_title_provisions,
    part_targets_from_structure,
)
from axiom_corpus.corpus.io import load_provisions
from axiom_corpus.corpus.models import ProvisionRecord

SAMPLE_STRUCTURE = {
    "identifier": "7",
    "label": "Title 7-Agriculture",
    "type": "title",
    "children": [
        {
            "identifier": "II",
            "label": "Chapter II-Food and Nutrition Service",
            "type": "chapter",
            "children": [
                {
                    "identifier": "C",
                    "label": "Subchapter C-Food Stamp Program",
                    "type": "subchapter",
                    "children": [
                        {
                            "identifier": "273",
                            "label": "Part 273-Certification of Eligible Households",
                            "type": "part",
                            "children": [
                                {
                                    "identifier": "273.1",
                                    "label": "§ 273.1 Household concept.",
                                    "label_description": "Household concept.",
                                    "type": "section",
                                },
                                {
                                    "identifier": "273.2",
                                    "label": "§ 273.2 Application processing.",
                                    "label_description": "Application processing.",
                                    "type": "section",
                                },
                            ],
                        }
                    ],
                }
            ],
        }
    ],
}

SAMPLE_SUBPART_STRUCTURE = {
    "identifier": "7",
    "label": "Title 7-Agriculture",
    "type": "title",
    "children": [
        {
            "identifier": "273",
            "label": "Part 273-Certification of Eligible Households",
            "type": "part",
            "children": [
                {
                    "identifier": "A",
                    "label": "Subpart A-General",
                    "type": "subpart",
                    "children": [
                        {
                            "identifier": "273.1",
                            "label": "§ 273.1 Household concept.",
                            "label_description": "Household concept.",
                            "type": "section",
                        }
                    ],
                }
            ],
        }
    ],
}

SAMPLE_TITLE_XML = """
<ECFR>
  <DIV5 N="273" TYPE="PART">
    <HEAD>PART 273-CERTIFICATION OF ELIGIBLE HOUSEHOLDS</HEAD>
    <DIV8 N="§ 273.1" TYPE="SECTION" NODE="7:4.1.1.2.1.1.1.1">
      <HEAD>§ 273.1 Household concept.</HEAD>
      <P>(a) General household definition.</P>
      <P>(b) Special households.</P>
    </DIV8>
    <DIV8 N="§ 273.2" TYPE="SECTION" NODE="7:4.1.1.2.1.1.1.2">
      <HEAD>§ 273.2 Application processing.</HEAD>
      <P>(a) Application filing.</P>
    </DIV8>
  </DIV5>
</ECFR>
"""

SAMPLE_SUBPART_XML = """
<ECFR>
  <DIV5 N="273" TYPE="PART">
    <HEAD>PART 273-CERTIFICATION OF ELIGIBLE HOUSEHOLDS</HEAD>
    <DIV6 N="A" TYPE="SUBPART">
      <HEAD>Subpart A-General</HEAD>
      <DIV8 N="§ 273.1" TYPE="SECTION" NODE="7:4.1.1.2.1.1.1.1">
        <HEAD>§ 273.1 Household concept.</HEAD>
        <P>(a) General household definition.</P>
      </DIV8>
    </DIV6>
  </DIV5>
</ECFR>
"""


def test_part_targets_from_structure_preserve_ancestry():
    targets = part_targets_from_structure(SAMPLE_STRUCTURE)

    assert targets == (
        EcfrPartTarget(
            title=7,
            part="273",
            chapter="II",
            subchapter="C",
            label="Part 273-Certification of Eligible Households",
        ),
    )


def test_build_ecfr_inventory_from_structure_sections():
    inventory = build_ecfr_inventory_from_structures((SAMPLE_STRUCTURE,))

    assert inventory.title_count == 1
    assert inventory.part_count == 1
    assert [item.citation_path for item in inventory.items] == [
        "us/regulation/7/273",
        "us/regulation/7/273/1",
        "us/regulation/7/273/2",
    ]
    assert inventory.items[0].source_format == "ecfr-xml"
    assert inventory.items[0].metadata["kind"] == "part"


def test_build_ecfr_inventory_from_structure_includes_subparts():
    inventory = build_ecfr_inventory_from_structures(
        (SAMPLE_SUBPART_STRUCTURE,),
        run_id="2026-04-29-title-7-part-273",
        only_part="273",
        source_sha256_by_title={7: "abc123"},
    )

    assert [item.citation_path for item in inventory.items] == [
        "us/regulation/7/273",
        "us/regulation/7/273/subpart-a",
        "us/regulation/7/273/1",
    ]
    assert inventory.items[0].source_path == (
        "sources/us/regulation/2026-04-29-title-7-part-273/ecfr/title-7-part-273.xml"
    )
    assert inventory.items[0].sha256 == "abc123"


def test_iter_ecfr_title_provisions_builds_normalized_records():
    records = tuple(
        iter_ecfr_title_provisions(
            SAMPLE_TITLE_XML,
            (EcfrPartTarget(title=7, part="273", chapter="II", subchapter="C"),),
            version="2026-04-29",
            source_path="ecfr/title-7.xml",
        )
    )

    assert [record.citation_path for record in records] == [
        "us/regulation/7/273",
        "us/regulation/7/273/1",
        "us/regulation/7/273/2",
    ]
    assert records[0].kind == "part"
    assert records[0].body is None
    assert records[1].document_class == "regulation"
    assert records[1].heading == "Household concept"
    assert records[1].parent_citation_path == "us/regulation/7/273"
    assert records[1].level == 1
    assert "General household" in records[1].body


def test_iter_ecfr_title_provisions_builds_subpart_hierarchy():
    records = tuple(
        iter_ecfr_title_provisions(
            SAMPLE_SUBPART_XML,
            (EcfrPartTarget(title=7, part="273"),),
            version="2026-04-29",
            source_path="sources/us/regulation/2026-04-29/ecfr/title-7.xml",
        )
    )

    assert [record.citation_path for record in records] == [
        "us/regulation/7/273",
        "us/regulation/7/273/subpart-a",
        "us/regulation/7/273/1",
    ]
    assert records[2].parent_citation_path == "us/regulation/7/273/subpart-a"
    assert records[2].level == 2


def test_extract_ecfr_writes_source_inventory_provisions_and_coverage(tmp_path, monkeypatch):
    import axiom_corpus.corpus.ecfr as ecfr

    monkeypatch.setattr(ecfr, "fetch_ecfr_structure", lambda title, as_of: SAMPLE_STRUCTURE)
    monkeypatch.setattr(
        ecfr,
        "fetch_ecfr_title_xml",
        lambda title, as_of: pytest.fail("part-scoped extract fetched a full title"),
    )
    monkeypatch.setattr(ecfr, "fetch_ecfr_part_xml", lambda title, part, as_of: SAMPLE_TITLE_XML)
    store = CorpusArtifactStore(tmp_path / "corpus")
    run_id = "2026-04-29-title-7-part-273"
    store.write_provisions(
        store.provisions_path("us", "regulation", run_id),
        [
            ProvisionRecord(
                jurisdiction="us",
                document_class="regulation",
                citation_path="us/regulation/7/999",
                body="stale",
            )
        ],
    )

    report = extract_ecfr(
        store,
        version="2026-04-29",
        as_of="2024-04-16",
        expression_date=date(2024, 4, 16),
        only_title=7,
        only_part="273",
    )

    assert report.coverage.complete
    assert report.provisions_written == 3
    assert (store.root / f"sources/us/regulation/{run_id}/ecfr/title-7-part-273.xml").exists()
    assert (store.root / f"inventory/us/regulation/{run_id}.json").exists()
    assert (store.root / f"provisions/us/regulation/{run_id}.jsonl").exists()
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us/regulation/7/273",
        "us/regulation/7/273/1",
        "us/regulation/7/273/2",
    ]
    assert records[1].source_path == (
        "sources/us/regulation/2026-04-29-title-7-part-273/ecfr/title-7-part-273.xml"
    )
    assert records[1].source_as_of == "2024-04-16"
    assert records[1].expression_date == "2024-04-16"


def test_build_ecfr_inventory_skips_missing_titles_in_full_mode(monkeypatch):
    import axiom_corpus.corpus.ecfr as ecfr

    def fake_fetch(title, as_of):
        if title == 2:
            raise HTTPError("https://example.test", 404, "Not Found", {}, None)
        return {**SAMPLE_STRUCTURE, "identifier": str(title)}

    monkeypatch.setattr(ecfr, "DEFAULT_CFR_TITLES", (1, 2))
    monkeypatch.setattr(ecfr, "fetch_ecfr_structure", fake_fetch)

    inventory = build_ecfr_inventory(as_of="2024-04-16")

    assert inventory.title_count == 1
    assert len(inventory.items) == 3
