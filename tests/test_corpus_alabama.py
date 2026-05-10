import json

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.alabama import (
    ALABAMA_GRAPHQL_SOURCE_FORMAT,
    extract_alabama_code,
    parse_alabama_deflated_table,
)

SAMPLE_SCAFFOLD = "†∫codeId†parentId†displayId∫T1∫C1†T1†1∫S1†C1†1-1-1"
SAMPLE_TITLES = (
    "†∫codeId†title†sectionRange†effectiveDate"
    "∫T1†Title 1 General Provisions.††"
    "∫C1†Chapter 1 Construction of Code and Statutes.†§1-1-1 to §1-1-1†"
    "∫S1†Section 1-1-1 Meaning of Certain Words and Terms.††"
)


def _graphql_response(key, value):
    return json.dumps({"data": {key: value}}, sort_keys=True).encode("utf-8")


def _sections_response():
    return json.dumps(
        {
            "data": {
                "codesOfAlabama": {
                    "count": 1,
                    "data": [
                        {
                            "id": "100",
                            "codeId": "S1",
                            "displayId": "1-1-1",
                            "title": "Section 1-1-1 Meaning of Certain Words and Terms.",
                            "content": (
                                "<p>The words in this code have their ordinary meaning.</p>"
                                "<p>See Section 1-1-2 for related terms.</p>"
                            ),
                            "history": "(Code 1852, §1.)",
                            "parentId": "C1",
                            "type": "Section",
                            "isContentNode": True,
                            "sectionRange": None,
                            "effectiveDate": None,
                            "supersessionDate": None,
                        }
                    ],
                }
            }
        },
        sort_keys=True,
    ).encode("utf-8")


def test_parse_alabama_deflated_table():
    rows = parse_alabama_deflated_table(SAMPLE_SCAFFOLD)

    assert rows == (
        {"codeId": "T1", "parentId": None, "displayId": None},
        {"codeId": "C1", "parentId": "T1", "displayId": "1"},
        {"codeId": "S1", "parentId": "C1", "displayId": "1-1-1"},
    )


def test_extract_alabama_code_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / ALABAMA_GRAPHQL_SOURCE_FORMAT).mkdir(parents=True)
    (source_dir / ALABAMA_GRAPHQL_SOURCE_FORMAT / "scaffold.json").write_bytes(
        _graphql_response("scaffold", SAMPLE_SCAFFOLD)
    )
    (source_dir / ALABAMA_GRAPHQL_SOURCE_FORMAT / "titles.json").write_bytes(
        _graphql_response("titles", SAMPLE_TITLES)
    )
    (
        source_dir / ALABAMA_GRAPHQL_SOURCE_FORMAT / "sections-current-offset-0-limit-1.json"
    ).write_bytes(_sections_response())
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_alabama_code(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2026-05-09",
        expression_date="2026-05-09",
        limit=1,
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 1
    assert report.provisions_written == 3
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert len(inventory) == 3
    assert [record.citation_path for record in records] == [
        "us-al/statute/title-1",
        "us-al/statute/title-1/chapter-1",
        "us-al/statute/1-1-1",
    ]
    assert records[2].body is not None
    assert "ordinary meaning" in records[2].body
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == ["us-al/statute/1-1-2"]
