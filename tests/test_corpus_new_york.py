import json
from pathlib import Path

import pytest

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.new_york import (
    extract_new_york_consolidated_laws,
    extract_new_york_openleg_api,
    parse_new_york_law_index,
    parse_new_york_law_page,
    parse_new_york_openleg_laws,
)

SAMPLE_INDEX = """<!doctype html>
<html>
<body>
  <main>
    <h2>Consolidated Laws of New York</h2>
    <a href="https://www.nysenate.gov/legislation/laws/ABC">ABC Alcoholic Beverage Control</a>
    <a href="https://www.nysenate.gov/legislation/laws/TAX">TAX Tax</a>
    <a href="/legislation/laws/CONSOLIDATED">Consolidated Laws of New York</a>
  </main>
</body>
</html>
"""

SAMPLE_TAX = """<!doctype html>
<html>
<body>
  <div class="nys-openleg-result-title">
    <h2 class="nys-openleg-result-title-headline">CHAPTER 60</h2>
    <h3 class="nys-openleg-result-title-short">Tax</h3>
  </div>
  <div class="nys-openleg-history-container">Viewing most recent revision (from 2026-01-30)</div>
  <div class="nys-openleg-items-container">
    <a class="nys-openleg-result-item-link" href="https://www.nysenate.gov/legislation/laws/TAX/A22">
      ARTICLE 22 Personal Income Tax
    </a>
  </div>
</body>
</html>
"""

SAMPLE_ARTICLE = """<!doctype html>
<html>
<body>
  <ol class="nys-openleg-result-breadcrumbs-container">
    <li><span class="nys-openleg-result-breadcrumb-name">CHAPTER 60</span></li>
  </ol>
  <div class="nys-openleg-result-title">
    <h2 class="nys-openleg-result-title-headline">ARTICLE 22</h2>
    <h3 class="nys-openleg-result-title-short">Personal Income Tax</h3>
    <h4 class="nys-openleg-result-title-location">Tax (TAX) CHAPTER 60</h4>
  </div>
  <div class="nys-openleg-history-container">Viewing most recent revision (from 2025-05-16)</div>
  <div class="nys-openleg-items-container">
    <a class="nys-openleg-result-item-link" href="https://www.nysenate.gov/legislation/laws/TAX/A22P1">
      PART 1 General
    </a>
  </div>
</body>
</html>
"""

SAMPLE_PART = """<!doctype html>
<html>
<body>
  <ol class="nys-openleg-result-breadcrumbs-container">
    <li><span class="nys-openleg-result-breadcrumb-name">CHAPTER 60</span></li>
    <li><span class="nys-openleg-result-breadcrumb-name">ARTICLE 22</span></li>
  </ol>
  <div class="nys-openleg-result-title">
    <h2 class="nys-openleg-result-title-headline">PART 1</h2>
    <h3 class="nys-openleg-result-title-short">General</h3>
  </div>
  <div class="nys-openleg-history-container">Viewing most recent revision (from 2014-09-22)</div>
  <div class="nys-openleg-items-container">
    <a class="nys-openleg-result-item-link" href="https://www.nysenate.gov/legislation/laws/TAX/601">
      SECTION 601 Imposition of tax
    </a>
    <a class="nys-openleg-result-item-link" href="https://www.nysenate.gov/legislation/laws/TAX/602">
      SECTION 602 Income tax rates
    </a>
  </div>
</body>
</html>
"""

SAMPLE_SECTION = """<!doctype html>
<html>
<body>
  <ol class="nys-openleg-result-breadcrumbs-container">
    <li><span class="nys-openleg-result-breadcrumb-name">CHAPTER 60</span></li>
    <li><span class="nys-openleg-result-breadcrumb-name">ARTICLE 22</span></li>
    <li><span class="nys-openleg-result-breadcrumb-name">PART 1</span></li>
  </ol>
  <div class="nys-openleg-result-title">
    <h2 class="nys-openleg-result-title-headline">SECTION 601</h2>
    <h3 class="nys-openleg-result-title-short">Imposition of tax</h3>
    <h4 class="nys-openleg-result-title-location">Tax (TAX) CHAPTER 60, ARTICLE 22, PART 1</h4>
  </div>
  <div class="nys-openleg-history-container">Viewing most recent revision (from 2025-07-11)</div>
  <div class="nys-openleg-content-container">
    <div class="nys-openleg-result-text">
      § 601. Imposition of tax.
      <p>(a) Resident married individuals filing joint returns are subject to tax.</p>
    </div>
  </div>
  <div class="nys-openleg-items-container"></div>
</body>
</html>
"""

SAMPLE_SECTION_602 = """<!doctype html>
<html>
<body>
  <ol class="nys-openleg-result-breadcrumbs-container">
    <li><span class="nys-openleg-result-breadcrumb-name">CHAPTER 60</span></li>
    <li><span class="nys-openleg-result-breadcrumb-name">ARTICLE 22</span></li>
    <li><span class="nys-openleg-result-breadcrumb-name">PART 1</span></li>
  </ol>
  <div class="nys-openleg-result-title">
    <h2 class="nys-openleg-result-title-headline">SECTION 602</h2>
    <h3 class="nys-openleg-result-title-short">Income tax rates</h3>
  </div>
  <div class="nys-openleg-history-container">Viewing most recent revision (from 2025-07-11)</div>
  <div class="nys-openleg-content-container">
    <div class="nys-openleg-result-text">
      § 602. Income tax rates.
      <p>The tax shall be computed using the rates in this section.</p>
    </div>
  </div>
  <div class="nys-openleg-items-container"></div>
</body>
</html>
"""

SAMPLE_OPENLEG_LAWS = {
    "success": True,
    "result": {
        "items": [
            {
                "lawId": "ABC",
                "name": "Alcoholic Beverage Control",
                "lawType": "CONSOLIDATED",
                "chapter": "3-B",
            },
            {
                "lawId": "TAX",
                "name": "Tax",
                "lawType": "CONSOLIDATED",
                "chapter": "60",
            },
            {
                "lawId": "SENRULE",
                "name": "Senate Rules",
                "lawType": "MISC",
            },
        ]
    },
}

SAMPLE_OPENLEG_TAX = {
    "success": True,
    "result": {
        "info": {
            "lawId": "TAX",
            "name": "Tax",
            "lawType": "CONSOLIDATED",
            "chapter": "60",
        },
        "documents": {
            "locationId": "-CH60",
            "docType": "CHAPTER",
            "title": "Tax",
            "documents": {
                "items": [
                    {
                        "locationId": "A22",
                        "docType": "ARTICLE",
                        "docLevelId": "22",
                        "title": "Personal Income Tax",
                        "sequenceNo": 1,
                        "documents": {
                            "items": [
                                {
                                    "locationId": "601",
                                    "docType": "SECTION",
                                    "docLevelId": "601",
                                    "title": "Imposition of tax",
                                    "text": "§ 601. Imposition of tax. Resident tax text.",
                                    "activeDate": "2025-07-11",
                                    "sequenceNo": 1,
                                }
                            ]
                        },
                    }
                ]
            },
        },
    },
}


def _write_new_york_fixture_tree(base: Path) -> Path:
    source_dir = base / "source"
    html_dir = source_dir / "new-york-senate-html"
    (html_dir / "TAX").mkdir(parents=True)
    (html_dir / "CONSOLIDATED.html").write_text(SAMPLE_INDEX, encoding="utf-8")
    (html_dir / "TAX" / "index.html").write_text(SAMPLE_TAX, encoding="utf-8")
    (html_dir / "TAX" / "A22.html").write_text(SAMPLE_ARTICLE, encoding="utf-8")
    (html_dir / "TAX" / "A22P1.html").write_text(SAMPLE_PART, encoding="utf-8")
    (html_dir / "TAX" / "601.html").write_text(SAMPLE_SECTION, encoding="utf-8")
    (html_dir / "TAX" / "602.html").write_text(SAMPLE_SECTION_602, encoding="utf-8")
    return source_dir


def _write_new_york_openleg_fixture_tree(base: Path) -> Path:
    source_dir = base / "source"
    json_dir = source_dir / "new-york-openleg-json"
    json_dir.mkdir(parents=True)
    (json_dir / "laws.json").write_text(json.dumps(SAMPLE_OPENLEG_LAWS), encoding="utf-8")
    (json_dir / "TAX.json").write_text(json.dumps(SAMPLE_OPENLEG_TAX), encoding="utf-8")
    return source_dir


def test_parse_new_york_law_index_extracts_laws_and_skips_index_link():
    laws = parse_new_york_law_index(SAMPLE_INDEX)

    assert [law.law_id for law in laws] == ["ABC", "TAX"]
    assert laws[1].name == "Tax"
    assert laws[1].citation_path == "us-ny/statute/TAX"


def test_parse_new_york_law_page_extracts_children_and_section_body():
    article = parse_new_york_law_page(
        SAMPLE_ARTICLE,
        source_url="https://www.nysenate.gov/legislation/laws/TAX/A22",
    )

    assert article.kind == "article"
    assert article.display_number == "22"
    assert article.heading == "Personal Income Tax"
    assert article.revision_date == "2025-05-16"
    assert article.child_links[0].location_id == "A22P1"

    section = parse_new_york_law_page(
        SAMPLE_SECTION,
        source_url="https://www.nysenate.gov/legislation/laws/TAX/601",
    )

    assert section.kind == "section"
    assert section.citation_path == "us-ny/statute/TAX/601"
    assert section.body is not None
    assert "Resident married individuals" in section.body
    assert section.breadcrumb_labels == ("CHAPTER 60", "ARTICLE 22", "PART 1")


def test_parse_new_york_openleg_laws_keeps_consolidated_laws():
    laws = parse_new_york_openleg_laws(json.dumps(SAMPLE_OPENLEG_LAWS))

    assert [law.law_id for law in laws] == ["ABC", "TAX"]
    assert laws[1].name == "Tax"
    assert laws[1].citation_path == "us-ny/statute/TAX"


def test_parse_new_york_openleg_laws_rejects_unsuccessful_response():
    payload = {
        "success": False,
        "message": "Invalid API key",
    }

    with pytest.raises(ValueError, match="OpenLegislation API response was unsuccessful"):
        parse_new_york_openleg_laws(json.dumps(payload))


def test_extract_new_york_openleg_api_writes_source_inventory_and_provisions(tmp_path):
    source_dir = _write_new_york_openleg_fixture_tree(tmp_path)
    store = CorpusArtifactStore(tmp_path / "artifacts")

    report = extract_new_york_openleg_api(
        store,
        version="2026-test",
        source_dir=source_dir,
        only_title="TAX",
        source_as_of="2026-01-30",
        expression_date="2026-01-30",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 1
    assert report.provisions_written == 3

    inventory = load_source_inventory(report.inventory_path)
    provisions = load_provisions(report.provisions_path)
    assert [item.source_format for item in inventory] == [
        "new-york-openleg-json",
        "new-york-openleg-json",
        "new-york-openleg-json",
    ]
    assert [provision.citation_path for provision in provisions] == [
        "us-ny/statute/TAX",
        "us-ny/statute/TAX/A22",
        "us-ny/statute/TAX/601",
    ]
    assert provisions[-1].body == "§ 601. Imposition of tax. Resident tax text."
    assert provisions[-1].parent_citation_path == "us-ny/statute/TAX/A22"


def test_extract_new_york_openleg_api_fails_when_selected_law_has_no_documents(tmp_path):
    source_dir = _write_new_york_openleg_fixture_tree(tmp_path)
    tax_payload = {
        "success": True,
        "result": {
            "info": {
                "lawId": "TAX",
                "name": "Tax",
                "lawType": "CONSOLIDATED",
                "chapter": "60",
            }
        },
    }
    (source_dir / "new-york-openleg-json" / "TAX.json").write_text(
        json.dumps(tax_payload),
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ValueError, match="missing OpenLegislation documents"):
        extract_new_york_openleg_api(
            store,
            version="2026-test",
            source_dir=source_dir,
            only_title="TAX",
            source_as_of="2026-01-30",
            expression_date="2026-01-30",
        )


def test_extract_new_york_openleg_api_reports_skipped_malformed_law(tmp_path):
    source_dir = tmp_path / "source"
    json_dir = source_dir / "new-york-openleg-json"
    json_dir.mkdir(parents=True)
    laws_payload = {
        "success": True,
        "result": {
            "items": [
                {
                    "lawId": "ABC",
                    "name": "Alcoholic Beverage Control",
                    "lawType": "CONSOLIDATED",
                    "chapter": "3-B",
                },
                {
                    "lawId": "TAX",
                    "name": "Tax",
                    "lawType": "CONSOLIDATED",
                    "chapter": "60",
                },
            ]
        },
    }
    abc_payload = json.loads(json.dumps(SAMPLE_OPENLEG_TAX))
    abc_payload["result"]["info"]["lawId"] = "ABC"
    tax_payload = {
        "success": True,
        "result": {
            "info": {
                "lawId": "TAX",
                "name": "Tax",
                "lawType": "CONSOLIDATED",
                "chapter": "60",
            }
        },
    }
    (json_dir / "laws.json").write_text(json.dumps(laws_payload), encoding="utf-8")
    (json_dir / "ABC.json").write_text(json.dumps(abc_payload), encoding="utf-8")
    (json_dir / "TAX.json").write_text(json.dumps(tax_payload), encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "artifacts")

    report = extract_new_york_openleg_api(
        store,
        version="2026-test",
        source_dir=source_dir,
        source_as_of="2026-01-30",
        expression_date="2026-01-30",
    )

    assert report.coverage.complete
    assert report.provisions_written == 3
    assert report.skipped_source_count == 1
    assert report.errors == ("TAX: missing OpenLegislation documents",)


def test_extract_new_york_consolidated_laws_writes_source_inventory_and_provisions(tmp_path):
    source_dir = _write_new_york_fixture_tree(tmp_path)
    store = CorpusArtifactStore(tmp_path / "artifacts")

    report = extract_new_york_consolidated_laws(
        store,
        version="2026-test",
        source_dir=source_dir,
        only_title="TAX",
        source_as_of="2026-01-30",
        expression_date="2026-01-30",
    )

    assert report.coverage.complete
    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 2
    assert report.provisions_written == 5
    assert report.provisions_path.name == "2026-test-us-ny-tax.jsonl"

    inventory = load_source_inventory(report.inventory_path)
    provisions = load_provisions(report.provisions_path)
    assert [item.citation_path for item in inventory] == [
        "us-ny/statute/TAX",
        "us-ny/statute/TAX/A22",
        "us-ny/statute/TAX/A22P1",
        "us-ny/statute/TAX/601",
        "us-ny/statute/TAX/602",
    ]
    assert provisions[-1].body is not None
    assert provisions[-1].parent_citation_path == "us-ny/statute/TAX/A22P1"

    coverage = json.loads(report.coverage_path.read_text())
    assert coverage["complete"] is True


def test_extract_new_york_consolidated_laws_uses_filtered_run_id(tmp_path):
    source_dir = _write_new_york_fixture_tree(tmp_path)
    store = CorpusArtifactStore(tmp_path / "artifacts")

    report = extract_new_york_consolidated_laws(
        store,
        version="2026-test",
        source_dir=source_dir,
        only_title="TAX",
        limit=1,
        source_as_of="2026-01-30",
        expression_date="2026-01-30",
    )

    assert report.section_count == 1
    assert report.provisions_path.name == "2026-test-us-ny-tax-limit-1.jsonl"


def test_extract_new_york_consolidated_laws_uses_version_run_id_for_full_scope(tmp_path):
    source_dir = _write_new_york_fixture_tree(tmp_path)
    (source_dir / "new-york-senate-html" / "CONSOLIDATED.html").write_text(
        SAMPLE_INDEX.replace(
            '<a href="https://www.nysenate.gov/legislation/laws/ABC">'
            "ABC Alcoholic Beverage Control</a>",
            "",
        ),
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "artifacts")

    report = extract_new_york_consolidated_laws(
        store,
        version="2026-test",
        source_dir=source_dir,
        source_as_of="2026-01-30",
        expression_date="2026-01-30",
    )

    assert report.section_count == 2
    assert report.provisions_path.name == "2026-test.jsonl"


def test_extract_new_york_consolidated_laws_parallelizes_leaf_sections(tmp_path):
    source_dir = _write_new_york_fixture_tree(tmp_path)
    store = CorpusArtifactStore(tmp_path / "artifacts")

    report = extract_new_york_consolidated_laws(
        store,
        version="2026-test",
        source_dir=source_dir,
        only_title="TAX",
        workers=4,
        source_as_of="2026-01-30",
        expression_date="2026-01-30",
    )

    provisions = load_provisions(report.provisions_path)
    assert report.section_count == 2
    assert [provision.citation_path for provision in provisions[-2:]] == [
        "us-ny/statute/TAX/601",
        "us-ny/statute/TAX/602",
    ]
