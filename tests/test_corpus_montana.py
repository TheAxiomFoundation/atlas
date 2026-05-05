import json
from datetime import date
from pathlib import Path

import pytest
import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.cli import main
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters import montana as montana_adapter
from axiom_corpus.corpus.state_adapters.montana import (
    extract_montana_code,
    parse_montana_section_html,
)

SAMPLE_MONTANA_SECTION_HTML = """<!doctype html>
<html>
<body>
<div class="section-header">
  <h4 class="section-title-title">TITLE 15. TAXATION</h4>
  <h3 class="section-chapter-title">CHAPTER 30. INDIVIDUAL INCOME TAX</h3>
  <h2 class="section-part-title">Part 21. Rate and General Provisions</h2>
  <h1 class="section-section-title">Definitions</h1>
</div>
<div class="section-doc" id="mca_0150-0300-0210-0010">
  <div class="section-content">
    <p class="line-indent">
      <span class="catchline"><span class="citation">15-30-2101</span>. Definitions.</span>
      The following definitions apply under <a href="../../../chapter_0310/part_0010/section_0010/0150-0310-0010-0010.html"><span class="citation">15-31-101</span></a>.
    </p>
    <p class="line-indent">(1) "Department" means the department of revenue.</p>
    <p class="line-indent">(2) "Knowingly" has the meaning provided in 45-2-101.</p>
  </div>
</div>
<div class="history-doc" id="mca_0150-0300-0210-0010_hist">
  <div class="history-content">
    <p class="line-indent"><span class="header">History:</span> En. Sec. 1, Ch. 181, L. 1933.</p>
  </div>
</div>
</body>
</html>
"""


SAMPLE_MONTANA_REPEALED_SECTION_HTML = """<!doctype html>
<html>
<body>
<h1 class="section-section-title">Repealed</h1>
<div class="section-doc" id="mca_0150-0300-0210-0100">
  <div class="section-content">
    <p class="line-indent"><span class="catchline"><span class="citation">15-30-2110</span>. Repealed.</span> Sec. 1, Ch. 1, L. 2025.</p>
  </div>
</div>
</body>
</html>
"""


SAMPLE_MONTANA_ALPHA_CHAPTER_SECTION_HTML = """<!doctype html>
<html>
<body>
<h1 class="section-section-title">Short Title</h1>
<div class="section-doc" id="mca_0300-002A-0010-0010">
  <div class="section-content">
    <p class="line-indent"><span class="catchline"><span class="citation">30-2A-101</span>. Short title.</span> This chapter may be cited as Uniform Commercial Code--Leases.</p>
  </div>
</div>
</body>
</html>
"""


SAMPLE_MONTANA_ALPHA_SECTION_HTML = """<!doctype html>
<html>
<body>
<h1 class="section-section-title">Control Of Electronic Chattel Paper</h1>
<div class="section-doc" id="mca_0300-009A-0010-007A">
  <div class="section-content">
    <p class="line-indent"><span class="catchline"><span class="citation">30-9A-107A</span>. Control of electronic chattel paper.</span> Control has the meaning provided in 30-9A-107B.</p>
  </div>
</div>
</body>
</html>
"""


SAMPLE_MONTANA_DECIMAL_SECTION_HTML = """<!doctype html>
<html>
<body>
<h1 class="section-section-title">Renumbered 20-7-308</h1>
<div class="section-doc" id="mca_0200-0070-0030-0021">
  <div class="section-content">
    <p class="line-indent"><span class="catchline"><span class="citation">20-7-302.1</span>. Renumbered <span class="citation">20-7-308</span>.</span> Code Commissioner, 2001.</p>
  </div>
</div>
</body>
</html>
"""


SAMPLE_MONTANA_CONSTITUTION_SECTION_HTML = """<!doctype html>
<html>
<body>
<h1 class="section-section-title">Compact With The United States</h1>
<div class="section-doc" id="mca_0000-0010-0010-0010">
  <div class="section-content">
    <p class="line-indent">All provisions of the enabling act continue in full force and effect.</p>
  </div>
</div>
</body>
</html>
"""


SAMPLE_MONTANA_TRANSITION_SECTION_HTML = """<!doctype html>
<html>
<body>
<h1 class="section-section-title">Transition Schedule</h1>
<div class="section-doc" id="mca_0000-0200-0000-000t">
  <div class="section-content">
    <p class="line-indent"><span class="catchline"><span class="citation">Transition Schedule</span>.</span> Temporary provisions remain part of this Constitution.</p>
  </div>
</div>
</body>
</html>
"""


def _write_montana_fixture_tree(base: Path) -> None:
    (base / "title_0150" / "chapter_0300" / "part_0210" / "section_0010").mkdir(
        parents=True
    )
    (base / "title_0150" / "chapter_0300" / "part_0210" / "section_0100").mkdir(
        parents=True
    )
    (base / "index.html").write_text(
        """<!doctype html><html><body>
        <div class="title-toc-content"><ul>
          <li><a data-titlenumber="15" href="./title_0150/chapters_index.html">TITLE 15. TAXATION</a></li>
        </ul></div>
        </body></html>""",
        encoding="utf-8",
    )
    (base / "title_0150" / "chapters_index.html").write_text(
        """<!doctype html><html><body>
        <h1 class="chapter-title-title">TITLE 15. TAXATION</h1>
        <div class="chapter-toc-content"><ul>
          <li><a href="./chapter_0300/parts_index.html">CHAPTER 30. INDIVIDUAL INCOME TAX</a></li>
        </ul></div>
        </body></html>""",
        encoding="utf-8",
    )
    (base / "title_0150" / "chapter_0300" / "parts_index.html").write_text(
        """<!doctype html><html><body>
        <h1 class="part-chapter-title">CHAPTER 30. INDIVIDUAL INCOME TAX</h1>
        <div class="part-toc-content"><ul>
          <li><a href="./part_0210/sections_index.html">Part 21. Rate and General Provisions</a></li>
        </ul></div>
        </body></html>""",
        encoding="utf-8",
    )
    (base / "title_0150" / "chapter_0300" / "part_0210" / "sections_index.html").write_text(
        """<!doctype html><html><body>
        <h1 class="section-part-title">Part 21. Rate and General Provisions</h1>
        <div class="section-toc-content"><ul>
          <li><a href="./section_0010/0150-0300-0210-0010.html"><span class="citation">15-30-2101</span>&nbsp;Definitions</a></li>
          <li><a href="./section_0100/0150-0300-0210-0100.html"><span class="citation">15-30-2110</span>&nbsp;Repealed</a></li>
        </ul></div>
        </body></html>""",
        encoding="utf-8",
    )
    (
        base
        / "title_0150"
        / "chapter_0300"
        / "part_0210"
        / "section_0010"
        / "0150-0300-0210-0010.html"
    ).write_text(SAMPLE_MONTANA_SECTION_HTML, encoding="utf-8")
    (
        base
        / "title_0150"
        / "chapter_0300"
        / "part_0210"
        / "section_0100"
        / "0150-0300-0210-0100.html"
    ).write_text(SAMPLE_MONTANA_REPEALED_SECTION_HTML, encoding="utf-8")


def _write_montana_alpha_chapter_fixture_tree(base: Path) -> None:
    (base / "title_0300" / "chapter_002A" / "part_0010" / "section_0010").mkdir(
        parents=True
    )
    (base / "index.html").write_text(
        """<!doctype html><html><body>
        <div class="title-toc-content"><ul>
          <li><a data-titlenumber="30" href="./title_0300/chapters_index.html">TITLE 30. TRADE AND COMMERCE</a></li>
        </ul></div>
        </body></html>""",
        encoding="utf-8",
    )
    (base / "title_0300" / "chapters_index.html").write_text(
        """<!doctype html><html><body>
        <h1 class="chapter-title-title">TITLE 30. TRADE AND COMMERCE</h1>
        <div class="chapter-toc-content"><ul>
          <li><a href="./chapter_002A/parts_index.html">CHAPTER 2A. UNIFORM COMMERCIAL CODE LEASES</a></li>
        </ul></div>
        </body></html>""",
        encoding="utf-8",
    )
    (base / "title_0300" / "chapter_002A" / "parts_index.html").write_text(
        """<!doctype html><html><body>
        <h1 class="part-chapter-title">CHAPTER 2A. UNIFORM COMMERCIAL CODE LEASES</h1>
        <div class="part-toc-content"><ul>
          <li><a href="./part_0010/sections_index.html">Part 1. General Provisions</a></li>
        </ul></div>
        </body></html>""",
        encoding="utf-8",
    )
    (base / "title_0300" / "chapter_002A" / "part_0010" / "sections_index.html").write_text(
        """<!doctype html><html><body>
        <h1 class="section-part-title">Part 1. General Provisions</h1>
        <div class="section-toc-content"><ul>
          <li><a href="./section_0010/0300-002A-0010-0010.html"><span class="citation">30-2A-101</span>&nbsp;Short title</a></li>
        </ul></div>
        </body></html>""",
        encoding="utf-8",
    )
    (
        base
        / "title_0300"
        / "chapter_002A"
        / "part_0010"
        / "section_0010"
        / "0300-002A-0010-0010.html"
    ).write_text(SAMPLE_MONTANA_ALPHA_CHAPTER_SECTION_HTML, encoding="utf-8")


def test_parse_montana_section_html_extracts_body_references_and_history():
    provision = parse_montana_section_html(
        SAMPLE_MONTANA_SECTION_HTML,
        fallback_source_id="15-30-2101",
        parent_source_id="15-30-21",
    )

    assert provision.citation_path == "us-mt/statute/15-30-2101"
    assert provision.parent_citation_path == "us-mt/statute/15-30-21"
    assert provision.heading == "Definitions"
    assert provision.body is not None
    assert "Department" in provision.body
    assert provision.references_to == (
        "us-mt/statute/15-31-101",
        "us-mt/statute/45-2-101",
    )
    assert provision.source_history == ("En. Sec. 1, Ch. 181, L. 1933.",)


def test_parse_montana_section_html_uses_path_id_for_constitution_sections():
    provision = parse_montana_section_html(
        SAMPLE_MONTANA_CONSTITUTION_SECTION_HTML,
        fallback_source_id="0-1-1-1",
        parent_source_id="0-1-1",
    )

    assert provision.citation_path == "us-mt/statute/0-1-1-1"
    assert provision.legal_identifier == "Mont. Const. 1"


def test_parse_montana_section_html_handles_transition_schedule_citation():
    provision = parse_montana_section_html(
        SAMPLE_MONTANA_TRANSITION_SECTION_HTML,
        fallback_source_id="0-20-0-t",
        parent_source_id="0-20-0",
    )

    assert provision.citation_path == "us-mt/statute/0-20-0-t"
    assert provision.display_number == "Transition Schedule"


def test_parse_montana_section_html_handles_alphanumeric_chapter_citation():
    provision = parse_montana_section_html(
        SAMPLE_MONTANA_ALPHA_CHAPTER_SECTION_HTML,
        fallback_source_id="30-2A-101",
        parent_source_id="30-2A-1",
    )

    assert provision.citation_path == "us-mt/statute/30-2A-101"
    assert provision.parent_citation_path == "us-mt/statute/30-2A-1"


def test_parse_montana_section_html_handles_letter_suffixed_section_citation():
    provision = parse_montana_section_html(
        SAMPLE_MONTANA_ALPHA_SECTION_HTML,
        fallback_source_id="30-9A-107A",
        parent_source_id="30-9A-1",
    )

    assert provision.citation_path == "us-mt/statute/30-9A-107A"
    assert provision.parent_citation_path == "us-mt/statute/30-9A-1"
    assert provision.references_to == ("us-mt/statute/30-9A-107B",)


def test_parse_montana_section_html_handles_decimal_section_citation():
    provision = parse_montana_section_html(
        SAMPLE_MONTANA_DECIMAL_SECTION_HTML,
        fallback_source_id="20-7-302.1",
        parent_source_id="20-7-3",
    )

    assert provision.citation_path == "us-mt/statute/20-7-302.1"
    assert provision.parent_citation_path == "us-mt/statute/20-7-3"
    assert provision.references_to == ("us-mt/statute/20-7-308",)


def test_parse_montana_section_html_requires_content():
    with pytest.raises(ValueError, match="section content not found"):
        parse_montana_section_html("<html></html>", fallback_source_id="15-30-2101", parent_source_id=None)


def test_parse_montana_section_html_falls_back_to_title_and_text_content():
    provision = parse_montana_section_html(
        """<!doctype html>
        <html><head><title>Fallback Heading, MCA</title></head><body>
        <div class="section-content">Loose text for 15-30-2101.</div>
        </body></html>""",
        fallback_source_id="15-30-2101",
        parent_source_id=None,
    )

    assert provision.heading == "Fallback Heading"
    assert provision.body == "Loose text for 15-30-2101."
    assert provision.parent_citation_path is None


def test_parse_montana_section_html_statuses_reserved_and_terminated():
    reserved = parse_montana_section_html(
        """<html><body><h1 class="section-section-title">Reserved</h1>
        <div class="section-content"><p><span class="citation">1-1-101</span>. Reserved.</p></div>
        </body></html>""",
        fallback_source_id="1-1-101",
        parent_source_id="1-1-1",
    )
    terminated = parse_montana_section_html(
        """<html><body><h1 class="section-section-title">Program</h1>
        <div class="section-content"><p><span class="citation">1-1-102</span>. This section terminated.</p></div>
        </body></html>""",
        fallback_source_id="1-1-102",
        parent_source_id="1-1-1",
    )

    assert reserved.status == "reserved"
    assert terminated.status == "terminated"


def test_parse_montana_section_html_extracts_href_reference_without_citation():
    provision = parse_montana_section_html(
        """<html><body><h1 class="section-section-title">Cross Reference</h1>
        <div class="section-content">
          <p><span class="citation">15-30-2101</span>. See
          <a href="../../../chapter_0310/part_0010/section_0010/0150-0310-0010-0010.html">corporate tax definitions</a>.</p>
        </div></body></html>""",
        fallback_source_id="15-30-2101",
        parent_source_id="15-30-21",
    )

    assert provision.references_to == ("us-mt/statute/15-31-101",)


def test_parse_montana_section_html_allows_missing_heading():
    provision = parse_montana_section_html(
        """<html><body>
        <div class="section-content"><p><span class="citation">15-30-2101</span>. Text.</p></div>
        </body></html>""",
        fallback_source_id="15-30-2101",
        parent_source_id="15-30-21",
    )

    assert provision.heading is None


def test_montana_fetcher_uses_cache_and_reports_missing_source(tmp_path, monkeypatch):
    missing_fetcher = montana_adapter._MontanaFetcher(
        base_url="https://example.test/mca",
        source_dir=tmp_path / "missing-source",
        download_dir=None,
    )
    with pytest.raises(ValueError, match="Montana source file does not exist"):
        missing_fetcher.fetch("index.html")

    calls: list[str] = []

    def fake_download(url: str) -> bytes:
        calls.append(url)
        return b"<html>cached</html>"

    monkeypatch.setattr(montana_adapter, "_download_montana_page", fake_download)
    fetcher = montana_adapter._MontanaFetcher(
        base_url="https://example.test/mca",
        source_dir=None,
        download_dir=tmp_path / "cache",
    )

    first = fetcher.fetch("./title_0010/page.html?ignored=true")
    second = fetcher.fetch("title_0010/page.html")

    assert first.data == b"<html>cached</html>"
    assert second.data == b"<html>cached</html>"
    assert calls == ["https://example.test/mca/title_0010/page.html"]


def test_download_montana_page_retries_and_reports_failure(monkeypatch):
    class FakeResponse:
        content = b"ok"

        def raise_for_status(self) -> None:
            return None

    attempts = 0

    def flaky_get(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise requests.Timeout("slow")
        return FakeResponse()

    monkeypatch.setattr(montana_adapter.requests, "get", flaky_get)
    monkeypatch.setattr(montana_adapter.time, "sleep", lambda _seconds: None)

    assert montana_adapter._download_montana_page("https://example.test/page.html") == b"ok"
    assert attempts == 2

    def failing_get(*_args, **_kwargs):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(montana_adapter.requests, "get", failing_get)
    with pytest.raises(ValueError, match="failed to fetch Montana source page"):
        montana_adapter._download_montana_page("https://example.test/page.html")


def test_fetch_pages_deduplicates_and_preserves_order_with_workers():
    class DummyFetcher:
        def fetch(self, path: str) -> montana_adapter._MontanaSourcePage:
            return montana_adapter._MontanaSourcePage(
                relative_path=path,
                source_url=f"https://example.test/{path}",
                data=path.encode(),
            )

    fetcher = DummyFetcher()

    assert montana_adapter._fetch_pages(fetcher, [], workers=2) == ()
    sequential = montana_adapter._fetch_pages(fetcher, ["b.html", "a.html"], workers=1)
    parallel = montana_adapter._fetch_pages(
        fetcher,
        ["b.html", "./a.html", "b.html"],
        workers=2,
    )

    assert [page.relative_path for page in sequential] == ["b.html", "a.html"]
    assert [page.relative_path for page in parallel] == ["b.html", "a.html"]


def test_extract_montana_code_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_montana_fixture_tree(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_montana_code(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        source_year=2025,
        source_as_of="2026-03-24",
        expression_date="2026-03-24",
        only_title="15",
        workers=1,
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 3
    assert report.section_count == 2
    assert report.provisions_written == 5
    assert len(report.source_paths) == 6
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[-1].source_format == "montana-code-html"
    assert records[0].citation_path == "us-mt/statute/15"
    part_record = next(record for record in records if record.citation_path == "us-mt/statute/15-30-21")
    assert part_record.parent_citation_path == "us-mt/statute/15-30"
    assert records[-1].citation_path == "us-mt/statute/15-30-2110"
    assert records[-1].metadata is not None
    assert records[-1].metadata["status"] == "repealed"


def test_extract_montana_code_limit_uses_scoped_run_id(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_montana_fixture_tree(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_montana_code(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        only_title="15",
        limit=4,
        workers=1,
    )

    assert report.provisions_written == 4
    assert report.provisions_path.name == "2026-05-05-us-mt-title-15-limit-4.jsonl"


def test_extract_montana_code_records_bad_section_parse_errors(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_montana_fixture_tree(source_dir)
    bad_section = (
        source_dir
        / "title_0150"
        / "chapter_0300"
        / "part_0210"
        / "section_0100"
        / "0150-0300-0210-0100.html"
    )
    bad_section.write_text("<html><body>No section content.</body></html>", encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_montana_code(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        only_title="15",
        workers=1,
    )

    assert report.errors == ("section 15-30-2110: section content not found",)
    assert report.provisions_written == 4


def test_extract_montana_code_deduplicates_repeated_section_links(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_montana_fixture_tree(source_dir)
    sections_index = (
        source_dir
        / "title_0150"
        / "chapter_0300"
        / "part_0210"
        / "sections_index.html"
    )
    html = sections_index.read_text(encoding="utf-8")
    sections_index.write_text(
        html.replace(
            "</ul>",
            '<li><a href="./section_0010/0150-0300-0210-0010.html"><span class="citation">15-30-2101</span>&nbsp;Definitions duplicate</a></li></ul>',
        ),
        encoding="utf-8",
    )

    report = extract_montana_code(
        CorpusArtifactStore(tmp_path / "corpus"),
        version="2026-05-05",
        source_dir=source_dir,
        only_title="15",
        workers=1,
    )

    assert report.provisions_written == 5


def test_extract_montana_code_handles_alphanumeric_chapter_containers(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_montana_alpha_chapter_fixture_tree(source_dir)
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_montana_code(
        store,
        version="2026-05-05",
        source_dir=source_dir,
        only_title="30",
        workers=1,
    )

    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-mt/statute/30",
        "us-mt/statute/30-2A",
        "us-mt/statute/30-2A-1",
        "us-mt/statute/30-2A-101",
    ]
    assert records[-1].parent_citation_path == "us-mt/statute/30-2A-1"


def test_montana_parser_helpers_cover_edge_cases():
    title_links = montana_adapter._parse_montana_title_links(
        """<html><body><div class="title-toc-content">
        <a href="./title_0000/chapters_index.html">THE CONSTITUTION OF THE STATE OF MONTANA</a>
        <a data-titlenumber="7" href="./title_0070/chapters_index.html">Agriculture</a>
        </div></body></html>"""
    )
    chapter_links = montana_adapter._parse_montana_chapter_links(
        """<html><body><div class="chapter-toc-content">
        <a href="./article_0010/parts_index.html">ARTICLE I. COMPACT</a>
        <a href="./bad.html">Not a chapter</a>
        </div></body></html>""",
        "title_0000/chapters_index.html",
        parent_title="0",
    )
    part_links = montana_adapter._parse_montana_part_links(
        """<html><body><div class="part-toc-content">
        <a href="./part_0010/sections_index.html">Preamble</a>
        <a href="./bad.html">Not a part</a>
        </div></body></html>""",
        "title_0000/article_0010/parts_index.html",
        parent_source_id="0-1",
    )
    section_links = montana_adapter._parse_montana_section_links(
        """<html><body><div class="section-toc-content">
        <a href="./section_0010/0000-0010-0010-0010.html">Compact</a>
        <a href="./bad.html"><span class="citation">1-1-101</span> Bad path</a>
        </div></body></html>""",
        "title_0000/article_0010/part_0010/sections_index.html",
        parent_source_id="0-1-1",
    )

    assert [link.source_id for link in title_links] == ["0", "7"]
    assert chapter_links[0].source_id == "0-1"
    assert part_links[0].source_id == "0-1-1"
    assert [link.source_id for link in section_links] == ["0-1-1-1", "1-1-101"]
    assert montana_adapter._source_id_from_section_relative("bad.html") == ""
    assert (
        montana_adapter._source_id_from_section_relative(
            "title_0000/article_0200/part_0000/section_000t/0000-0200-0000-000t.html"
        )
        == "0-20-0-t"
    )
    assert (
        montana_adapter._source_id_from_section_relative(
            "title_0300/chapter_009A/part_0010/section_007B/0300-009A-0010-007B.html"
        )
        == "30-9A-107B"
    )
    assert (
        montana_adapter._source_id_from_section_relative(
            "title_0300/chapter_009A/part_0010/section_00AB/0300-009A-0010-00AB.html"
        )
        == "30-9A-1-AB"
    )
    assert montana_adapter._normalize_montana_source_id("0001-0002-0003-0004") == "1-2-3-4"
    assert montana_adapter._normalize_montana_source_id("Transition Schedule") == "Transition Schedule"
    assert montana_adapter._parent_source_id("1", 1) is None
    assert montana_adapter._section_parent_source_id("0-20-0-t") == "0-20-0"
    assert montana_adapter._section_parent_source_id("30-9A-1-AB") == "30-9A-1"
    assert montana_adapter._encoded_path_number("bad.html", "title") is None
    assert montana_adapter._encoded_path_token("bad.html", "chapter") is None
    assert montana_adapter._decode_montana_token("ABCD") == "ABCD"
    assert montana_adapter._decode_montana_section_token("000t") == "T"
    assert montana_adapter._normalize_montana_section_number("ABC") == "ABC"
    with pytest.raises(ValueError, match="invalid Montana section number"):
        montana_adapter._montana_part_number_from_section_token("ABC")
    assert montana_adapter._display_from_title_text("Other", "7") == "Title 7"
    assert montana_adapter._chapter_display("article", "No Roman", "0", "20") == "Article 20"
    assert (
        montana_adapter._chapter_heading(
            '<html><h1 class="part-chapter-title">Article Heading</h1></html>',
            "article",
        )
        == "Article Heading"
    )
    assert montana_adapter._part_display("Transition", "0-20", 0) == "Part 0"
    assert montana_adapter._part_display("Transition", "1-1", 1) == "1-1"
    numeric_citation = BeautifulSoup("<p>12. Topic</p>", "lxml").find("p")
    assert numeric_citation is not None
    assert montana_adapter._first_citation(numeric_citation) == "12"
    with pytest.raises(ValueError, match="invalid Montana title href"):
        montana_adapter._id_from_title_href("bad.html")
    assert montana_adapter._montana_title_filter(None) is None
    assert montana_adapter._date_text(None, "2026-05-05") == "2026-05-05"
    assert montana_adapter._date_text(date(2026, 3, 24), "fallback") == "2026-03-24"
    assert montana_adapter._montana_run_id("2026-05-05", only_title=None, limit=None) == "2026-05-05"
    with pytest.raises(ValueError, match="invalid Montana title filter"):
        montana_adapter._montana_title_filter("constitution")


def test_extract_montana_code_rejects_empty_title_selection(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_montana_fixture_tree(source_dir)

    with pytest.raises(ValueError, match="no Montana Code title sources selected"):
        extract_montana_code(
            CorpusArtifactStore(tmp_path / "corpus"),
            version="2026-05-05",
            source_dir=source_dir,
            only_title="99",
        )


def test_extract_montana_code_cli(tmp_path, capsys):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_montana_fixture_tree(source_dir)
    base = tmp_path / "corpus"

    exit_code = main(
        [
            "extract-montana-code",
            "--base",
            str(base),
            "--version",
            "2026-05-05",
            "--source-dir",
            str(source_dir),
            "--only-title",
            "15",
            "--source-as-of",
            "2026-03-24",
            "--expression-date",
            "2026-03-24",
            "--workers",
            "1",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["adapter"] == "montana-code"
    assert payload["coverage_complete"] is True
    assert Path(payload["provisions_path"]).exists()
