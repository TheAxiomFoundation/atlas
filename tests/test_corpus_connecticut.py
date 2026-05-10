from __future__ import annotations

import json

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_source_inventory
from axiom_corpus.corpus.state_adapters.connecticut import (
    CONNECTICUT_CURRENT_SOURCE_FORMAT,
    CONNECTICUT_SUPPLEMENT_SOURCE_FORMAT,
    _RecordedSource,
    extract_connecticut_statutes,
    parse_connecticut_chapter_page,
    parse_connecticut_title_index,
    parse_connecticut_title_page,
)

SAMPLE_TITLES = """
<html><body>
  <table>
    <tr>
      <td><a href="title_12.htm"><span class="toc_ttl_desig">Title 12</span></a></td>
      <td><a href="title_12.htm"><span class="toc_ttl_name">Taxation</span></a></td>
    </tr>
  </table>
</body></html>
"""

SAMPLE_TITLE = """
<html><body>
  <h1 class="title-no">TITLE 12</h1>
  <h1 class="title-name">TAXATION</h1>
  <table>
    <tr>
      <td class="left_40pct"><a class="toc_ch_link" href="chap_203.htm">Chapter 203</a>
      <span class="toc_rng_secs">Secs. 12-40 to 12-121z</span></td>
      <td><a class="toc_ch_link" href="chap_203.htm">Property Tax Assessment</a></td>
    </tr>
  </table>
</body></html>
"""

SAMPLE_CHAPTER = """
<html><body>
<div id="chap_203.htm">
  <h2 class="chap-no">CHAPTER 203</h2>
  <h2 class="chap-name">PROPERTY TAX ASSESSMENT</h2>
  <p class="toc_catchln"><a href="#sec_12-41">Sec. 12-41. Filing of declaration.</a></p>
  <hr class="chaps_pg_bar"/>
  <p><span class="catchln" id="sec_12-41">Sec. 12-41. Filing of declaration.</span> (a) Base text cites section <a href="chap_203.htm#sec_12-42">12-42</a>.</p>
  <p class="source-first">(1949 Rev., S. 1719.)</p>
  <p class="history-first">History: Base history.</p>
  <p class="cross-ref-first">See Sec. 14-163.</p>
  <p class="annotation-first">Base annotation.</p>
  <table class="nav_tbl"><tr><td>nav</td></tr></table>
</div>
</body></html>
"""

SAMPLE_SUPPLEMENT_CHAPTER = """
<html><body>
<div id="chap_203.htm">
  <h2 class="chap-no">CHAPTER 203</h2>
  <h2 class="chap-name">PROPERTY TAX ASSESSMENT</h2>
  <p class="toc_catchln"><a href="#sec_12-41">Sec. 12-41. Filing of declaration.</a></p>
  <hr class="chaps_pg_bar"/>
  <p><span class="catchln" id="sec_12-41">Sec. 12-41. Filing of declaration.</span> (a) Supplement text cites section <a href="chap_203.htm#sec_12-43">12-43</a>.</p>
  <p class="source-first">(P.A. 25-1, S. 1.)</p>
</div>
</body></html>
"""

SAMPLE_RECORDED = _RecordedSource(
    source_url="https://www.cga.ct.gov/current/pub/titles.htm",
    source_path="sources/us-ct/statute/test/titles.html",
    source_format=CONNECTICUT_CURRENT_SOURCE_FORMAT,
    sha256="abc",
)


def test_parse_connecticut_pages():
    titles = parse_connecticut_title_index(SAMPLE_TITLES, source=SAMPLE_RECORDED)
    assert [title.number for title in titles] == ["12"]
    assert titles[0].heading == "Taxation"

    chapters = parse_connecticut_title_page(
        SAMPLE_TITLE,
        title=titles[0],
        source=SAMPLE_RECORDED,
    )
    assert [chapter.chapter for chapter in chapters] == ["203"]
    assert chapters[0].heading == "Property Tax Assessment"

    sections = parse_connecticut_chapter_page(
        SAMPLE_CHAPTER,
        chapter=chapters[0],
        source=SAMPLE_RECORDED,
    )
    assert [section.section for section in sections] == ["12-41"]
    assert sections[0].body == "(a) Base text cites section 12-42."
    assert sections[0].source_history == ("(1949 Rev., S. 1719.)",)
    assert sections[0].amendment_history == ("History: Base history.",)
    assert sections[0].cross_references == ("See Sec. 14-163.",)
    assert sections[0].annotations == ("Base annotation.",)
    assert sections[0].references_to == ("us-ct/statute/12-42", "us-ct/statute/14-163")


def test_extract_connecticut_statutes_applies_supplement_override(tmp_path):
    source_dir = tmp_path / "source"
    for source_format, chapter_html in (
        (CONNECTICUT_CURRENT_SOURCE_FORMAT, SAMPLE_CHAPTER),
        (CONNECTICUT_SUPPLEMENT_SOURCE_FORMAT, SAMPLE_SUPPLEMENT_CHAPTER),
    ):
        base = source_dir / source_format
        base.mkdir(parents=True)
        (base / "titles.html").write_text(SAMPLE_TITLES, encoding="utf-8")
        (base / "title-12.html").write_text(SAMPLE_TITLE, encoding="utf-8")
        (base / "chapter-203.html").write_text(chapter_html, encoding="utf-8")

    store = CorpusArtifactStore(tmp_path / "corpus")
    report = extract_connecticut_statutes(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        only_title="12",
        source_as_of="2026-01-01",
        expression_date="2026-01-01",
    )

    assert report.title_count == 1
    assert report.container_count == 2
    assert report.section_count == 1
    assert report.coverage.complete
    inventory = load_source_inventory(report.inventory_path)
    records = [
        json.loads(line)
        for line in report.provisions_path.read_text().splitlines()
        if line.strip()
    ]
    assert len(inventory) == 3
    section = next(record for record in records if record["citation_path"] == "us-ct/statute/12-41")
    assert section["body"] == "(a) Supplement text cites section 12-43."
    assert section["source_format"] == CONNECTICUT_SUPPLEMENT_SOURCE_FORMAT
    assert section["metadata"]["references_to"] == ["us-ct/statute/12-43"]
