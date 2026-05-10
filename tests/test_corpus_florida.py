from __future__ import annotations

import json

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.state_adapters.florida import extract_florida_statutes

SAMPLE_TITLE_INDEX = """<!doctype html><html><body>
<table>
<tr valign="top">
  <td nowrap><b><font><a name="TitleXXX"></a><a href="index.cfm?App_mode=Display_Index&Title_Request=XXX#TitleXXX">TITLE XXX</a></font></b></td>
  <td><font><b>SOCIAL WELFARE</b></font></td>
  <td nowrap><font><b>Ch.409-430</b></font></td>
</tr>
<tr valign="top">
  <td nowrap align="Right"><font>
    <a href="index.cfm?App_mode=Display_Statute&URL=0400-0499/0409/0409ContentsIndex.html&StatuteYear=2025&Title=%2D%3E2025%2D%3EChapter%20409">Chapter 409</a>
  </font></td>
  <td class="ChapterTOC"><font>SOCIAL AND ECONOMIC ASSISTANCE</font></td>
</tr>
</table>
</body></html>
"""


SAMPLE_CHAPTER = """<!doctype html><html><body>
<div class="Chapters">
<div class="Chapter">
  <div class="Title"><div class="TitleNumber">TITLE XXX</div><span class="TitleName">SOCIAL WELFARE</span></div>
  <div class="ChapterTitle"><div class="ChapterNumber">CHAPTER 409</div><div class="ChapterName">SOCIAL AND ECONOMIC ASSISTANCE</div></div>
  <div class="Part">
    <div class="PartTitle"><div class="PartNumber">PART I</div><span class="PartTitle">SOCIAL AND ECONOMIC ASSISTANCE</span></div>
    <div class="Section">
      <span class="SectionNumber">409.016&#x2003;</span>
      <span class="Catchline"><span class="CatchlineText">Definitions.</span><span class="EmDash">&#x2014;</span></span>
      <span class="SectionBody"><span class="Text Intro Justify">As used in this chapter, see s. <a href="index.cfm?App_mode=Display_Statute&URL=0400-0499/0409/Sections/0409.145.html">409.145</a>.</span></span>
      <div class="History"><span class="HistoryTitle">History.</span><span class="HistoryText">s. 1, ch. 2025-1.</span></div>
    </div>
    <div class="Section">
      <span class="SectionNumber">409.145&#x2003;</span>
      <span class="Catchline"><span class="CatchlineText">Care of children.</span><span class="EmDash">&#x2014;</span></span>
      <span class="SectionBody"><div class="Subsection"><span class="Number">(1)&#x2003;</span><span class="Text Intro Justify">The department shall operate a system of care.</span></div></span>
      <div class="Note"><span class="NoteTitle">Note.</span><span class="Text Intro Justify">Sample note.</span></div>
    </div>
  </div>
</div>
</div>
</body></html>
"""


def test_extract_florida_statutes_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    title_path = source_dir / "florida-statutes-title-index-html" / "title-XXX.html"
    chapter_path = (
        source_dir
        / "florida-statutes-chapter-html"
        / "0400-0499"
        / "0409"
        / "0409.html"
    )
    title_path.parent.mkdir(parents=True)
    title_path.write_text(SAMPLE_TITLE_INDEX)
    chapter_path.parent.mkdir(parents=True)
    chapter_path.write_text(SAMPLE_CHAPTER)

    store = CorpusArtifactStore(tmp_path / "corpus")
    report = extract_florida_statutes(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        only_title="XXX",
        source_as_of="2025-09-19",
        expression_date="2025-09-19",
    )

    assert report.title_count == 1
    assert report.container_count == 3
    assert report.section_count == 2
    assert report.coverage.complete
    records = [
        json.loads(line)
        for line in report.provisions_path.read_text().splitlines()
        if line.strip()
    ]
    paths = {record["citation_path"] for record in records}
    assert "us-fl/statute/title-xxx" in paths
    assert "us-fl/statute/chapter-409" in paths
    assert "us-fl/statute/chapter-409/part-i" in paths
    assert "us-fl/statute/409.016" in paths
    section = next(record for record in records if record["citation_path"] == "us-fl/statute/409.016")
    assert section["parent_citation_path"] == "us-fl/statute/chapter-409/part-i"
    assert section["metadata"]["references_to"] == ["us-fl/statute/409.145"]
