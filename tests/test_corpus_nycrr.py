from __future__ import annotations

from dataclasses import dataclass

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.ny_rulemaking import extract_ny_state_register
from axiom_corpus.corpus.nycrr import extract_nycrr

ROOT_HTML = """
<html><body><main><section class="co_innertube">
<h1>Unofficial New York Codes, Rules and Regulations</h1>
<a href="/nycrr/Browse/Home/NewYork/UnofficialNewYorkCodesRulesandRegulations?guid=title10&amp;originationContext=documenttoc&amp;transitionType=Default&amp;contextData=(sc.Default)">Title 10 Department of Health</a>
<a href="/nycrr/Browse/Home/NewYork/UnofficialNewYorkCodesRulesandRegulations?guid=title18&amp;originationContext=documenttoc&amp;transitionType=Default&amp;contextData=(sc.Default)">Title 18 Department of Social Services</a>
</section></main></body></html>
"""

TITLE_18_HTML = """
<html><body><main><section class="co_innertube">
<a href="/nycrr/Browse/Home/NewYork/UnofficialNewYorkCodesRulesandRegulations">Home</a>
<h1>Title 18 Department of Social Services</h1>
<a name="doc353" href="/nycrr/Document/doc353?viewType=FullText&amp;originationContext=documenttoc&amp;transitionType=CategoryPageItem&amp;contextData=(sc.Default)">s 353.1 Introduction and applicability.</a>
</section></main></body></html>
"""

DOCUMENT_HTML = """
<html><body><main><section class="co_innertube">
<div class="co_genericWhiteBox">
<div id="co_docHeaderTitle"><ul id="co_docHeaderCitation"><li id="citation">18 CRR-NY 353.1</li><li id="pubname">NY-CRR</li></ul></div>
<div id="co_document" class="co_document co_codesStateAdminCodesNY">
<div class="co_cites">18 CRR-NY 353.1</div>
<div class="co_title"><div class="co_headtext">353.1 Introduction and applicability.</div></div>
<div class="co_contentBlock co_section"><div class="co_contentBlock co_body">
<div class="co_paragraph"><div class="co_paragraphText">This Part implements subdivision 5 of section 211 of the Social Services Law.</div></div>
</div></div>
<div>Current through September 15, 2021</div><table id="co_endOfDocument"><tr><td>End of Document</td></tr></table>
</div></div>
</section></main></body></html>
"""

STATE_REGISTER_HTML = """
<html><body>
<article data-history-node-id="87691" about="/may-6-2026vol-xlviii-issue-18" class="webny-teaser teaser--type--webny-document">
  <div class="field-content"><a href="/may-6-2026vol-xlviii-issue-18">May 6, 2026/Vol XLVIII, Issue 18</a></div>
  <a href="/may-6-2026vol-xlviii-issue-18">Download</a>
</article>
<article data-history-node-id="87551" about="/april-29-2026vol-xlviii-issue-17" class="webny-teaser teaser--type--webny-document">
  <div class="field-content"><a href="/april-29-2026vol-xlviii-issue-17">April 29, 2026/Vol XLVIII, Issue 17</a></div>
  <a href="/april-29-2026vol-xlviii-issue-17">Download</a>
</article>
</body></html>
"""

STATE_REGISTER_NOTICE_TEXT = """
RULE MAKING ACTIVITIES

Department of Audit and
Control
NOTICE OF ADOPTION
Expedited Payment Program
I.D. No. AAC-06-26-00008-A
Filing No. 310
Filing Date: 2026-04-15
Effective Date: 2026-05-06
PURSUANT TO THE PROVISIONS OF THE State Administrative Procedure Act, NOTICE is hereby given of the following action:
Action taken: Addition of section 123.10 to Title 2 NYCRR.
Text of rule and any required statements and analyses may be obtained from: Marcella Buell.
"""


@dataclass
class _FakeResponse:
    content: bytes
    url: str

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def raise_for_status(self) -> None:
        return None


class _NycrrSession:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def get(self, url: str, *, timeout: int = 30) -> _FakeResponse:
        self.urls.append(url)
        if "guid=title18" in url:
            return _FakeResponse(TITLE_18_HTML.encode(), url)
        if "/nycrr/Document/doc353" in url:
            return _FakeResponse(DOCUMENT_HTML.encode(), url)
        return _FakeResponse(ROOT_HTML.encode(), url)


class _StateRegisterSession:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def get(self, url: str, *, timeout: int = 30) -> _FakeResponse:
        self.urls.append(url)
        if url == "https://dos.ny.gov/state-register":
            return _FakeResponse(STATE_REGISTER_HTML.encode(), url)
        return _FakeResponse(_pdf_bytes(STATE_REGISTER_NOTICE_TEXT), url.replace("dos.ny.gov/", "dos.ny.gov/system/files/"))


def _pdf_bytes(text: str) -> bytes:
    import fitz

    document = fitz.open()
    page = document.new_page()
    y = 72
    for line in text.splitlines():
        page.insert_text((72, y), line, fontsize=10)
        y += 14
    data = document.tobytes()
    document.close()
    return data


def test_extract_nycrr_writes_source_first_records(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    session = _NycrrSession()

    report = extract_nycrr(
        store,
        version="2026-05-10",
        only_title=18,
        limit=3,
        delay_seconds=0,
        session=session,
    )

    assert report.coverage.complete
    assert report.page_count == 3
    assert report.browse_page_count == 2
    assert report.document_page_count == 1
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-ny/regulation",
        "us-ny/regulation/title-18",
        "us-ny/regulation/title-18/353.1",
    ]
    assert records[1].heading == "Title 18 Department of Social Services"
    assert records[0].kind == "collection"
    assert records[2].citation_label == "18 CRR-NY 353.1"
    assert records[2].source_as_of == "September 15, 2021"
    assert records[2].metadata["source_caveat"] == "Online NYCRR is unofficial and not for evidentiary use."
    assert "Social Services Law" in records[2].body
    inventory = load_source_inventory(report.inventory_path)
    assert inventory[2].source_format == "nycrr-westlaw-html"
    assert inventory[2].metadata["guid"] == "doc353"
    assert len(session.urls) == 3


def test_extract_ny_state_register_writes_issue_records(tmp_path):
    store = CorpusArtifactStore(tmp_path / "corpus")
    session = _StateRegisterSession()

    report = extract_ny_state_register(
        store,
        version="2026-05-10",
        limit=1,
        session=session,
    )

    assert report.coverage.complete
    assert report.issue_count == 1
    assert report.notice_count == 1
    assert report.provisions_written == 3
    records = load_provisions(report.provisions_path)
    assert [record.citation_path for record in records] == [
        "us-ny/rulemaking/state-register",
        "us-ny/rulemaking/state-register/may-6-2026-vol-xlviii-issue-18",
        "us-ny/rulemaking/state-register/may-6-2026-vol-xlviii-issue-18/notice/aac-06-26-00008-a",
    ]
    assert records[1].heading == "May 6, 2026/Vol XLVIII, Issue 18"
    assert records[1].source_format == "ny-dos-state-register-pdf"
    assert records[2].heading == "Expedited Payment Program"
    assert records[2].metadata["agency"] == "Department of Audit and Control"
    assert records[2].metadata["action_type"] == "NOTICE OF ADOPTION"
    assert "Title 2 NYCRR" in records[2].body
    inventory = load_source_inventory(report.inventory_path)
    assert inventory[0].source_format == "ny-dos-state-register-html"
    assert inventory[1].metadata["notice_count"] == 1
    assert inventory[2].sha256
    assert session.urls == [
        "https://dos.ny.gov/state-register",
        "https://dos.ny.gov/may-6-2026vol-xlviii-issue-18",
    ]
