from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.pennsylvania import (
    PENNSYLVANIA_SOURCE_FORMAT,
    extract_pennsylvania_statutes,
    parse_pennsylvania_title_html,
)

SAMPLE_PENNSYLVANIA_TITLE_HTML = """<!doctype html>
<html>
<body>
<div class="BodyContainer">
<div class="Comment">72cc</div>
<p>TABLE OF CONTENTS</p>
<p>TITLE 72</p>
<p>TAXATION AND FISCAL AFFAIRS</p>

<div class="Comment">72c3101h</div>
<p>CHAPTER 31</p>
<p>MICROENTERPRISE ASSISTANCE</p>
<p>SUBCHAPTER B</p>
<p>MICROENTERPRISE LOAN PROGRAMS</p>

<div class="Comment">72c3116s</div>
<p><b>§ 3116.  Microenterprise loans.</b></p>
<p><b>(a)  Loan issuance.--</b>An administrative entity may issue a loan.</p>
<p>The department shall publish guidelines under
<a href="/statutes/consolidated/view-statute?txtType=HTM&amp;ttl=72&amp;sctn=3121&amp;iFrame=true">section 3121</a>.</p>
<p><b>Cross References.  </b>Section 3116 is referred to in section 3121.</p>
<p><b>History.--</b>Act 2004-32, effective 60 days after July 7, 2004.</p>

<div class="Comment">72c3117s</div>
<p><b>§ 3117.  Administration of program.</b></p>
<p>The department shall administer the program.</p>
</div>
</body>
</html>
"""

SAMPLE_PENNSYLVANIA_SINGLE_DIGIT_TITLE_HTML = """<!doctype html>
<html>
<body>
<div class="BodyContainer">
<div class="Comment">01cc</div>
<p>TABLE OF CONTENTS</p>
<p>TITLE 1</p>
<p>GENERAL PROVISIONS</p>
<div class="Comment">01c101h</div>
<p>CHAPTER 1</p>
<p>SHORT TITLE, FORM OF CITATION AND</p>
<p>EFFECTIVE DATE</p>
<div class="Comment">01c101s</div>
<p><b>§ 101. Short title.</b></p>
<p>This title shall be known as the Consolidated Statutes.</p>
</div>
</body>
</html>
"""


def test_parse_pennsylvania_title_html_extracts_real_comment_marker_structure():
    provisions = parse_pennsylvania_title_html(SAMPLE_PENNSYLVANIA_TITLE_HTML, title=72)

    assert [provision.kind for provision in provisions] == [
        "title",
        "chapter",
        "section",
        "section",
    ]
    title, chapter, section, next_section = provisions
    assert title.citation_path == "us-pa/statute/title-72"
    assert title.heading == "Taxation and Fiscal Affairs"
    assert chapter.citation_path == "us-pa/statute/title-72/chapter-31"
    assert chapter.heading == "Microenterprise Assistance"
    assert section.citation_path == "us-pa/statute/72/3116"
    assert section.heading == "Microenterprise loans"
    assert section.parent_citation_path == "us-pa/statute/title-72/chapter-31"
    assert section.body is not None
    assert "Loan issuance" in section.body
    assert "History.-- Act 2004-32" in section.body
    assert section.references_to == ("us-pa/statute/72/3121",)
    assert section.source_history == (
        "History.-- Act 2004-32, effective 60 days after July 7, 2004.",
    )
    assert section.notes == ("Cross References. Section 3116 is referred to in section 3121.",)
    assert next_section.citation_path == "us-pa/statute/72/3117"


def test_parse_pennsylvania_title_html_handles_zero_padded_single_digit_markers():
    provisions = parse_pennsylvania_title_html(
        SAMPLE_PENNSYLVANIA_SINGLE_DIGIT_TITLE_HTML,
        title=1,
    )

    assert [provision.citation_path for provision in provisions] == [
        "us-pa/statute/title-1",
        "us-pa/statute/title-1/chapter-1",
        "us-pa/statute/1/101",
    ]
    assert provisions[1].heading == "Short Title, Form of Citation and Effective Date"
    assert provisions[2].legal_identifier == "1 Pa.C.S. § 101"


def test_extract_pennsylvania_statutes_from_source_dir_writes_complete_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "title-72.html").write_text(
        SAMPLE_PENNSYLVANIA_TITLE_HTML,
        encoding="utf-8",
    )
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_pennsylvania_statutes(
        store,
        version="2026-05-08",
        source_dir=source_dir,
        source_as_of="2026-05-08",
        expression_date="2026-05-08",
        only_title=72,
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 2
    assert report.provisions_written == 4
    assert len(report.source_paths) == 1
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[0].source_format == PENNSYLVANIA_SOURCE_FORMAT
    assert records[2].citation_path == "us-pa/statute/72/3116"
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == ["us-pa/statute/72/3121"]
    assert records[2].source_path is not None
    assert records[2].source_path.endswith(
        "/pennsylvania-consolidated-statutes-html/title-72.html"
    )
