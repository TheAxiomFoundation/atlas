import json

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.state_adapters.maryland import (
    MARYLAND_CODE_SOURCE_FORMAT,
    MarylandArticle,
    MarylandSectionTarget,
    extract_maryland_code,
    parse_maryland_articles,
    parse_maryland_section,
    parse_maryland_sections,
)

SAMPLE_ARTICLES = [
    {"DisplayText": "Agriculture - (gag)", "Value": "gag"},
    {"DisplayText": "Declaration of Rights - (c0)", "Value": "c0"},
]

SAMPLE_SECTIONS = [
    {"DisplayText": "1-101", "Value": "100"},
    {"DisplayText": "1-102", "Value": "200"},
]

SAMPLE_SECTION_HTML = """<!DOCTYPE html>
<html lang="en">
<body>
<div id="StatuteText">
<html><div style="text-align: center;"><span style="font-weight: bold;">Article - Agriculture</span></div><br><br>
<div class="row"><div class="btn-group" role="group"><button>Previous</button><button>Next</button></div></div>
<br><br>&sect;1&ndash;101.<br><br>
&nbsp;&nbsp;&nbsp;&nbsp;(a)&nbsp;&nbsp;&nbsp;&nbsp;In this article the following words have the meanings indicated.
<br><br>
&nbsp;&nbsp;&nbsp;&nbsp;(b)&nbsp;&nbsp;&nbsp;&nbsp;A reference to &sect; 1-102 of this article means the next section.
<br><br>
<div class="row"><div class="btn-group" role="group"><button>Next</button></div></div><br></html>
</div>
</body>
</html>
"""

SAMPLE_EMPTY_SECTION_HTML = """<!DOCTYPE html>
<html lang="en">
<body>
<div id="StatuteText">
<html><div style="text-align: center;"><span style="font-weight: bold;">Article - Agriculture</span></div>
<br><br>&sect;1&ndash;103.<br><br></html>
</div>
</body>
</html>
"""


def test_parse_maryland_articles_filters_code_article_metadata():
    articles = parse_maryland_articles(SAMPLE_ARTICLES)

    assert [article.code for article in articles] == ["gag", "c0"]
    assert articles[0].heading == "Agriculture"
    assert articles[0].citation_path == "us-md/statute/gag"
    assert articles[0].legal_identifier == "Md. Code, Agriculture"


def test_parse_maryland_sections_and_section_body():
    article = MarylandArticle(code="gag", heading="Agriculture", ordinal=1)
    targets = parse_maryland_sections(SAMPLE_SECTIONS, article=article)

    assert [target.section for target in targets] == ["1-101", "1-102"]
    assert targets[0].title == "1"
    assert targets[0].parent_citation_path == "us-md/statute/gag/title-1"

    parsed = parse_maryland_section(SAMPLE_SECTION_HTML, target=targets[0])

    assert parsed.body is not None
    assert parsed.body.startswith("(a) In this article")
    assert "Article - Agriculture" not in parsed.body
    assert "\u00a71-101" not in parsed.body.replace(" ", "")
    assert parsed.references_to == ("us-md/statute/gag/1-102",)


def test_parse_maryland_empty_source_section_marks_status():
    target = MarylandSectionTarget(
        article_code="gag",
        article_heading="Agriculture",
        section="1-103",
        ordinal=3,
    )

    parsed = parse_maryland_section(SAMPLE_EMPTY_SECTION_HTML, target=target)

    assert parsed.body is None
    assert parsed.status == "source-empty"


def test_extract_maryland_code_from_source_dir_writes_artifacts(tmp_path):
    source_dir = tmp_path / "source"
    (source_dir / "maryland-code-json" / "sections").mkdir(parents=True)
    (source_dir / "maryland-code-html" / "gag").mkdir(parents=True)
    (source_dir / "maryland-code-json" / "articles.json").write_text(
        json.dumps(SAMPLE_ARTICLES),
        encoding="utf-8",
    )
    (source_dir / "maryland-code-json" / "sections" / "gag.json").write_text(
        json.dumps(SAMPLE_SECTIONS),
        encoding="utf-8",
    )
    (source_dir / "maryland-code-html" / "gag" / "1-101.html").write_text(
        SAMPLE_SECTION_HTML,
        encoding="utf-8",
    )
    target = MarylandSectionTarget(
        article_code="gag",
        article_heading="Agriculture",
        section="1-102",
        ordinal=2,
    )
    (source_dir / target.relative_path).write_text(SAMPLE_SECTION_HTML, encoding="utf-8")
    store = CorpusArtifactStore(tmp_path / "corpus")

    report = extract_maryland_code(
        store,
        version="2026-05-09",
        source_dir=source_dir,
        source_as_of="2026-01-01",
        expression_date="2026-01-01",
    )

    assert report.coverage.complete is True
    assert report.title_count == 1
    assert report.container_count == 1
    assert report.section_count == 2
    assert report.provisions_written == 4
    inventory = load_source_inventory(report.inventory_path)
    records = load_provisions(report.provisions_path)
    assert inventory[0].source_format == MARYLAND_CODE_SOURCE_FORMAT
    assert [record.citation_path for record in records] == [
        "us-md/statute/gag",
        "us-md/statute/gag/title-1",
        "us-md/statute/gag/1-101",
        "us-md/statute/gag/1-102",
    ]
    assert records[2].metadata is not None
    assert records[2].metadata["references_to"] == ["us-md/statute/gag/1-102"]
