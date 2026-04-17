"""Tests for atlas.citations.extractor."""

from __future__ import annotations

import pytest

from atlas.citations import (
    CFRExtractor,
    DCExtractor,
    ExtractedRef,
    USCExtractor,
    all_extractors,
    extract_all,
)

# --- USC ------------------------------------------------------------------


class TestUSCExtractor:
    def test_basic_usc_with_section_marker(self) -> None:
        body = "established under 42 U.S.C. § 9902."
        refs = USCExtractor().extract(body)
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us/statute/42/9902"
        assert refs[0].pattern_kind == "usc"
        assert refs[0].confidence == 1.0
        assert body[refs[0].start_offset : refs[0].end_offset] == refs[0].raw_text

    def test_subsection_chain(self) -> None:
        refs = USCExtractor().extract("see 42 U.S.C. 9902(2) and related provisions")
        assert refs[0].target_citation_path == "us/statute/42/9902/2"

    def test_deep_subsection_chain(self) -> None:
        refs = USCExtractor().extract("under 26 U.S.C. § 32(a)(1)(A)")
        assert refs[0].target_citation_path == "us/statute/26/32/a/1/A"

    def test_compact_usc_form(self) -> None:
        refs = USCExtractor().extract("26 USC 32")
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us/statute/26/32"

    def test_double_section_sign(self) -> None:
        refs = USCExtractor().extract("pursuant to 26 U.S.C. §§ 32")
        assert refs[0].target_citation_path == "us/statute/26/32"

    def test_section_with_letter_suffix(self) -> None:
        refs = USCExtractor().extract("7 U.S.C. 2014a")
        assert refs[0].target_citation_path == "us/statute/7/2014a"

    def test_title_out_of_range_rejected(self) -> None:
        # 99 is not a valid USC title today — reject rather than pollute
        # the refs table with bogus paths.
        assert USCExtractor().extract("99 USC 9999") == []

    def test_no_match_without_section(self) -> None:
        assert USCExtractor().extract("under the United States Code generally") == []

    def test_multiple_citations_in_text(self) -> None:
        body = "as defined in 26 U.S.C. § 32 and 42 U.S.C. 9902(2)"
        refs = USCExtractor().extract(body)
        paths = {r.target_citation_path for r in refs}
        assert paths == {"us/statute/26/32", "us/statute/42/9902/2"}

    # IRC prose form — same extractor, same pattern_kind, target pinned to 26.
    def test_irc_prose_simple(self) -> None:
        refs = USCExtractor().extract(
            "section 32 of the Internal Revenue Code"
        )
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us/statute/26/32"
        assert refs[0].pattern_kind == "usc"

    def test_irc_prose_with_subsection(self) -> None:
        refs = USCExtractor().extract(
            "section 170(C) of the Internal Revenue Code"
        )
        assert refs[0].target_citation_path == "us/statute/26/170/C"

    def test_irc_prose_lowercase(self) -> None:
        refs = USCExtractor().extract(
            "see section 168 of the internal revenue code"
        )
        assert refs[0].target_citation_path == "us/statute/26/168"

    def test_irc_prose_with_year(self) -> None:
        refs = USCExtractor().extract(
            "section 32 of the Internal Revenue Code of 1986"
        )
        assert refs[0].target_citation_path == "us/statute/26/32"

    def test_irc_prose_with_united_states_qualifier(self) -> None:
        refs = USCExtractor().extract(
            "section 162 of the United States Internal Revenue Code"
        )
        assert refs[0].target_citation_path == "us/statute/26/162"

    def test_irc_prose_rejects_unqualified_section_refs(self) -> None:
        # "section 32" alone must not be claimed by the IRC extractor —
        # only "section N ... Internal Revenue Code" matches.
        assert USCExtractor().extract("section 32 of the tax law") == []
        assert USCExtractor().extract("subsection (a) of this section") == []


# --- CFR ------------------------------------------------------------------


class TestCFRExtractor:
    def test_section_form(self) -> None:
        refs = CFRExtractor().extract("under 7 CFR 273.9")
        assert refs[0].target_citation_path == "us/regulation/7/273/9"
        assert refs[0].pattern_kind == "cfr"

    def test_section_with_subsection_chain(self) -> None:
        refs = CFRExtractor().extract("see 7 C.F.R. § 273.9(a)(1)")
        assert refs[0].target_citation_path == "us/regulation/7/273/9/a/1"

    def test_part_only(self) -> None:
        refs = CFRExtractor().extract("See the rules at 7 CFR Part 273 for details.")
        assert refs[0].target_citation_path == "us/regulation/7/273"

    def test_section_with_letter_suffix(self) -> None:
        refs = CFRExtractor().extract("per 20 CFR 404.1a")
        assert refs[0].target_citation_path == "us/regulation/20/404/1a"

    def test_compact_form(self) -> None:
        refs = CFRExtractor().extract("42 CFR 435.110 governs")
        assert refs[0].target_citation_path == "us/regulation/42/435/110"

    def test_title_out_of_range_rejected(self) -> None:
        assert CFRExtractor().extract("99 CFR 100.1") == []

    def test_no_match_on_usc(self) -> None:
        # Ensure we don't accidentally match USC text.
        assert CFRExtractor().extract("26 USC 32") == []


# --- DC -------------------------------------------------------------------


class TestDCExtractor:
    def test_basic_dc_cite(self) -> None:
        body = "See § 47-1801.04 for definitions."
        refs = DCExtractor().extract(body)
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-dc/statute/47/47-1801.04"
        assert refs[0].pattern_kind == "dc"
        assert refs[0].confidence == 1.0
        assert body[refs[0].start_offset : refs[0].end_offset] == refs[0].raw_text

    def test_subsection_chain(self) -> None:
        refs = DCExtractor().extract("see § 47-1801.04(a)(1)(A)")
        assert refs[0].target_citation_path == "us-dc/statute/47/47-1801.04/a/1/A"

    def test_alpha_suffix_title(self) -> None:
        refs = DCExtractor().extract("pursuant to § 29A-1001")
        assert refs[0].target_citation_path == "us-dc/statute/29A/29A-1001"

    def test_alpha_suffix_section(self) -> None:
        refs = DCExtractor().extract("under § 2-1204.11b(a)")
        assert refs[0].target_citation_path == "us-dc/statute/2/2-1204.11b/a"

    def test_colon_form_title(self) -> None:
        # DC UCC titles use a colon, e.g. 28:9 — treated as a title
        # string, not split.
        refs = DCExtractor().extract("see § 28:9-316(i)(1)")
        assert refs[0].target_citation_path == "us-dc/statute/28:9/28:9-316/i/1"

    def test_en_space_between_section_sign_and_number(self) -> None:
        # DC bodies use U+2002 (en space) between § and the number.
        refs = DCExtractor().extract("see §\u200247-181")
        assert refs[0].target_citation_path == "us-dc/statute/47/47-181"

    def test_thin_space_between_section_sign_and_number(self) -> None:
        # DC bodies also use U+2009 (thin space).
        refs = DCExtractor().extract("see §\u200947-1801.04")
        assert refs[0].target_citation_path == "us-dc/statute/47/47-1801.04"

    def test_nbsp_between_section_sign_and_number(self) -> None:
        # And sometimes U+00A0 (non-breaking space).
        refs = DCExtractor().extract("see §\u00a047-181")
        assert refs[0].target_citation_path == "us-dc/statute/47/47-181"

    def test_no_match_without_section_sign(self) -> None:
        # Bare "47-1801.04" without § is ambiguous (could be Pub. L.,
        # phone number, etc.) — require the § marker.
        assert DCExtractor().extract("reference 47-1801.04 applies") == []

    def test_multiple_in_one_body(self) -> None:
        refs = DCExtractor().extract(
            "see § 47-1801.04 and also § 47-1805.02a(a) and § 29A-1001."
        )
        paths = [r.target_citation_path for r in refs]
        assert paths == [
            "us-dc/statute/47/47-1801.04",
            "us-dc/statute/47/47-1805.02a/a",
            "us-dc/statute/29A/29A-1001",
        ]


# --- Jurisdiction routing -------------------------------------------------


class TestAllExtractorsJurisdictionRouting:
    def test_no_jurisdiction_runs_only_federal(self) -> None:
        kinds = {type(e).__name__ for e in all_extractors()}
        assert kinds == {"USCExtractor", "CFRExtractor"}

    def test_us_dc_adds_dc_extractor(self) -> None:
        kinds = {type(e).__name__ for e in all_extractors("us-dc")}
        assert kinds == {"USCExtractor", "CFRExtractor", "DCExtractor"}

    def test_us_ny_does_not_add_dc_extractor(self) -> None:
        kinds = {type(e).__name__ for e in all_extractors("us-ny")}
        assert "DCExtractor" not in kinds

    def test_extract_all_without_jurisdiction_skips_dc_pattern(self) -> None:
        # The DC pattern would greedily match this if it were active.
        # With no jurisdiction hint, the federal extractors don't match
        # the dashed form either, so we expect no refs.
        refs = extract_all("see § 47-1801.04(a)")
        assert refs == []

    def test_extract_all_with_us_dc_returns_dc_refs(self) -> None:
        refs = extract_all("see § 47-1801.04(a)", jurisdiction="us-dc")
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-dc/statute/47/47-1801.04/a"

    def test_extract_all_us_dc_still_catches_federal_cites(self) -> None:
        # A DC body citing 42 U.S.C. must still produce the USC ref.
        body = "as defined in 42 U.S.C. § 9902 and also § 47-1801.04"
        paths = {r.target_citation_path for r in extract_all(body, jurisdiction="us-dc")}
        assert "us/statute/42/9902" in paths
        assert "us-dc/statute/47/47-1801.04" in paths


# --- Combined extract_all -------------------------------------------------


class TestExtractAll:
    def test_mixed_corpus(self) -> None:
        body = (
            "For purposes of this chapter, 42 U.S.C. 9902(2) establishes the "
            "poverty guidelines. See implementing regulations at 7 CFR 273.9 "
            "and eligibility standards in 7 C.F.R. Part 273."
        )
        refs = extract_all(body)
        targets = [r.target_citation_path for r in refs]
        assert "us/statute/42/9902/2" in targets
        assert "us/regulation/7/273/9" in targets
        assert "us/regulation/7/273" in targets

    def test_sorted_by_start_offset(self) -> None:
        body = "Cross-ref: 7 CFR 273.9, then 26 U.S.C. § 32, then 42 USC 9902."
        refs = extract_all(body)
        offsets = [r.start_offset for r in refs]
        assert offsets == sorted(offsets)

    def test_offsets_reproduce_raw_text(self) -> None:
        body = "See 42 U.S.C. 9902(2) and 7 CFR Part 273 for authority."
        for ref in extract_all(body):
            assert body[ref.start_offset : ref.end_offset] == ref.raw_text

    def test_empty_body(self) -> None:
        assert extract_all("") == []

    def test_no_citations(self) -> None:
        assert extract_all("The program was established by act of Congress.") == []

    def test_dedup_at_same_span(self) -> None:
        # Hypothetically, two extractors could fire on the same span. We
        # construct that case by forcing an overlap via a raw ref list
        # and running it through the dedup helper.
        from atlas.citations.extractor import _dedupe

        low_conf = ExtractedRef(
            raw_text="x",
            pattern_kind="heuristic",
            target_citation_path="us/statute/26/32",
            start_offset=10,
            end_offset=20,
            confidence=0.5,
        )
        high_conf = ExtractedRef(
            raw_text="x",
            pattern_kind="usc",
            target_citation_path="us/statute/26/32",
            start_offset=10,
            end_offset=20,
            confidence=1.0,
        )
        deduped = _dedupe([low_conf, high_conf])
        assert len(deduped) == 1
        assert deduped[0].confidence == 1.0
        assert deduped[0].pattern_kind == "usc"


# --- Regression cases that shouldn't match --------------------------------


class TestNonMatches:
    @pytest.mark.parametrize(
        "body",
        [
            "The section begins here.",
            "citizens of the United States",
            "1234 Elm Street, Anytown USA",
            "see Title 42",
            "42 CFR",  # No part/section — incomplete cite
        ],
    )
    def test_does_not_match(self, body: str) -> None:
        assert extract_all(body) == []
