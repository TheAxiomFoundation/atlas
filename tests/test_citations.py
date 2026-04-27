"""Tests for atlas.citations.extractor."""

from __future__ import annotations

import pytest

from atlas.citations import (
    CAExtractor,
    CFRExtractor,
    DCExtractor,
    ExtractedRef,
    NYExtractor,
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

    def test_out_of_range_title_rejected(self) -> None:
        # DC Code runs 1-51 plus alpha variants; a "§ 100-110"-style
        # range enumeration in text must not produce a bogus rule.
        assert DCExtractor().extract("see §§ 100-110") == []

    def test_boundary_titles_accepted(self) -> None:
        # Title 51 is the largest real DC title today; the whitelist
        # pads up to 60 to cover future adds.
        refs = DCExtractor().extract("see § 51-1001 and § 60-101")
        paths = {r.target_citation_path for r in refs}
        assert "us-dc/statute/51/51-1001" in paths
        assert "us-dc/statute/60/60-101" in paths

    def test_title_61_rejected(self) -> None:
        # Just above the cap — catches larger obvious non-cites.
        assert DCExtractor().extract("see § 61-100") == []

    def test_alpha_suffix_title_still_ranged(self) -> None:
        # '29A' parses as numeric-head 29; within the cap.
        refs = DCExtractor().extract("see § 29A-1001")
        assert len(refs) == 1

    def test_colon_form_title_ranged_on_numeric_head(self) -> None:
        # '28:9' parses as numeric-head 28; within the cap.
        refs = DCExtractor().extract("see § 28:9-316")
        assert len(refs) == 1


# --- NY -------------------------------------------------------------------


class TestNYExtractorCrossLaw:
    """Cross-law references like 'section N of the X Law'."""

    def test_tax_law(self) -> None:
        refs = NYExtractor().extract("see section 606 of the tax law")
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-ny/statute/tax/606"
        assert refs[0].pattern_kind == "ny"
        assert refs[0].confidence == 1.0

    def test_environmental_conservation_law_with_dash_section(self) -> None:
        refs = NYExtractor().extract(
            "pursuant to section 19-0309 of the environmental conservation law"
        )
        assert refs[0].target_citation_path == "us-ny/statute/env/19-0309"

    def test_penal_law(self) -> None:
        refs = NYExtractor().extract("violates section 130.25 of the penal law")
        assert refs[0].target_citation_path == "us-ny/statute/pen/130.25"

    def test_public_health_law_with_subsection_chain(self) -> None:
        refs = NYExtractor().extract(
            "as defined in section 2805-b(1)(a) of the public health law"
        )
        assert refs[0].target_citation_path == "us-ny/statute/pbh/2805-b/1/a"

    def test_case_insensitive(self) -> None:
        refs = NYExtractor().extract("Section 606 Of The Tax Law applies")
        assert refs[0].target_citation_path == "us-ny/statute/tax/606"

    def test_unknown_law_name_not_matched(self) -> None:
        # "Made-up Law" isn't in the map — skip rather than fabricate.
        assert (
            NYExtractor().extract("see section 123 of the made-up law") == []
        )

    def test_internal_revenue_code_not_claimed_by_ny(self) -> None:
        # This is an IRC reference, not a NY cross-law ref.
        assert (
            NYExtractor().extract(
                "section 32 of the internal revenue code"
            )
            == []
        )


class TestNYExtractorIntraCode:
    """Bare 'section N' inside a NY rule resolves to the enclosing law code."""

    def test_bare_section_resolves_against_source_path(self) -> None:
        ext = NYExtractor(source_citation_path="us-ny/statute/tax/606")
        refs = ext.extract("refer to section 32 for the base amount")
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-ny/statute/tax/32"
        assert refs[0].confidence == 0.7  # lower — bare refs are ambiguous

    def test_bare_section_with_subsection_chain(self) -> None:
        ext = NYExtractor(source_citation_path="us-ny/statute/tax/606/a")
        refs = ext.extract("under section 612(c)(3)")
        assert refs[0].target_citation_path == "us-ny/statute/tax/612/c/3"

    def test_no_intra_code_without_source_path(self) -> None:
        # The extractor must not invent a scope.
        assert NYExtractor().extract("under section 612") == []

    def test_no_intra_code_when_source_not_in_us_ny(self) -> None:
        ext = NYExtractor(source_citation_path="us/statute/26/32")
        assert ext.extract("under section 612") == []

    def test_bare_section_followed_by_cross_law_not_double_emitted(self) -> None:
        # "section 19-0309 of the environmental conservation law" should be
        # produced by the cross-law branch, not doubled by the intra branch.
        ext = NYExtractor(source_citation_path="us-ny/statute/tax/606")
        refs = ext.extract(
            "see section 19-0309 of the environmental conservation law"
        )
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-ny/statute/env/19-0309"
        assert refs[0].confidence == 1.0

    def test_bare_section_followed_by_irc_not_claimed_as_intra(self) -> None:
        # IRC prose form must not be double-claimed as NY intra-code.
        ext = NYExtractor(source_citation_path="us-ny/statute/tax/606")
        refs = ext.extract("section 32 of the internal revenue code")
        # The NY intra branch must skip this match.
        assert refs == []

    def test_mixed_cross_law_and_intra_code_in_same_body(self) -> None:
        ext = NYExtractor(source_citation_path="us-ny/statute/tax/606")
        refs = ext.extract(
            "cross-refs section 612 elsewhere and section 19-0309 of the environmental conservation law"
        )
        paths = {r.target_citation_path for r in refs}
        assert paths == {
            "us-ny/statute/tax/612",
            "us-ny/statute/env/19-0309",
        }


# --- CA -------------------------------------------------------------------


class TestCAExtractorCrossCode:
    """Cross-code references like 'Section N of the X Code'."""

    def test_government_code(self) -> None:
        refs = CAExtractor().extract("see Section 8571 of the Government Code")
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-ca/statute/gov/8571"
        assert refs[0].pattern_kind == "ca"
        assert refs[0].confidence == 1.0

    def test_labor_code_with_dot_section(self) -> None:
        refs = CAExtractor().extract(
            "pursuant to Section 1182.12 of the Labor Code"
        )
        assert refs[0].target_citation_path == "us-ca/statute/lab/1182.12"

    def test_revenue_and_taxation_code(self) -> None:
        refs = CAExtractor().extract(
            "under Section 10754 of the Revenue and Taxation Code"
        )
        assert refs[0].target_citation_path == "us-ca/statute/rtc/10754"

    def test_family_code(self) -> None:
        refs = CAExtractor().extract("in Section 8612 of the Family Code")
        assert refs[0].target_citation_path == "us-ca/statute/fam/8612"

    def test_with_subsection_chain(self) -> None:
        refs = CAExtractor().extract(
            "see Section 17041(a)(1) of the Revenue and Taxation Code"
        )
        assert refs[0].target_citation_path == "us-ca/statute/rtc/17041/a/1"

    def test_case_insensitive(self) -> None:
        refs = CAExtractor().extract(
            "SECTION 8571 OF THE GOVERNMENT CODE applies"
        )
        assert refs[0].target_citation_path == "us-ca/statute/gov/8571"

    def test_unknown_code_not_matched(self) -> None:
        assert (
            CAExtractor().extract("see Section 123 of the Made-up Code") == []
        )

    def test_internal_revenue_code_not_claimed_by_ca(self) -> None:
        assert (
            CAExtractor().extract("section 32 of the internal revenue code")
            == []
        )


class TestCAExtractorIntraCode:
    """Bare 'section N' inside a CA rule resolves to the enclosing code."""

    def test_bare_section_resolves_against_source_path(self) -> None:
        ext = CAExtractor(source_citation_path="us-ca/statute/rtc/17041")
        refs = ext.extract("refer to section 17052 for the credit")
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-ca/statute/rtc/17052"
        assert refs[0].confidence == 0.7

    def test_bare_section_with_subsection_chain(self) -> None:
        ext = CAExtractor(source_citation_path="us-ca/statute/rtc/17041/a")
        refs = ext.extract("under section 17054(b)(2)")
        assert refs[0].target_citation_path == "us-ca/statute/rtc/17054/b/2"

    def test_no_intra_code_without_source_path(self) -> None:
        assert CAExtractor().extract("under section 17052") == []

    def test_no_intra_code_when_source_not_in_us_ca(self) -> None:
        ext = CAExtractor(source_citation_path="us/statute/26/32")
        assert ext.extract("under section 17052") == []

    def test_bare_section_followed_by_cross_code_not_double_emitted(self) -> None:
        ext = CAExtractor(source_citation_path="us-ca/statute/rtc/17041")
        refs = ext.extract(
            "see Section 8571 of the Government Code and related rules"
        )
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-ca/statute/gov/8571"
        assert refs[0].confidence == 1.0

    def test_bare_section_followed_by_irc_not_claimed_as_intra(self) -> None:
        ext = CAExtractor(source_citation_path="us-ca/statute/rtc/17041")
        refs = ext.extract("section 32 of the internal revenue code")
        assert refs == []

    def test_mixed_cross_code_and_intra_code_in_same_body(self) -> None:
        ext = CAExtractor(source_citation_path="us-ca/statute/rtc/17041")
        refs = ext.extract(
            "cross-refs section 17052 elsewhere and Section 8571 of the Government Code"
        )
        paths = {r.target_citation_path for r in refs}
        assert paths == {
            "us-ca/statute/rtc/17052",
            "us-ca/statute/gov/8571",
        }


# --- Jurisdiction routing -------------------------------------------------


class TestAllExtractorsJurisdictionRouting:
    def test_state_extractor_requires_law_codes(self) -> None:
        from atlas.citations.extractor import _StateSectionExtractor

        with pytest.raises(TypeError, match="must define a non-empty"):
            type(
                "EmptyStateExtractor",
                (_StateSectionExtractor,),
                {"jurisdiction": "us-zz"},
            )

    def test_no_jurisdiction_runs_only_federal(self) -> None:
        kinds = {type(e).__name__ for e in all_extractors()}
        assert kinds == {"USCExtractor", "CFRExtractor"}

    def test_us_dc_adds_dc_extractor(self) -> None:
        kinds = {type(e).__name__ for e in all_extractors("us-dc")}
        assert kinds == {"USCExtractor", "CFRExtractor", "DCExtractor"}

    def test_us_ny_does_not_add_dc_extractor(self) -> None:
        kinds = {type(e).__name__ for e in all_extractors("us-ny")}
        assert "DCExtractor" not in kinds

    def test_us_ny_adds_ny_extractor(self) -> None:
        kinds = {type(e).__name__ for e in all_extractors("us-ny")}
        assert "NYExtractor" in kinds

    def test_us_ca_adds_ca_extractor(self) -> None:
        kinds = {type(e).__name__ for e in all_extractors("us-ca")}
        assert "CAExtractor" in kinds

    def test_ca_extractor_gets_source_path(self) -> None:
        extractors = all_extractors(
            "us-ca", source_citation_path="us-ca/statute/rtc/17041"
        )
        ca = [e for e in extractors if isinstance(e, CAExtractor)]
        assert len(ca) == 1
        assert ca[0].source_citation_path == "us-ca/statute/rtc/17041"

    def test_extract_all_us_ca_cross_code(self) -> None:
        refs = extract_all(
            "under Section 8571 of the Government Code",
            jurisdiction="us-ca",
            source_citation_path="us-ca/statute/rtc/17041",
        )
        paths = {r.target_citation_path for r in refs}
        assert "us-ca/statute/gov/8571" in paths

    def test_extract_all_us_ca_intra_code(self) -> None:
        refs = extract_all(
            "see section 17052 for details",
            jurisdiction="us-ca",
            source_citation_path="us-ca/statute/rtc/17041",
        )
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-ca/statute/rtc/17052"

    def test_ny_extractor_gets_source_path(self) -> None:
        extractors = all_extractors(
            "us-ny", source_citation_path="us-ny/statute/tax/606"
        )
        ny = [e for e in extractors if isinstance(e, NYExtractor)]
        assert len(ny) == 1
        assert ny[0].source_citation_path == "us-ny/statute/tax/606"

    def test_extract_all_us_ny_intra_code(self) -> None:
        refs = extract_all(
            "see section 32 for the base",
            jurisdiction="us-ny",
            source_citation_path="us-ny/statute/tax/606",
        )
        assert len(refs) == 1
        assert refs[0].target_citation_path == "us-ny/statute/tax/32"

    def test_extract_all_us_ny_cross_law(self) -> None:
        refs = extract_all(
            "under section 2805-b of the public health law",
            jurisdiction="us-ny",
            source_citation_path="us-ny/statute/tax/606",
        )
        paths = {r.target_citation_path for r in refs}
        assert "us-ny/statute/pbh/2805-b" in paths

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
