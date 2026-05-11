"""Tests for rulespec-* repo path discovery and citation-path mapping."""

from __future__ import annotations

from pathlib import Path

from axiom_corpus.corpus.rulespec_paths import (
    discover_encoded_paths,
    discover_encoded_paths_for_jurisdictions,
)


def _touch(path: Path, body: str = "rule: {}\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


def test_us_statute_yaml_maps_to_canonical_citation_path(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "statutes" / "7" / "2014" / "e" / "2.yaml")

    encoded = discover_encoded_paths(repo, "us")

    assert encoded == {"us/statute/7/2014/e/2"}


def test_us_statute_top_section_yaml_maps_to_path(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "statutes" / "26" / "3111" / "a.yaml")

    encoded = discover_encoded_paths(repo, "us")

    assert encoded == {"us/statute/26/3111/a"}


def test_us_regulations_strip_cfr_suffix(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "regulations" / "7-cfr" / "273" / "7.yaml")

    encoded = discover_encoded_paths(repo, "us")

    # 7-cfr collapses to bare title 7 to align with corpus citation_path.
    assert encoded == {"us/regulation/7/273/7"}


def test_state_regulations_keep_cfr_style_title(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us-co"
    _touch(repo / "regulations" / "10-ccr-2506-1" / "4.306.1.yaml")

    encoded = discover_encoded_paths(repo, "us-co")

    # Non-federal jurisdictions don't strip the publication-system suffix.
    assert encoded == {"us-co/regulation/10-ccr-2506-1/4.306.1"}


def test_test_yaml_files_are_excluded(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "statutes" / "26" / "3111" / "a.yaml")
    _touch(repo / "statutes" / "26" / "3111" / "a.test.yaml")

    encoded = discover_encoded_paths(repo, "us")

    assert encoded == {"us/statute/26/3111/a"}


def test_meta_yaml_files_are_excluded(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "statutes" / "26" / "3111" / "a.yaml")
    _touch(repo / "statutes" / "26" / "3111" / "a.meta.yaml")

    encoded = discover_encoded_paths(repo, "us")

    assert encoded == {"us/statute/26/3111/a"}


def test_files_under_tests_directory_are_skipped(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "tests" / "fixture.yaml")
    _touch(repo / "statutes" / "26" / "3111" / "a.yaml")

    encoded = discover_encoded_paths(repo, "us")

    assert encoded == {"us/statute/26/3111/a"}


def test_hidden_directories_are_skipped(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / ".github" / "workflows" / "ci.yaml")
    _touch(repo / "statutes" / "26" / "3111" / "a.yaml")

    encoded = discover_encoded_paths(repo, "us")

    assert encoded == {"us/statute/26/3111/a"}


def test_policies_bucket_maps_to_policy(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "policies" / "irs" / "rev-proc-2025-32" / "standard-deduction.yaml")

    encoded = discover_encoded_paths(repo, "us")

    assert encoded == {"us/policy/irs/rev-proc-2025-32/standard-deduction"}


def test_missing_repo_returns_empty_set(tmp_path: Path) -> None:
    assert discover_encoded_paths(tmp_path / "does-not-exist", "us") == set()


def test_root_discovery_for_multiple_jurisdictions(tmp_path: Path) -> None:
    _touch(tmp_path / "rulespec-us" / "statutes" / "26" / "3111" / "a.yaml")
    _touch(tmp_path / "rulespec-us-co" / "regulations" / "10-ccr-2506-1" / "4.306.1.yaml")

    discovered = discover_encoded_paths_for_jurisdictions(tmp_path, ["us", "us-co", "uk"])

    assert discovered["us"] == {"us/statute/26/3111/a"}
    assert discovered["us-co"] == {"us-co/regulation/10-ccr-2506-1/4.306.1"}
    assert discovered["uk"] == set()


def test_unknown_bucket_passes_through(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "manuals" / "irs" / "irm-1.yaml")

    encoded = discover_encoded_paths(repo, "us")

    # No collapse for unmapped buckets — they reach the citation path
    # verbatim so unknown shapes surface in the data instead of crashing.
    assert encoded == {"us/manuals/irs/irm-1"}


def test_files_at_repo_root_are_skipped(tmp_path: Path) -> None:
    repo = tmp_path / "rulespec-us"
    _touch(repo / "config.yaml")
    _touch(repo / "statutes" / "26" / "3111" / "a.yaml")

    encoded = discover_encoded_paths(repo, "us")

    assert encoded == {"us/statute/26/3111/a"}
