"""Tests for the ``scripts/extract_references.py`` driver.

We exercise the pure helpers directly (``_prefix_upper_bound``) and the
query-string shape produced by ``fetch_rules_page`` via a mocked
``_retrying_urlopen`` — the goal is to pin down the PostgREST params
the script emits so the per-jurisdiction backfill walks the right
range.
"""

from __future__ import annotations

import importlib.util
import sys
import urllib.parse
from pathlib import Path
from unittest.mock import patch

import pytest

# The script is under ``scripts/`` so it isn't on the package path. Load
# it directly. This mirrors the loader trick the script itself uses to
# reach ``ingest_cfr_parts``.
_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "extract_references.py"
_spec = importlib.util.spec_from_file_location("extract_references", _SCRIPT)
assert _spec and _spec.loader
extract_references = importlib.util.module_from_spec(_spec)
sys.modules["extract_references"] = extract_references
_spec.loader.exec_module(extract_references)


# --- _prefix_upper_bound --------------------------------------------------


class TestPrefixUpperBound:
    def test_slash_bumps_to_zero(self) -> None:
        # '/' is 0x2F, incrementing gives '0' (0x30) — perfect upper bound
        # for a trailing-slash prefix like 'us-ny/'.
        assert extract_references._prefix_upper_bound("us-ny/") == "us-ny0"

    def test_letter_prefix_bumps_last_letter(self) -> None:
        assert extract_references._prefix_upper_bound("us-nz") == "us-n{"

    def test_single_character_prefix(self) -> None:
        assert extract_references._prefix_upper_bound("a") == "b"

    def test_empty_prefix_rejected(self) -> None:
        with pytest.raises(ValueError):
            extract_references._prefix_upper_bound("")


# --- fetch_rules_page query construction ---------------------------------


def _decode_params(url: str) -> list[tuple[str, str]]:
    """Return PostgREST query params as (key, decoded_value) pairs."""
    query = url.split("?", 1)[1]
    out: list[tuple[str, str]] = []
    for chunk in query.split("&"):
        key, _, val = chunk.partition("=")
        out.append((key, urllib.parse.unquote(val)))
    return out


class TestFetchRulesPageParams:
    """Pin the query shape for each (prefix, since_citation_path) combo."""

    def _captured_url(
        self,
        **kwargs: object,
    ) -> str:
        with patch.object(
            extract_references,
            "_retrying_urlopen",
            return_value=b"[]",
        ) as mock:
            extract_references.fetch_rules_page(
                service_key="svc-xxx",
                offset=0,
                **kwargs,  # type: ignore[arg-type]
            )
        req = mock.call_args.args[0]
        return req.full_url

    def test_prefix_first_page_uses_gte_and_lt(self) -> None:
        url = self._captured_url(
            doc_type="statute",
            since_citation_path=None,
            prefix="us-ny/",
        )
        params = _decode_params(url)
        assert ("citation_path", "gte.us-ny/") in params
        assert ("citation_path", "lt.us-ny0") in params
        assert ("doc_type", "eq.statute") in params

    def test_prefix_resume_uses_gt_cursor_plus_lt_bound(self) -> None:
        url = self._captured_url(
            doc_type="statute",
            since_citation_path="us-ny/statute/tax/606",
            prefix="us-ny/",
        )
        params = _decode_params(url)
        assert ("citation_path", "gt.us-ny/statute/tax/606") in params
        assert ("citation_path", "lt.us-ny0") in params
        # On resume we do NOT emit a redundant gte bound.
        assert ("citation_path", "gte.us-ny/") not in params

    def test_no_prefix_preserves_original_cursor_behavior(self) -> None:
        url = self._captured_url(
            doc_type=None,
            since_citation_path="us/statute/42/9902",
            prefix=None,
        )
        params = _decode_params(url)
        assert ("citation_path", "gt.us/statute/42/9902") in params
        # No upper bound when prefix is unset.
        assert not any(k == "citation_path" and v.startswith("lt.") for k, v in params)

    def test_no_prefix_no_cursor_emits_no_citation_path_filter(self) -> None:
        url = self._captured_url(
            doc_type="statute",
            since_citation_path=None,
            prefix=None,
        )
        params = _decode_params(url)
        assert not any(k == "citation_path" for k, _ in params)

    def test_page_has_stable_shape(self) -> None:
        url = self._captured_url(
            doc_type="statute",
            since_citation_path=None,
            prefix="us-dc/",
        )
        params = dict(_decode_params(url))
        # `jurisdiction` is selected so extract_all can route DC patterns.
        assert params["select"] == "id,citation_path,body,jurisdiction"
        assert params["body"] == "not.is.null"
        assert params["order"] == "citation_path.asc"
        assert params["limit"] == str(extract_references.PAGE_SIZE)
