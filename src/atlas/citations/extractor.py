"""Pattern-based citation extractors for US statutes and regulations.

The goal is recall with high-precision scoring rather than perfect parsing:
every extracted ref carries a ``confidence`` so downstream consumers can
choose the threshold that suits them (the viewer wants recall; RAC
tooling probably wants precision).

Patterns covered today:

* **USC** — ``NN U.S.C. § NNN(a)(1)`` / ``NN USC NNN`` / ``NN U.S.C. NNN``,
  with an optional sequence of parenthesized subsection ids.
* **CFR** — ``NN CFR § M.NN`` / ``NN C.F.R. M.NN`` / ``NN CFR Part M``,
  likewise with optional subsection ids.

Out of scope today — worth following up with their own extractors:

* Internal refs (``this section``, ``subsection (a)``) — context-dependent
  resolution.
* Public laws (``Pub. L. 110-246``) — no stable ``citation_path`` in arch.
* Session laws / Stat. cites — same reason.
* Act-name + section (``section 673(2) of the Community Services Block
  Grant Act``) — requires an act-name → USC-title map.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import ClassVar

# --- Data model -----------------------------------------------------------


@dataclass(frozen=True)
class ExtractedRef:
    """A single citation found in source text.

    Attributes mirror the columns of ``akn.rule_references`` — a caller
    can append ``source_rule_id`` and insert directly.
    """

    raw_text: str
    """The citation as it appears in the source, including punctuation
    ('42 U.S.C. 9902(2)'). Stored for display."""

    pattern_kind: str
    """Classifier: ``usc``, ``cfr``, etc. Extensible as new extractors
    land."""

    target_citation_path: str
    """Canonical Atlas path the citation resolves to. Always populated —
    even if the target isn't yet ingested, the path still links the refs
    table to a future row."""

    start_offset: int
    end_offset: int

    confidence: float = 1.0
    """0.0–1.0. 1.0 is pattern-unambiguous; lower when the resolution
    relied on a heuristic (e.g., a bare ``section 32`` resolved against
    an enclosing title)."""


# --- Helpers --------------------------------------------------------------


_SUBSECTION_CHAIN = r"(?:\s*\([A-Za-z0-9]+\))*"
"""Matches one or more parenthesized subsection ids: ``(a)(1)(A)``."""


def _subsection_segments(chain: str) -> list[str]:
    """Split ``(a)(1)(A)`` → ``['a', '1', 'A']``."""
    return re.findall(r"\(([A-Za-z0-9]+)\)", chain)


# --- Extractor protocol ---------------------------------------------------


@dataclass
class Extractor:
    """Base class for all citation extractors.

    Subclasses override :attr:`pattern` and :meth:`to_ref`. The base
    implementation walks the compiled pattern over the input and calls
    :meth:`to_ref` for each match; subclasses can skip a match by
    returning ``None``.
    """

    pattern_kind: ClassVar[str] = ""
    pattern: ClassVar[re.Pattern[str]] = re.compile("")

    def extract(self, body: str) -> list[ExtractedRef]:
        refs: list[ExtractedRef] = []
        for m in self.pattern.finditer(body):
            ref = self.to_ref(m)
            if ref is not None:
                refs.append(ref)
        return refs

    def to_ref(self, match: re.Match[str]) -> ExtractedRef | None:
        raise NotImplementedError


# --- USC extractor --------------------------------------------------------


class USCExtractor(Extractor):
    """Matches US Code citations.

    Examples::

        42 U.S.C. 9902(2)
        42 U.S.C. § 9902
        26 USC 32
        26 U.S.C. §§ 32(a)(1)

    Rejects bare ``NN USC`` (no section). Accepts the double ``§§`` form
    by consuming one or both leading section markers.
    """

    pattern_kind: ClassVar[str] = "usc"

    # Title, optional section marker, section (with optional letter suffix),
    # optional chain of parenthesized subsections.
    pattern: ClassVar[re.Pattern[str]] = re.compile(
        r"\b(?P<title>\d{1,2})\s+U\.?\s?S\.?\s?C\.?\s*(?:§{1,2}\s*)?"
        r"(?P<section>\d+[A-Za-z]?)"
        rf"(?P<sub>{_SUBSECTION_CHAIN})"
    )

    def to_ref(self, match: re.Match[str]) -> ExtractedRef | None:
        title = match.group("title")
        section = match.group("section")
        sub = match.group("sub") or ""

        # Guard against silly matches like "100 USC 9999999" where the
        # title is plausibly a stat-volume rather than a title number.
        # USC titles are 1–54 today.
        try:
            if not 1 <= int(title) <= 54:
                return None
        except ValueError:
            return None

        path_parts = ["us", "statute", title, section]
        path_parts.extend(_subsection_segments(sub))
        target = "/".join(path_parts)

        return ExtractedRef(
            raw_text=match.group(0),
            pattern_kind=self.pattern_kind,
            target_citation_path=target,
            start_offset=match.start(),
            end_offset=match.end(),
            confidence=1.0,
        )


# --- CFR extractor --------------------------------------------------------


class CFRExtractor(Extractor):
    """Matches Code of Federal Regulations citations.

    Examples::

        7 CFR 273.9
        7 C.F.R. § 273.9(a)
        7 CFR Part 273
        42 CFR § 435.110

    A CFR cite is either a part-only form (``NN CFR Part MMM``) or a
    section form (``NN CFR MMM.NNN``). Both resolve to an Atlas path:

    * Part-only → ``us/regulation/NN/MMM``
    * Section → ``us/regulation/NN/MMM/NNN`` plus any subsection chain
    """

    pattern_kind: ClassVar[str] = "cfr"

    pattern: ClassVar[re.Pattern[str]] = re.compile(
        r"\b(?P<title>\d{1,2})\s+C\.?\s?F\.?\s?R\.?\s*"
        r"(?:"
        r"(?:§{1,2}\s*)?(?P<part>\d+)\.(?P<section>\d+[a-z]?)"
        rf"(?P<sub>{_SUBSECTION_CHAIN})"
        r"|"
        r"Part\s+(?P<partonly>\d+)\b"
        r")"
    )

    def to_ref(self, match: re.Match[str]) -> ExtractedRef | None:
        title = match.group("title")
        try:
            if not 1 <= int(title) <= 50:
                return None
        except ValueError:
            return None

        if match.group("partonly"):
            target = f"us/regulation/{title}/{match.group('partonly')}"
        else:
            path_parts = [
                "us",
                "regulation",
                title,
                match.group("part"),
                match.group("section"),
            ]
            path_parts.extend(_subsection_segments(match.group("sub") or ""))
            target = "/".join(path_parts)

        return ExtractedRef(
            raw_text=match.group(0),
            pattern_kind=self.pattern_kind,
            target_citation_path=target,
            start_offset=match.start(),
            end_offset=match.end(),
            confidence=1.0,
        )


# --- Public API -----------------------------------------------------------


def all_extractors() -> list[Extractor]:
    """All registered extractors, in a stable order.

    Callers typically want :func:`extract_all` rather than this, but
    exposed so tests and downstream users can reason about what's
    running.
    """
    return [USCExtractor(), CFRExtractor()]


def _dedupe(refs: Iterable[ExtractedRef]) -> list[ExtractedRef]:
    """Keep one ref per ``(start_offset, end_offset)``.

    The USC and CFR patterns are disjoint by shape, but a text like
    ``7 USC 2014`` could be partially matched by a future extractor that
    also greedily reads digits. Sort by confidence DESC so the highest-
    confidence interpretation wins.
    """
    seen: dict[tuple[int, int], ExtractedRef] = {}
    for r in sorted(refs, key=lambda x: -x.confidence):
        key = (r.start_offset, r.end_offset)
        seen.setdefault(key, r)
    return sorted(seen.values(), key=lambda x: x.start_offset)


def extract_all(body: str) -> list[ExtractedRef]:
    """Run every registered extractor over ``body`` and merge results.

    Overlapping matches at the same span are deduplicated in favor of
    the highest-confidence extractor. The returned list is sorted by
    ``start_offset`` so callers can stream-process body text.
    """
    refs: list[ExtractedRef] = []
    for e in all_extractors():
        refs.extend(e.extract(body))
    return _dedupe(refs)
