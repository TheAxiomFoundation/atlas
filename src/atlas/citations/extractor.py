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
    """Matches US Code citations, whether formal or by prose.

    Two forms, both resolving to ``us/statute/{title}/{section}``:

    1. Formal volume.USC form::

         42 U.S.C. 9902(2)
         42 U.S.C. § 9902
         26 USC 32
         26 U.S.C. §§ 32(a)(1)

    2. "Section N of the Internal Revenue Code" prose — common in
       state tax statutes that incorporate federal provisions by
       reference. Title is pinned to 26 (IRC == Title 26 USC)::

         section 170(C) of the Internal Revenue Code
         section 168 of the internal revenue code of 1986
         section 32 of the United States Internal Revenue Code

    Both produce ``pattern_kind = "usc"`` because the target namespace
    is identical; the prose-vs-formal split is an extractor detail
    downstream consumers shouldn't care about.
    """

    pattern_kind: ClassVar[str] = "usc"

    # Form 1 — formal "NN U.S.C." notation, with optional section sign,
    # optional letter suffix, optional chain of parenthesized subsections.
    _FORMAL = re.compile(
        r"\b(?P<title>\d{1,2})\s+U\.?\s?S\.?\s?C\.?\s*(?:§{1,2}\s*)?"
        r"(?P<section>\d+[A-Za-z]?)"
        rf"(?P<sub>{_SUBSECTION_CHAIN})"
    )

    # Form 2 — "section N ... Internal Revenue Code". The "Internal
    # Revenue Code" marker pins the title to 26, so no title group.
    _IRC = re.compile(
        r"\bsection\s+(?P<section>\d+[A-Za-z]?)"
        rf"(?P<sub>{_SUBSECTION_CHAIN})"
        r"\s+of\s+the\s+(?:United\s+States\s+)?"
        r"[Ii]nternal\s+[Rr]evenue\s+[Cc]ode"
        r"(?:\s+of\s+\d{4})?",
        re.IGNORECASE,
    )

    def extract(self, body: str) -> list[ExtractedRef]:
        refs: list[ExtractedRef] = []
        for m in self._FORMAL.finditer(body):
            r = self._to_formal(m)
            if r is not None:
                refs.append(r)
        for m in self._IRC.finditer(body):
            refs.append(self._to_irc(m))
        return refs

    def _to_formal(self, match: re.Match[str]) -> ExtractedRef | None:
        title = match.group("title")
        section = match.group("section")
        sub = match.group("sub") or ""

        # Guard against stat-volume look-alikes. USC titles are 1–54.
        try:
            if not 1 <= int(title) <= 54:
                return None
        except ValueError:  # pragma: no cover — regex forbids non-digits
            return None

        path_parts = ["us", "statute", title, section]
        path_parts.extend(_subsection_segments(sub))
        return ExtractedRef(
            raw_text=match.group(0),
            pattern_kind=self.pattern_kind,
            target_citation_path="/".join(path_parts),
            start_offset=match.start(),
            end_offset=match.end(),
            confidence=1.0,
        )

    def _to_irc(self, match: re.Match[str]) -> ExtractedRef:
        section = match.group("section")
        sub = match.group("sub") or ""
        path_parts = ["us", "statute", "26", section]
        path_parts.extend(_subsection_segments(sub))
        return ExtractedRef(
            raw_text=match.group(0),
            pattern_kind=self.pattern_kind,
            target_citation_path="/".join(path_parts),
            start_offset=match.start(),
            end_offset=match.end(),
            confidence=1.0,
        )


# --- DC extractor ---------------------------------------------------------


class DCExtractor(Extractor):
    """Matches District of Columbia Code intra-citations.

    DC statutes refer to other DC sections as ``§ 47-1801.04(a)(1)``,
    where the dash separates title from section. Three numbering
    variants occur in the corpus, matching how DC paths are stored:

        §\u00a047-1801.04          → us-dc/statute/47/47-1801.04
        § 29A-1001              → us-dc/statute/29A/29A-1001
        § 28:9-316(i)(1)        → us-dc/statute/28:9/28:9-316/i/1

    The ``§`` marker is required — without it we'd false-match things
    like public-law numbers ("Pub. L. 110-46"), so the cheap guard is
    to insist on the section sign.

    DC bodies variously use regular space, U+2002 (en space), or U+2009
    (thin space) between ``§`` and the number. All are matched.

    This extractor only applies to DC source rules; the runtime filters
    it out elsewhere via :func:`extract_all` 's ``jurisdiction`` arg.
    """

    pattern_kind: ClassVar[str] = "dc"

    pattern: ClassVar[re.Pattern[str]] = re.compile(
        r"§[\s\u2002\u2009\u00A0]*"
        r"(?P<title>\d+[A-Za-z]?|\d+:\d+)"
        r"-(?P<section>\d+(?:\.\d+)?[A-Za-z]?)"
        rf"(?P<sub>{_SUBSECTION_CHAIN})"
    )

    def to_ref(self, match: re.Match[str]) -> ExtractedRef | None:
        title = match.group("title")
        section = match.group("section")
        sub = match.group("sub") or ""

        path_parts = [
            "us-dc",
            "statute",
            title,
            f"{title}-{section}",
        ]
        path_parts.extend(_subsection_segments(sub))
        return ExtractedRef(
            raw_text=match.group(0),
            pattern_kind=self.pattern_kind,
            target_citation_path="/".join(path_parts),
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
        # Regex forbids non-digits in <title>; the except is defensive.
        try:
            if not 1 <= int(title) <= 50:
                return None
        except ValueError:  # pragma: no cover — regex forbids non-digits
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


def all_extractors(jurisdiction: str | None = None) -> list[Extractor]:
    """Extractors active for ``jurisdiction``.

    USC and CFR are universal — every corpus can cite federal statutes
    and regulations. Jurisdiction-specific extractors (``DCExtractor``)
    only activate when the source rule's jurisdiction matches, so we
    don't generate spurious ``us-dc/...`` targets from a federal body
    that happens to contain a look-alike pattern.
    """
    extractors: list[Extractor] = [USCExtractor(), CFRExtractor()]
    if jurisdiction == "us-dc":
        extractors.append(DCExtractor())
    return extractors


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


def extract_all(body: str, jurisdiction: str | None = None) -> list[ExtractedRef]:
    """Run every applicable extractor over ``body`` and merge results.

    ``jurisdiction`` routes jurisdiction-scoped extractors (e.g. DC
    intra-code cites). Defaults to None, which runs only the universal
    (federal) extractors — backward-compatible with callers that don't
    know about jurisdiction.

    Overlapping matches at the same span are deduplicated in favor of
    the highest-confidence extractor. The returned list is sorted by
    ``start_offset`` so callers can stream-process body text.
    """
    refs: list[ExtractedRef] = []
    for e in all_extractors(jurisdiction):
        refs.extend(e.extract(body))
    return _dedupe(refs)
