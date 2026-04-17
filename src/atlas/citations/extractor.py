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


# --- NY extractor ---------------------------------------------------------


# Map from lowercased, punctuation-light law names to the directory
# codes used as the second path segment in ``us-ny/statute/{code}/...``.
# Populated from the 40 law codes present in the corpus — see the
# ingest driver's source repo (``rules-us-ny``) for the canonical set.
# Entries kept conservative: we'd rather miss a cross-law ref than
# mis-resolve one. Alternative spellings (with/without Oxford comma,
# apostrophe) are duplicated so regex text matches regardless of
# punctuation.
_NY_LAW_CODES: dict[str, str] = {
    "alcoholic beverage control": "abc",
    "agriculture and markets": "agm",
    "banking": "bnk",
    "business corporation": "bsc",
    "criminal procedure": "cpl",
    "civil rights": "cvr",
    "civil service": "cvs",
    "domestic relations": "dom",
    "education": "edn",
    "elder": "eld",
    "election": "eln",
    "environmental conservation": "env",
    "estates, powers and trusts": "ept",
    "estates powers and trusts": "ept",
    "executive": "exc",
    "general business": "gbs",
    "general municipal": "gmu",
    "general obligations": "gob",
    "highway": "hay",
    "labor": "lab",
    "limited liability company": "llc",
    "mental hygiene": "mhy",
    "military": "mil",
    "public health": "pbh",
    "penal": "pen",
    "social services": "sos",
    "tax": "tax",
    "town": "twn",
    "vehicle and traffic": "vat",
    "village": "vil",
    "workers' compensation": "wkc",
    "workers compensation": "wkc",
}


class NYExtractor(Extractor):
    """Matches New York statute references.

    Two forms, both resolving to ``us-ny/statute/{code}/{section}``:

    1. **Cross-law**: ``section N of the {LAW_NAME} law``. The law
       name is mapped to a code via ``_NY_LAW_CODES``. Example::

           section 19-0309 of the environmental conservation law
           → us-ny/statute/env/19-0309

    2. **Intra-code**: bare ``section N`` inside a body whose source
       rule is at ``us-ny/statute/{code}/...``. The enclosing code
       is the default scope. Example (source rule ``us-ny/statute/tax/606``)::

           section 32              → us-ny/statute/tax/32

       Intra-code matches are emitted at confidence 0.7 (down from 1.0)
       because bare ``section N`` is inherently ambiguous — the caller
       may choose to threshold on confidence downstream.

    False-positive guards:

    * Intra-code form skips matches followed by ``of the ... Code``
      (federal IRC) or ``of the ... Law`` (cross-law — let the cross-law
      pattern claim those).
    * Cross-law form requires the law name to be in the map; unknown
      names produce no ref.
    """

    pattern_kind: ClassVar[str] = "ny"

    # Cross-law form: "section N[sub] of the {LAW_NAME} law". Built
    # dynamically below from the law-name map so adding a law is a
    # one-line edit to the dict.
    _CROSS_LAW: ClassVar[re.Pattern[str]] = re.compile(
        r"\bsection\s+"
        r"(?P<section>\d+[A-Za-z]?(?:-[A-Za-z0-9]+)?(?:\.[A-Za-z0-9]+)?)"
        rf"(?P<sub>{_SUBSECTION_CHAIN})"
        r"\s+of\s+the\s+"
        r"(?P<law>"
        + "|".join(
            re.escape(name) for name in sorted(_NY_LAW_CODES, key=len, reverse=True)
        )
        + r")"
        r"\s+law\b",
        re.IGNORECASE,
    )

    # Bare intra-code form. Followed-by guards are applied in code,
    # not in the regex, to keep the pattern readable.
    _INTRA: ClassVar[re.Pattern[str]] = re.compile(
        r"\bsection\s+"
        r"(?P<section>\d+[A-Za-z]?(?:-[A-Za-z0-9]+)?(?:\.[A-Za-z0-9]+)?)"
        rf"(?P<sub>{_SUBSECTION_CHAIN})",
        re.IGNORECASE,
    )

    # Tail text patterns that indicate the match is NOT an intra-code
    # NY reference — another extractor (or nothing) will claim it.
    _TAIL_NOT_INTRA: ClassVar[re.Pattern[str]] = re.compile(
        r"^\s*(?:of\s+the\s+[A-Za-z, ']+?\s+(?:law|code|act)\b)",
        re.IGNORECASE,
    )

    source_citation_path: str | None = None

    def __init__(self, source_citation_path: str | None = None) -> None:
        self.source_citation_path = source_citation_path

    @staticmethod
    def _enclosing_code(source_citation_path: str | None) -> str | None:
        """Return the NY law code embedded in a source rule path, or None.

        ``us-ny/statute/tax/606/a`` → ``tax``
        ``us-ny/statute/env/19-0309`` → ``env``
        """
        if not source_citation_path:
            return None
        parts = source_citation_path.split("/")
        if len(parts) >= 3 and parts[0] == "us-ny" and parts[1] == "statute":
            return parts[2]
        return None

    def extract(self, body: str) -> list[ExtractedRef]:
        refs: list[ExtractedRef] = []
        for m in self._CROSS_LAW.finditer(body):
            law = m.group("law").lower().strip()
            code = _NY_LAW_CODES.get(law)
            if code is None:  # pragma: no cover — regex alternation forbids
                continue
            refs.append(self._build_ref(m, code, confidence=1.0))

        enclosing_code = self._enclosing_code(self.source_citation_path)
        if enclosing_code is not None and enclosing_code in set(_NY_LAW_CODES.values()):
            # Intra-code bare "section N" — only when we know the scope
            # and the enclosing code is a known NY law.
            for m in self._INTRA.finditer(body):
                # Skip if this span was already consumed by the cross-law
                # match above — the tail would start with "of the X law".
                tail = body[m.end() :]
                if self._TAIL_NOT_INTRA.match(tail):
                    continue
                refs.append(self._build_ref(m, enclosing_code, confidence=0.7))
        return refs

    def _build_ref(
        self, match: re.Match[str], code: str, confidence: float
    ) -> ExtractedRef:
        section = match.group("section")
        sub = match.group("sub") or ""
        path_parts = ["us-ny", "statute", code, section]
        path_parts.extend(_subsection_segments(sub))
        return ExtractedRef(
            raw_text=match.group(0),
            pattern_kind=self.pattern_kind,
            target_citation_path="/".join(path_parts),
            start_offset=match.start(),
            end_offset=match.end(),
            confidence=confidence,
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

    # DC Code has 51 numbered titles; we pad to 60 to cover any future
    # additions and alpha-suffix variants (24A, 29A, ...). Anything
    # higher is almost certainly a range enumeration or a misread.
    _MAX_TITLE = 60

    def to_ref(self, match: re.Match[str]) -> ExtractedRef | None:
        title = match.group("title")
        section = match.group("section")
        sub = match.group("sub") or ""

        # Reject out-of-range titles: matches like "§§ 100-110" (a cross-
        # reference range) would otherwise produce bogus us-dc/statute/100
        # rows that never resolve. Parse the leading digits of whichever
        # title form we matched (plain, alpha-suffix, or colon).
        numeric_head = re.match(r"\d+", title)
        if not numeric_head:  # pragma: no cover — regex forbids non-digit start
            return None
        if int(numeric_head.group(0)) > self._MAX_TITLE:
            return None

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


def all_extractors(
    jurisdiction: str | None = None,
    source_citation_path: str | None = None,
) -> list[Extractor]:
    """Extractors active for ``jurisdiction``.

    USC and CFR are universal — every corpus can cite federal statutes
    and regulations. Jurisdiction-specific extractors activate when
    the source rule's jurisdiction matches, so we don't generate
    spurious state targets from a federal body that happens to
    contain a look-alike pattern.

    ``source_citation_path`` is passed to extractors (currently NY)
    that use the enclosing rule's path to resolve ambiguous references
    like bare ``section N``.
    """
    extractors: list[Extractor] = [USCExtractor(), CFRExtractor()]
    if jurisdiction == "us-dc":
        extractors.append(DCExtractor())
    if jurisdiction == "us-ny":
        extractors.append(NYExtractor(source_citation_path=source_citation_path))
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


def extract_all(
    body: str,
    jurisdiction: str | None = None,
    source_citation_path: str | None = None,
) -> list[ExtractedRef]:
    """Run every applicable extractor over ``body`` and merge results.

    ``jurisdiction`` routes jurisdiction-scoped extractors (e.g. DC
    intra-code cites). Defaults to None, which runs only the universal
    (federal) extractors — backward-compatible with callers that don't
    know about jurisdiction.

    ``source_citation_path`` is used by extractors that disambiguate
    bare references against the enclosing rule (e.g. NY intra-code
    "section 32" resolving against the source's law code).

    Overlapping matches at the same span are deduplicated in favor of
    the highest-confidence extractor. The returned list is sorted by
    ``start_offset`` so callers can stream-process body text.
    """
    refs: list[ExtractedRef] = []
    for e in all_extractors(jurisdiction, source_citation_path):
        refs.extend(e.extract(body))
    return _dedupe(refs)
