"""Citation extraction — find references in rule body text.

The extractor produces :class:`ExtractedRef` records suitable for the
``corpus.provision_references`` table. One table, two consumers:

* Axiom viewer renders body text with clickable ``<a>`` tags at the
  recorded ``(start_offset, end_offset)`` spans.
* RuleSpec tooling (axiom-encode, axiom-rules-engine) uses the outgoing refs of an
  encoded rule as the candidate list for its ``imports:`` block.

The extractor is scope-limited today to USC and CFR citation patterns —
these cover the overwhelming majority of inter-rule references in the
federal corpus. Adding a new pattern is an additive change: register a
new :class:`Extractor` subclass in :func:`all_extractors`.
"""

from __future__ import annotations

from .extractor import (
    CAExtractor,
    CFRExtractor,
    DCExtractor,
    ExtractedRef,
    Extractor,
    NYExtractor,
    USCExtractor,
    all_extractors,
    extract_all,
)

__all__ = [
    "CAExtractor",
    "CFRExtractor",
    "DCExtractor",
    "ExtractedRef",
    "Extractor",
    "NYExtractor",
    "USCExtractor",
    "all_extractors",
    "extract_all",
]
