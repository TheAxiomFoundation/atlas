-- Normalize literal `\n` escape sequences in NY rule bodies to real newlines.
--
-- Problem
-- -------
-- ``scripts/ingest_ny_laws.py``'s ``extract_text`` uses ``"\n\n".join(...)``
-- to stitch paragraphs, which produces real U+000A characters in the
-- Python string. But somewhere in the ingest pipeline these got double-
-- escaped — bodies in ``akn.rules`` now contain a mixture of real U+000A
-- and literal two-character ``\n`` (U+005C + U+006E) sequences.
--
-- Consequences
-- ------------
-- * The viewer renders ``whitespace-pre-wrap`` and therefore shows
--   literal ``\n`` as text rather than a line break — ugly on the page.
-- * Citation extractors use ``\s+`` between tokens. Literal backslash-n
--   is not whitespace, so patterns like "section 19-0309 of\\nthe
--   environmental conservation law" fail to match, forcing the
--   intra-code fallback to produce incorrect ``us-ny/statute/tax/...``
--   targets for what should be cross-law refs to ``us-ny/statute/env/...``.
--
-- Fix
-- ---
-- One-shot: replace every literal ``\n`` in NY bodies with a real
-- newline. After this runs, a re-run of ``extract_references.py
-- --prefix us-ny/`` rebuilds refs against the normalized bodies, and
-- ``akn.rule_references`` rows whose offsets point into the pre-
-- normalization bodies get deleted as part of the re-run's
-- idempotent DELETE-then-INSERT per source rule.
--
-- This is scoped to ``us-ny`` because DC and USC/CFR bodies were
-- verified clean via a spot check — only the NY ingest had the
-- escape-handling bug.

BEGIN;

UPDATE akn.rules
SET body = replace(body, '\n', E'\n')
WHERE jurisdiction = 'us-ny'
  AND body LIKE '%\n%';

-- Sanity: no NY body should still contain a literal two-char \n.
DO $$
DECLARE
  stragglers integer;
BEGIN
  SELECT COUNT(*) INTO stragglers
  FROM akn.rules
  WHERE jurisdiction = 'us-ny'
    AND body LIKE '%\n%';
  IF stragglers > 0 THEN
    RAISE EXCEPTION 'NY newline normalization incomplete: % rows still have literal \n', stragglers;
  END IF;
END $$;

COMMIT;
