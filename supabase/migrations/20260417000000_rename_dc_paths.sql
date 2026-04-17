-- Rename DC Code citation_paths from the sentinel "0" title slot to the
-- real title number embedded in the section identifier.
--
-- Background
-- ----------
-- The upstream ``DCConverter._to_section`` populates ``Citation(title=0,
-- section="DC-{number}")`` because the USC-oriented ``Citation`` model
-- wants an integer title and DC's numbering includes alpha and colon
-- forms (29A, 28:9, etc.). The downstream ``section_to_rules`` then
-- builds ``us-dc/statute/0/DC-47-1801.04`` — correct internally but
-- ugly as a URL and not what anyone citing DC Code § 47-1801.04 would
-- naturally type.
--
-- Effect
-- ------
-- Rewrites every ``us-dc/statute/0/DC-{TITLE}-{REST}`` path to
-- ``us-dc/statute/{TITLE}/{TITLE}-{REST}``. 130,617 rows, 0 collisions
-- verified pre-flight. The regex ``[^-]+`` for the title captures all
-- three DC numbering shapes we see in the corpus:
--
--   * plain numeric:   DC-47-1801.04    →  us-dc/statute/47/47-1801.04
--   * alpha suffix:    DC-29A-1001      →  us-dc/statute/29A/29A-1001
--   * UCC colon form:  DC-28:9-316/i/1  →  us-dc/statute/28:9/28:9-316/i/1
--
-- Scope
-- -----
-- citation_path only. Row ``id`` stays the same (deterministic UUID
-- seeded on the old path, but the id is an opaque identifier — nothing
-- outside ``akn.rules.id`` looks at how it was constructed). No
-- references in ``akn.rule_references.target_citation_path`` point at
-- DC paths today, since the USC/CFR/IRC extractors don't emit
-- ``us-dc/...`` targets; no refs-table fixups needed.
--
-- Future DC ingests
-- -----------------
-- ``scripts/ingest_dc_code.py`` has been updated in the same PR to
-- build clean paths directly, so re-runs don't regress to the old
-- shape. That driver also uses deterministic IDs seeded on the NEW
-- path, so re-ingestion post-rename upserts onto the renamed rows
-- cleanly.

BEGIN;

UPDATE akn.rules
SET citation_path = regexp_replace(
    citation_path,
    '^us-dc/statute/0/DC-([^-]+)-',
    'us-dc/statute/\1/\1-'
  )
WHERE jurisdiction = 'us-dc'
  AND citation_path LIKE 'us-dc/statute/0/DC-%';

-- Sanity: no rows should still carry the old "/0/DC-" prefix.
DO $$
DECLARE
  stragglers integer;
BEGIN
  SELECT COUNT(*) INTO stragglers
  FROM akn.rules
  WHERE jurisdiction = 'us-dc'
    AND citation_path LIKE 'us-dc/statute/0/DC-%';
  IF stragglers > 0 THEN
    RAISE EXCEPTION 'DC rename incomplete: % rows still have old prefix', stragglers;
  END IF;
END $$;

COMMIT;
