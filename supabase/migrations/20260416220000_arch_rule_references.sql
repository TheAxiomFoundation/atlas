-- Citation graph across arch.rules.
--
-- One row per cite extracted from a source rule's body text. A row
-- captures WHERE the cite appeared, WHAT text it was, and WHAT it
-- resolves to — even when the target isn't yet ingested.
--
-- Dual consumer:
--   * Atlas viewer — render body text with clickable <a> tags at
--     (start_offset, end_offset), linking to /atlas/{target_citation_path}.
--   * RAC tooling (autorac, rac-compile) — use the outgoing refs of an
--     encoded rule as the candidate list for its `imports:` block.
--
-- The table is append-only on extraction; the extractor is idempotent
-- per (source_rule_id, start_offset) via a unique index. Re-running the
-- extractor on a rule whose body changed will produce duplicates with
-- stale offsets — callers should DELETE the existing rows for that
-- source_rule_id first. We don't enforce this via trigger because the
-- extractor runs as a batch and owns the lifecycle.

CREATE TABLE IF NOT EXISTS arch.rule_references (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- The rule containing the citation (always resolved).
  source_rule_id        UUID NOT NULL REFERENCES arch.rules(id) ON DELETE CASCADE,

  -- The rule being cited. `target_citation_path` is always present;
  -- `target_rule_id` is resolved iff that path exists in arch.rules.
  -- Unresolved refs still carry the path so re-running the linker after
  -- future ingestions can connect them.
  target_citation_path  TEXT NOT NULL,
  target_rule_id        UUID REFERENCES arch.rules(id) ON DELETE SET NULL,

  -- Raw text of the citation as it appears in the body, for display.
  citation_text         TEXT NOT NULL,

  -- Classifier: "usc" | "cfr" | "internal" | "public_law" | "stat" | ...
  -- Extensible — new extractors just pick a new string.
  pattern_kind          TEXT NOT NULL,

  -- Char offsets inside `arch.rules.body`, suitable for the viewer to
  -- wrap a substring in an <a> tag without reparsing.
  start_offset          INTEGER NOT NULL,
  end_offset            INTEGER NOT NULL CHECK (end_offset > start_offset),

  -- 1.0 = pattern-unambiguous (e.g. "42 U.S.C. 9902(2)"). Lower for
  -- heuristic resolutions (e.g. bare "section 32" resolved against the
  -- enclosing title). Callers can filter.
  confidence            REAL NOT NULL DEFAULT 1.0
                          CHECK (confidence >= 0 AND confidence <= 1),

  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency guard: the same extractor run on the same body should
-- upsert, not duplicate.
CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_references_source_span
  ON arch.rule_references (source_rule_id, start_offset, end_offset);

-- Outgoing lookups: "what does THIS rule cite?" — primary path for both
-- the viewer's body-render and RAC's import candidates.
CREATE INDEX IF NOT EXISTS idx_rule_references_source
  ON arch.rule_references (source_rule_id);

-- Incoming lookups: "what cites THIS rule?" — powers a future
-- back-references panel on the rule page, and analytics.
CREATE INDEX IF NOT EXISTS idx_rule_references_target_id
  ON arch.rule_references (target_rule_id)
  WHERE target_rule_id IS NOT NULL;

-- Unresolved citations grouped by path — used when backfilling after a
-- new ingestion to find refs that can now be resolved.
CREATE INDEX IF NOT EXISTS idx_rule_references_target_path_unresolved
  ON arch.rule_references (target_citation_path)
  WHERE target_rule_id IS NULL;

-- Count references by kind for telemetry.
CREATE INDEX IF NOT EXISTS idx_rule_references_kind
  ON arch.rule_references (pattern_kind);


-- ======================================================================
-- RPC: get_references(citation_path)
-- ======================================================================
-- Returns outgoing and incoming refs for a given rule, oriented for the
-- viewer's render path: outgoing refs carry offsets (for body rewrites)
-- and the resolved/unresolved target; incoming refs carry the source's
-- citation_path and heading so they display as a back-references list.

CREATE OR REPLACE FUNCTION arch.get_references(citation_path_in text)
RETURNS TABLE (
  direction              text,           -- 'outgoing' | 'incoming'
  citation_text          text,
  pattern_kind           text,
  confidence             real,
  start_offset           integer,
  end_offset             integer,

  -- For outgoing: the thing being cited.
  -- For incoming: the thing doing the citing.
  other_citation_path    text,
  other_rule_id          uuid,
  other_heading          text,

  -- Only set for outgoing rows — tells the viewer whether the link
  -- target is in the archive yet.
  target_resolved        boolean
)
LANGUAGE sql
STABLE
AS $$
  WITH self AS (
    SELECT id FROM arch.rules
    WHERE citation_path = citation_path_in
    LIMIT 1
  )
  SELECT
    'outgoing'::text AS direction,
    r.citation_text,
    r.pattern_kind,
    r.confidence,
    r.start_offset,
    r.end_offset,
    r.target_citation_path AS other_citation_path,
    r.target_rule_id AS other_rule_id,
    tgt.heading AS other_heading,
    (r.target_rule_id IS NOT NULL) AS target_resolved
  FROM arch.rule_references r
  JOIN self ON r.source_rule_id = self.id
  LEFT JOIN arch.rules tgt ON tgt.id = r.target_rule_id

  UNION ALL

  SELECT
    'incoming'::text AS direction,
    r.citation_text,
    r.pattern_kind,
    r.confidence,
    r.start_offset,
    r.end_offset,
    src.citation_path AS other_citation_path,
    r.source_rule_id AS other_rule_id,
    src.heading AS other_heading,
    TRUE AS target_resolved
  FROM arch.rule_references r
  JOIN self ON r.target_rule_id = self.id
  JOIN arch.rules src ON src.id = r.source_rule_id

  ORDER BY direction, start_offset NULLS LAST;
$$;

GRANT EXECUTE ON FUNCTION arch.get_references(text) TO anon;
GRANT EXECUTE ON FUNCTION arch.get_references(text) TO authenticated;


-- ======================================================================
-- Grants + RLS (matches prod; Supabase defaults don't cover new tables)
-- ======================================================================

GRANT ALL ON TABLE arch.rule_references TO postgres, service_role;
GRANT SELECT ON TABLE arch.rule_references TO anon, authenticated;

ALTER TABLE arch.rule_references ENABLE ROW LEVEL SECURITY;

-- Public-read policies, mirroring how arch.rules is configured: the data
-- is an open archive, RLS is on for defense-in-depth. Writes are
-- restricted to postgres/service_role through grant scoping above.
CREATE POLICY anon_read ON arch.rule_references
  FOR SELECT TO anon USING (true);
CREATE POLICY authenticated_read ON arch.rule_references
  FOR SELECT TO authenticated USING (true);
