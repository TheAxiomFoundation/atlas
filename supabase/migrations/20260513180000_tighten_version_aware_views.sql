-- Phase 3: tighten version-aware visibility — drop the NULL-fallback.
--
-- Background:
--   * 20260513140000 installed the version-aware design with a
--     NULL-fallback so existing un-backfilled rows stayed visible while
--     the rolling backfill ran.
--   * The chunked backfill (`axiom-corpus-ingest backfill-versions`) +
--     20260513170000 (us/guidance source_path parse) have now populated
--     `version` on every row of corpus.provisions and
--     corpus.navigation_nodes.
--
-- Tightening means: a row is visible only when an active release_scope
-- matches its EXACT version. NULL-version rows become invisible — which
-- is the correct end state, because:
--   * load-supabase always writes the row's version
--   * a NULL version after this migration is a bug, not a wildcard
--
-- Prerequisite: zero rows with version IS NULL in either table. The CI
-- invariant `verify-release-coverage` and a smoke query (see PR body)
-- should confirm before applying.

-- ============================================================================
-- 1. Tightened views (drop the `p.version IS NULL OR` clause).
-- ============================================================================

CREATE OR REPLACE VIEW corpus.current_provisions AS
SELECT p.*
FROM corpus.provisions p
WHERE EXISTS (
  SELECT 1
  FROM corpus.current_release_scopes s
  WHERE s.jurisdiction = p.jurisdiction
    AND s.document_class = COALESCE(NULLIF(p.doc_type, ''), 'unknown')
    AND s.version = p.version
);

CREATE OR REPLACE VIEW corpus.legacy_provisions AS
SELECT p.*
FROM corpus.provisions p
WHERE NOT EXISTS (
  SELECT 1
  FROM corpus.current_release_scopes s
  WHERE s.jurisdiction = p.jurisdiction
    AND s.document_class = COALESCE(NULLIF(p.doc_type, ''), 'unknown')
    AND s.version = p.version
);

CREATE OR REPLACE VIEW corpus.current_navigation_nodes AS
SELECT n.*
FROM corpus.navigation_nodes n
WHERE EXISTS (
  SELECT 1
  FROM corpus.current_release_scopes s
  WHERE s.jurisdiction = n.jurisdiction
    AND s.document_class = COALESCE(NULLIF(n.doc_type, ''), 'unknown')
    AND s.version = n.version
);

GRANT SELECT ON corpus.current_navigation_nodes TO anon, authenticated;
GRANT SELECT ON corpus.current_navigation_nodes TO postgres, service_role;

-- ============================================================================
-- 2. RLS policies — same tightening (drop NULL-fallback).
-- ============================================================================

DROP POLICY IF EXISTS anon_read ON corpus.navigation_nodes;
CREATE POLICY anon_read ON corpus.navigation_nodes
  FOR SELECT TO anon
  USING (
    EXISTS (
      SELECT 1
      FROM corpus.current_release_scopes s
      WHERE s.jurisdiction = navigation_nodes.jurisdiction
        AND s.document_class = COALESCE(NULLIF(navigation_nodes.doc_type, ''), 'unknown')
        AND s.version = navigation_nodes.version
    )
  );

DROP POLICY IF EXISTS authenticated_read ON corpus.navigation_nodes;
CREATE POLICY authenticated_read ON corpus.navigation_nodes
  FOR SELECT TO authenticated
  USING (
    EXISTS (
      SELECT 1
      FROM corpus.current_release_scopes s
      WHERE s.jurisdiction = navigation_nodes.jurisdiction
        AND s.document_class = COALESCE(NULLIF(navigation_nodes.doc_type, ''), 'unknown')
        AND s.version = navigation_nodes.version
    )
  );

-- ============================================================================
-- 3. Refresh derived counts + reload PostgREST schema cache.
--
-- The non-concurrent variant of REFRESH MATERIALIZED VIEW on a 5M-row MV
-- exceeds the pooler statement_timeout. CONCURRENTLY is bounded and
-- non-blocking against readers, provided a unique index exists (it does:
-- idx_current_provision_counts_jurisdiction_document_class). We disable
-- statement_timeout for the refresh only.
-- ============================================================================

DO $$
BEGIN
  SET LOCAL statement_timeout = 0;
  REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.current_provision_counts;
END $$;

NOTIFY pgrst, 'reload schema';
