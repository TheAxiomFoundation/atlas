-- Robust version-aware corpus visibility (no synchronous backfill).
--
-- Context: the earlier migrations 20260513100000 + 20260513101000 tried
-- to install a version-aware visibility model. They failed because the
-- synchronous UPDATE backfill on corpus.provisions (~5M rows) hit the
-- pooler's statement_timeout. The transactions rolled back but
-- intermediate effects (RLS policy + dropped path index) leaked,
-- leaving the navigation tree returning 503/timeouts to the app.
--
-- This migration installs the version-aware design WITHOUT requiring a
-- backfill. The view and policy treat NULL version as "match any active
-- scope for this jurisdiction × document_class," preserving today's
-- behavior for un-backfilled rows. New loads (which carry a specific
-- version per ProvisionRecord.version) get version-precise filtering.
--
-- Migration path:
--   * Phase 1 (this migration): add column nullable, install
--     NULL-fallback views/policies. App restored. Backward compatible.
--   * Phase 2 (deferred): backfill the version column in chunks via a
--     CLI tool or a pg_cron job, outside any pooler timeout window.
--   * Phase 3 (deferred): once all rows are backfilled, tighten the
--     view to require an exact version match (drop the NULL fallback).
--
-- All statements are idempotent (IF NOT EXISTS / OR REPLACE).

-- ============================================================================
-- 1. Add nullable version columns (instant; no backfill triggered).
-- ============================================================================

ALTER TABLE corpus.provisions
  ADD COLUMN IF NOT EXISTS version TEXT;

COMMENT ON COLUMN corpus.provisions.version IS
  'Source/release version label. NULL until backfilled or until written by '
  'a versioned load. Visibility rules treat NULL as wildcard for backward '
  'compatibility during the rolling migration.';

ALTER TABLE corpus.navigation_nodes
  ADD COLUMN IF NOT EXISTS version TEXT;

COMMENT ON COLUMN corpus.navigation_nodes.version IS
  'Source/release version label, mirrors corpus.provisions.version semantics.';

-- ============================================================================
-- 2. Restore the path index that 101000 dropped, plus add version-scoped
--    indexes for efficient version filtering.
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_navigation_nodes_path
  ON corpus.navigation_nodes (path);

CREATE INDEX IF NOT EXISTS idx_provisions_release_scope_version
  ON corpus.provisions (
    jurisdiction,
    (COALESCE(NULLIF(doc_type, ''), 'unknown')),
    version
  );

CREATE INDEX IF NOT EXISTS idx_navigation_nodes_scope_version_parent_sort
  ON corpus.navigation_nodes (jurisdiction, doc_type, version, parent_path, sort_key);

-- ============================================================================
-- 3. Version-aware views with NULL fallback.
--
-- A row is visible when there is an active release_scope for its
-- (jurisdiction, document_class) AND EITHER the row has a specific
-- version that matches the scope OR the row's version is NULL (treated
-- as wildcard).
-- ============================================================================

CREATE OR REPLACE VIEW corpus.current_provisions AS
SELECT p.*
FROM corpus.provisions p
WHERE EXISTS (
  SELECT 1
  FROM corpus.current_release_scopes s
  WHERE s.jurisdiction = p.jurisdiction
    AND s.document_class = COALESCE(NULLIF(p.doc_type, ''), 'unknown')
    AND (p.version IS NULL OR s.version = p.version)
);

CREATE OR REPLACE VIEW corpus.legacy_provisions AS
SELECT p.*
FROM corpus.provisions p
WHERE NOT EXISTS (
  SELECT 1
  FROM corpus.current_release_scopes s
  WHERE s.jurisdiction = p.jurisdiction
    AND s.document_class = COALESCE(NULLIF(p.doc_type, ''), 'unknown')
    AND (p.version IS NULL OR s.version = p.version)
);

CREATE OR REPLACE VIEW corpus.current_navigation_nodes AS
SELECT n.*
FROM corpus.navigation_nodes n
WHERE EXISTS (
  SELECT 1
  FROM corpus.current_release_scopes s
  WHERE s.jurisdiction = n.jurisdiction
    AND s.document_class = COALESCE(NULLIF(n.doc_type, ''), 'unknown')
    AND (n.version IS NULL OR s.version = n.version)
);

GRANT SELECT ON corpus.current_navigation_nodes TO anon, authenticated;
GRANT SELECT ON corpus.current_navigation_nodes TO postgres, service_role;

-- ============================================================================
-- 4. RLS policies on navigation_nodes — same NULL-fallback semantics.
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
        AND (navigation_nodes.version IS NULL OR s.version = navigation_nodes.version)
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
        AND (navigation_nodes.version IS NULL OR s.version = navigation_nodes.version)
    )
  );

-- ============================================================================
-- 5. RPC update — return counts from the version-aware view (which now
--    includes NULL-version rows via the fallback).
-- ============================================================================

CREATE OR REPLACE FUNCTION corpus.get_navigation_node_counts()
RETURNS TABLE (
  jurisdiction text,
  document_class text,
  node_count bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
AS $$
  SELECT
    jurisdiction,
    COALESCE(NULLIF(doc_type, ''), 'unknown') AS document_class,
    COUNT(*)::bigint AS node_count
  FROM corpus.current_navigation_nodes
  WHERE jurisdiction IS NOT NULL
  GROUP BY jurisdiction, COALESCE(NULLIF(doc_type, ''), 'unknown')
  ORDER BY jurisdiction, document_class
$$;

GRANT EXECUTE ON FUNCTION corpus.get_navigation_node_counts() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.get_navigation_node_counts() TO postgres, service_role;

-- ============================================================================
-- 6. Refresh the materialized view + reload PostgREST schema cache.
-- ============================================================================

REFRESH MATERIALIZED VIEW corpus.current_provision_counts;

NOTIFY pgrst, 'reload schema';
