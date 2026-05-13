-- Make corpus.get_navigation_node_counts() fast again by querying the
-- base table directly instead of the version-aware view.
--
-- The 20260513140000 migration pointed the RPC at corpus.current_navigation_nodes,
-- which evaluates an EXISTS subquery (against corpus.current_release_scopes,
-- with the NULL-fallback OR clause) for each of ~2.6M navigation rows.
-- The pooler's statement_timeout kills the aggregate query before it
-- finishes — verify-release-coverage and any consumer of the RPC times out.
--
-- The verify-release-coverage check compares nav-row counts to
-- corpus.current_provision_counts (a materialized view that's already
-- version-aware). For the verify check's purpose ("nav rows exist where
-- current_provisions doesn't"), counting all nav rows (regardless of
-- version filter) is correct: the comparison logic catches "loaded but
-- unpublished" jurisdictions either way.
--
-- The right long-term shape is a materialized view of nav counts
-- refreshed alongside corpus.current_provision_counts. Tracked for
-- a follow-up; for now the base-table aggregate is both fast and
-- correct for the check's purpose.

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
  FROM corpus.navigation_nodes
  WHERE jurisdiction IS NOT NULL
  GROUP BY jurisdiction, COALESCE(NULLIF(doc_type, ''), 'unknown')
  ORDER BY jurisdiction, document_class
$$;

GRANT EXECUTE ON FUNCTION corpus.get_navigation_node_counts() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.get_navigation_node_counts() TO postgres, service_role;

NOTIFY pgrst, 'reload schema';
