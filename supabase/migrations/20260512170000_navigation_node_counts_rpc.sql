-- Server-side aggregation for `corpus.navigation_nodes` so the
-- verify_release_coverage check (CLI: axiom-corpus-ingest
-- verify-release-coverage) doesn't have to paginate the full table.
--
-- corpus.navigation_nodes is ~2.4M rows at the time of writing. PostgREST
-- caps response payloads at 1000 rows, so client-side counting via
-- pagination needs ~2,400 round trips — too slow for CI and brittle in
-- general. This function returns one row per (jurisdiction, doc_type)
-- in a single call.

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
