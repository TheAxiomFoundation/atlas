-- RPC for chunked backfill of the version column on corpus.provisions
-- and corpus.navigation_nodes.
--
-- The original 20260513100000 migration tried to backfill in one shot
-- via a single UPDATE on ~5M rows and timed out via the pooler's
-- statement_timeout. The transaction rolled back but left side effects
-- behind that took the app offline.
--
-- This RPC updates one chunk per call. The caller (e.g.,
-- ``axiom-corpus-ingest backfill-versions``) loops the RPC until it
-- returns 0 rows. Each chunk fits comfortably within statement_timeout,
-- so progress is steady and resumable.

CREATE OR REPLACE FUNCTION corpus.backfill_version_chunk(
  p_jurisdiction TEXT,
  p_document_class TEXT,
  p_version TEXT,
  p_table_name TEXT,
  p_chunk_size INT DEFAULT 50000
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = corpus, public
AS $$
DECLARE
  rows_updated BIGINT;
BEGIN
  IF p_chunk_size IS NULL OR p_chunk_size <= 0 THEN
    RAISE EXCEPTION 'chunk_size must be positive';
  END IF;

  IF p_table_name = 'provisions' THEN
    UPDATE corpus.provisions
    SET version = p_version
    WHERE id IN (
      SELECT id FROM corpus.provisions
      WHERE jurisdiction = p_jurisdiction
        AND COALESCE(NULLIF(doc_type, ''), 'unknown') = p_document_class
        AND version IS NULL
      LIMIT p_chunk_size
    );
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    RETURN rows_updated;
  ELSIF p_table_name = 'navigation_nodes' THEN
    UPDATE corpus.navigation_nodes
    SET version = p_version
    WHERE id IN (
      SELECT id FROM corpus.navigation_nodes
      WHERE jurisdiction = p_jurisdiction
        AND COALESCE(NULLIF(doc_type, ''), 'unknown') = p_document_class
        AND version IS NULL
      LIMIT p_chunk_size
    );
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    RETURN rows_updated;
  ELSE
    RAISE EXCEPTION 'Unknown table_name: % (must be ''provisions'' or ''navigation_nodes'')', p_table_name;
  END IF;
END;
$$;

-- Service role only — this is a write operation on production data.
GRANT EXECUTE ON FUNCTION corpus.backfill_version_chunk(TEXT, TEXT, TEXT, TEXT, INT) TO postgres, service_role;
REVOKE EXECUTE ON FUNCTION corpus.backfill_version_chunk(TEXT, TEXT, TEXT, TEXT, INT) FROM anon, authenticated, PUBLIC;

-- Helper: returns single-active scopes that need backfilling.
-- Multi-active scopes are intentionally skipped — for them, the per-row
-- version cannot be determined retroactively without external context.
CREATE OR REPLACE FUNCTION corpus.list_single_active_release_scopes()
RETURNS TABLE (
  jurisdiction TEXT,
  document_class TEXT,
  version TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
AS $$
  SELECT
    jurisdiction,
    document_class,
    MAX(version) AS version
  FROM corpus.current_release_scopes
  GROUP BY jurisdiction, document_class
  HAVING COUNT(*) = 1
  ORDER BY jurisdiction, document_class
$$;

GRANT EXECUTE ON FUNCTION corpus.list_single_active_release_scopes() TO postgres, service_role;
REVOKE EXECUTE ON FUNCTION corpus.list_single_active_release_scopes() FROM anon, authenticated, PUBLIC;

NOTIFY pgrst, 'reload schema';
