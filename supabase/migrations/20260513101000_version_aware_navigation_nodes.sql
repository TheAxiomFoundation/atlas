-- Navigation rows are derived from provisions, so they need the same release
-- version boundary. Otherwise a staged navigation rebuild can overwrite the
-- currently published tree for the same citation paths.

ALTER TABLE corpus.navigation_nodes
  ADD COLUMN IF NOT EXISTS version TEXT;

COMMENT ON COLUMN corpus.navigation_nodes.version IS
  'Source/release version label used to join navigation rows to corpus.release_scopes.';

WITH single_active_scope AS (
  SELECT
    jurisdiction,
    document_class,
    MAX(version) AS version
  FROM corpus.current_release_scopes
  GROUP BY jurisdiction, document_class
  HAVING COUNT(*) = 1
)
UPDATE corpus.navigation_nodes n
SET version = s.version
FROM single_active_scope s
WHERE n.version IS NULL
  AND s.jurisdiction = n.jurisdiction
  AND s.document_class = COALESCE(NULLIF(n.doc_type, ''), 'unknown');

DROP INDEX IF EXISTS corpus.idx_navigation_nodes_path;

CREATE UNIQUE INDEX IF NOT EXISTS idx_navigation_nodes_path_version
  ON corpus.navigation_nodes (path, (COALESCE(version, '')));

CREATE INDEX IF NOT EXISTS idx_navigation_nodes_scope_version_parent_sort
  ON corpus.navigation_nodes (jurisdiction, doc_type, version, parent_path, sort_key);

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

NOTIFY pgrst, 'reload schema';
