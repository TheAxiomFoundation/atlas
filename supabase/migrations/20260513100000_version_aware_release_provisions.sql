-- Complete the release-scope model by carrying the source version on each
-- provision row. A current release row now exposes only provisions for its
-- exact (jurisdiction, document_class, version), not every row in that
-- jurisdiction/document class.

ALTER TABLE corpus.provisions
  ADD COLUMN IF NOT EXISTS version TEXT;

COMMENT ON COLUMN corpus.provisions.version IS
  'Source/release version label used to join provisions to corpus.release_scopes.';

WITH single_active_scope AS (
  SELECT
    jurisdiction,
    document_class,
    MAX(version) AS version
  FROM corpus.current_release_scopes
  GROUP BY jurisdiction, document_class
  HAVING COUNT(*) = 1
)
UPDATE corpus.provisions p
SET version = s.version
FROM single_active_scope s
WHERE p.version IS NULL
  AND s.jurisdiction = p.jurisdiction
  AND s.document_class = COALESCE(NULLIF(p.doc_type, ''), 'unknown');

CREATE INDEX IF NOT EXISTS idx_provisions_release_scope_version
  ON corpus.provisions (
    jurisdiction,
    (COALESCE(NULLIF(doc_type, ''), 'unknown')),
    version
  );

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

REFRESH MATERIALIZED VIEW corpus.current_provision_counts;

NOTIFY pgrst, 'reload schema';
