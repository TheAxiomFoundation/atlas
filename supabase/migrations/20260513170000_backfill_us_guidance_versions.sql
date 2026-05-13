-- Backfill version on us/guidance rows that were skipped by the
-- single-active-scope chunked backfill.
--
-- The CLI `axiom-corpus-ingest backfill-versions` deliberately skips
-- multi-active scopes (those with >1 active release_scope row for the
-- same jurisdiction × document_class), because the per-row version
-- cannot be inferred from `release_scopes` alone. The federal guidance
-- corpus has four concurrently-active versions (HHS poverty guidelines,
-- IRS Rev. Proc. 2025-32, two SNAP FY26 docs), so all 49 of its
-- provisions + 49 navigation_nodes were left with version IS NULL.
--
-- Those rows can still be deterministically backfilled because each
-- provision's `source_path` encodes the version segment:
--   sources/{jurisdiction}/{document_class}/{version}/...
-- So we parse the version out of source_path for provisions, then carry
-- it to navigation_nodes via the provision_id foreign key.
--
-- The update set is small (~49 rows per table), so this runs inline.

-- ============================================================================
-- 1. Provisions: derive version from the 4th path segment of source_path.
-- ============================================================================

UPDATE corpus.provisions
SET version = substring(source_path FROM '^sources/[^/]+/[^/]+/([^/]+)/')
WHERE jurisdiction = 'us'
  AND COALESCE(NULLIF(doc_type, ''), 'unknown') = 'guidance'
  AND version IS NULL
  AND source_path ~ '^sources/[^/]+/[^/]+/[^/]+/';

-- ============================================================================
-- 2. Navigation nodes: inherit version from the linked provision.
-- ============================================================================

UPDATE corpus.navigation_nodes n
SET version = p.version
FROM corpus.provisions p
WHERE n.provision_id = p.id::text
  AND n.jurisdiction = 'us'
  AND COALESCE(NULLIF(n.doc_type, ''), 'unknown') = 'guidance'
  AND n.version IS NULL
  AND p.version IS NOT NULL;

-- ============================================================================
-- 3. Refresh derived counts.
-- ============================================================================

REFRESH MATERIALIZED VIEW corpus.current_provision_counts;
