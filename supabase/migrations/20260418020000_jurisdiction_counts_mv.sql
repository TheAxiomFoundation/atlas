-- Materialized view backing the per-jurisdiction breakdown on the
-- Atlas landing page.
--
-- An exact GROUP BY jurisdiction over 600k rows runs ~3s on the live
-- cluster and busts PostgREST's statement timeout. A materialized
-- view gives us instant reads at the cost of a periodic REFRESH.
-- Staleness isn't a concern for a landing-page stat — the counts
-- refresh with each ingest run (call REFRESH MATERIALIZED VIEW at
-- the tail of the driver or manually).
--
-- Schema
-- ------
-- jurisdiction  — e.g. 'us', 'us-ny', 'us-dc', 'uk', 'ca'
-- rule_count    — approximate count (exact at last refresh)

CREATE MATERIALIZED VIEW IF NOT EXISTS akn.jurisdiction_counts AS
SELECT
  jurisdiction,
  COUNT(*)::bigint AS rule_count
FROM akn.rules
WHERE jurisdiction IS NOT NULL
GROUP BY jurisdiction;

-- Unique index lets us REFRESH CONCURRENTLY without blocking readers
-- once the view is populated.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jurisdiction_counts_jurisdiction
  ON akn.jurisdiction_counts (jurisdiction);

-- Initial populate. Subsequent refreshes use CONCURRENTLY.
REFRESH MATERIALIZED VIEW akn.jurisdiction_counts;

GRANT SELECT ON akn.jurisdiction_counts TO anon, authenticated;

-- Expand get_atlas_stats to include the per-jurisdiction breakdown
-- as a sorted array of {jurisdiction, count} pairs.
CREATE OR REPLACE FUNCTION akn.get_atlas_stats()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = akn, public
AS $$
  SELECT jsonb_build_object(
    'rules_count',
      GREATEST(
        (
          SELECT reltuples::bigint
          FROM pg_class
          WHERE oid = 'akn.rules'::regclass
        ),
        0
      ),
    'references_count',
      GREATEST(
        (
          SELECT reltuples::bigint
          FROM pg_class
          WHERE oid = 'akn.rule_references'::regclass
        ),
        0
      ),
    'jurisdictions_count',
      (
        SELECT COUNT(*)::int
        FROM akn.jurisdiction_counts
      ),
    'jurisdictions',
      COALESCE(
        (
          SELECT jsonb_agg(
                   jsonb_build_object(
                     'jurisdiction', jurisdiction,
                     'count', rule_count
                   )
                   ORDER BY rule_count DESC
                 )
          FROM akn.jurisdiction_counts
        ),
        '[]'::jsonb
      )
  )
$$;

NOTIFY pgrst, 'reload schema';
