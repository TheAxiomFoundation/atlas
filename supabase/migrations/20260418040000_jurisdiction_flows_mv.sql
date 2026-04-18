-- Materialized view of the jurisdiction-to-jurisdiction citation flow,
-- powering the chord/Sankey visualization on the Atlas landing page.
--
-- Exact counts require joining akn.rule_references (148k rows) against
-- akn.rules twice to resolve source + target jurisdiction. That blows
-- the PostgREST statement timeout on every call. Precompute once and
-- refresh at end of each ingest via refresh_jurisdiction_counts.
--
-- Target jurisdiction is derived from pattern_kind rather than from a
-- JOIN on target_rule_id so unresolved refs (target_rule_id IS NULL)
-- still count. The mapping is closed: each extractor emits refs into
-- exactly one jurisdiction namespace.
--
-- Schema
-- ------
-- source_jurisdiction — jurisdiction of the citing rule (e.g. 'us-ny')
-- target_jurisdiction — jurisdiction of the cited rule (e.g. 'us')
-- ref_count            — how many citations go that direction

CREATE MATERIALIZED VIEW IF NOT EXISTS akn.jurisdiction_flows AS
SELECT
  src.jurisdiction AS source_jurisdiction,
  CASE r.pattern_kind
    WHEN 'usc' THEN 'us'
    WHEN 'cfr' THEN 'us'
    WHEN 'dc'  THEN 'us-dc'
    WHEN 'ny'  THEN 'us-ny'
    WHEN 'ca'  THEN 'us-ca'
    ELSE 'unknown'
  END AS target_jurisdiction,
  COUNT(*)::bigint AS ref_count
FROM akn.rule_references r
JOIN akn.rules src ON src.id = r.source_rule_id
GROUP BY source_jurisdiction, target_jurisdiction;

-- Unique index for CONCURRENTLY refresh; also the natural lookup key.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jurisdiction_flows_pair
  ON akn.jurisdiction_flows (source_jurisdiction, target_jurisdiction);

REFRESH MATERIALIZED VIEW akn.jurisdiction_flows;

GRANT SELECT ON akn.jurisdiction_flows TO anon, authenticated;

-- RPC returns the array of flow edges sorted by weight, for the
-- landing-page visual.
CREATE OR REPLACE FUNCTION akn.get_jurisdiction_flows()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = akn, public
AS $$
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'source', source_jurisdiction,
        'target', target_jurisdiction,
        'count', ref_count
      )
      ORDER BY ref_count DESC
    ),
    '[]'::jsonb
  )
  FROM akn.jurisdiction_flows
$$;

GRANT EXECUTE ON FUNCTION akn.get_jurisdiction_flows() TO anon, authenticated;

-- Extend the ingest-driver refresh helper to cover the new MV too,
-- so driver scripts don't need to call two RPCs.
CREATE OR REPLACE FUNCTION akn.refresh_jurisdiction_counts()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = akn, public
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY akn.jurisdiction_counts;
  REFRESH MATERIALIZED VIEW CONCURRENTLY akn.jurisdiction_flows;
END
$$;

NOTIFY pgrst, 'reload schema';
