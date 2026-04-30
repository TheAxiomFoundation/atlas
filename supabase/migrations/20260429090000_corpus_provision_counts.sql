-- Production corpus analytics grouped by jurisdiction and document class.
--
-- `corpus.jurisdiction_counts` is kept for backwards compatibility with the
-- existing landing-page stats contract. New analytics should read
-- `corpus.provision_counts` or `corpus.get_corpus_stats()`, both of which are
-- document-class aware.

CREATE MATERIALIZED VIEW IF NOT EXISTS corpus.provision_counts AS
SELECT
  jurisdiction,
  COALESCE(NULLIF(doc_type, ''), 'unknown') AS document_class,
  COUNT(*)::bigint AS provision_count,
  COUNT(*) FILTER (
    WHERE body IS NOT NULL
      AND BTRIM(body) <> ''
  )::bigint AS body_count,
  COUNT(*) FILTER (
    WHERE parent_id IS NULL
  )::bigint AS top_level_count,
  COUNT(*) FILTER (
    WHERE has_rulespec IS TRUE
  )::bigint AS rulespec_count,
  now() AS refreshed_at
FROM corpus.provisions
WHERE jurisdiction IS NOT NULL
GROUP BY jurisdiction, COALESCE(NULLIF(doc_type, ''), 'unknown')
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_provision_counts_jurisdiction_document_class
  ON corpus.provision_counts (jurisdiction, document_class);

REFRESH MATERIALIZED VIEW corpus.provision_counts;

GRANT SELECT ON corpus.provision_counts TO anon, authenticated;

CREATE OR REPLACE FUNCTION corpus.get_corpus_stats()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
AS $$
  WITH
  by_jurisdiction AS (
    SELECT
      jurisdiction,
      SUM(provision_count)::bigint AS provision_count
    FROM corpus.provision_counts
    GROUP BY jurisdiction
  ),
  by_document_class AS (
    SELECT
      document_class,
      SUM(provision_count)::bigint AS provision_count,
      SUM(body_count)::bigint AS body_count,
      SUM(top_level_count)::bigint AS top_level_count,
      SUM(rulespec_count)::bigint AS rulespec_count,
      MAX(refreshed_at) AS refreshed_at
    FROM corpus.provision_counts
    GROUP BY document_class
  ),
  totals AS (
    SELECT
      COALESCE(SUM(provision_count), 0)::bigint AS provision_count,
      COALESCE(SUM(body_count), 0)::bigint AS body_count,
      COALESCE(SUM(top_level_count), 0)::bigint AS top_level_count,
      COALESCE(SUM(rulespec_count), 0)::bigint AS rulespec_count
    FROM corpus.provision_counts
  )
  SELECT jsonb_build_object(
    'provisions_count',
      totals.provision_count,
    'body_count',
      totals.body_count,
    'top_level_count',
      totals.top_level_count,
    'rulespec_count',
      totals.rulespec_count,
    'refreshed_at',
      (SELECT MAX(refreshed_at) FROM corpus.provision_counts),
    'statutes_count',
      COALESCE(
        (
          SELECT provision_count
          FROM by_document_class
          WHERE document_class = 'statute'
        ),
        0
      ),
    'regulations_count',
      COALESCE(
        (
          SELECT provision_count
          FROM by_document_class
          WHERE document_class = 'regulation'
        ),
        0
      ),
    'references_count',
      (SELECT COUNT(*)::bigint FROM corpus.provision_references),
    'jurisdictions_count',
      (SELECT COUNT(*)::int FROM by_jurisdiction),
    'document_classes_count',
      (SELECT COUNT(*)::int FROM by_document_class),
    'document_classes',
      COALESCE(
        (
          SELECT jsonb_agg(
                   jsonb_build_object(
                     'document_class', document_class,
                     'count', provision_count,
                     'body_count', body_count,
                     'top_level_count', top_level_count,
                     'rulespec_count', rulespec_count,
                     'refreshed_at', refreshed_at
                   )
                   ORDER BY provision_count DESC, document_class ASC
                 )
          FROM by_document_class
        ),
        '[]'::jsonb
      ),
    'jurisdictions',
      COALESCE(
        (
          SELECT jsonb_agg(
                   jsonb_build_object(
                     'jurisdiction', jurisdiction,
                     'count', provision_count
                   )
                   ORDER BY provision_count DESC, jurisdiction ASC
                 )
          FROM by_jurisdiction
        ),
        '[]'::jsonb
      ),
    'provision_counts',
      COALESCE(
        (
          SELECT jsonb_agg(
                   jsonb_build_object(
                     'jurisdiction', jurisdiction,
                     'document_class', document_class,
                     'count', provision_count,
                     'body_count', body_count,
                     'top_level_count', top_level_count,
                     'rulespec_count', rulespec_count,
                     'refreshed_at', refreshed_at
                   )
                   ORDER BY provision_count DESC, jurisdiction ASC, document_class ASC
                 )
          FROM corpus.provision_counts
        ),
        '[]'::jsonb
      )
  )
  FROM totals
$$;

CREATE OR REPLACE FUNCTION corpus.refresh_corpus_analytics()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = corpus, public
SET statement_timeout = 0
SET lock_timeout = 0
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.provision_counts;

  IF to_regclass('corpus.jurisdiction_counts') IS NOT NULL THEN
    REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.jurisdiction_counts;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION corpus.refresh_jurisdiction_counts()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = corpus, public
SET statement_timeout = 0
SET lock_timeout = 0
AS $$
BEGIN
  PERFORM corpus.refresh_corpus_analytics();
END;
$$;

GRANT EXECUTE ON FUNCTION corpus.get_corpus_stats() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() TO anon, authenticated;

NOTIFY pgrst, 'reload schema';
