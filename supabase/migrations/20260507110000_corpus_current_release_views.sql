-- Make the public corpus read path source-first by default.
--
-- `corpus.provisions` can still hold legacy rows during migration, but public
-- search/stats and ordinary PostgREST reads should use the active `current`
-- release manifest boundary.

CREATE TABLE IF NOT EXISTS corpus.release_scopes (
  release_name text NOT NULL,
  jurisdiction text NOT NULL,
  document_class text NOT NULL,
  version text NOT NULL,
  active boolean NOT NULL DEFAULT true,
  synced_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (release_name, jurisdiction, document_class, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_release_scopes_one_active_version
  ON corpus.release_scopes (release_name, jurisdiction, document_class)
  WHERE active IS TRUE;

CREATE INDEX IF NOT EXISTS idx_release_scopes_current_active
  ON corpus.release_scopes (jurisdiction, document_class, version)
  WHERE release_name = 'current' AND active IS TRUE;

ALTER TABLE corpus.release_scopes ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS release_scopes_anon_read ON corpus.release_scopes;
CREATE POLICY release_scopes_anon_read
  ON corpus.release_scopes
  FOR SELECT
  TO anon
  USING (active IS TRUE);

DROP POLICY IF EXISTS release_scopes_authenticated_read ON corpus.release_scopes;
CREATE POLICY release_scopes_authenticated_read
  ON corpus.release_scopes
  FOR SELECT
  TO authenticated
  USING (active IS TRUE);

GRANT SELECT ON corpus.release_scopes TO anon, authenticated;
GRANT ALL ON corpus.release_scopes TO postgres, service_role;

CREATE OR REPLACE VIEW corpus.current_release_scopes AS
SELECT
  release_name,
  jurisdiction,
  document_class,
  version,
  synced_at
FROM corpus.release_scopes
WHERE release_name = 'current'
  AND active IS TRUE;

CREATE OR REPLACE VIEW corpus.current_provisions AS
SELECT p.*
FROM corpus.provisions p
WHERE EXISTS (
  SELECT 1
  FROM corpus.current_release_scopes s
  WHERE s.jurisdiction = p.jurisdiction
    AND s.document_class = COALESCE(NULLIF(p.doc_type, ''), 'unknown')
);

CREATE OR REPLACE VIEW corpus.legacy_provisions AS
SELECT p.*
FROM corpus.provisions p
WHERE NOT EXISTS (
  SELECT 1
  FROM corpus.current_release_scopes s
  WHERE s.jurisdiction = p.jurisdiction
    AND s.document_class = COALESCE(NULLIF(p.doc_type, ''), 'unknown')
);

GRANT SELECT ON corpus.current_release_scopes TO anon, authenticated;
GRANT SELECT ON corpus.current_provisions TO anon, authenticated;
GRANT SELECT ON corpus.legacy_provisions TO postgres, service_role;

CREATE MATERIALIZED VIEW IF NOT EXISTS corpus.current_provision_counts AS
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
FROM corpus.current_provisions
WHERE jurisdiction IS NOT NULL
GROUP BY jurisdiction, COALESCE(NULLIF(doc_type, ''), 'unknown')
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_current_provision_counts_jurisdiction_document_class
  ON corpus.current_provision_counts (jurisdiction, document_class);

REFRESH MATERIALIZED VIEW corpus.current_provision_counts;

GRANT SELECT ON corpus.current_provision_counts TO anon, authenticated;
GRANT SELECT ON corpus.current_provision_counts TO postgres, service_role;

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
    FROM corpus.current_provision_counts
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
    FROM corpus.current_provision_counts
    GROUP BY document_class
  ),
  totals AS (
    SELECT
      COALESCE(SUM(provision_count), 0)::bigint AS provision_count,
      COALESCE(SUM(body_count), 0)::bigint AS body_count,
      COALESCE(SUM(top_level_count), 0)::bigint AS top_level_count,
      COALESCE(SUM(rulespec_count), 0)::bigint AS rulespec_count
    FROM corpus.current_provision_counts
  )
  SELECT jsonb_build_object(
    'release_name',
      'current',
    'provisions_count',
      totals.provision_count,
    'body_count',
      totals.body_count,
    'top_level_count',
      totals.top_level_count,
    'rulespec_count',
      totals.rulespec_count,
    'refreshed_at',
      (SELECT MAX(refreshed_at) FROM corpus.current_provision_counts),
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
          FROM corpus.current_provision_counts
        ),
        '[]'::jsonb
      )
  )
  FROM totals
$$;

CREATE OR REPLACE FUNCTION corpus.get_all_corpus_stats()
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
    'release_name',
      'all',
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

CREATE OR REPLACE FUNCTION corpus.search_provisions(
  q text,
  jurisdiction_in text DEFAULT NULL,
  doc_type_in text DEFAULT NULL,
  limit_in int DEFAULT 30
)
RETURNS TABLE (
  id uuid,
  jurisdiction text,
  doc_type text,
  citation_path text,
  heading text,
  snippet text,
  has_rulespec boolean,
  rank real
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
SET statement_timeout = 0
SET lock_timeout = 0
AS $$
  WITH parsed AS (
    SELECT websearch_to_tsquery('english', q) AS tsq
  ), ranked AS (
    SELECT
      p.id,
      p.jurisdiction,
      p.doc_type,
      p.citation_path,
      p.heading,
      p.body,
      p.has_rulespec,
      ts_rank_cd(p.fts, parsed.tsq) AS rank,
      parsed.tsq
    FROM corpus.current_provisions p
    CROSS JOIN parsed
    WHERE p.fts @@ parsed.tsq
      AND (jurisdiction_in IS NULL OR p.jurisdiction = jurisdiction_in)
      AND (doc_type_in IS NULL OR p.doc_type = doc_type_in)
    ORDER BY rank DESC, p.citation_path ASC
    LIMIT GREATEST(1, LEAST(limit_in, 100))
  )
  SELECT
    ranked.id,
    ranked.jurisdiction,
    ranked.doc_type,
    ranked.citation_path,
    ranked.heading,
    ts_headline(
      'english',
      COALESCE(ranked.body, ranked.heading, ''),
      ranked.tsq,
      'StartSel=<mark>,StopSel=</mark>,MaxWords=30,MinWords=15,ShortWord=3,MaxFragments=1'
    ) AS snippet,
    ranked.has_rulespec,
    ranked.rank
  FROM ranked;
$$;

CREATE OR REPLACE FUNCTION corpus.search_all_provisions(
  q text,
  jurisdiction_in text DEFAULT NULL,
  doc_type_in text DEFAULT NULL,
  limit_in int DEFAULT 30
)
RETURNS TABLE (
  id uuid,
  jurisdiction text,
  doc_type text,
  citation_path text,
  heading text,
  snippet text,
  has_rulespec boolean,
  rank real
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
SET statement_timeout = 0
SET lock_timeout = 0
AS $$
  WITH parsed AS (
    SELECT websearch_to_tsquery('english', q) AS tsq
  ), ranked AS (
    SELECT
      p.id,
      p.jurisdiction,
      p.doc_type,
      p.citation_path,
      p.heading,
      p.body,
      p.has_rulespec,
      ts_rank_cd(p.fts, parsed.tsq) AS rank,
      parsed.tsq
    FROM corpus.provisions p
    CROSS JOIN parsed
    WHERE p.fts @@ parsed.tsq
      AND (jurisdiction_in IS NULL OR p.jurisdiction = jurisdiction_in)
      AND (doc_type_in IS NULL OR p.doc_type = doc_type_in)
    ORDER BY rank DESC, p.citation_path ASC
    LIMIT GREATEST(1, LEAST(limit_in, 100))
  )
  SELECT
    ranked.id,
    ranked.jurisdiction,
    ranked.doc_type,
    ranked.citation_path,
    ranked.heading,
    ts_headline(
      'english',
      COALESCE(ranked.body, ranked.heading, ''),
      ranked.tsq,
      'StartSel=<mark>,StopSel=</mark>,MaxWords=30,MinWords=15,ShortWord=3,MaxFragments=1'
    ) AS snippet,
    ranked.has_rulespec,
    ranked.rank
  FROM ranked;
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
  REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.current_provision_counts;
END;
$$;

GRANT EXECUTE ON FUNCTION corpus.get_corpus_stats() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.get_all_corpus_stats() TO postgres, service_role;
GRANT EXECUTE ON FUNCTION corpus.search_provisions(text, text, text, int) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.search_all_provisions(text, text, text, int)
  TO postgres, service_role;
GRANT EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() TO postgres, service_role;
REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM anon;
REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM authenticated;

NOTIFY pgrst, 'reload schema';
