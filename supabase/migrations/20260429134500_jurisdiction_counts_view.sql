-- Make the legacy jurisdiction count surface derive from the document-class
-- analytics MV instead of refreshing a second full materialized view.

DROP MATERIALIZED VIEW IF EXISTS corpus.jurisdiction_counts;

CREATE VIEW corpus.jurisdiction_counts AS
SELECT
  jurisdiction,
  SUM(provision_count)::bigint AS provision_count
FROM corpus.provision_counts
GROUP BY jurisdiction;

GRANT SELECT ON corpus.jurisdiction_counts TO anon, authenticated;

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

REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() TO postgres, service_role;

REVOKE EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() TO postgres, service_role;

NOTIFY pgrst, 'reload schema';
