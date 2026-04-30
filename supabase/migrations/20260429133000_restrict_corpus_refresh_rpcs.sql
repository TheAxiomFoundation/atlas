-- Keep corpus analytics reads public, but restrict expensive refresh RPCs to
-- internal/service callers. Public clients should not be able to trigger
-- concurrent materialized-view refreshes.

REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() TO postgres, service_role;

REVOKE EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() TO postgres, service_role;

NOTIFY pgrst, 'reload schema';
