-- Allow service-role ingestion jobs to read refreshed corpus analytics.

GRANT SELECT ON corpus.provision_counts TO postgres, service_role, anon, authenticated;
GRANT SELECT ON corpus.jurisdiction_counts TO postgres, service_role, anon, authenticated;

NOTIFY pgrst, 'reload schema';
