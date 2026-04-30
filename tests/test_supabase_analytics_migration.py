from pathlib import Path

MIGRATION = Path("supabase/migrations/20260429090000_corpus_provision_counts.sql")
RESTRICT_REFRESH_MIGRATION = Path(
    "supabase/migrations/20260429133000_restrict_corpus_refresh_rpcs.sql"
)
JURISDICTION_COUNTS_VIEW_MIGRATION = Path(
    "supabase/migrations/20260429134500_jurisdiction_counts_view.sql"
)
METADATA_ALIGNMENT_MIGRATION = Path(
    "supabase/migrations/20260429140000_corpus_provision_metadata_alignment.sql"
)
ANALYTICS_GRANT_MIGRATION = Path(
    "supabase/migrations/20260429143000_grant_corpus_analytics_service_role.sql"
)


def test_corpus_analytics_migration_is_document_class_aware():
    sql = MIGRATION.read_text()

    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS corpus.provision_counts" in sql
    assert "document_class" in sql
    assert "GROUP BY jurisdiction, COALESCE(NULLIF(doc_type, ''), 'unknown')" in sql
    assert "WITH NO DATA" in sql
    assert "refreshed_at" in sql
    assert "statutes_count" in sql
    assert "regulations_count" in sql
    assert "CREATE OR REPLACE FUNCTION corpus.refresh_corpus_analytics()" in sql
    assert "CREATE OR REPLACE FUNCTION corpus.refresh_jurisdiction_counts()" in sql


def test_refresh_rpcs_are_service_only():
    sql = RESTRICT_REFRESH_MIGRATION.read_text()

    assert "REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM PUBLIC" in sql
    assert "REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM anon" in sql
    assert (
        "GRANT EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() TO postgres, service_role"
        in sql
    )
    assert "REVOKE EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() FROM PUBLIC" in sql
    assert (
        "GRANT EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() TO postgres, service_role"
        in sql
    )


def test_legacy_jurisdiction_counts_is_view_over_document_class_counts():
    sql = JURISDICTION_COUNTS_VIEW_MIGRATION.read_text()

    assert "DROP MATERIALIZED VIEW IF EXISTS corpus.jurisdiction_counts" in sql
    assert "CREATE VIEW corpus.jurisdiction_counts AS" in sql
    assert "FROM corpus.provision_counts" in sql
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.provision_counts" in sql
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.jurisdiction_counts" not in sql


def test_corpus_provision_metadata_alignment_columns():
    sql = METADATA_ALIGNMENT_MIGRATION.read_text()

    assert "ADD COLUMN IF NOT EXISTS source_as_of DATE" in sql
    assert "ADD COLUMN IF NOT EXISTS expression_date DATE" in sql
    assert "ADD COLUMN IF NOT EXISTS language TEXT" in sql
    assert "ADD COLUMN IF NOT EXISTS legal_identifier TEXT" in sql
    assert "ADD COLUMN IF NOT EXISTS identifiers JSONB NOT NULL DEFAULT '{}'::jsonb" in sql


def test_corpus_analytics_views_are_service_readable():
    sql = ANALYTICS_GRANT_MIGRATION.read_text()

    assert "GRANT SELECT ON corpus.provision_counts TO postgres, service_role" in sql
    assert "GRANT SELECT ON corpus.jurisdiction_counts TO postgres, service_role" in sql
