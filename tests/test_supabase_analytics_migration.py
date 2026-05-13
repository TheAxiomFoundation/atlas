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
CURRENT_RELEASE_MIGRATION = Path(
    "supabase/migrations/20260507110000_corpus_current_release_views.sql"
)
RELEASE_SCOPES_MULTI_VERSION_MIGRATION = Path(
    "supabase/migrations/20260507113000_release_scopes_allow_multi_version.sql"
)
PUBLIC_CORPUS_BOUNDARY_MIGRATION = Path(
    "supabase/migrations/20260507150000_restrict_public_corpus_base_reads.sql"
)
VERSION_AWARE_RELEASE_MIGRATION = Path(
    "supabase/migrations/20260513100000_version_aware_release_provisions.sql"
)
VERSION_AWARE_NAVIGATION_MIGRATION = Path(
    "supabase/migrations/20260513101000_version_aware_navigation_nodes.sql"
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
    assert "refresh_jurisdiction_counts" not in sql


def test_refresh_rpcs_are_service_only():
    sql = RESTRICT_REFRESH_MIGRATION.read_text()

    assert "REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM PUBLIC" in sql
    assert "REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM anon" in sql
    assert (
        "GRANT EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() TO postgres, service_role"
        in sql
    )
    assert "refresh_jurisdiction_counts" not in sql


def test_jurisdiction_count_aliases_are_dropped():
    sql = JURISDICTION_COUNTS_VIEW_MIGRATION.read_text()

    assert "DROP VIEW IF EXISTS corpus.jurisdiction_counts" in sql
    assert "DROP MATERIALIZED VIEW corpus.jurisdiction_counts" in sql
    assert "DROP FUNCTION IF EXISTS corpus.refresh_jurisdiction_counts()" in sql
    assert "CREATE VIEW corpus.jurisdiction_counts" not in sql


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
    assert "corpus.jurisdiction_counts" not in sql


def test_current_release_migration_defines_release_boundary():
    sql = CURRENT_RELEASE_MIGRATION.read_text()

    assert "CREATE TABLE IF NOT EXISTS corpus.release_scopes" in sql
    assert "CREATE OR REPLACE VIEW corpus.current_release_scopes" in sql
    assert "CREATE OR REPLACE VIEW corpus.current_provisions" in sql
    assert "CREATE OR REPLACE VIEW corpus.legacy_provisions" in sql
    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS corpus.current_provision_counts" in sql
    assert "FROM corpus.current_provisions" in sql
    assert "GRANT SELECT ON corpus.current_provisions TO anon, authenticated" in sql
    assert "GRANT SELECT ON corpus.legacy_provisions TO postgres, service_role" in sql


def test_current_release_migration_switches_default_rpcs():
    sql = CURRENT_RELEASE_MIGRATION.read_text()

    assert "CREATE OR REPLACE FUNCTION corpus.get_corpus_stats()" in sql
    assert "FROM corpus.current_provision_counts" in sql
    assert "CREATE OR REPLACE FUNCTION corpus.get_all_corpus_stats()" in sql
    assert "CREATE OR REPLACE FUNCTION corpus.search_provisions" in sql
    assert "FROM corpus.current_provisions p" in sql
    assert "CREATE OR REPLACE FUNCTION corpus.search_all_provisions" in sql
    assert "FROM corpus.provisions p" in sql
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.provision_counts" in sql
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.current_provision_counts" in sql


def test_release_scopes_allow_multiple_versions_per_document_class():
    sql = RELEASE_SCOPES_MULTI_VERSION_MIGRATION.read_text()

    assert "DROP INDEX IF EXISTS corpus.idx_release_scopes_one_active_version" in sql
    assert "jurisdiction/document" in sql
    assert "unique active version" in sql


def test_public_corpus_boundary_revokes_base_reads():
    sql = PUBLIC_CORPUS_BOUNDARY_MIGRATION.read_text()

    assert "REVOKE SELECT ON corpus.provisions FROM anon, authenticated" in sql
    assert "REVOKE SELECT ON corpus.provision_counts FROM anon, authenticated" in sql
    assert "REVOKE SELECT ON corpus.provision_references FROM anon, authenticated" in sql
    assert "GRANT SELECT ON corpus.current_provisions TO anon, authenticated" in sql
    assert "GRANT SELECT ON corpus.current_provision_counts TO anon, authenticated" in sql


def test_public_references_rpc_is_current_scoped():
    sql = PUBLIC_CORPUS_BOUNDARY_MIGRATION.read_text()

    assert "CREATE OR REPLACE FUNCTION corpus.get_provision_references" in sql
    assert "SECURITY DEFINER" in sql
    assert "FROM corpus.current_provisions" in sql
    assert "LEFT JOIN corpus.current_provisions tgt" in sql
    assert "JOIN corpus.current_provisions src" in sql
    assert "GRANT EXECUTE ON FUNCTION corpus.get_provision_references(text) TO anon" in sql


def test_current_provisions_are_release_version_scoped():
    sql = VERSION_AWARE_RELEASE_MIGRATION.read_text()

    assert "ADD COLUMN IF NOT EXISTS version TEXT" in sql
    assert "single_active_scope" in sql
    assert "UPDATE corpus.provisions p" in sql
    assert "idx_provisions_release_scope_version" in sql
    assert "CREATE OR REPLACE VIEW corpus.current_provisions" in sql
    assert "CREATE OR REPLACE VIEW corpus.legacy_provisions" in sql
    assert "s.version = p.version" in sql
    assert "REFRESH MATERIALIZED VIEW corpus.current_provision_counts" in sql


def test_navigation_nodes_are_release_version_scoped():
    sql = VERSION_AWARE_NAVIGATION_MIGRATION.read_text()

    assert "ADD COLUMN IF NOT EXISTS version TEXT" in sql
    assert "UPDATE corpus.navigation_nodes n" in sql
    assert "DROP INDEX IF EXISTS corpus.idx_navigation_nodes_path" in sql
    assert "idx_navigation_nodes_path_version" in sql
    assert "CREATE OR REPLACE VIEW corpus.current_navigation_nodes" in sql
    assert "s.version = n.version" in sql
    assert "FROM corpus.current_navigation_nodes" in sql
    assert "CREATE OR REPLACE FUNCTION corpus.get_navigation_node_counts()" in sql


def test_all_corpus_rpcs_are_service_only():
    sql = PUBLIC_CORPUS_BOUNDARY_MIGRATION.read_text()

    assert "REVOKE EXECUTE ON FUNCTION corpus.get_all_corpus_stats() FROM PUBLIC" in sql
    assert (
        "REVOKE EXECUTE ON FUNCTION corpus.get_all_corpus_stats() FROM anon, authenticated"
        in sql
    )
    assert (
        "REVOKE EXECUTE ON FUNCTION corpus.search_all_provisions(text, text, text, int) FROM PUBLIC"
        in sql
    )
    assert (
        "GRANT EXECUTE ON FUNCTION corpus.search_all_provisions(text, text, text, int)"
        in sql
    )
