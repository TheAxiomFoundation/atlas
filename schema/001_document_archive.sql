-- Document Archive Schema for Atlas Document Archive
-- Stores raw legal documents (PDFs, HTML) with version tracking
--
-- Architecture:
--   R2: Stores actual files (us/guidance/irs/rp-23-34.pdf)
--   Supabase: Stores metadata, versions, references (this schema)

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- SOURCES: Document identifiers and metadata
-- ============================================================================
-- Path structure mirrors rules-us: us/guidance/irs/rp-23-34
-- This is the stable identifier for a document across all versions

CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Path is the stable identifier, matches rules-us structure
    -- e.g., "us/guidance/irs/rp-23-34", "us/statute/26/32"
    path TEXT NOT NULL UNIQUE,

    -- Document classification
    jurisdiction TEXT NOT NULL DEFAULT 'us',  -- us, uk, ca, etc.
    doc_type TEXT NOT NULL,  -- statute, guidance, regulation, form

    -- Source URL for crawling
    source_url TEXT,

    -- Human-readable title
    title TEXT,

    -- Crawl configuration
    crawl_enabled BOOLEAN DEFAULT true,
    crawl_frequency_hours INTEGER DEFAULT 24,
    last_crawl_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sources_jurisdiction ON sources(jurisdiction);
CREATE INDEX idx_sources_doc_type ON sources(doc_type);
CREATE INDEX idx_sources_crawl ON sources(crawl_enabled, last_crawl_at);

-- ============================================================================
-- VERSIONS: Immutable snapshots of documents
-- ============================================================================
-- Each version is identified by content hash (SHA-256)
-- R2 path: {source.path}/{content_hash}.{extension}

CREATE TABLE IF NOT EXISTS versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,

    -- Content identification
    content_hash TEXT NOT NULL,  -- SHA-256 of file content

    -- R2 storage location
    r2_key TEXT NOT NULL,  -- e.g., "us/guidance/irs/rp-23-34/abc123.pdf"
    file_size_bytes BIGINT,
    mime_type TEXT,

    -- Temporal validity
    -- published_at: when the source published this version
    -- retrieved_at: when we fetched it
    -- applies_from/to: tax years this guidance applies to
    published_at DATE,
    retrieved_at TIMESTAMPTZ DEFAULT NOW(),
    applies_from_year INTEGER,  -- e.g., 2024 for rp-23-34
    applies_to_year INTEGER,    -- NULL if still current

    -- Is this the current/latest version?
    is_current BOOLEAN DEFAULT false,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(source_id, content_hash)
);

CREATE INDEX idx_versions_source ON versions(source_id);
CREATE INDEX idx_versions_current ON versions(source_id, is_current) WHERE is_current = true;
CREATE INDEX idx_versions_applies ON versions(applies_from_year, applies_to_year);
CREATE INDEX idx_versions_retrieved ON versions(retrieved_at);

-- ============================================================================
-- REFS: References from encoded law to source documents
-- ============================================================================
-- Links rules-us variables to their authoritative sources

CREATE TABLE IF NOT EXISTS refs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- What's referencing (rules-us path)
    variable_path TEXT NOT NULL,  -- e.g., "us/26/32/eitc"

    -- What's being referenced
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    version_id UUID REFERENCES versions(id) ON DELETE SET NULL,  -- specific version, or NULL for "latest"

    -- Reference details
    ref_type TEXT NOT NULL,  -- 'authority', 'parameter_source', 'example'
    excerpt TEXT,  -- Relevant quote from the document
    page_number INTEGER,
    section_ref TEXT,  -- e.g., "Section 3.01(1)"

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(variable_path, source_id, ref_type)
);

CREATE INDEX idx_refs_variable ON refs(variable_path);
CREATE INDEX idx_refs_source ON refs(source_id);

-- ============================================================================
-- CRAWL_LOG: Track crawler activity
-- ============================================================================

CREATE TABLE IF NOT EXISTS crawl_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,

    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    status TEXT NOT NULL,  -- 'success', 'failed', 'no_change'
    new_version_id UUID REFERENCES versions(id),

    -- For debugging
    http_status INTEGER,
    error_message TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_crawl_log_source ON crawl_log(source_id, started_at DESC);

-- ============================================================================
-- FUNCTIONS: Helper functions for version queries
-- ============================================================================

-- Get the version that was current as of a specific date
CREATE OR REPLACE FUNCTION get_version_as_of(
    p_source_path TEXT,
    p_as_of DATE
) RETURNS TABLE (
    version_id UUID,
    content_hash TEXT,
    r2_key TEXT,
    retrieved_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT v.id, v.content_hash, v.r2_key, v.retrieved_at
    FROM versions v
    JOIN sources s ON s.id = v.source_id
    WHERE s.path = p_source_path
      AND v.retrieved_at <= p_as_of
    ORDER BY v.retrieved_at DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Get the version applicable to a specific tax year
CREATE OR REPLACE FUNCTION get_version_for_year(
    p_source_path TEXT,
    p_tax_year INTEGER
) RETURNS TABLE (
    version_id UUID,
    content_hash TEXT,
    r2_key TEXT,
    applies_from_year INTEGER,
    applies_to_year INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT v.id, v.content_hash, v.r2_key, v.applies_from_year, v.applies_to_year
    FROM versions v
    JOIN sources s ON s.id = v.source_id
    WHERE s.path = p_source_path
      AND v.applies_from_year <= p_tax_year
      AND (v.applies_to_year IS NULL OR v.applies_to_year >= p_tax_year)
    ORDER BY v.applies_from_year DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- TRIGGERS: Auto-update timestamps
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sources_updated_at
    BEFORE UPDATE ON sources
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER refs_updated_at
    BEFORE UPDATE ON refs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- RLS: Row Level Security for Supabase
-- ============================================================================

ALTER TABLE sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE crawl_log ENABLE ROW LEVEL SECURITY;

-- Public read access (documents are public)
CREATE POLICY "Public read access" ON sources FOR SELECT USING (true);
CREATE POLICY "Public read access" ON versions FOR SELECT USING (true);
CREATE POLICY "Public read access" ON refs FOR SELECT USING (true);

-- Service role write access
CREATE POLICY "Service write access" ON sources FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service write access" ON versions FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service write access" ON refs FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service write access" ON crawl_log FOR ALL USING (auth.role() = 'service_role');
