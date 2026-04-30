-- Light standards-aligned metadata for source-first corpus provisions.
--
-- These nullable columns let the normalized JSONL contract map cleanly into
-- `corpus.provisions` without making any interchange schema the database
-- spine. They are intentionally generic enough to project to ELI and
-- schema.org Legislation metadata when we publish interchange exports.

ALTER TABLE corpus.provisions
  ADD COLUMN IF NOT EXISTS source_as_of DATE,
  ADD COLUMN IF NOT EXISTS expression_date DATE,
  ADD COLUMN IF NOT EXISTS language TEXT,
  ADD COLUMN IF NOT EXISTS legal_identifier TEXT,
  ADD COLUMN IF NOT EXISTS identifiers JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_provisions_legal_identifier
  ON corpus.provisions (legal_identifier)
  WHERE legal_identifier IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_provisions_identifiers_gin
  ON corpus.provisions USING gin (identifiers);

NOTIFY pgrst, 'reload schema';
