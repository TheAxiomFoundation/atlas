-- A release can include multiple source versions for the same
-- jurisdiction/document class, especially guidance and policy documents.
--
-- Until `corpus.provisions` stores version per row, current-release reads are
-- intentionally scoped by the existence of an active jurisdiction/document
-- class pair rather than by a unique active version.

DROP INDEX IF EXISTS corpus.idx_release_scopes_one_active_version;

NOTIFY pgrst, 'reload schema';
