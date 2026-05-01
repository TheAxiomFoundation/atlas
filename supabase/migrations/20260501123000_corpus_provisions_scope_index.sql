-- Support production scope replacement and scoped corpus reporting.
--
-- Large loads replace one jurisdiction/document class at a time. Without this
-- index, PostgREST scope scans can hit the API statement timeout before even
-- returning the first row for broad scopes such as federal regulations.

CREATE INDEX IF NOT EXISTS idx_provisions_jurisdiction_doc_type_id
  ON corpus.provisions (jurisdiction, doc_type, id)
  INCLUDE (level);

NOTIFY pgrst, 'reload schema';
