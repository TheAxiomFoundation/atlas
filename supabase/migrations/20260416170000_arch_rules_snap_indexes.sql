-- Indexes that unblock Atlas title- and part-level browsing.
--
-- The viewer renders a rule's children by looking up all `arch.rules` rows
-- whose `citation_path` starts with the parent's path, restricted to orphan
-- rows (parent_id IS NULL) so only top-level statute/regulation sections
-- appear at that tier.
--
-- Without these two indexes, the query
--     SELECT * FROM arch.rules
--      WHERE citation_path LIKE 'us/statute/7/%'
--        AND parent_id IS NULL
--      ORDER BY ordinal
--      LIMIT 100;
-- degenerates into a scan of ~65k rows on the existing partial index
-- `idx_rules_toplevel_sort (jurisdiction, source_path) WHERE parent_id IS NULL`
-- because `citation_path LIKE` is not backed by a btree with
-- `text_pattern_ops`. Measured latency: >10s on prod before, ~400ms after.
--
-- `text_pattern_ops` is required because the default `collate` opclass on
-- `text` cannot support `LIKE 'prefix%'` lookups; only the C-locale pattern
-- ops can. The `WHERE parent_id IS NULL` partial predicate keeps the index
-- small (only top-level rows qualify for tree nodes) and aligned with how
-- the viewer queries.
--
-- Original ad-hoc creation date: 2026-04-16 (post-facto formalization).
--
-- Both indexes kept after EXPLAIN ANALYZE review (2026-04-16): the slim
-- index is narrower and the planner prefers it for count/existence queries
-- without ORDER BY (e.g. tree-node badge counts in the viewer), where the
-- composite's trailing `ordinal` column would be dead weight. The composite
-- is what unlocks the ordered prefix scan below — without it, the same
-- LIKE + ORDER BY degrades back to the 10s scan. Representative plans:
--
--   -- (a) Ordered prefix scan -> picks slim index when present:
--   EXPLAIN ANALYZE SELECT * FROM arch.rules
--    WHERE citation_path LIKE 'us/statute/7/%' AND parent_id IS NULL
--    ORDER BY ordinal LIMIT 100;
--   Limit  (cost=1248.97..1249.22 rows=100 width=790) (actual 66.144..66.167)
--     ->  Sort  (Sort Key: ordinal)
--           ->  Index Scan using idx_rules_orphan_citation_prefix
--                Index Cond: ((citation_path ~>=~ 'us/statute/7/')
--                             AND (citation_path ~<~ 'us/statute/70'))
--   Execution Time: 67.473 ms
--
--   -- (b) Count-only -> picks slim (Index Only Scan, no heap fetches for ordinal):
--   EXPLAIN ANALYZE SELECT COUNT(*) FROM arch.rules
--    WHERE citation_path LIKE 'us/statute/7/%' AND parent_id IS NULL;
--   Aggregate  (actual 1.785..1.785)
--     ->  Index Only Scan using idx_rules_orphan_citation_prefix
--   Execution Time: 1.906 ms
--
-- When the slim index is hypothetically dropped, (a) falls back to the
-- composite and still runs in ~5ms, but (b) loses its index-only scan and
-- does a wider read. Keeping both preserves the fast count path.

CREATE INDEX IF NOT EXISTS idx_rules_orphan_citation_prefix_ordinal
  ON arch.rules (citation_path text_pattern_ops, ordinal)
  WHERE parent_id IS NULL;

-- Slim companion for count-only queries that don't need `ordinal`.
-- Kept separate so EXPLAIN can pick the cheaper index when ordering
-- doesn't matter.
CREATE INDEX IF NOT EXISTS idx_rules_orphan_citation_prefix
  ON arch.rules (citation_path text_pattern_ops)
  WHERE parent_id IS NULL;
