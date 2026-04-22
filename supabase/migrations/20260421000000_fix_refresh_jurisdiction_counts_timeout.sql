-- Fix refresh_jurisdiction_counts() 500ing when called via PostgREST.
--
-- The authenticator role (from which PostgREST derives its session)
-- carries statement_timeout=8s and lock_timeout=8s. REFRESH MATERIALIZED
-- VIEW CONCURRENTLY on akn.jurisdiction_counts takes ~14s on the live
-- cluster, so the session-level timeout cancels the statement with
-- SQLSTATE 57014 ("canceling statement due to statement timeout") and
-- PostgREST surfaces a 500 to the ingest driver.
--
-- Per-function SET clauses apply with LOCAL semantics: they take effect
-- for the duration of the call and revert on exit. Clearing both
-- timeouts inside the SECURITY DEFINER function is the minimal fix —
-- it doesn't change defaults for any other role or query path.

CREATE OR REPLACE FUNCTION akn.refresh_jurisdiction_counts()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = akn, public
SET statement_timeout = 0
SET lock_timeout = 0
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY akn.jurisdiction_counts;
END
$$;

NOTIFY pgrst, 'reload schema';
