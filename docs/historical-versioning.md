# Historical versioning (`as_of`)

**Status: incomplete — partial support.**

This doc describes what the `as_of` parameter actually does today, what it
does not do, and what a full implementation would require. It exists
because the README advertises point-in-time queries across the archive,
which is aspirational rather than uniformly implemented.

Last updated: 2026-04-16.

## Current behavior

`as_of` is accepted on the public API surface but is only honored by one
subsystem:

- **Accepted**
  - `axiom.archive.AxiomArchive.get(citation, as_of=...)`
  - `axiom.storage.base.StorageBackend.get_section(..., as_of=...)`
  - REST endpoints under `src/axiom/api/main.py`
    (`/v1/sections/...`, search) accept `as_of` as a query parameter.
- **Honored (point-in-time retrieval works)**
  - **eCFR regulations** — `axiom.converters.ecfr.ECFRConverter` passes
    `as_of` through to the eCFR API, which natively serves historical
    snapshots. Calls like `fetch("26/1.32", as_of=date(2020, 1, 1))`
    return the CFR text that was in effect on that date.
- **Silently ignored (parameter accepted, current version returned)**
  - **SQLite storage backend** — `src/axiom/storage/sqlite.py`
    `get_section()` has a `# TODO: Implement historical versions
    (as_of parameter)` comment and queries the `sections` table without
    any date predicate. Any call path that goes through
    `axiom-corpus get "26 USC 32" --as-of 2020-01-01` therefore returns the
    **currently ingested** section, regardless of the date supplied.
  - **Postgres storage backend** — `src/axiom/storage/postgres.py`
    accepts `as_of` but has no version table and likewise returns the
    current row.
  - **US Code, state statutes, IRS guidance, UK/Canada statutes** — no
    ingest pipeline currently captures amendment history. Each re-ingest
    overwrites prior text.

In short: **`as_of` works for eCFR and is a no-op for everything else.**

## Known gaps

1. **No versioned schema.** `sections` has `enacted_date` and
   `last_amended` columns but stores a single row per `(title, section)`.
   There is no `valid_from` / `valid_to` or history table.
2. **No amendment-aware ingest.** Parsers (`parsers/uslm.py`, state
   parsers, CLML/LIMS) extract the text as of the XML they were given
   and discard prior versions.
3. **No snapshot archive.** Axiom Corpus does not retain USLM/CLML XML from
   prior downloads. The corpus R2 bucket stores the most recent crawl
   only (R2 versioning is not enabled on source prefixes).
4. **No test coverage.** There are no tests that exercise `as_of` on the
   SQLite backend or the API layer.

## What "full support" would require

A minimum viable implementation, roughly in order:

1. **Schema migration**: add `valid_from DATE NOT NULL`, `valid_to DATE`
   to `sections` (and mirror on subsections). A NULL `valid_to` means
   "currently in force". Index on `(title, section, valid_from,
   valid_to)`.
2. **Ingest rewrite**: on re-ingest, diff the incoming section against
   the latest row. If the text changed, close the prior row
   (`valid_to = today`) and insert a new row (`valid_from = today`,
   `valid_to = NULL`). Carry an optional `effective_date` when the
   source XML provides one (USLM `sourceCredit`, CLML enactment
   metadata).
3. **Storage query**: update `SQLiteStorage.get_section` /
   `PostgresStorage.get_section` to filter
   `valid_from <= as_of AND (valid_to IS NULL OR as_of < valid_to)`.
4. **Source snapshots**: preserve each upstream download as a dated
   artifact in R2 (`corpus/<jurisdiction>/<date>/...`) so versions can be
   reconstructed if the DB is lost.
5. **Public law provenance**: for federal statutes, link amendments to
   their enacting public law numbers (already parsed into
   `public_laws_json`) so we can answer "what did 26 USC 32 look like
   after Public Law 117-169?".
6. **Tests**: round-trip tests that ingest two versions of the same
   section and verify `as_of` returns each.

Until those land, `as_of` should be treated as **eCFR-only** and the
other code paths documented (as here) rather than silently returning
current text.

## References

- `src/axiom/storage/sqlite.py:230` — the `TODO` noting `as_of` is
  unimplemented.
- `src/axiom/converters/ecfr.py` — working `as_of` implementation via
  eCFR API.
- README section on historical queries.
