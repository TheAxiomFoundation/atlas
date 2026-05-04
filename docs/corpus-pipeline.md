# Corpus Pipeline

The durable corpus contract is source-first:

1. snapshot official source material
2. build an independent source inventory
3. extract normalized provision records with provenance
4. compare inventory coverage to normalized provisions
5. load Supabase/search/reference indexes from normalized provisions
6. publish corpus-specific JSONL or database exports from normalized provisions

## Source Eligibility

Default corpus inputs are primary official sources: enacted statutes, adopted
regulations, agency manuals, state plans, waiver approvals, policy memoranda,
and other documents issued by the authority responsible for the policy.

Compiled or analytical secondary sources, including federal or third-party
reports that summarize state choices, should not be ingested as corpus sources
by default. They can be used as QA checklists or gap-finding references outside
the corpus contract. If a compiled source becomes necessary, add it deliberately
as its own document class with explicit provenance and do not let it substitute
for available primary documents.

## Storage

The artifact root is designed to map directly to R2:

```text
sources/{jurisdiction}/{document_class}/{run_id}/...
inventory/{jurisdiction}/{document_class}/{run_id}.json
provisions/{jurisdiction}/{document_class}/{version}.jsonl
coverage/{jurisdiction}/{document_class}/{version}.json
exports/{format}/{jurisdiction}/{document_class}/{version}/...
analytics/{version}.json
releases/{release}.artifacts.json
```

R2 should hold official source snapshots, source inventories, normalized
provision JSONL, coverage reports, analytics snapshots, and corpus exports.
Supabase should hold derived indexes such as `corpus.provisions`,
`corpus.provision_references`, search functions, embeddings, and count views.

Do not put generated interchange XML in the main source/provision path. Format
conversion belongs outside `axiom_corpus.corpus`; corpus ingestion should depend
only on source snapshots, inventories, normalized provisions, coverage, and
derived database/search indexes.

Artifact sync is explicit and dry-run first:

```bash
axiom-corpus-ingest sync-r2 \
  --base data/corpus

axiom-corpus-ingest sync-r2 \
  --base data/corpus \
  --prefix sources \
  --prefix inventory \
  --jurisdiction us-co \
  --document-class policy \
  --version 2026-04-30 \
  --apply
```

Use `artifact-report` to compare local artifacts, optional R2 objects, coverage,
and Supabase count snapshots by `jurisdiction + document_class + version`:

```bash
axiom-corpus-ingest artifact-report \
  --base data/corpus \
  --supabase-counts data/corpus/snapshots/provision-counts-2026-04-30.json \
  --include-r2 \
  --output data/corpus/analytics/artifact-report-2026-04-30.json
```

Named releases resolve to `data/corpus/releases/<name>.json` first, then the
tracked `manifests/releases/<name>.json`. Unscoped reports auto-use the
`current` release when it exists, so production dashboards exclude old probes
and superseded source snapshots by default. Use `--release <name>` for a
specific release, or `--all-scopes` for an exhaustive diagnostic report.

Release publication should also write an immutable artifact manifest with exact
keys, sizes, and SHA-256 digests:

```bash
axiom-corpus-ingest release-artifact-manifest \
  --base data/corpus \
  --release current \
  --output data/corpus/releases/current.artifacts.json
```

Before promoting or publishing a release, validate the release against local
artifacts, optional R2 presence, Supabase count snapshots, persisted coverage,
and basic provision invariants:

```bash
axiom-corpus-ingest validate-release \
  --base data/corpus \
  --release current \
  --supabase-counts data/corpus/snapshots/provision-counts-2026-05-02.json \
  --include-r2
```

## Federal eCFR

The eCFR adapter uses structure JSON for independent inventory and full title
XML for official source snapshots plus normalized provision extraction.

```bash
axiom-corpus-ingest inventory-ecfr \
  --base data/corpus \
  --version 2026-04-29 \
  --as-of 2024-04-16

axiom-corpus-ingest extract-ecfr \
  --base data/corpus \
  --version 2026-04-29 \
  --as-of 2024-04-16 \
  --expression-date 2024-04-16 \
  --workers 2 \
  --allow-incomplete
```

Use an eCFR date that the public API actually serves. The corpus version can be
the local build or release date; the source `as_of` date remains provenance.

Targeted rebuilds are scoped and do not certify the whole source:

```bash
axiom-corpus-ingest extract-ecfr \
  --base data/corpus \
  --version 2026-04-29 \
  --as-of 2024-04-16 \
  --only-title 7 \
  --only-part 273
```

When `--only-title`, `--only-part`, or `--limit` is supplied, the run id is
derived from the base version, for example
`2026-04-29-title-7-part-273`. Part-scoped eCFR extraction snapshots the part
XML endpoint, not a whole-title XML file.

## Federal US Code

The US Code adapter ingests official USLM XML by title. It snapshots the XML
under `sources/us/statute/{run_id}/uslm/`, derives a title/section source
inventory from the XML identifiers, and writes normalized provision JSONL.

```bash
axiom-corpus-ingest extract-usc \
  --base data/corpus \
  --version 2026-04-29 \
  --source-xml data/uscode/usc26.xml
```

The title is inferred from USLM `docNumber` or identifiers by default. If
`--source-as-of` and `--expression-date` are omitted, the adapter uses each
USLM file's `dcterms:created` date when present. Targeted smoke runs can use
`--limit`; that produces a scoped run id such as `2026-04-29-title-26-limit-25`
and only certifies coverage for that scoped inventory.

For a complete local US Code source directory:

```bash
axiom-corpus-ingest extract-usc-dir \
  --base data/corpus \
  --version 2026-04-29 \
  --source-dir data/uscode
```

That writes combined artifacts at `inventory/us/statute/{version}.json`,
`provisions/us/statute/{version}.jsonl`, and
`coverage/us/statute/{version}.json`.

## Colorado

Colorado statutes are ingested from the official CRS DOCX release:

```bash
axiom-corpus-ingest extract-colorado-docx \
  --base data/corpus \
  --version 2026-04-29 \
  --release-dir data/statutes/us-co/2025-crs
```

Colorado regulations are ingested from current Secretary of State CCR PDFs and
rule-info pages. Use `--download-dir` for a first official snapshot; use
`--release-dir` to rebuild normalized records from a saved snapshot without
re-fetching the state site.

```bash
axiom-corpus-ingest extract-colorado-ccr \
  --base data/corpus \
  --version 2026-04-29 \
  --download-dir data/regulations/us-co/ccr-2026-04-13
```

The SNAP rule manual is part of the CCR source as `10 CCR 2506-1`, so it is
loaded under `us-co/regulation` with rule-manual metadata rather than as a
separate corpus class.

## State Statutes

Production state statute ingestion should be manifest-driven so each active
scope has a reproducible adapter, source path, source date, and corpus version.
The current active state statute batch is tracked at
`manifests/state-statutes.current.yaml`.

```bash
axiom-corpus-ingest extract-state-statutes \
  --base data/corpus \
  --manifest manifests/state-statutes.current.yaml \
  --dry-run

axiom-corpus-ingest extract-state-statutes \
  --base data/corpus \
  --manifest manifests/state-statutes.current.yaml
```

Use `--only-jurisdiction`, `--only-source-id`, and `--limit-per-source` for
smoke runs or targeted rebuilds. Supported state statute adapters are
`dc-code`, `cic-html`, `cic-odt`, `colorado-docx`, `texas-tcas`,
`ohio-revised-code`, `minnesota-statutes`,
`nebraska-revised-statutes`, `california-codes-bulk`, and `local-state-html`.

The `nebraska-revised-statutes` adapter snapshots the official Nebraska
Legislature statute index, chapter TOCs, and per-section HTML pages. Its
manifest entry can fetch live official sources or rebuild from a saved
`source_dir`; do not add the Nebraska release scope to `current` until a full
run has completed, coverage validates, and the artifacts are published.

`local-state-html` snapshots cached official HTML files and converts them into
the same source-first inventory, provision JSONL, and coverage artifacts as the
other adapters. Treat the checked-in
`manifests/state-statutes.local-html-smoke.yaml` manifest as a migration smoke
path only: its cached directories are not presumed to be complete official
state-code releases, so they should not be added to `current` until source
completeness has been separately established.

Use `state-statute-completion` for the production completion view across all
50 states plus DC. The report compares expected jurisdictions against the
current release, local source-first artifacts, optional R2 objects, Supabase
count snapshots, and the latest `validate-release` output. It distinguishes
productionized states from local-but-unpromoted, Supabase-only legacy, partial,
and missing source-first extractions.

```bash
axiom-corpus-ingest state-statute-completion \
  --base data/corpus \
  --release current \
  --supabase-counts data/corpus/snapshots/provision-counts-2026-05-02.json \
  --include-r2 \
  --output data/corpus/analytics/state-statute-completion-current.json
```

Primary SNAP policy documents that are not codified in CCR can be ingested from
an explicit official-document manifest. This is for primary sources such as
state plans, waiver approvals, agency memoranda, and agency policy pages. Do not
add compiled State Options Report-style summaries to this manifest.

```bash
axiom-corpus-ingest extract-official-documents \
  --base data/corpus \
  --version 2026-04-30 \
  --manifest manifests/us-co-snap-primary-policy.yaml
```

## Coverage

Coverage compares expected source inventory citations to normalized provision
citations. It does not depend on any external interchange format.

```bash
axiom-corpus-ingest coverage \
  --base data/corpus \
  --source-inventory data/corpus/inventory/us/regulation/2026-04-29.json \
  --provisions data/corpus/provisions/us/regulation/2026-04-29.jsonl \
  --jurisdiction us \
  --document-class regulation \
  --version 2026-04-29 \
  --write
```

## Supabase

Supabase is current derived/indexed state, not the durable historical corpus.
R2 artifacts and release artifact manifests are the source of truth for
historical versions. Supabase rows are keyed for the current searchable corpus,
so loading a newer provision with the same citation path updates that current
index rather than preserving side-by-side historical versions.

The ingestion importer maps normalized provision JSONL into
`corpus.provisions`, then refreshes `corpus.provision_counts`.

The exact row projection is generated with:

```bash
axiom-corpus-ingest export-supabase \
  --provisions data/corpus/provisions/us/regulation/2026-04-29-title-7-part-273.jsonl \
  --output data/corpus/exports/supabase/us/regulation/2026-04-29-title-7-part-273.jsonl
```

The projection includes deterministic `id`/`parent_id`, hierarchy (`level`,
`ordinal`), provenance (`source_url`, `source_path`, `source_document_id`,
`source_as_of`, `expression_date`), and lightweight metadata fields
(`language`, `legal_identifier`, `identifiers`) that can map to ELI and
schema.org Legislation without making either ontology the internal schema.

To load the projected records into Supabase directly from normalized provision
JSONL:

```bash
SUPABASE_SERVICE_ROLE_KEY=... axiom-corpus-ingest load-supabase \
  --provisions data/corpus/provisions/us/regulation/2026-04-29-title-7-part-273.jsonl
```

If `SUPABASE_SERVICE_ROLE_KEY` is not set, the loader can retrieve it through
the Supabase Management API using `SUPABASE_ACCESS_TOKEN`. Use `--dry-run` to
validate row counts and projection without credentials or network writes.

Production count analytics are document-class aware:

```bash
axiom-corpus-ingest analytics \
  --base data/corpus \
  --version 2026-04-29 \
  --supabase-counts data/corpus/snapshots/provision-counts-2026-04-29.json \
  --write
```

`corpus.provision_counts` is a derived-row count surface. It is not a source
completeness claim; use coverage reports for that.
