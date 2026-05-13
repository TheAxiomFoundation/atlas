# CLAUDE.md

This file gives agent-facing guidance for working in `axiom-corpus`.

## Repository Role

`axiom-corpus` owns official source-document ingestion. It downloads, snapshots,
normalizes, and publishes source text into corpus artifacts and Supabase. It does
not own executable policy encodings.

Encodings live in jurisdiction rules repositories such as `rulespec-us` and
`rulespec-us-co` as RuleSpec `.yaml` files. Encoder and validation behavior lives in
`axiom-encode`.

## Current Architecture

```
official source document
  -> manifest/catalog entry
  -> axiom-corpus-ingest extractor
  -> data/corpus/{sources,inventory,provisions,coverage}
  -> R2 bucket: axiom-corpus
  -> Supabase schema: corpus
  -> corpus.provisions
```

The source document itself may be stored in R2 for provenance. Generated
normalized provision rows are loaded into Supabase. Do not store executable
encodings in this repo.

## Infrastructure

- R2 bucket: `axiom-corpus`
- R2 credentials: `~/.config/axiom-foundation/r2-credentials.json`
- Supabase source text: `corpus.provisions`
- Local converter cache root: `~/.axiom/`
- Local encoding scratch root, when needed: `~/.axiom/workspace`

## Commands

```bash
uv sync

# Focused corpus tests
uv run pytest -q -m "not integration and not slow"

# Extract official manifest-driven documents
uv run axiom-corpus-ingest extract-official-documents \
  --base data/corpus \
  --version <version> \
  --manifest manifests/<manifest>.yaml

# Extract California CalFresh regulations (CDSS MPP §63 DOCX)
uv run axiom-corpus-ingest extract-california-mpp-calfresh \
  --base data/corpus \
  --version <version> \
  --manifest manifests/us-ca-cdss-mpp-calfresh.yaml \
  --download-dir <local-cache-dir>

# Upload generated artifacts to R2
uv run axiom-corpus-ingest sync-r2 \
  --base data/corpus \
  --jurisdiction <jurisdiction> \
  --document-class <document-class> \
  --version <version> \
  --apply

# Load normalized provisions into Supabase
# By default, auto-registers exact release_scopes version rows with
# active=true so the data is visible in corpus.current_provisions. Pass
# --stage to load with active=false (staged but invisible); promote later
# with `axiom-corpus-ingest publish --version <version>`.
uv run axiom-corpus-ingest load-supabase \
  --provisions data/corpus/provisions/<scope>/<version>.jsonl

# Flip visibility for a previously loaded scope
uv run axiom-corpus-ingest publish \
  --jurisdiction us-ms --doc-type statute --version <version>

# Reverse (mark a scope inactive)
uv run axiom-corpus-ingest unpublish \
  --jurisdiction us-ms --doc-type statute --version <version>

# Find inactive release-scope version rows
uv run axiom-corpus-ingest list-unpublished

# Check that every navigation_nodes scope has matching current_provisions
uv run axiom-corpus-ingest verify-release-coverage
```

## Release-scopes visibility model

`corpus.current_provisions` is a view filtered by `corpus.release_scopes`.
Rows in `corpus.provisions` are visible to the app only if a matching row
in `corpus.release_scopes` has the same `(jurisdiction, document_class,
version)` and `active = true`.
`corpus.navigation_nodes` follows the same version boundary; public reads are
limited to active release-scope versions, and navigation rows generated for
staged versions can coexist without replacing the current tree.

As of the 2026-05-13 version-aware auto-register refactor:

- `load-supabase` auto-inserts a `release_scopes` row per loaded
  `(jurisdiction, document_class, version)` with `active = true` by default.
  Forgetting is no longer the failure mode that silently hides data.
- `--stage` loads with `active = false`; promote with `publish`.
- `--no-auto-register` skips the release_scopes write entirely (legacy
  workflow; not recommended for new loads).
- `sync-release-scopes` is upsert-incremental by default (changed from
  destructive-replace 2026-05-12). Pass `--exclusive` for the older
  deactivate-all-then-reinsert behavior — but only when you are certain
  the manifest is the complete set of intended active scopes.

## Repo Boundaries

- Source text and provenance: this repo.
- RuleSpec encodings: rules repositories.
- Encoder/validator logic: `axiom-encode`.
- App/browser UI: `axiom-foundation.org`.

When a provision repeats a value from another source, represent that in the
rules repo with RuleSpec metadata and source verification. The corpus repo should
only make the source text available.
