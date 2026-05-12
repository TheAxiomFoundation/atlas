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
uv run axiom-corpus-ingest load-supabase \
  --provisions data/corpus/provisions/<scope>/<version>.jsonl \
  --preserve-existing-ids
```

## Repo Boundaries

- Source text and provenance: this repo.
- RuleSpec encodings: rules repositories.
- Encoder/validator logic: `axiom-encode`.
- App/browser UI: `axiom-foundation.org`.

When a provision repeats a value from another source, represent that in the
rules repo with RuleSpec metadata and source verification. The corpus repo should
only make the source text available.
