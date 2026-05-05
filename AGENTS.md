# Axiom Corpus Agent Rules

This repository owns source-first legal corpus ingestion. Prefer the corpus
pipeline under `src/axiom_corpus/corpus/` for new work.

## Corpus Architecture

- Do not add durable AKN/Akoma Ntoso outputs to this repository, R2, or the
  Supabase schema.
- Durable corpus artifacts are official source snapshots, source inventory,
  normalized provision JSONL, coverage reports, analytics snapshots, and
  release manifests.
- Treat Supabase `corpus.provisions` as the serving database projection, not as
  the canonical source of truth.
- Use primary official government sources. Do not ingest secondary summaries
  such as State Options Reports, Justia, FindLaw, or LegiScan unless explicitly
  directed for a separate non-canonical experiment.

## State Statute Work

- Pick one jurisdiction at a time from
  `manifests/state-statute-agent-queue.yaml`.
- Add or repair one source-first adapter, then wire it through
  `extract-state-statutes-batch` or a dedicated CLI command.
- A successful state statute task writes all four scoped artifacts:
  `sources/`, `inventory/`, `provisions/`, and `coverage/`.
- Coverage must be complete before a state is proposed for release promotion.
- Do not publish to R2, load Supabase, merge to `main`, or delete old production
  rows unless the user explicitly asks for publication.

## Required Checks

Run focused tests for the adapter and CLI you changed. Before handing off, run:

```bash
uv run --extra dev ruff check .
uv run --extra dev mypy src/axiom_corpus/corpus --ignore-missing-imports
uv run --extra dev python -m pytest -q
uv run --extra dev towncrier check
```

For a production-ready state statute scope, also run:

```bash
uv run --extra dev axiom-corpus-ingest coverage \
  --base data/corpus \
  --source-inventory data/corpus/inventory/<jurisdiction>/statute/<version>.json \
  --provisions data/corpus/provisions/<jurisdiction>/statute/<version>.jsonl \
  --jurisdiction <jurisdiction> \
  --document-class statute \
  --version <version> \
  --write
```

