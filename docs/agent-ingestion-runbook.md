# Agent Ingestion Runbook

This runbook is for Codex agents implementing corpus ingestion work in parallel.
The goal is to make each agent task narrow, reviewable, and safe to merge.

## Operating Model

Each agent owns one jurisdiction and document class on one branch. For the
state statute push, that means one `us-xx/statute` scope from
`manifests/state-statute-agent-queue.yaml`. Agents should avoid shared files
except the adapter registry, CLI wiring, tests, docs, and manifest entries
needed for their own jurisdiction.

The corpus pipeline is source-first. The adapter snapshots official source
material, builds a source inventory, emits normalized provision JSONL, and
writes a coverage report. Supabase loading and R2 publication are separate
controller steps after review.

## Scope Selection

Start with `agent_ready` queue items. Avoid `blocked_release_repair` items
unless the controller explicitly assigns them; many of those states already
have R2 artifacts but are missing local artifacts in this checkout, so adapter
work is not always the right fix. Do not touch `done` states except for
targeted bug fixes.

For each assigned state, first find the current primary official source. If the
state provides bulk XML, JSON, ZIP, SQL, or downloadable HTML, prefer that over
scraping page-by-page. If no bulk source exists, snapshot the official HTML
index and all official section/chapter pages needed to prove completeness.

## Adapter Requirements

Adapters should live in the corpus package and return `StateStatuteExtractReport`
or the closest existing report type. They must write:

```text
data/corpus/sources/<jurisdiction>/statute/<version>/...
data/corpus/inventory/<jurisdiction>/statute/<version>.json
data/corpus/provisions/<jurisdiction>/statute/<version>.jsonl
data/corpus/coverage/<jurisdiction>/statute/<version>.json
```

The inventory should represent the official source's own hierarchy. The
provision JSONL should include containers when useful for hierarchy and all
section-level text the official source exposes. `citation_path` values should be
stable, lowercase jurisdiction-prefixed paths such as
`us-wa/statute/42/56/010`.

Do not persist AKN XML. Do not add a second schema for one state. If a source
requires a parser helper, keep it local and covered by tests.

## Smoke Run

Run a small bounded extraction before a full run:

```bash
uv run --extra dev axiom-corpus-ingest <extract-command> \
  --base /tmp/axiom-<jurisdiction>-smoke-corpus \
  --download-dir /tmp/axiom-<jurisdiction>-smoke-download \
  --version <yyyy-mm-dd> \
  --source-as-of <yyyy-mm-dd> \
  --expression-date <yyyy-mm-dd> \
  --limit 10
```

The smoke run should prove that official source discovery, source snapshots,
provision parsing, and coverage comparison all work.

## Full Run

For a full run, write to `data/corpus` and a jurisdiction-scoped download cache:

```bash
uv run --extra dev axiom-corpus-ingest <extract-command> \
  --base data/corpus \
  --download-dir data/statutes/<jurisdiction>/<yyyy-mm-dd> \
  --version <yyyy-mm-dd> \
  --source-as-of <yyyy-mm-dd> \
  --expression-date <yyyy-mm-dd> \
  --workers 4
```

Then recompute coverage explicitly:

```bash
uv run --extra dev axiom-corpus-ingest coverage \
  --base data/corpus \
  --source-inventory data/corpus/inventory/<jurisdiction>/statute/<yyyy-mm-dd>.json \
  --provisions data/corpus/provisions/<jurisdiction>/statute/<yyyy-mm-dd>.jsonl \
  --jurisdiction <jurisdiction> \
  --document-class statute \
  --version <yyyy-mm-dd> \
  --write
```

Coverage must have `complete: true`, no duplicate citation paths, no missing
inventory entries, and no unexpected extras before the branch is ready for
publication review.

## Review Package

Every agent handoff should include the source URL, extraction command, counts,
coverage summary, generated artifact paths, and test commands. The controller
can then decide whether to add the state to `manifests/releases/current.json`,
sync to R2, load Supabase with `--replace-scope`, and refresh analytics.

## Publication Gate

Only publish after code review and green CI. The controller publication sequence
is:

```bash
uv run --extra dev axiom-corpus-ingest sync-r2 \
  --base data/corpus \
  --credentials-file ~/.config/axiom-foundation/r2-credentials.json \
  --jurisdiction <jurisdiction> \
  --document-class statute \
  --version <yyyy-mm-dd> \
  --workers 4 \
  --apply

SUPABASE_ACCESS_TOKEN="$(agent-secret get SUPABASE_ACCESS_TOKEN maxghenis)" \
uv run --extra dev axiom-corpus-ingest load-supabase \
  --provisions data/corpus/provisions/<jurisdiction>/statute/<yyyy-mm-dd>.jsonl \
  --replace-scope

SUPABASE_ACCESS_TOKEN="$(agent-secret get SUPABASE_ACCESS_TOKEN maxghenis)" \
uv run --extra dev axiom-corpus-ingest snapshot-provision-counts \
  --output data/corpus/snapshots/provision-counts-<yyyy-mm-dd>.json
```

After publication, refresh `artifact-report`, `analytics`, and
`state-statute-completion`, then sync those analytics artifacts to R2.

