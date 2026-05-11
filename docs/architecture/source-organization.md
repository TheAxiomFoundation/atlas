# Source Document Organization

## Core Principle

`axiom-corpus` is organized around official source documents and normalized
provision records. Encodings live outside this repo.

- Source files are stored in R2 under `sources/`.
- Inventories, provisions, and coverage reports are stored under
  `inventory/`, `provisions/`, and `coverage/`.
- Queryable source text is loaded into Supabase `corpus.provisions`.
- RuleSpec `.yaml` files live in jurisdiction rules repos.

## Repository Split

| Repo | Purpose |
|---|---|
| `axiom-corpus` | Source-document ingestion, corpus artifacts, Supabase loads |
| `rulespec-*` | Jurisdiction RuleSpec YAML encodings and tests |
| `axiom-encode` | Encoder, validation, source verification, CI helpers |
| `axiom-foundation.org` | Axiom web app |

## Corpus Artifact Layout

```
axiom-corpus (R2 bucket)/
├── sources/
│   └── <jurisdiction>/<document_class>/<version>/...
├── inventory/
│   └── <jurisdiction>/<document_class>/<version>.json
├── provisions/
│   └── <jurisdiction>/<document_class>/<version>.jsonl
├── coverage/
│   └── <jurisdiction>/<document_class>/<version>.json
└── analytics/
```

Local generated artifacts use the same structure under `data/corpus/`.

## Supabase Layout

Normalized source text is loaded into:

```
corpus.provisions
```

Important fields:

- `citation_path`: canonical corpus path, for example
  `us/guidance/usda/fns/snap-fy2026-cola/page-1`
- `source_path`: R2 object key for the source artifact
- `body`: normalized provision or page text
- `doc_type`: statute, regulation, policy, guidance, etc.
- `source_as_of` and `expression_date`: source provenance dates

## Rules

1. Source artifacts and normalized provision rows belong here.
2. RuleSpec encodings belong in rules repos.
3. A source manifest may supply a `citation_path` when the default
   source-id-derived path is not canonical enough.
4. Reiterated values should be represented in RuleSpec metadata and verified
   against `corpus.provisions`, not duplicated into the corpus schema.
5. Do not keep obsolete generated database snapshots or old branded buckets.
