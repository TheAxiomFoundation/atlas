# Architecture Overview

A visual map of how `axiom-corpus` works end to end. Sibling docs zoom in
([corpus-pipeline.md](../corpus-pipeline.md),
[source-organization.md](source-organization.md)); this one is the picture
you point at to explain the whole thing.

> **Interactive version:** an explorable React app lives in
> [`docs/architecture-viewer/`](../architecture-viewer/). Run
> `cd docs/architecture-viewer && npm install && npm run dev` and open
> <http://localhost:5179>. Click any node to read what it owns and how it
> connects.

## The Big Picture

```mermaid
flowchart TB
    UP[Official government sources<br/>eCFR, USC, state codes,<br/>laws-lois.justice.gc.ca, …]

    subgraph CORPUS[axiom-corpus pipeline]
        direction TB
        FETCH[Fetchers<br/><i>HTTP download</i>]
        PARSE[Parsers<br/><i>XML / HTML / PDF → typed objects</i>]
        ADAPT[Source-first adapters<br/><i>typed objects → ProvisionRecord</i>]
        ARTIFACTS[(data/corpus/<br/>sources / inventory /<br/>provisions / coverage)]
        FETCH --> PARSE --> ADAPT --> ARTIFACTS
    end

    UP --> FETCH

    R2[(R2 bucket<br/>axiom-corpus<br/><i>provenance store</i>)]
    SUPABASE[(Supabase<br/>corpus schema)]

    ARTIFACTS -->|sync-r2| R2
    ARTIFACTS -->|load-supabase| SUPABASE

    subgraph SCHEMA[corpus schema]
        direction TB
        PROV[corpus.provisions<br/><b>truth for legal text</b>]
        NAV[corpus.navigation_nodes<br/><i>derived serving index</i>]
        COUNTS[corpus.provision_counts<br/><i>materialized view</i>]
        PROV -.->|build-navigation-index<br/>or auto post-load| NAV
        PROV -.->|refresh| COUNTS
    end

    SUPABASE --- SCHEMA

    APP[axiom-foundation.org<br/>finbot, dashboard-builder, …]
    SCHEMA -->|REST<br/>Accept-Profile: corpus| APP

    RULES[(rules-us, rules-us-co,<br/>rules-uk, rules-ca<br/><i>RuleSpec YAML</i>)]
    RULES -.->|has_rulespec<br/>+ encoded_descendant_count| NAV
```

Three flows, three colors of arrow:

- **Solid:** the ingest path that produces source-of-truth data.
- **Dotted:** derivations from that source — nav rebuilds, materialized
  views, encoded coverage.
- **One-way out:** apps read; they never write back.

## Repository Boundaries

```mermaid
flowchart LR
    subgraph IN["Ingest side"]
        AC[axiom-corpus<br/><i>this repo</i>]
        AE[axiom-encode<br/><i>encoder pipeline</i>]
    end

    subgraph RULES["Rule encodings"]
        RUS[rules-us]
        RUSCO[rules-us-co]
        RUK[rules-uk]
        RCA[rules-ca]
    end

    subgraph OUT["Read side"]
        APP[axiom-foundation.org]
        FB[finbot]
        DB[dashboard-builder]
    end

    AC -->|writes| SB[(Supabase<br/>corpus.*)]
    AC -->|writes| R2[(R2)]

    AE -->|reads corpus text| SB
    AE -->|writes YAML| RULES

    RULES -->|local checkout drives<br/>has_rulespec on rebuild| AC

    SB -->|reads| APP
    SB -->|reads| FB
    SB -->|reads| DB
```

Hard rules across the boundary:

- `axiom-corpus` owns source text (`corpus.provisions`) and the derived
  serving index (`corpus.navigation_nodes`).
- `rules-*` repos own RuleSpec YAML encodings. Their existence is observed
  by the navigation builder; they are never authoritative for legal text.
- `axiom-encode` reads corpus text and writes YAML; never the other way.
- Apps are read-only consumers via PostgREST.

## The Five Pipeline Stages

```text
   ┌────────┐    ┌────────┐    ┌─────────┐    ┌──────────┐    ┌──────────┐
   │ FETCH  │ →  │ PARSE  │ →  │ ADAPT   │ →  │ STORE    │ →  │ PUBLISH  │
   └────────┘    └────────┘    └─────────┘    └──────────┘    └──────────┘
   raw bytes     typed         Provision      JSONL on        R2 +
   from HTTP     domain        Record +       disk under      Supabase
                 models        Inventory      data/corpus/    corpus.*
                               Item

   src/.../      src/.../      src/axiom_corpus/corpus/{adapter}.py
   fetchers/     parsers/
                                              CorpusArtifactStore
```

Each stage is responsible for one thing. The shape that crosses every
boundary is `ProvisionRecord` (in code) and the JSONL line (on disk and
in R2).

## On-Disk Artifact Layout

```text
data/corpus/
├── sources/
│   └── canada/statute/2026-05-06/
│       └── I-3.3.xml                    ← raw upstream bytes (sha256 tracked)
├── inventory/
│   └── canada/statute/2026-05-06.json   ← expected citation paths
├── provisions/
│   └── canada/statute/2026-05-06.jsonl  ← one ProvisionRecord per line
└── coverage/
    └── canada/statute/2026-05-06.json   ← inventory vs provisions diff
```

The same key structure mirrors into the `axiom-corpus` R2 bucket.

## The JSONL Contract

```mermaid
flowchart LR
    PR[ProvisionRecord<br/>Python dataclass]
    JSONL["data/corpus/provisions/<br/>{jur}/{doc}/{run_id}.jsonl<br/><i>one JSON object per line</i>"]
    ROW[corpus.provisions row<br/>+ deterministic UUID5 id]

    PR -->|to_mapping<br/>sort_keys=True| JSONL
    JSONL -->|load_provisions<br/>line by line| PR2[ProvisionRecord]
    PR2 -->|provision_to_supabase_row| ROW
```

Required keys: `jurisdiction`, `document_class`, `citation_path`.
Everything else is optional and emitted only when non-null.

## Citation Path Convention

```text
canonical citation_path  =  {jurisdiction}/{document_class}/{path_segments…}

      jurisdiction ──┐                ┌── doc_class
                     │                │
                     ▼                ▼
              canada / statute / I-3.3 / 2 / 1 / a
                                  │     │   │   │
                       act code ──┘     │   │   │
                       section number ──┘   │   │
                       subsection label ────┘   │
                       paragraph label ─────────┘
```

Same shape for every jurisdiction:

| Jurisdiction | Example path | Notes |
|---|---|---|
| us | `us/statute/26/3111/a` | USC title / section / subsection |
| us | `us/regulation/7/273/7` | CFR title / part / section (no `-cfr` suffix) |
| us-co | `us-co/regulation/10-ccr-2506-1/4.306.1` | preserves CCR publication number |
| us-co | `us-co/policy/cdhs/snap/fy-2026-benefit-calculation` | policy under cdhs publisher |
| canada | `canada/statute/I-3.3/2/1/a` | act / section / subsection / paragraph |

`citation_path` is the canonical identifier; the row's UUID5 id is
deterministic from it.

## Storage Layers

```mermaid
flowchart TB
    subgraph LOCAL["Local"]
        L[data/corpus/<br/><i>filesystem JSONL</i>]
    end

    subgraph DURABLE["Durable / cold"]
        R2STORE[("R2 bucket<br/>axiom-corpus")]
    end

    subgraph SERVING["Serving / hot"]
        PROV[("corpus.provisions<br/>~1.75M rows<br/><b>SoT for legal text</b>")]
        NAV[("corpus.navigation_nodes<br/>~1.75M rows<br/><i>tree index</i>")]
        REFS[("corpus.provision_references<br/><i>cross-references</i>")]
        PC[("corpus.provision_counts<br/><i>materialized view</i>")]
    end

    L -.->|sync-r2| R2STORE
    L -->|load-supabase| PROV
    PROV -.->|build-navigation-index| NAV
    PROV -.->|REST RPC refresh| PC
    PROV -.->|extract-references| REFS
```

Which layer can you rebuild from?

| Loss | Recoverable from |
|---|---|
| `corpus.navigation_nodes` deleted | `corpus.provisions` (`build-navigation-index --all --from-supabase`) |
| `corpus.provisions` deleted | local JSONL or R2 (`load-supabase`) |
| local `data/corpus/` deleted | R2 (pull) or full re-extract |
| R2 contents deleted | full re-extract from upstream |
| Upstream sources change | nothing — we snapshot, but no time machine for the source |

## A Single Provision's Journey

Tracing `canada/statute/I-3.3/2/1/a` from upstream to app.

```mermaid
sequenceDiagram
    participant U as laws-lois.justice.gc.ca
    participant F as CanadaLegislationFetcher
    participant P as CanadaStatuteParser
    participant A as extract_canada_acts
    participant D as data/corpus/
    participant S as corpus.provisions
    participant N as corpus.navigation_nodes
    participant App as app

    U->>F: GET /eng/XML/I-3.3.xml
    F->>A: bytes
    A->>D: write sources/canada/statute/2026-05-06/I-3.3.xml
    A->>P: parse(xml_path)
    P->>A: CanadaSection(section_number="2", subsections=[…])
    A->>A: project → ProvisionRecord<br/>(citation_path="canada/statute/I-3.3/2/1/a", …)
    A->>D: append to provisions/canada/statute/2026-05-06.jsonl
    D->>S: load-supabase: POST /provisions<br/>(deterministic UUID5 id)
    S->>N: post-step rebuild (auto)
    App->>N: GET /navigation_nodes?parent_path=…/2/1
    N->>App: list of children (sort_key ordered)
    App->>S: GET /provisions?id=…
    S->>App: body text + source_url back to upstream
```

## CLI Surface

```mermaid
flowchart LR
    subgraph EXTRACT[Extract: produce JSONL]
        E1[extract-ecfr]
        E2[extract-usc]
        E3[extract-state-statutes]
        E4[extract-canada-acts]
        E5[extract-indiana-code]
        E6[extract-colorado-ccr]
        EX[extract-…]
    end

    subgraph PUBLISH[Publish: push JSONL outward]
        S1[sync-r2]
        L1[load-supabase]
        B1[build-navigation-index]
    end

    subgraph INSPECT[Inspect: report]
        C1[coverage]
        A1[analytics]
        V1[validate-release]
        SC[snapshot-provision-counts]
    end

    EXTRACT --> PUBLISH
    PUBLISH --> INSPECT
```

The typical operator flow for a new ingest:

```text
extract-{adapter} → sync-r2 → load-supabase → (auto) build-navigation-index
                                                       │
                                          coverage / analytics / validate-release
```

## Failure Surfaces

```mermaid
flowchart TB
    F1[Fetcher network fail] -->|adapter logs + skip| OK1[Other acts unaffected]
    F2[Parser bug on one act] -->|adapter logs + skip| OK2[Other acts unaffected]
    F3[load-supabase chunk fails mid-stream] -->|raises| PARTIAL[Partial scope in DB<br/>Re-run cleans up]
    F4[Post-load nav rebuild fails] -->|raises non-zero| MANUAL[Manual<br/>build-navigation-index]
    F5[Refresh of provision_counts times out] -->|--allow-refresh-failure| COSMETIC[Counts lag<br/>Re-refresh fixes]
    F6[R2 sync fails] -->|surfaces clearly| RETRY[Re-run sync-r2]
```

Loud failures everywhere by default; opt-in escape hatches
(`--allow-incomplete`, `--allow-refresh-failure`, `--no-build-navigation`)
for known transient cases.

## What's Outside This Diagram

- **`axiom-encode`** — encoder pipeline. Reads `corpus.provisions`, writes RuleSpec YAML into `rules-*` repos. Not part of this repo's ingest path.
- **App-side rendering, search, AI assistants** — `axiom-foundation.org` and downstream demos. They read this corpus; they don't shape it.
- **RuleSpec compiler / runtime** — separate stack that turns YAML into runnable benefit calculations. Anchors on citation paths but doesn't write here.

## See Also

- [corpus-pipeline.md](../corpus-pipeline.md) — pipeline contract in prose
- [source-organization.md](source-organization.md) — repo split & artifact layout
- [STATE_SCRAPERS.md](../STATE_SCRAPERS.md) — per-state adapter notes
- [agent-ingestion-runbook.md](../agent-ingestion-runbook.md) — operator runbook
