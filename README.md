# Axiom Corpus

**Comprehensive map of government legal sources.**

Axiom Corpus is the unified source of truth for statutes, regulations, guidance, and related source documents that power the Axiom ecosystem.

## Features

- **Federal statutes** — All 54 titles of the US Code from official USLM XML
- **IRS guidance** — Revenue Procedures, Revenue Rulings, Notices (570+ documents)
- **State codes** — NY Open Legislation API, more states coming
- **Regulations** — CFR titles, Treasury regulations, agency rules
- **Provenance** — Every file tracked with fetch date, source URL, checksums
- **REST API** — Query documents by citation, keyword, or path (self-hostable)
- **Change detection** — Know when upstream sources update

## Quick Start

```bash
# Install
pip install -e .

# Run the API server
axiom-corpus serve

# Or use the CLI
axiom-corpus get "26 USC 32"        # Get IRC § 32 (EITC)
axiom-corpus search "earned income" # Search across documents
```

## CLI Usage

```bash
# Download sources
axiom-corpus download 26                    # Download Title 26 (IRC) from uscode.gov
axiom-corpus download-state ny              # Download NY state laws
axiom-corpus irs-guidance --year 2024       # Fetch IRS guidance for 2024

# Query
axiom-corpus get "26 USC 32"                # Get specific section
axiom-corpus search "child tax credit"      # Full-text search
axiom-corpus stats                          # Show database stats

# API
axiom-corpus serve                          # Start REST API at localhost:8000
```

## Python API

```python
from axiom_corpus import AxiomArchive

archive = AxiomArchive()

# Get a specific section
eitc = archive.get("26 USC 32")
print(eitc.title)        # "Earned income"
print(eitc.text)         # Full section text
print(eitc.subsections)  # Hierarchical structure

# Search
results = archive.search("child tax credit", title=26)
for section in results:
    print(f"{section.citation}: {section.title}")

# Get historical version (see status note below)
eitc_2020 = archive.get("26 USC 32", as_of="2020-01-01")
```

> **Status: `as_of` historical versioning is incomplete.** It is honored
> for eCFR regulations (via the eCFR API's native point-in-time support)
> but is currently a no-op on statutes stored in SQLite/Postgres — the
> parameter is accepted and the current version is returned. See
> [`docs/historical-versioning.md`](docs/historical-versioning.md) for
> the known gaps and what full support would require.

## REST API

```bash
# Get section by citation
curl http://localhost:8000/v1/sections/26/32

# Search
curl "http://localhost:8000/v1/search?q=earned+income&title=26"

# Get specific subsection
curl http://localhost:8000/v1/sections/26/32/a/1

# Historical version
curl "http://localhost:8000/v1/sections/26/32?as_of=2020-01-01"
```

## Data Sources

| Category | Source | Format | Files |
|----------|--------|--------|-------|
| Statutes | uscode.house.gov | USLM XML | 8 titles, 20k+ sections |
| IRS Guidance | irs.gov/pub/irs-drop | PDF/HTML | 570+ documents |
| State Laws | NY Open Legislation | JSON | Tax, Social Services |
| Regulations | eCFR | XML | Treasury, agency rules |

## Architecture

```
axiom-corpus/
├── src/axiom_corpus/
│   ├── __init__.py
│   ├── archive.py        # Main Axiom archive class
│   ├── models.py         # Pydantic models for statutes
│   ├── models_guidance.py # Models for IRS guidance
│   ├── parsers/
│   │   ├── uslm.py       # USLM XML parser
│   │   └── ny_laws.py    # NY Open Legislation parser
│   ├── fetchers/
│   │   ├── irs_bulk.py   # IRS bulk guidance fetcher
│   │   └── irs_guidance.py
│   ├── api/
│   │   └── main.py       # FastAPI app
│   ├── cli.py            # Command-line interface
│   └── storage/
│       ├── base.py       # Storage interface
│       ├── sqlite.py     # SQLite + FTS5 backend
│       └── postgres.py   # PostgreSQL backend
├── data/                  # Downloaded data (gitignored)
├── catalog/               # Structured document catalog
│   ├── guidance/          # IRS guidance documents
│   ├── statute/           # Statute extracts
│   └── parameters/        # Policy parameters by year
└── sources/               # Raw source archives
```

## Storage

Axiom uses SQLite + FTS5 for local development. For production deployments:

- **Cloudflare R2** — Raw files (PDFs, XML)
- **PostgreSQL** — Parsed content, metadata, full-text search

### Navigation index

The Supabase corpus schema also exposes `corpus.navigation_nodes`, a derived
parent/child serving index for browsing the corpus tree. `load-supabase`
rebuilds the index for the loaded scope by default (pass
`--no-build-navigation` to skip), and `axiom-corpus-ingest
build-navigation-index` (or `scripts/build_navigation_index.py`) can rebuild
it on demand from a provisions JSONL or directly from
`corpus.provisions`. `corpus.provisions` remains the source of truth for
legal text; navigation rows only exist so app navigation can be served from a
simple indexed `parent_path` lookup instead of repeated prefix `LIKE` scans.

```bash
# Rebuild one jurisdiction. --from-supabase prunes stale rows by default,
# since the Supabase snapshot is the full scope.
uv run python scripts/build_navigation_index.py --jurisdiction us-co --from-supabase

# Rebuild one (jurisdiction, doc_type) scope.
uv run python scripts/build_navigation_index.py --jurisdiction us-co --doc-type regulation --from-supabase

# Rebuild from a freshly extracted provisions JSONL. --provisions does NOT
# prune by default, because a local JSONL is often a partial slice of the
# corpus. Pass --replace-scope to prune stale rows in the touched scopes.
uv run python scripts/build_navigation_index.py --provisions data/corpus/provisions/us-co/regulation-2026.jsonl
```

## Deployment

### Local

```bash
# Build and run
pip install -e .
axiom-corpus serve
```

### Docker

```bash
# Build and run
docker build -t axiom-corpus .
docker run -p 8000:8000 -v $(pwd)/axiom.db:/app/axiom.db axiom-corpus
```

## License

Apache 2.0

## Related Repos

- [axiom-rules](https://github.com/TheAxiomFoundation/axiom-rules) — RuleSpec compiler and runtime
- [axiom-encode](https://github.com/TheAxiomFoundation/axiom-encode) — Encoder pipeline for generating RuleSpec from source law
- [rules-us](https://github.com/TheAxiomFoundation/rules-us) — US federal rules in RuleSpec
