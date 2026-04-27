# Atlas

**Comprehensive map of government legal sources.**

Atlas is the unified source of truth for statutes, regulations, and IRS guidance that powers the The Axiom Foundation ecosystem.

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
pip install atlas-archive

# Run the API server
atlas serve

# Or use the CLI
atlas get "26 USC 32"        # Get IRC § 32 (EITC)
atlas search "earned income" # Search across documents
```

## CLI Usage

```bash
# Download sources
atlas download 26                    # Download Title 26 (IRC) from uscode.gov
atlas download-state ny              # Download NY state laws
atlas irs-guidance --year 2024       # Fetch IRS guidance for 2024

# Query
atlas get "26 USC 32"                # Get specific section
atlas search "child tax credit"      # Full-text search
atlas stats                          # Show database stats

# API
atlas serve                          # Start REST API at localhost:8000
```

## Python API

```python
from atlas import Atlas

archive = Atlas()

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
atlas/
├── src/atlas/
│   ├── __init__.py
│   ├── archive.py        # Main Atlas class
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

Atlas uses SQLite + FTS5 for local development. For production deployments:

- **Cloudflare R2** — Raw files (PDFs, XML)
- **PostgreSQL** — Parsed content, metadata, full-text search

## Deployment

### Local

```bash
# Build and run
pip install -e .
atlas serve
```

### Docker

```bash
# Build and run
docker build -t atlas .
docker run -p 8000:8000 -v $(pwd)/atlas.db:/app/atlas.db atlas
```

## License

Apache 2.0

## Related Repos

- [rulespec-compile](https://github.com/TheAxiomFoundation/rulespec-compile) — RuleSpec compiler and DSL tooling
- [autorulespec](https://github.com/TheAxiomFoundation/autorulespec) — AI-powered statute encoding
- [rules-us](https://github.com/TheAxiomFoundation/rules-us) — US federal rules in RuleSpec
