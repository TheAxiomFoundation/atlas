# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Atlas is Rules Foundation's legal document archive. It downloads, parses, and stores legal and regulatory documents from official government sources, enabling programmatic access with full-text search.

### Scope

Atlas archives **legal sources only**:
- **Federal Statutes**: US Code (USLM XML from uscode.house.gov)
- **State Statutes**: All 50 states (crawled from official legislative sites)
- **IRS Guidance**: Revenue Procedures, Revenue Rulings, Notices, Publications
- **Regulations**: CFR titles (from eCFR)

This repo is part of Rules Foundation (see parent CLAUDE.md). For microdata and calibration targets, see the `arch` repo.

### Infrastructure

- **R2 Bucket**: `atlas` (Cloudflare R2)
- **Credentials**: `~/.config/rulesfoundation/r2-credentials.json`
- **113k+ documents**, 2GB total

## Commands

```bash
# Install dependencies
uv sync                          # Or: pip install -e ".[dev,verify]"

# CLI usage
arch download 26           # Download US Code Title 26 (IRC)
arch ingest data/uscode/usc26.xml  # Ingest into SQLite
arch get "26 USC 32"       # Get a specific section
arch search "earned income" --title 26  # Full-text search
arch serve                 # Start REST API at localhost:8000

# AI encoding pipeline
arch encode "26 USC 32"    # Encode statute into RAC (Rules as Code)
arch validate ~/.rac/workspace/federal/statute/26/32
arch verify ~/.rac/workspace/federal/statute/26/32 -v eitc

# Testing
pytest                           # Run all tests
pytest tests/test_models.py -v   # Run specific test file
pytest -k "test_parse"           # Run tests matching pattern

# Linting
ruff check src/                  # Lint
ruff format src/                 # Format
mypy src/arch/                   # Type check
```

## Architecture

### Core Pipeline

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Official XML   │────▶│  USLM Parser    │────▶│  SQLite/FTS5   │
│  (uscode.gov)   │     │  parsers/uslm.py│     │  arch.db       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                       │
┌─────────────────┐     ┌─────────────────┐            │
│  Claude AI      │◀────│  Encoder        │◀───────────┘
│  (DSL gen)      │     │  encoder.py     │
└─────────────────┘     └─────────────────┘
        │
        ▼
┌─────────────────┐     ┌─────────────────┐
│  .rac      │────▶│  Verifier       │────▶ PolicyEngine
│  rules + tests  │     │  verifier.py    │      comparison
└─────────────────┘     └─────────────────┘
```

### Key Modules

- **`archive.py`** - Main `Arch` class (public API)
- **`models.py`** - Pydantic models: `Citation`, `Section`, `Subsection`, `SearchResult`
- **`models_guidance.py`** - IRS guidance document models (Rev. Procs, Rulings)
- **`parsers/uslm.py`** - USLM XML parser for US Code
- **`storage/sqlite.py`** - SQLite backend with FTS5 full-text search
- **`encoder.py`** - AI pipeline: statute -> RAC (Rules as Code)
- **`verifier.py`** - Compare DSL outputs vs PolicyEngine
- **`cli.py`** - Click CLI commands

### Data Flow

1. **Download**: `arch download 26` fetches XML from uscode.house.gov
2. **Ingest**: Parser extracts sections, subsections, cross-references -> SQLite
3. **Query**: FTS5-powered search, citation lookup, cross-reference graph
4. **Encode**: Claude generates DSL code from statute text
5. **Verify**: Compare DSL test cases against PolicyEngine calculations

### Directory Structure

```
data/           # Downloaded/ingested data (gitignored)
  uscode/       # Raw USLM XML files
  federal/      # Processed federal statutes
  microdata/    # CPS, ACS microdata files
    census_blocks/  # Census PL 94-171 block-level data
      pl94171_blocks_2020.parquet  # ~11M blocks with population
      metadata.json                 # Source and fetch info
  crosstabs/    # SOI, Census tabulations
catalog/        # Structured statute catalog
  guidance/     # IRS guidance documents
  statute/      # Statute extracts
  parameters/   # Policy parameters by year
sources/        # Source document archives (state codes, etc.)
output/         # Generated outputs (DSL, verification reports)
schema/         # SQL migration files
```

### Data Flow: Raw Files vs Processed Tables

- **arch (this repo)**: Raw government files (XML, PDF, CSV, Parquet)
  - Synced to Cloudflare R2 for durable storage
  - Source of truth for original data
- **Supabase PostgreSQL**: Processed/derived tables
  - `block_probabilities`: Block sampling probabilities by state/CD
  - Queryable for microsimulation

Example: Census blocks
1. Raw: `arch/data/microdata/census_blocks/pl94171_blocks_2020.parquet`
2. Processed: Supabase `block_probabilities` table (state_fips, cd_id, prob)

## Key Patterns

### Citation Parsing

Citations follow USC format and convert to filesystem paths:
- `"26 USC 32"` -> `Citation(title=26, section="32")`
- `"26 USC 32(a)(1)"` -> subsection `"a/1"`, path `"statute/26/32/a/1"`

### Storage Backend Interface

`StorageBackend` abstract class (storage/base.py) defines the interface. SQLite implementation uses FTS5 for search with triggers to keep index in sync.

### DSL Encoding Output

`arch encode` generates four files per section:
- `rules.rac` - Executable DSL code
- `tests.yaml` - Test cases for verification
- `statute.md` - Original statute text
- `metadata.json` - Provenance (model, tokens, timestamp)

## Testing

Tests use pytest with async support. Key test files:
- `test_models.py` - Citation parsing, model validation
- `test_storage.py` - SQLite backend operations
- `test_document_writer.py` - Output generation

Run a single test:
```bash
pytest tests/test_models.py::TestCitation::test_parse_simple_citation -v
```

## Notes

This repo was moved from CosilicoAI to RulesFoundation in Jan 2026. All references have since been updated to Rules Foundation.

- **GitHub**: `RulesFoundation/atlas`
- **Local**: `/Users/maxghenis/RulesFoundation/atlas`
- **R2 Bucket**: `atlas` (migrated from `arch`)
- **Credentials**: `~/.config/rulesfoundation/r2-credentials.json`
