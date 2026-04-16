# Atlas Data Inventory

This document catalogs data sources that have been downloaded and scraped into
the Atlas archive. **Treat existing caches as the source of truth** — don't
re-scrape unless upstream content has changed.

**Last hand-curated:** 2026-04-16

> Status values used below:
>
> - **Complete** — parsed, ingested, and queryable. Covers the full corpus.
> - **Partial** — some content present but coverage is incomplete (missing
>   codes, subset of titles, or known gaps).
> - **Navigation-only** — we scraped the site but only captured TOC / nav
>   pages, not actual statute section text. **Not usable for queries.**
> - **Broken** — download failed (404s, empty files, or wrong MIME). **Not
>   usable.**
> - **Not started** — repo/placeholder exists but no ingestion has run.

## Jurisdictions at a glance

### Federal / international (complete)

| Jurisdiction | Source | Format | Location | Status |
|---|---|---|---|---|
| US Code (federal) | uscode.house.gov | USLM XML | `data/uscode/` | Complete — 53 titles |
| UK Public General Acts | legislation.gov.uk | CLML XML | `~/.arch/uk/ukpga/` | Complete — 3,240 acts, 1916-2025 [^counts] |
| Canada federal statutes | laws-lois.justice.gc.ca | LIMS XML | `~/.arch/canada/` | Complete — 956 files [^counts] |
| DC Code | lims.dccouncil.gov | DC XML | `sources/dc/dc-law-xml/` | Complete — 21,163 sections across 28,432 files |
| 7 CFR 271–283 (SNAP) | ecfr.gov | eCFR XML | `arch.rules` (Supabase) | Complete — 210 rows, eCFR snapshot 2024-04-16, ingested 2026-04-16 via `scripts/ingest_cfr_parts.py` |

### US states — complete

| Jurisdiction | Sections | Repo |
|---|---|---|
| DC | 21,163 | `rules-us-dc` |
| NV | 11,842 | `rules-us-nv` |
| NY (selected consolidated laws) | 10,976 | `rules-us-ny` |

### US states — partial (content present, gaps known)

| Jurisdiction | Repo | Notes |
|---|---|---|
| AZ | `rules-us-az` | Arizona Revised Statutes, partial |
| DE | `rules-us-de` | Delaware Code, partial |
| IA | `rules-us-ia` | Iowa Code, partial |
| KY | `rules-us-ky` | Kentucky Revised Statutes, partial |
| ME | `rules-us-me` | Maine Revised Statutes, partial |
| ND | `rules-us-nd` | North Dakota Century Code, partial |
| NE | `rules-us-ne` | Nebraska Revised Statutes, partial |
| NH | `rules-us-nh` | New Hampshire Revised Statutes, partial |
| OH | `rules-us-oh` | Ohio Revised Code, partial |
| RI | `rules-us-ri` | Rhode Island General Laws, partial |
| TX | `rules-us-tx` | Texas Statutes, partial |
| VA | `rules-us-va` | Virginia Code, partial |
| VT | `rules-us-vt` | Vermont Statutes, partial |
| WY | `rules-us-wy` | Wyoming Statutes, partial |

### US states — repo exists but 0 `.rac` files

These state repos were scaffolded but do not yet contain any encoded rules.
Content ingestion either hasn't run or hasn't produced usable output:

| Jurisdiction | Repo | Likely reason |
|---|---|---|
| AL | `rules-us-al` | Crawled; converter/ingest not run |
| AR | `rules-us-ar` | LexisNexis host; scrape captured only default pages (see below) |
| FL | `rules-us-fl` | Scrape present in `data/statutes/`; converter output empty |
| GA | `rules-us-ga` | Archive.org bulk download available; ingestion not yet run |
| MD | `rules-us-md` | Not yet scraped |
| NC | `rules-us-nc` | Archive.org bulk download available; ingestion not yet run |
| SC | `rules-us-sc` | Scrape present; converter output empty |
| TN | `rules-us-tn` | Archive.org bulk download available; ingestion not yet run |

### US states — navigation-only scrapes (NOT usable)

`data/statutes/us-{code}/` contains HTML, but the captured pages are
Tables of Contents or site navigation — **no section text was retrieved**.
These need revised crawl patterns in
`src/atlas/sources/specs/` and/or `src/atlas/crawl.py:SECTION_PATTERNS`
before re-ingesting.

| Jurisdiction | Symptom |
|---|---|
| LA | Only TOC/index pages captured |
| MN | Only navigation/UI pages captured |
| PA | Only navigation/UI pages captured |
| WA | Only navigation/UI pages captured |
| WI | Only navigation/UI pages captured |
| OK | Only index pages captured |

### US states — broken downloads

| Jurisdiction | Symptom |
|---|---|
| AK | All downloads returned 404 error pages |
| TX (`data/texas_cache/`) | Empty directory — prior crawl produced no output |

### US states — not yet scraped

Remaining state repositories are scaffolded but have no source data in
`data/statutes/` and no Archive.org mirror configured. Needs source
decisions before crawling.

## Data locations

### `~/.arch/` (user cache directory)

| Directory | Contents | Count | Format | Status |
|---|---|---|---|---|
| `~/.arch/canada/` | Canada federal statutes | 956 files | LIMS XML | Complete |
| `~/.arch/federal/` | US federal agency guidance | varies | HTML/PDF | Partial |
| `~/.arch/irs/` | IRS guidance documents | varies | PDF | Partial |
| `~/.arch/policyengine-us/` | State tax forms/instructions | ~40 states | PDF | Reference docs |
| `~/.arch/uk/ukpga/` | UK Public General Acts | 3,240 files | CLML XML | Complete (1916-2025) |
| `~/.arch/us-ca/` | California statutes | 3 files | HTML | Partial |

### `data/` (repository data directory)

| Directory | Contents | Count | Format | Status |
|---|---|---|---|---|
| `data/uscode/` | US Code titles | 53 titles | USLM XML | Complete |
| `data/statutes/us-*` | State statute downloads | 48 states | Mixed | **Varies — see tables above** |
| `data/microdata/census_blocks/` | Census PL 94-171 data | 1 file | Parquet | Complete |
| `data/us/guidance/irs/` | IRS guidance by year/title | varies | Mixed | Partial |
| `data/ca/` | California statutes/guidance | ~33 items | Mixed | Partial |
| `data/texas_cache/` | Texas statutes | empty | — | Broken |

### `sources/` (source document archives)

| Directory | Contents | Count | Format | Status |
|---|---|---|---|---|
| `sources/dc/dc-law-xml/` | DC Council Law Library | 28,432 files | DC XML | Complete |
| `sources/policyengine-us/` | PolicyEngine state data | varies | Mixed | Reference |

## Known upstream issues

When a source is flagged broken or navigation-only, the fix usually lives
upstream or requires a revised crawl pattern rather than re-running the
existing scraper.

- **Alaska (AK)** — `akleg.gov` returns 404 pages for the patterns in
  `SECTION_PATTERNS` at `src/atlas/crawl.py`. Needs a new crawl strategy
  (likely the legislature's document server, not the public site).
- **Louisiana (LA)** — `legis.la.gov` serves statute text inside
  ASP.NET postback forms; the current crawler only captures the landing
  TOC. Needs a Playwright-based fetcher or a Public.Resource.org mirror.
- **Minnesota (MN)** — `revisor.mn.gov/statutes/` pattern matches the
  chapter browser UI, not individual sections. Needs `/statutes/cite/...`
  pattern refinement.
- **Washington (WA)** — Similar to MN; `app.leg.wa.gov` routes through a
  JavaScript-rendered TOC. Needs Playwright or the bulk RCW XML dump.
- **Wisconsin (WI)** — `docs.legis.wisconsin.gov` PDF-first delivery;
  HTML pages captured are navigation only.
- **Pennsylvania (PA)** — `legis.state.pa.us` uses dynamic
  `view-statute?...` links not currently resolved by the crawler.
- **Oklahoma (OK)** — `oscn.net` requires session cookies and
  `DeliverDocument.asp` CiteID resolution; current crawl gets index
  pages only.
- **Arkansas (AR) and Mississippi (MS)** — Both are LexisNexis-hosted.
  Archive.org has historical dumps for AR volumes but they are split
  across multiple items (not yet wired into `ARCHIVE_ORG_STATES` in
  `src/atlas/crawl.py`).

## Rules repositories

Akoma Ntoso XML files pushed to GitHub:

### Complete repositories

| Repository | Sections | Source |
|---|---|---|
| `RulesFoundation/rules-us` | 60,204 | US Code USLM XML |
| `RulesFoundation/rules-ca` | 601 | Canada LIMS XML |
| `RulesFoundation/rules-us-dc` | 21,163 | DC Law XML |
| `RulesFoundation/rules-uk` | ~3,236 | UK CLML XML |

### State repositories

See "US states" tables above for per-jurisdiction status.

## Adding new data

1. **Check this inventory first** — data may already exist
2. **Use existing scrapers** in `src/atlas/parsers/` and `src/atlas/converters/`
3. **Cache to `~/.arch/`** for reuse across workflows
4. **Update this file** after scraping — include a status flag and date

## Converter reference

Converters in `src/atlas/converters/`:

| Converter | Input format | Output format |
|---|---|---|
| `uk_clml.py` | UK CLML XML | Atlas models |
| `ca_laws.py` | Canada LIMS XML | Atlas models |
| `ecfr.py` | eCFR XML | Atlas models |
| `nz_pco.py` | NZ PCO XML | Atlas models |
| `us_states/*.py` | State HTML/XML | USLM XML |

[^counts]: Counts last verified 2026-04-16 via `find ~/.arch/{uk/ukpga,canada} -maxdepth 1 -type f | wc -l`.
