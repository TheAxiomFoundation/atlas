# State Statute Sources

## Architecture Overview

Axiom Corpus uses a unified statute model with source adapters for each
jurisdiction:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      UNIFIED STATUTE MODEL                          │
├─────────────────────────────────────────────────────────────────────┤
│  Statute(                                                           │
│    jurisdiction="us-ca",        # Matches RuleSpec repo naming           │
│    code="RTC",                  # Code/title identifier             │
│    section="17041",             # Section number                    │
│    title="Personal income tax", # Section heading                   │
│    text="...",                  # Full text content                 │
│  )                                                                  │
├─────────────────────────────────────────────────────────────────────┤
│  RuleSpec path: rulespec-us-ca/statute/RTC/17041.yaml                  │
│  Corpus path:   us-ca/statute/RTC/17041                             │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      SOURCE ADAPTERS                                 │
├─────────────────────────────────────────────────────────────────────┤
│  USLMSource   - Federal US Code (XML from uscode.house.gov)         │
│  APISource    - JSON APIs (NY Open Legislation, LegiScan)           │
│  HTMLSource   - Web scraping (most states)                          │
│  BulkSource   - Bulk downloads (CA MySQL, etc.)                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Jurisdiction IDs

Jurisdiction IDs match RuleSpec repo naming:
- `us` → `rulespec-us` (federal)
- `us-ca` → `rulespec-us-ca` (California)
- `us-ny` → `rulespec-us-ny` (New York)

### Storage

- **Supabase (Production)**: `corpus.provisions`
- **SQLite (Local)**: For development and testing

## Priority Tiers

### Tier 1: Large Income Tax States (implement first)
| State | Pop (M) | Source | Format | Status |
|-------|---------|--------|--------|--------|
| CA | 39.0 | leginfo.legislature.ca.gov | HTML scrape | ✅ Done |
| NY | 19.5 | legislation.nysenate.gov | JSON API | ✅ Done |
| TX | 30.0 | statutes.capitol.texas.gov | HTML | ✅ Done |
| FL | 22.6 | leg.state.fl.us | HTML | ✅ Done |
| PA | 12.9 | palegis.us | HTML scrape | ✅ Done |
| IL | 12.6 | ilga.gov | HTML | ✅ Config |
| OH | 11.8 | codes.ohio.gov | HTML scrape | ✅ Done |
| GA | 10.9 | legis.ga.gov | PDF/complex | ✅ Config |
| NC | 10.7 | ncleg.gov | HTML scrape | ✅ Done |
| MI | 10.0 | legislature.mi.gov | HTML | ✅ Config |

### Tier 2: Medium Income Tax States
| State | Pop (M) | Source | Format | Status |
|-------|---------|--------|--------|--------|
| NJ | 9.3 | njleg.state.nj.us | HTML | ✅ Config* |
| VA | 8.6 | law.lis.virginia.gov | HTML | ✅ Config |
| WA | 7.8 | leg.wa.gov | HTML | ✅ Config |
| AZ | 7.4 | azleg.gov | HTML | ✅ Config |
| MA | 7.0 | malegislature.gov | HTML | ✅ Config |
| CO | 5.8 | content.leg.colorado.gov | PDF bulk | ✅ Config |
| MD | 6.2 | mgaleg.maryland.gov | HTML | ✅ Config |
| MN | 5.7 | revisor.mn.gov | HTML | ✅ Config |
| WI | 5.9 | docs.legis.wisconsin.gov | HTML | ✅ Config |
| MO | 6.2 | revisor.mo.gov | HTML | ✅ Config |

*NJ uses complex gateway system, may need custom parser

### Tier 3: Smaller States & No Income Tax
| State | Notes |
|-------|-------|
| NV, WY, SD, AK | No income tax |
| NH, TN | Limited income tax (investment only) |
| Remaining 20+ states | Lower priority |

## Data Sources by Type

### Official Bulk Downloads
- **CA**: downloads.leginfo.legislature.ca.gov (ZIP archives, SQL database)
- **TX**: statutes.capitol.texas.gov (PDF, RTF, HTML bulk)

### Official APIs
- **NY**: legislation.nysenate.gov/api/3 (JSON, free API key)

### Web Scraping Required
- Most other states require HTML parsing from legislature websites

### Third-Party Sources
- **LegiScan**: legiscan.com/datasets (all 50 states, JSON/XML, free registration)
- **Open States**: open.pluralpolicy.com/data (bills, not codified statutes)
- **Justia**: law.justia.com/codes (readable, but TOS may restrict scraping)

## Implementation Notes

### Common Patterns
Many state legislatures use similar CMS platforms:
- Some use LegiStar
- Many have similar URL structures for sections

### Key Codes to Prioritize
For each state, focus on:
1. **Tax Code** (Revenue & Taxation, Tax Law, etc.)
2. **Welfare/Benefits Code** (Human Services, Social Services, etc.)
3. **Unemployment Insurance Code**
4. **Labor Code**

## Progress Tracking

### Fully Implemented (tested scrapers)
- [x] Federal US Code (USLM parser)
- [x] CA (HTML scraper) - 28 codes including RTC, WIC
- [x] NY (Open Legislation API)
- [x] FL (HTML scraper)
- [x] TX (HTML scraper)
- [x] PA (generic scraper)
- [x] OH (generic scraper)
- [x] NC (generic scraper)

### Configured (URL patterns ready, needs testing)
- [x] IL - ilga.gov
- [x] GA - legis.ga.gov (PDF-based, may need custom)
- [x] MI - legislature.mi.gov
- [x] VA - law.lis.virginia.gov
- [x] WA - leg.wa.gov
- [x] AZ - azleg.gov
- [x] MA - malegislature.gov
- [x] MD - mgaleg.maryland.gov
- [x] MN - revisor.mn.gov
- [x] WI - docs.legis.wisconsin.gov
- [x] MO - revisor.mo.gov
- [x] NJ - njleg.state.nj.us (complex gateway, may need custom)

### Bulk/PDF Sources (needs PDF parser)
- [x] CO - content.leg.colorado.gov (official PDFs by title)

### Not Yet Configured
- [ ] ... (remaining ~30 states)
