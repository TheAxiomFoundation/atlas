# DC Tax Law Exploration for Axiom Corpus

**Date:** 2025-12-25
**Issue:** rules-us-mat
**Purpose:** Assess feasibility of importing DC tax law into Axiom Corpus

## Executive Summary

DC is an excellent candidate for Axiom Corpus import because the Council of the District of Columbia publishes their entire legal code on GitHub in structured XML format under CC0 license. This is the only jurisdiction in the world that publishes law on GitHub as an authoritative source.

**Recommendation:** Proceed with DC as the first state/territory implementation. The XML structure is well-documented and the tax code is contained in Title 47.

## Source Repositories

### Primary Sources

| Repository | Purpose | URL |
|------------|---------|-----|
| DCCouncil/law-xml | Raw uncodified laws + code (March 2016 baseline) | https://github.com/DCCouncil/law-xml |
| dccouncil/law-xml-codified | Fully codified laws (build product) | https://github.com/dccouncil/law-xml-codified |
| DCCouncil/dc-law | Documentation and issue tracker | https://github.com/DCCouncil/dc-law |
| DCCouncil/dc-law-tools | Build tools (Python, XSLT) | https://github.com/DCCouncil/dc-law-tools |
| D.C. Law Library | Official web interface | https://code.dccouncil.gov |

### Key Characteristics

- **License:** CC0 (public domain dedication)
- **Format:** Custom XSD-based XML schema
- **Authoritative:** This IS the official digital source, not a copy
- **Update Frequency:** Continuous (laws are added as enacted)
- **Build System:** Python + XSLT transforms

## XML Format Analysis

### Schema Files

The DC Council uses custom XSD schemas located in `schemas/`:

```
schemas/
├── XInclude.xsd       # XML inclusion handling
├── annotation-types.xsd  # Annotation structures
├── codified.xsd       # Codified content rules
├── codify.xsd         # Codification processes
└── dc-library.xsd     # Library-level definitions
```

### Document Structure

Sample section structure from `47-1806.03.xml` (individual income tax rates):

```xml
<section>
  <num>47-1806.03</num>
  <heading>Tax on residents and nonresidents — Rates of tax.</heading>

  <para>
    <num>(a)</num>
    <!-- Tax rate tables by effective date -->
    <para>
      <num>(10)</num>
      <text>For taxable years beginning after December 31, 2021:</text>
      <table>
        <tbody>
          <tr>
            <td>Not over $10,000</td>
            <td>4%</td>
          </tr>
          <!-- Additional brackets -->
        </tbody>
      </table>
    </para>
  </para>

  <annotations>
    <!-- Legislative history, emergency amendments, cross-references -->
  </annotations>
</section>
```

### Key Elements

| Element | Purpose |
|---------|---------|
| `<section>` | Root element for a code section |
| `<num>` | Section/subsection number |
| `<heading>` | Section title |
| `<para>` | Paragraph with nested structure |
| `<text>` | Actual legal text |
| `<table>` | Tabular data (tax brackets, etc.) |
| `<annotations>` | Legislative history and metadata |
| `<xi:include>` | Cross-file references |

## DC Tax Code Structure (Title 47, Chapter 18)

### Chapter Organization

Title 47 (Taxation) contains Chapter 18 (Income and Franchise Taxes) with these subchapters:

| Subchapter | Sections | Content |
|------------|----------|---------|
| I | 47-1801.01-05 | Definitions and applicability |
| II | 47-1802.01-04 | Exempt organizations |
| III | 47-1803.01-04 | Gross income, deductions |
| VI | 47-1806.01-17 | **Individual tax rates and credits** |
| VII-A | — | Job Growth Tax Credit |
| XII-XV | — | Returns, assessment, penalties |
| XVII | — | High Technology Companies |
| XIX | — | Vacant/Blighted Property Credit |

### Key Individual Tax Sections

| Section | Title | Content |
|---------|-------|---------|
| 47-1806.01 | Taxable income defined | Definition of taxable income |
| 47-1806.02 | Personal exemptions | **[REPEALED]** - No longer in effect |
| 47-1806.03 | Rates of tax | **Tax brackets** (7 rates: 4%-10.75%) |
| 47-1806.04 | Credits - In general | EITC, CDCC, out-of-state, low-income |
| 47-1806.06 | Credits - Property taxes | Schedule H property tax credit |

## Current DC Individual Income Tax Parameters

### Tax Brackets (2022+)

| If taxable income is: | Tax = |
|----------------------|-------|
| Not over $10,000 | 4% of taxable income |
| $10,001 - $40,000 | $400 + 6% of excess over $10,000 |
| $40,001 - $60,000 | $2,200 + 6.5% of excess over $40,000 |
| $60,001 - $250,000 | $3,500 + 8.5% of excess over $60,000 |
| $250,001 - $500,000 | $19,650 + 9.25% of excess over $250,000 |
| $500,001 - $1,000,000 | $42,775 + 9.75% of excess over $500,000 |
| Over $1,000,000 | $91,525 + 10.75% of excess over $1,000,000 |

**Source:** DC Code 47-1806.03(a)(10)

### Standard Deductions (2024)

| Filing Status | Amount |
|--------------|--------|
| Single | $14,600 |
| MFJ / Qualifying Widow(er) | $29,200 |
| MFS | $14,600 |
| Head of Household | $21,900 |
| Additional (Age 65+ / Blind) | $1,550 ($1,950 if also unmarried) |

### DC EITC

The DC EITC is a percentage of the federal EITC:

| Period | Match Rate | Source |
|--------|-----------|--------|
| Before 2022 | 40% of federal EITC | 47-1806.04(f)(1) |
| 2022-2024 | 70% of federal EITC | 47-1806.04(f)(1-A) |
| 2025+ | 100% of federal EITC | 47-1806.04(f)(1-B) |

**Special provisions:**
- Available to non-citizens/aliens with ITIN (after 12/31/2022)
- Non-custodial parents aged 18-30 eligible via Schedule N
- Amounts $1,200+ can be received in monthly installments

### Other Credits

| Credit | Rate/Amount | Section |
|--------|-------------|---------|
| Child & Dependent Care | 32% of federal (24.25% after 2025) | 47-1806.04(c) |
| Property Tax Credit | Up to $1,375 | 47-1806.06 |
| Out-of-State Income Tax | Dollar-for-dollar credit | 47-1806.04(a) |

## Import Strategy

### Phase 1: Direct XML Import

1. **Clone law-xml-codified repository**
   ```bash
   git clone https://github.com/dccouncil/law-xml-codified.git
   ```

2. **Navigate to Title 47**
   ```
   dc/council/code/titles/47/chapters/18/sections/
   ```

3. **Parse XML sections into Axiom Corpus format**
   - Use existing USLM parser as template
   - Map DC XML elements to Axiom models

### Phase 2: Schema Mapping

| DC XML Element | Lawarchive Model Field |
|----------------|----------------------|
| `<section><num>` | `section.citation` |
| `<heading>` | `section.title` |
| `<text>` | `section.text` |
| `<table>` | Structured data extraction |
| `<annotations>` | `section.metadata` |

### Phase 3: Parameter Extraction

Extract structured parameters from tables:

```python
# Example: Tax bracket extraction from 47-1806.03
brackets = [
    {"threshold": 10000, "rate": 0.04, "base_tax": 0},
    {"threshold": 40000, "rate": 0.06, "base_tax": 400},
    {"threshold": 60000, "rate": 0.065, "base_tax": 2200},
    {"threshold": 250000, "rate": 0.085, "base_tax": 3500},
    {"threshold": 500000, "rate": 0.0925, "base_tax": 19650},
    {"threshold": 1000000, "rate": 0.0975, "base_tax": 42775},
    {"threshold": float("inf"), "rate": 0.1075, "base_tax": 91525},
]
```

## Comparison: DC vs Federal Format

| Aspect | Federal (USLM) | DC |
|--------|---------------|-----|
| Schema | USLM XML (derived from Akoma Ntoso) | Custom XSD |
| Hosting | uscode.house.gov (download only) | GitHub (git native) |
| License | Public domain | CC0 |
| Structure | Hierarchical (Title > Chapter > Section) | Same |
| Tables | Supported | Supported |
| History | Release points | Git commits |
| API | None (must download) | Git API + web |

## Advantages of DC for Lawarchive

1. **Git-native:** No scraping needed; just `git pull`
2. **CC0 License:** Explicitly public domain
3. **Authoritative:** Official source, not a copy
4. **Structured XML:** Well-defined schemas
5. **Regular updates:** Continuous as laws are enacted
6. **Manageable size:** Single jurisdiction (not 50 states)
7. **Complete tax code:** All individual tax provisions in Chapter 18

## Challenges and Mitigations

| Challenge | Mitigation |
|-----------|------------|
| Custom schema (not USLM) | Write DC-specific parser (one-time effort) |
| Nested paragraph structure | Recursive parsing with depth tracking |
| Table extraction | Use existing patterns from federal parser |
| Amendment tracking | Use git history + annotations |
| Schema changes | Monitor law-xml-codified releases |

## Recommended Implementation

### File Structure

```
axiom-corpus/
├── src/axiom/parsers/
│   ├── uslm.py           # Existing federal parser
│   └── dc/
│       ├── __init__.py
│       ├── parser.py     # DC XML parser
│       └── schemas/      # Copy of DC XSD files
│
├── catalog/
│   └── statute/
│       └── dc/
│           └── 47/
│               └── 1806.03.yaml  # Metadata for DC tax rates
│
└── scripts/
    └── ingest_dc.py      # DC ingestion script
```

### R2 Storage

```
axiom-corpus (R2)/
└── dc/statute/
    └── 47/
        ├── 1806.01.xml   # Taxable income definition
        ├── 1806.03.xml   # Tax rates
        ├── 1806.04.xml   # Credits
        └── ...
```

## Provisions to Encode First

Priority order for rules-us encoding:

1. **DC Individual Income Tax Rates** (47-1806.03)
   - 7-bracket graduated rate structure
   - Parameters: thresholds, rates, base taxes

2. **DC EITC** (47-1806.04(f))
   - Match rate to federal EITC
   - Parameters: match_rate by year

3. **DC Standard Deduction** (from OTR guidance)
   - By filing status
   - Additional amounts for age/blindness

4. **DC Child and Dependent Care Credit** (47-1806.04(c))
   - Federal credit match rate
   - Parameters: match_rate by year

5. **DC Property Tax Credit** (47-1806.06)
   - Schedule H income limits
   - Credit calculation formula

## Next Steps

1. [ ] Create DC XML parser in `src/axiom/parsers/dc/`
2. [ ] Clone law-xml-codified and test parsing
3. [ ] Add DC sections to `axiom.db`
4. [ ] Create catalog entries for key tax sections
5. [ ] Implement DC individual income tax in rules-us

## Sources

- [DCCouncil GitHub Organization](https://github.com/DCCouncil)
- [DC Law Library](https://code.dccouncil.gov)
- [DC Code Title 47](https://code.dccouncil.gov/us/dc/council/code/titles/47)
- [DC Chapter 18 - Income and Franchise Taxes](https://code.dccouncil.gov/us/dc/council/code/titles/47/chapters/18)
- [DC Individual Tax Rates (OTR)](https://otr.cfo.dc.gov/page/dc-individual-and-fiduciary-income-tax-rates)
- [DC EITC Information](https://otr.cfo.dc.gov/page/dc-eitc)
- [DC Office of Tax and Revenue](https://otr.cfo.dc.gov)
- [Washington DC Made GitHub Its Official Digital Source For Laws (Slashdot)](https://yro.slashdot.org/story/18/11/25/2335229/washington-dc-made-github-its-official-digital-source-for-laws)
- [Akoma Ntoso Standard](https://docs.oasis-open.org/legaldocml/akn-core/v1.0/akn-core-v1.0-part1-vocabulary.html)
