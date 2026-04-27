# Source Document Organization

## Core Principle

**Everything is organized by source document type.**

- Statute documents → `catalog/statute/` and `sources/statutes/` in R2
- Guidance documents → `catalog/guidance/` and `sources/guidance/` in R2

Never mix document types. A guidance document (IRS Rev. Proc., USDA COLA memo) never lives in the statute path.

## Repo Separation

| Repo | Purpose |
|------|---------|
| **arch** | Archive tooling + document catalog (this repo) |
| **rules-us** | Executable encodings (.yaml formulas, parameters.yaml, tests.yaml) |

## Arch Repo Structure

```
arch/
├── catalog/                      # What's in R2
│   ├── statute/
│   │   ├── 26/63.yaml           # Metadata for sources/statutes/us/usc/26/63.xml
│   │   └── 7/2017.yaml
│   └── guidance/
│       ├── irs/rev-proc-2023-34.yaml
│       └── usda/fns/snap-fy2024-cola.yaml
│
├── scripts/                      # Archive tooling
│   └── catalog_snap.py
│
└── docs/                         # Documentation
```

## R2 Bucket Structure

```
arch (R2 bucket)/
├── sources/
│   ├── statutes/
│   │   └── us/
│   │       └── usc/
│   │           ├── 26/63.xml        # USC Title 26 § 63
│   │           └── 7/2017.xml       # USC Title 7 § 2017
│   │
│   ├── guidance/
│   │   └── irs/
│   │       ├── rev-proc/rev-proc-2023-34.pdf
│   │       └── notices/notice-2024-01.pdf
│   │
│   ├── microdata/
│   │   ├── cps-asec/
│   │   └── acs/
│   │
│   └── crosstabs/
│       └── soi/
```

See [R2 Setup Guide](../infrastructure/R2_SETUP.md) for full details.

## rules-us Structure (Encodings)

```
rules-us/
├── 26/                          # Title 26 statutes (path = citation)
│   ├── 24/a/credit.yaml
│   └── 63/c/standard_deduction.yaml
│
├── 7/                           # Title 7 statutes
│   └── 2017/a/allotment.yaml
│
├── irs/                         # IRS guidance encodings
│   └── rev-proc-2023-34/
│       └── parameters.yaml      # Inflation-adjusted values
│
└── usda/fns/                    # USDA guidance encodings
    └── snap-fy2024-cola/
        └── parameters.yaml      # COLA-adjusted values
```

## Variable Precedence Logic

When a statute defines a base value but guidance provides an adjusted value:

```yaml
references {
  # From statute encoding
  base_amount: statute/26/63/c/2/basic_amounts

  # From guidance encoding
  adjusted_amount: guidance/irs/rev-proc-2023-34/standard_deduction
}

variable standard_deduction {
  formula {
    # Use guidance value if available, else statute base
    return adjusted_amount ?? base_amount
  }
}
```

## Rules

1. **Catalog entries reference their source document type only**
   - `catalog/statute/26/63.yaml` → describes `sources/statutes/us/usc/26/63.xml` in R2
   - `catalog/guidance/irs/...` → describes `sources/guidance/irs/...` in R2

2. **Encodings live in rules-us, not here**

3. **Archive tooling lives in scripts/ and src/arch/**
