# Rule references

`akn.rule_references` is Atlas's citation graph. One row per cite found
inside a rule's body text. The same table serves two consumers:

- **Atlas viewer** — renders body text with clickable `<a>` tags at the
  recorded `(start_offset, end_offset)` spans.
- **RAC tooling** (autorac, rac-compile) — uses the outgoing refs of an
  encoded rule as the candidate list for its `imports:` block.

## Schema

```
source_rule_id        UUID  → akn.rules(id)
target_citation_path  TEXT  "us/statute/42/9902/2"
target_rule_id        UUID  → akn.rules(id)  NULL if target not yet ingested
citation_text         TEXT  "42 U.S.C. 9902(2)"
pattern_kind          TEXT  "usc" | "cfr" | ...
start_offset          INT
end_offset            INT
confidence            REAL  0.0–1.0 (1.0 = pattern-unambiguous)
```

`target_rule_id` is nullable because many citations resolve to rules not
yet in the archive. The `target_citation_path` is stored either way, so
a later ingestion automatically activates the link the next time the
extractor runs.

## The RPC

```
akn.get_references(citation_path_in text)
```

Returns one row per outgoing and incoming ref for the given rule,
ordered to support direct rendering:

| direction | citation_text | pattern_kind | other_citation_path | other_heading | target_resolved | start_offset | end_offset |
|---|---|---|---|---|---|---|---|
| outgoing | "42 U.S.C. 9902(2)" | usc | us/statute/42/9902/2 | Community Services Block Grant Act | true | 451 | 470 |
| outgoing | "section 32" | usc | us/statute/26/32 | Earned income | true | 820 | 830 |
| incoming | "7 U.S.C. 2014" | usc | us/regulation/7/273/9 | Income and deductions | true | 302 | 315 |

Outgoing rows carry offsets so the viewer can wrap the citation in an
`<a>` tag inside the body text. Incoming rows point back to the citing
rule and are rendered as a "referenced by" panel on the rule page.

## Consumer: Atlas viewer

Typescript:

```ts
const { data } = await supabaseAkn.rpc('get_references', {
  citation_path_in: rule.citation_path,
})

const outgoing = data.filter(r => r.direction === 'outgoing')
const incoming = data.filter(r => r.direction === 'incoming')
```

Render outgoing refs by walking `rule.body` and splicing `<a>` tags at
each `(start_offset, end_offset)` — the same pattern `atlas-search.tsx`
uses for `ts_headline`'s `<mark>` markers. Unresolved targets
(`target_resolved === false`) should render with a muted style so users
know the link works but the target isn't in the archive yet.

## Consumer: RAC encoding (autorac, rac-compile)

When encoding a statute or regulation, call `get_references` for that
rule's `citation_path` and use the outgoing refs as the candidate
`imports:` list.

The RAC import format is:

```yaml
imports:
  - us/statute/42/9902/2#poverty_guidelines
```

— i.e. `{citation_path}#{variable_name}`. `target_citation_path` gives
you the left side verbatim; the `{variable_name}` has to come from the
target's own encoding.

Two workflows:

**1. Target is already encoded.** Query the target's `.rac` file or a
future `arch.rule_encodings` table that maps
`citation_path → exported variable names`. Pick the name that matches
the statutory context. autorac's encoder prompt can include this as
part of the input:

```
Candidate imports (outgoing citations from this rule):
  - us/statute/42/9902/2  (Community Services Block Grant Act § 673(2))
    → exports: poverty_guidelines, family_size_adjustment
  - us/statute/26/32      (Earned income)
    → exports: earned_income_credit, qualifying_child
```

**2. Target is not yet encoded.** Emit the import without a fragment
and flag the rule as needing a dependency. When the dependency is
encoded, a resolver pass fills in the variable name.

## Re-running the extractor

The extractor owns its rows per source rule. To re-extract (e.g. after
improving the patterns), the backfill script does:

```
DELETE FROM akn.rule_references WHERE source_rule_id IN (...);
INSERT INTO akn.rule_references ... (fresh rows);
```

per 500-rule batch. This keeps the table clean when body text or
offsets shift.

After a new CFR ingest lands, re-run the backfill to resolve any
previously-unresolved `target_rule_id`s:

```
uv run python scripts/extract_references.py
```

The unique index on `(source_rule_id, start_offset, end_offset)` makes
double-runs safe.

## Extending with new patterns

New extractor = new subclass of `atlas.citations.extractor.Extractor`,
registered in `all_extractors()`. Bias toward high-precision patterns;
`confidence` exists for the cases that can't be certain (internal refs,
act-name lookups). Low-confidence refs should ship gated — the viewer
can default-hide them and the RAC prompt can exclude them from its
candidate list.

Scope not yet covered:

| Pattern | Example | Target | Notes |
|---|---|---|---|
| internal | "subsection (a)" | same-rule | Needs enclosing-rule context |
| public_law | "Pub. L. 110-246" | no `citation_path` | Would need a Public Laws table first |
| stat | "122 Stat. 1664" | no `citation_path` | Same |
| act-name | "section 673(2) of the Community Services Block Grant Act" | maps to USC | Needs an act-name → title map |
