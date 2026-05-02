# Rule references

`corpus.provision_references` is Axiom's citation graph. One row is stored
for each citation found inside a provision's body text. The table serves two
consumers:

- **Axiom app** — renders body text with clickable `<a>` tags at the recorded
  `(start_offset, end_offset)` spans.
- **RuleSpec tooling** (`axiom-encode`, `axiom-rules`) — uses outgoing
  references as candidate dependencies for a generated `imports:` block.

## Schema

```
source_provision_id     UUID  -> corpus.provisions(id)
target_citation_path    TEXT  "us/statute/42/9902/2"
target_provision_id     UUID  -> corpus.provisions(id)  NULL if target not yet ingested
citation_text           TEXT  "42 U.S.C. 9902(2)"
pattern_kind            TEXT  "usc" | "cfr" | ...
start_offset            INT
end_offset              INT
confidence              REAL  0.0-1.0 (1.0 = pattern-unambiguous)
```

`target_provision_id` is nullable because many citations resolve to
provisions not yet in the corpus. The `target_citation_path` is stored either
way, so a later ingestion can activate the link the next time the extractor
runs.

## The RPC

```
corpus.get_provision_references(citation_path_in text)
```

Returns one row per outgoing and incoming reference for the given provision,
ordered to support direct rendering:

| direction | citation_text | pattern_kind | other_citation_path | other_heading | target_resolved | start_offset | end_offset |
|---|---|---|---|---|---|---|---|
| outgoing | "42 U.S.C. 9902(2)" | usc | us/statute/42/9902/2 | Community Services Block Grant Act | true | 451 | 470 |
| outgoing | "section 32" | usc | us/statute/26/32 | Earned income | true | 820 | 830 |
| incoming | "7 U.S.C. 2014" | usc | us/regulation/7/273/9 | Income and deductions | true | 302 | 315 |

Outgoing rows carry offsets so the app can wrap the citation in an `<a>` tag
inside the body text. Incoming rows point back to the citing provision and are
rendered as a "referenced by" panel on the provision page.

## Consumer: Axiom app

Typescript:

```ts
const { data } = await supabaseCorpus.rpc("get_provision_references", {
  citation_path_in: provision.citation_path,
})

const outgoing = data.filter((r) => r.direction === "outgoing")
const incoming = data.filter((r) => r.direction === "incoming")
```

Render outgoing references by walking `provision.body` and splicing `<a>` tags
at each `(start_offset, end_offset)`, the same pattern search rendering uses
for `ts_headline`'s `<mark>` markers. Unresolved targets
(`target_resolved === false`) should render with a muted style so users know
the link works but the target is not in the corpus yet.

## Consumer: RuleSpec encoding

When encoding a statute, regulation, or policy document, call
`get_provision_references` for that provision's `citation_path` and use the
outgoing references as the candidate `imports:` list.

The RuleSpec import format is:

```yaml
imports:
  - us/statute/42/9902/2#poverty_guidelines
```

That is `{citation_path}#{variable_name}`. `target_citation_path` gives the
left side verbatim; the `{variable_name}` comes from the target's own
encoding.

Two workflows:

**1. Target is already encoded.** Query the target's RuleSpec file or the
future `encodings` metadata surface that maps
`citation_path -> exported variable names`. Pick the name that matches the
statutory context. The Encoder prompt can include this as part of the input:

```
Candidate imports (outgoing citations from this provision):
  - us/statute/42/9902/2  (Community Services Block Grant Act section 673(2))
    -> exports: poverty_guidelines, family_size_adjustment
  - us/statute/26/32      (Earned income)
    -> exports: earned_income_credit, qualifying_child
```

**2. Target is not yet encoded.** Emit the import without a fragment and flag
the provision as needing a dependency. When the dependency is encoded, a
resolver pass fills in the variable name.

## Re-running the extractor

The extractor owns its rows per source provision. To re-extract after body text
or extraction logic changes, the backfill script replaces rows per batch:

```
DELETE FROM corpus.provision_references WHERE source_provision_id IN (...);
INSERT INTO corpus.provision_references ... (fresh rows);
```

After a new ingest lands, re-run the backfill to resolve any previously
unresolved `target_provision_id`s:

```
uv run python scripts/extract_references.py
```

The unique index on `(source_provision_id, start_offset, end_offset)` makes
double-runs safe.

## Extending with new patterns

New extractor = new subclass of the citation extractor, registered in
`all_extractors()`. Bias toward high-precision patterns; `confidence` exists
for cases that cannot be certain, such as internal references and act-name
lookups. Low-confidence references should ship gated: the app can default-hide
them and the RuleSpec prompt can exclude them from its candidate list.

Scope not yet covered:

| Pattern | Example | Target | Notes |
|---|---|---|---|
| internal | "subsection (a)" | same provision | Needs enclosing-provision context |
| public_law | "Pub. L. 110-246" | no `citation_path` | Would need a Public Laws table first |
| stat | "122 Stat. 1664" | no `citation_path` | Same |
| act-name | "section 673(2) of the Community Services Block Grant Act" | maps to USC | Needs an act-name -> title map |
