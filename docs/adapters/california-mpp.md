# California MPP §63 (CalFresh) Adapter

Author memo for whoever picks up this adapter next. Not a tutorial; a record
of decisions made, alternatives rejected, things deliberately skipped, and
debatable choices worth revisiting.

## What it does

Snapshots and normalizes the CalFresh slice of the CDSS Manual of Policies
and Procedures — Division 63, the operational manual California counties
use day-to-day for SNAP/CalFresh administration. Output lands at
`us-ca/regulation/mpp/...` in the standard corpus artifact layout.

Sources: 5 Microsoft Word `.docx` files at
`https://www.cdss.ca.gov/Portals/9/Regs/Man/Fsman/fsman0{3,4a,4b,5,6}.docx`.
Together they cover the encoding-essential subset per
[encoding playbook § 14](https://github.com/TheAxiomFoundation/axiom-architecture)
— eligibility, deductions, resources, eligibility determination, allotment
computation. ~150 pages.

## Why these 5 files, not all 14

Full MPP §63 is ~2,000 pages across 14 files. The MVP scope was chosen by
asking what an encoder actually needs to produce a working CalFresh
benefit calculator. The answer is the eligibility/income/resource/allotment
core, not the appeals process or program integrity sections.

Other files exist (`fsman01`/`fsman02` general provisions and definitions;
`fsman07`/`fsman08` continuation of §63-504 onward; `fsman09`-`fsman12`
chapters 600–1400 on issuance, work requirements, fair hearings, etc.).
They're real, they're CalFresh, and they're out of scope for v1 because the
encoder doesn't yet need them. Add a new entry to
`manifests/us-ca-cdss-mpp-calfresh.yaml` when the encoder reaches that
section.

## Design decisions made

### Decision 1: No chapter container

MPP uses the word "chapter" loosely. "Chapter 63-300" and "section §63-300"
refer to the same thing. Modeling them as separate hierarchy levels
produces a citation-path collision — both want `us-ca/regulation/mpp/63-300`.

**Decision:** drop chapters as a level. Parent sections directly to
Division 63. Hierarchy is `manual → division → section → subsection`,
four levels.

**Alternative considered:** give chapters a distinct path like
`us-ca/regulation/mpp/chap-63-300` or `us-ca/regulation/mpp/63-300-chapter`.
Rejected because nothing downstream actually needs the chapter as a
separate level — encoders cite by section number, not chapter number.

**Debate worth having:** if encoders eventually want to group sections by
chapter for navigation, we either re-introduce the level under a different
path, or compute it derivatively at query time. The "compute it at query
time" option is probably right — chapters in MPP are a documentation
convenience, not a legal authority level.

### Decision 2: Subsection regex matches by prefix only

First version used `^\.(?P<num>\d{1,4})\s+(?P<title>[A-Z][...]+?)\s*$`,
requiring title-cased words. This failed against MPP's most common
subsection pattern, which is a single paragraph whose entire content is
the rule:

```
.31 The CWD shall not deny eligibility to a household due to failure
    to cooperate when the person(s) who fails to cooperate is outside
    of the household.
```

The original regex didn't match (trailing period, parens, sentence-form
content) so `.31` was folded into `.3`'s body. Inflated `.3` from 190
chars to 3,569 chars and made 4/5 spot checks fail.

**Decision:** match `^\.\d{1,4}\s+` prefix only. Everything after the
whitespace is the heading. Whether it's "Screening" or a full sentence,
the parser doesn't care.

**Tradeoff:** false positives become possible. Any paragraph that begins
with `.N ` (a numbered list item that happens to start a paragraph, a
fragment, etc.) is treated as a subsection. In practice MPP doesn't have
those — content paragraphs almost always start with capital letters and
words, not `.N`. But it's a fragility worth knowing about.

### Decision 3: Dedup section numbers within a file

MPP DOCX files repeat the section header on every page as a running
header — `63-405 CITIZENSHIP OR ELIGIBLE NONCITIZEN STATUS` appears 8x
in a 30-page section. First version emitted a new section row for each
occurrence.

**Decision:** track section numbers as we walk paragraphs. First
occurrence is the real header. Subsequent appearances within the same
file are running headers — skip without resetting the current section's
subsection accumulator.

**Alternative considered:** look for the `(Continued)` or `(Cont.)`
marker that some running headers carry. Rejected because not all running
headers carry it; some are bare `63-405 TITLE`. Per-file dedup is more
robust.

**Debatable:** a file that contains multiple distinct sections of the
same number across different versions would break (we'd silently drop
all but the first). MPP doesn't do this in practice — but if CDSS ever
publishes an archive file containing both the current and an old
superseded version of the same section, this assumption fails.

### Decision 4: Cross-document section overflow silently filtered

§63-405 starts in `fsman04a.docx` and continues in `fsman04b.docx`. My
manifest declares each file's `sections:` explicitly. If the parser
sees §63-405 in `fsman04b.docx` (which has §63-405 declared in
`fsman04a.docx`'s manifest), we drop it silently.

**Decision:** trust the manifest's section assignment. The first
declaring file owns the section.

**Risk:** if a section's content genuinely spans two files (Word page
break mid-section), the back half of the content gets dropped, not
appended to the section row from the other file. We haven't seen this
in practice — MPP's section breaks align with file breaks — but we'd
miss it silently if it happened.

**Better long-term:** detect cross-file overflow during extraction,
warn or error. Not done in v1.

### Decision 5: Don't model parts/articles

MPP's actual legal hierarchy is `Title 22 CCR → Division 3 → Subdivision 1
→ Chapter 19 → Subchapter X → Article Y → Section`. Each section in §63
technically lives in this deeper tree.

**Decision:** flatten. Adapter only emits `manual → division → section →
subsection`. The deeper Title 22 hierarchy is documentation context, not
something the encoder operates on.

**Alternative considered:** model the full Title 22 → Division 3 → ...
tree. Rejected as over-engineering for the citation paths the encoder
actually uses. CDSS itself cites these provisions as "MPP §63-301.3,"
not "22 CCR Div 3 Subdiv 1 Chap 19 §63-301.3."

**Debatable if:** someone later wants to cross-reference CalFresh with
CalWORKs (which lives in Title 22 Chapter 18) under a shared parent.
Then a flatter model becomes inadequate. Defer until we know.

## Known gaps with concrete recommendations

### Gap 1: MR/QR variant subsections

Many MPP subsections have Monthly Reporting (MR) and Quarterly Reporting
(QR) variants that share the same number:

```
.2 Application Form and Form Deviation
   Section 63-300.2(MR) shall become inoperative and Section 63-300.2(QR)
   shall become operative in a county on the date QR is implemented.
   (MR) All applications for Food Stamp Program eligibility shall be
       made on the DFA 285-A1, DFA 285-A2, and DFA 285-A3...
   (QR) All applications for Food Stamp Program eligibility shall be
       made on the DFA 285-A1, DFA 285-A2, and DFA 285-A3 QR...
```

Currently both variants get merged into one subsection body. Encoders
relying on MR-vs-QR distinction (rare but possible for backward-compat)
can't tell them apart.

Three options, ranked by preference:

1. **Emit as `@variant` suffixes** on citation path:
   `us-ca/regulation/mpp/63-300.2@mr` and `us-ca/regulation/mpp/63-300.2@qr`.
   Precedent: rules-us-co uses `@variant` for amendment-versioned sections.
   Most consistent with existing namespace.
2. **Pick QR only** (CA fully transitioned to QR years ago). Drop MR text.
   Simpler, slight loss of historical fidelity.
3. **Status quo** — preserve in body as-is. Encoder reads the (MR)/(QR)
   markers and disambiguates at encoding time.

Recommendation: option 3 for v1 (we already do this), option 1 for v2.

### Gap 2: Tables are extracted as run-on text

MPP has critical tables: Standard Utility Allowance tiers, deduction
amounts, allotment computation steps, etc. The current parser concatenates
table cells into the surrounding paragraph. An encoder reading the body
for §63-503 won't be able to programmatically extract "FY 2026 SUA tier 1
is $X" — it'll see a soup of numbers and words.

**Recommendation:** at the parser level, detect `w:tbl` elements in the
DOCX XML (alongside `w:p` paragraphs we currently handle) and emit them
as structured `tables: [...]` metadata on the containing provision.
Encoders that need table values read `metadata.tables`; the body string
stays prose-only.

**Effort:** ~2 days for first pass. The Word DOCX table model is
hierarchical (table → row → cell → paragraph) and well-documented.

**Why we didn't do it in v1:** the encoder isn't reading MPP tables yet.
This will block real CalFresh encoding the moment someone tries to
encode §63-503 allotment computation. File a follow-up when that
happens.

### Gap 3: Deeper subsection nesting flattened

MPP nests subsections to 3 levels: `.1`, `.11`, `.111`. The current
parser treats all three as the same level (flat list under the parent
section). Reading the JSONL, you can reconstruct hierarchy by inspecting
the number — `.111` is a child of `.11` is a child of `.1` — but it's
not modeled in the row structure.

**Decision deferred:** model the deeper hierarchy as actual
`parent_citation_path` chains, or leave flat and let consumers compute
parent-of relationships from the dot pattern?

The flat model is easier and matches how MPP is actually cited (people
write "MPP §63-301.31" not "MPP §63-301.3.31"). Modeling the
hierarchy more honestly would require parsing the dot-prefix tree and
emitting `parent_citation_path: us-ca/regulation/mpp/63-301.3` on the
`.31` row.

**Recommendation:** add hierarchy when a consumer asks for it. Until
then, flat is fine.

### Gap 4: No ACL/ACIN overlay handling

MPP is amended via CDSS All County Letter (ACL) and All County
Information Notice (ACIN) letters between formal manual revisions. The
SUA tier values for FY 2026 don't live in MPP — they live in the
relevant ACL. MPP says "as established by the department."

This is handled (or should be) by a separate adapter for ACL/ACIN —
issue [#37](https://github.com/TheAxiomFoundation/axiom-corpus/issues/37).
Decoupled from this adapter on purpose: ACL/ACIN are individual PDFs,
much simpler ingestion, and they overlay onto MPP rather than belonging
to its hierarchy.

**Worth knowing:** any encoder using MPP rows alone will get the
*authority* but not the *current values*. Wire ACL/ACIN before going
to production with a CalFresh calculator.

## Local dev pattern

The MPP DOCX URLs return 870MB+ total over 5 files. For repeated runs,
cache them locally first:

```bash
mkdir -p /tmp/mpp63
cd /tmp/mpp63
for f in fsman03 fsman04a fsman04b fsman05 fsman06; do
  curl -sL -o ${f}.docx "https://www.cdss.ca.gov/Portals/9/Regs/Man/Fsman/${f}.docx"
done

axiom-corpus-ingest extract-california-mpp-calfresh \
  --base data/corpus --version 2026-05-12 \
  --manifest manifests/us-ca-cdss-mpp-calfresh.yaml \
  --download-dir /tmp/mpp63 \
  --source-as-of 2026-05-12 --expression-date 2026-05-12
```

The adapter reads from `--download-dir` if the file exists there, falls
back to HTTP otherwise. Local cache makes the extract idempotent and
fast (sub-minute on cached files vs minutes on cold fetch).

## Verification expected after any change

Per encoding playbook § 11 — don't trust a passing extract without these:

1. `coverage_complete: true` in the report (Level 1 — necessary, not sufficient)
2. Spot-check 5 subsections at different depths against the canonical
   DOCX paragraphs (Level 3 — the actually-accurate gate). The v1 first
   pass failed 4/5 on this, surfacing the regex bug — don't skip.
3. Re-extract on the same input; `sha256` of the JSONL must be byte-identical
   (Level 3.5 — guards against parser non-determinism).
4. After a regex or boundary change, expect provision count to change
   significantly. The v1 regex fix took the count from 175 to 559. Be
   ready to explain large deltas.

## Things to NOT do

- **Don't hand-edit a DOCX file to make parsing easier.** If MPP changes
  upstream, the adapter must handle it. If our parser can't handle
  current MPP shape, fix the parser.
- **Don't trust matched_count without spot-checking bodies.** matched_count
  is a citation-path set check. It says nothing about whether the text
  in those rows is correct.
- **Don't promote to production (load-supabase) before Level 3 passes.**
  Once a citation path is published in `corpus.provisions`, a future
  body correction is either a silent overwrite (no consumer notification)
  or a path rename (breaks every downstream reference). Neither is good.
- **Don't delete the existing data and re-extract under pressure.** If
  something seems off in production, query first; rebuild from JSONL
  second. The JSONL on disk is the authoritative pre-Supabase snapshot.

## Cross-references

- Architecture viewer playbook: `https://axiom-architecture-one.vercel.app` → § Encoding playbook → § 14 (CA SNAP source inventory).
- Tracking issue: [#36](https://github.com/TheAxiomFoundation/axiom-corpus/issues/36)
- Companion issues: [#37](https://github.com/TheAxiomFoundation/axiom-corpus/issues/37) (CalFresh ACL/ACIN), [#38](https://github.com/TheAxiomFoundation/axiom-corpus/issues/38) (uv.lock refresh)
- Closest existing precedent: `src/axiom_corpus/corpus/nycrr.py` (NYCRR — NY SNAP regulations).
