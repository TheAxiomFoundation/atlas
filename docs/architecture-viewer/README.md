# axiom-corpus architecture viewer

Interactive React app that renders the `axiom-corpus` pipeline as
node-and-edge diagrams. Styled to match the Axiom Foundation design
system. Useful for onboarding, reviews, and explaining the system in
meetings.

## Run

```bash
cd docs/architecture-viewer
npm install
npm run dev
```

Opens at <http://localhost:5179>.

## What it shows

Five scenes switchable from the left sidebar:

1. **Overview** — full pipeline left to right.
2. **Ingest stages** — fetch → parse → adapt → JSONL → load.
3. **Storage layers** — source of truth vs derived.
4. **Repository boundaries** — who writes what; hard rules across repos.
5. **By repository** — every component grouped by the repo that owns it.

Each node card shows:

- The **repository** it lives in (mono eyebrow at the top).
- A layer indicator on the top edge (subtle accent rule).
- Title and short summary.

## Relationship view

Click any node and the canvas highlights its direct neighbors:

- **Outgoing edges** light up in accent brown; everything else fades.
- **Related nodes** stay sharp; unrelated nodes dim to 40 % opacity.
- The right-hand detail panel splits into:
  - **Receives from** — every node that writes into this one.
  - **Sends to** — every downstream consumer.
  Click any item in those lists to jump to that node.

Each relationship is tagged with its verb (`writes` / `derives` /
`reads`) so the kind of dependency is unambiguous.

## Architecture model

Everything lives in `src/architecture.ts`:

- `NODES` — one `NodeSpec` per component (id, label, layer, **repo**,
  summary, detail).
- `EDGES` — one `EdgeSpec` per relationship, kind ∈ `solid` / `derived`
  / `read`.
- `LAYOUTS` — per-scene positions and edge subsets.
- `REPOS` — repository registry.

To add a component: append a `NodeSpec`, add `EdgeSpec` entries, then
reference its id in whichever layouts should show it. UI / panels /
legends / repo grouping all derive from this data.

## Visual language

Matches the main app's "statute paper" aesthetic.

- Cream paper background, hairline borders, warm brown accent.
- Mono labels and eyebrows; sans titles with letter-spacing.
- Edge kinds:
  - **Solid black** — source-of-truth data flow.
  - **Dashed brown** — derived (rebuildable from upstream); animated
    when in focus.
  - **Dashed gray** — read-only consumer.

## Build

```bash
npm run build
```

Outputs static files to `dist/` — deploys anywhere. No backend.
