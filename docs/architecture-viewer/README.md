# axiom-corpus architecture viewer

Interactive React app that renders the `axiom-corpus` pipeline as
node-and-edge diagrams. Useful for onboarding, reviews, and explaining
the system in meetings.

## Run

```bash
cd docs/architecture-viewer
npm install
npm run dev
```

Opens at <http://localhost:5179>. The dev server hot-reloads on every
edit.

## What's in here

- `src/architecture.ts` — single source of truth for nodes, edges,
  and per-scene layouts. **Add components here once**, opt them into
  scenes by id.
- `src/views/`, `src/components/` — UI shell.
- `src/App.tsx` — wires the data into a `@xyflow/react` canvas with a
  scene switcher and a detail panel.

## Add a new node

1. Append a `NodeSpec` to `NODES` in `src/architecture.ts` with a
   unique `id`, a `layer`, and short `summary` + longer `detail` text.
2. Add `EdgeSpec` entries to `EDGES` describing how it connects.
3. Reference the new id in one or more `LAYOUTS` entries to position
   it on a scene.

The visual layer styling, the detail panel, and the legend all derive
from the data file — no UI changes needed.

## Scenes

- **Overview** — the full pipeline left-to-right.
- **Ingest stages** — fetch → parse → adapt → JSONL → load.
- **Storage layers** — what's source of truth vs derived.
- **Repo boundaries** — which repo owns which surface.

## Edge styles

- **Solid black** — source-of-truth data flow (something writes data
  somewhere).
- **Dashed purple, animated** — derived (computed from upstream data;
  rebuildable).
- **Dashed pink** — read-only consumer (apps).

## Build

```bash
npm run build
```

Outputs static files to `dist/` for hosting anywhere. The app has no
runtime dependencies on a backend.
