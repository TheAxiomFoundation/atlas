// Source-of-truth data model for the architecture viewer.
//
// Each scene picks a subset of these nodes and edges and applies a layout.
// Adding a new component? Add it once here, then opt it into the scenes
// that should show it. Avoids duplicating descriptions across views.

export type Layer =
  | "upstream"
  | "ingest"
  | "storage-cold"
  | "storage-hot"
  | "rules"
  | "consumer";

export interface NodeSpec {
  id: string;
  label: string;
  layer: Layer;
  summary: string;
  detail: string;
  // Optional path inside the repo so users can jump to source.
  source?: string;
}

export interface EdgeSpec {
  from: string;
  to: string;
  label?: string;
  // "solid" = source-of-truth data flow; "derived" = computed downstream;
  // "read" = downstream consumer (no writes).
  kind: "solid" | "derived" | "read";
}

export const NODES: NodeSpec[] = [
  // Upstream
  {
    id: "ecfr",
    label: "eCFR",
    layer: "upstream",
    summary: "Federal regulations (CFR)",
    detail: "ecfr.gov publishes the Code of Federal Regulations as XML. Ingested by extract-ecfr.",
  },
  {
    id: "usc",
    label: "USC (USLM)",
    layer: "upstream",
    summary: "US Code in USLM XML",
    detail: "uscode.house.gov publishes the US Code as USLM XML. Ingested by extract-usc.",
  },
  {
    id: "state-sources",
    label: "State publishers",
    layer: "upstream",
    summary: "State legislature / agency sites",
    detail:
      "Per-state HTML, PDF, ZIP downloads. Each state adapter (delaware.py, indiana.py, " +
      "montana.py, …) knows its own source.",
  },
  {
    id: "canada-source",
    label: "laws-lois.justice.gc.ca",
    layer: "upstream",
    summary: "Canadian federal acts (LIMS XML)",
    detail: "Department of Justice publishes consolidated Acts as LIMS XML. Ingested by extract-canada-acts.",
  },
  {
    id: "irs-bulk",
    label: "IRS bulk",
    layer: "upstream",
    summary: "IRS revenue procedures / rulings",
    detail: "Bulk IRS guidance from irs.gov. Ingested via the IRS bulk fetcher path.",
  },

  // Ingest
  {
    id: "fetchers",
    label: "Fetchers",
    layer: "ingest",
    summary: "HTTP download",
    detail:
      "Thin clients with rate-limiting / retries. Return raw bytes only — no parsing. " +
      "One module per source family.",
    source: "src/axiom_corpus/fetchers/",
  },
  {
    id: "parsers",
    label: "Parsers",
    layer: "ingest",
    summary: "Bytes → typed domain models",
    detail:
      "Format-bound (USLM, LIMS, eCFR XML, state HTML). Produce typed Pydantic models " +
      "like CanadaSection or IndianaCodeProvision.",
    source: "src/axiom_corpus/parsers/",
  },
  {
    id: "adapters",
    label: "Source-first adapters",
    layer: "ingest",
    summary: "Typed models → ProvisionRecord + JSONL",
    detail:
      "Project parser output into the canonical corpus shape (ProvisionRecord with " +
      "citation_path), write sources / inventory / provisions / coverage artifacts via " +
      "CorpusArtifactStore.",
    source: "src/axiom_corpus/corpus/",
  },
  {
    id: "artifacts",
    label: "data/corpus/",
    layer: "ingest",
    summary: "Local JSONL artifact store",
    detail:
      "sources/{jur}/{doc}/{run_id}/...    raw bytes\n" +
      "inventory/{jur}/{doc}/{run_id}.json  expected citations\n" +
      "provisions/{jur}/{doc}/{run_id}.jsonl normalized rows\n" +
      "coverage/{jur}/{doc}/{run_id}.json   inventory ↔ provisions diff",
  },

  // Cold storage
  {
    id: "r2",
    label: "R2 bucket\naxiom-corpus",
    layer: "storage-cold",
    summary: "Durable provenance store",
    detail:
      "Mirror of data/corpus/. Same key layout. Holds raw upstream bytes for forensic " +
      "replay and to keep Supabase rebuildable.",
  },

  // Hot storage (Supabase)
  {
    id: "supabase",
    label: "Supabase\ncorpus.*",
    layer: "storage-hot",
    summary: "Live serving database",
    detail: "Postgres + PostgREST. App-facing via Accept-Profile: corpus header.",
  },
  {
    id: "provisions",
    label: "corpus.provisions",
    layer: "storage-hot",
    summary: "Source of truth for legal text",
    detail:
      "Normalized rows from JSONL. Indexed by citation_path. Holds body text, " +
      "metadata, deterministic UUID5 ids.",
  },
  {
    id: "navigation",
    label: "corpus.navigation_nodes",
    layer: "storage-hot",
    summary: "Derived tree-navigation index",
    detail:
      "Precomputed parent/child rows rebuilt from corpus.provisions. Replaces the " +
      "app's prefix-LIKE scans against provisions. Carries has_rulespec and " +
      "encoded_descendant_count for encoded-only browsing.",
  },
  {
    id: "counts",
    label: "corpus.provision_counts",
    layer: "storage-hot",
    summary: "Materialized view",
    detail: "Per-(jurisdiction, doc_type) totals. Refreshed via RPC at load time.",
  },
  {
    id: "references",
    label: "corpus.provision_references",
    layer: "storage-hot",
    summary: "Cross-reference graph",
    detail: "Inter-provision citations extracted by extract-references.",
  },

  // Rules
  {
    id: "rules-us",
    label: "rules-us",
    layer: "rules",
    summary: "US federal RuleSpec YAML",
    detail:
      "External git repo at TheAxiomFoundation/rules-us. YAML encodings of US federal " +
      "statutes / regulations / policies. Local checkouts drive has_rulespec at nav " +
      "rebuild time.",
  },
  {
    id: "rules-state",
    label: "rules-us-co, rules-us-{*}",
    layer: "rules",
    summary: "Per-state RuleSpec YAML",
    detail: "Per-state encoded benefit rules. Read by the navigation builder.",
  },
  {
    id: "rules-other",
    label: "rules-uk, rules-ca",
    layer: "rules",
    summary: "Non-US RuleSpec",
    detail: "UK and Canadian encoded rules. Same convention as us repos.",
  },

  // Consumers
  {
    id: "axiom-foundation",
    label: "axiom-foundation.org",
    layer: "consumer",
    summary: "Main web app",
    detail:
      "Public-facing browser of the corpus. Reads corpus.navigation_nodes for tree " +
      "navigation; reads corpus.provisions for body text.",
  },
  {
    id: "finbot",
    label: "finbot",
    layer: "consumer",
    summary: "Financial advice demo",
    detail: "Demo that calls Supabase + RuleSpec to answer benefit / tax questions.",
  },
  {
    id: "dashboard-builder",
    label: "dashboard-builder",
    layer: "consumer",
    summary: "Dashboard demo",
    detail: "Demo for assembling policy dashboards on top of the corpus.",
  },
  {
    id: "axiom-encode",
    label: "axiom-encode",
    layer: "consumer",
    summary: "Encoder pipeline (writes rules-*)",
    detail:
      "Reads corpus.provisions to know what to encode against, then writes RuleSpec " +
      "YAML into the rules-* repos. Loosely coupled to corpus via citation paths only.",
  },
];

export const EDGES: EdgeSpec[] = [
  // Upstream → fetchers
  { from: "ecfr", to: "fetchers", kind: "solid" },
  { from: "usc", to: "fetchers", kind: "solid" },
  { from: "state-sources", to: "fetchers", kind: "solid" },
  { from: "canada-source", to: "fetchers", kind: "solid" },
  { from: "irs-bulk", to: "fetchers", kind: "solid" },

  // Ingest chain
  { from: "fetchers", to: "parsers", kind: "solid", label: "bytes" },
  { from: "parsers", to: "adapters", kind: "solid", label: "typed models" },
  { from: "adapters", to: "artifacts", kind: "solid", label: "JSONL" },

  // Publish
  { from: "artifacts", to: "r2", kind: "solid", label: "sync-r2" },
  { from: "artifacts", to: "provisions", kind: "solid", label: "load-supabase" },

  // Derivations inside Supabase
  { from: "provisions", to: "navigation", kind: "derived", label: "build-nav-index" },
  { from: "provisions", to: "counts", kind: "derived", label: "RPC refresh" },
  { from: "provisions", to: "references", kind: "derived", label: "extract-references" },

  // Rules influence nav
  { from: "rules-us", to: "navigation", kind: "derived", label: "has_rulespec" },
  { from: "rules-state", to: "navigation", kind: "derived", label: "has_rulespec" },
  { from: "rules-other", to: "navigation", kind: "derived", label: "has_rulespec" },

  // axiom-encode reads provisions and writes rules
  { from: "provisions", to: "axiom-encode", kind: "read" },
  { from: "axiom-encode", to: "rules-us", kind: "solid", label: "writes YAML" },
  { from: "axiom-encode", to: "rules-state", kind: "solid", label: "writes YAML" },
  { from: "axiom-encode", to: "rules-other", kind: "solid", label: "writes YAML" },

  // Apps read from Supabase
  { from: "navigation", to: "axiom-foundation", kind: "read", label: "REST" },
  { from: "provisions", to: "axiom-foundation", kind: "read", label: "REST" },
  { from: "provisions", to: "finbot", kind: "read", label: "REST" },
  { from: "provisions", to: "dashboard-builder", kind: "read", label: "REST" },
];

// Layouts: one entry per "scene"; each maps node ids to (x, y) and which
// subset of nodes to render. Keeping layout data here keeps the View
// components dumb.

export type Layout = {
  id: string;
  title: string;
  description: string;
  nodes: Array<{ id: string; x: number; y: number }>;
  edges: EdgeSpec[];
};

const N = (id: string, x: number, y: number) => ({ id, x, y });

export const LAYOUTS: Layout[] = [
  {
    id: "overview",
    title: "Overview",
    description:
      "The end-to-end pipeline from upstream sources through R2 and Supabase to consumers.",
    nodes: [
      N("ecfr", 40, 20),
      N("usc", 40, 100),
      N("state-sources", 40, 180),
      N("canada-source", 40, 260),
      N("irs-bulk", 40, 340),
      N("fetchers", 320, 100),
      N("parsers", 320, 200),
      N("adapters", 320, 300),
      N("artifacts", 600, 200),
      N("r2", 880, 80),
      N("supabase", 880, 280),
      N("provisions", 1160, 200),
      N("navigation", 1160, 320),
      N("rules-us", 880, 480),
      N("rules-state", 880, 560),
      N("rules-other", 880, 640),
      N("axiom-foundation", 1440, 200),
      N("finbot", 1440, 320),
      N("dashboard-builder", 1440, 440),
    ],
    edges: EDGES.filter((e) =>
      [
        "ecfr",
        "usc",
        "state-sources",
        "canada-source",
        "irs-bulk",
        "fetchers",
        "parsers",
        "adapters",
        "artifacts",
        "r2",
        "supabase",
        "provisions",
        "navigation",
        "rules-us",
        "rules-state",
        "rules-other",
        "axiom-foundation",
        "finbot",
        "dashboard-builder",
      ].includes(e.from) &&
      [
        "ecfr",
        "usc",
        "state-sources",
        "canada-source",
        "irs-bulk",
        "fetchers",
        "parsers",
        "adapters",
        "artifacts",
        "r2",
        "supabase",
        "provisions",
        "navigation",
        "rules-us",
        "rules-state",
        "rules-other",
        "axiom-foundation",
        "finbot",
        "dashboard-builder",
      ].includes(e.to),
    ),
  },
  {
    id: "ingest",
    title: "Ingest stages",
    description:
      "How a single upstream document becomes a row in corpus.provisions. Five linear stages, " +
      "JSONL as the contract between adapter and loader.",
    nodes: [
      N("ecfr", 60, 60),
      N("canada-source", 60, 160),
      N("state-sources", 60, 260),
      N("fetchers", 340, 160),
      N("parsers", 600, 160),
      N("adapters", 860, 160),
      N("artifacts", 1140, 160),
      N("provisions", 1420, 60),
      N("r2", 1420, 260),
    ],
    edges: [
      { from: "ecfr", to: "fetchers", kind: "solid" },
      { from: "canada-source", to: "fetchers", kind: "solid" },
      { from: "state-sources", to: "fetchers", kind: "solid" },
      { from: "fetchers", to: "parsers", kind: "solid", label: "bytes" },
      { from: "parsers", to: "adapters", kind: "solid", label: "typed models" },
      { from: "adapters", to: "artifacts", kind: "solid", label: "JSONL" },
      { from: "artifacts", to: "provisions", kind: "solid", label: "load-supabase" },
      { from: "artifacts", to: "r2", kind: "solid", label: "sync-r2" },
    ],
  },
  {
    id: "storage",
    title: "Storage layers",
    description:
      "Which surface is source of truth and which are derived. Everything downstream of " +
      "corpus.provisions is rebuildable.",
    nodes: [
      N("artifacts", 60, 200),
      N("r2", 360, 60),
      N("provisions", 360, 240),
      N("navigation", 660, 80),
      N("counts", 660, 200),
      N("references", 660, 320),
      N("rules-us", 360, 480),
      N("rules-state", 660, 480),
    ],
    edges: [
      { from: "artifacts", to: "r2", kind: "solid", label: "sync-r2" },
      { from: "artifacts", to: "provisions", kind: "solid", label: "load-supabase" },
      { from: "provisions", to: "navigation", kind: "derived", label: "build-nav-index" },
      { from: "provisions", to: "counts", kind: "derived", label: "RPC refresh" },
      { from: "provisions", to: "references", kind: "derived", label: "extract-references" },
      { from: "rules-us", to: "navigation", kind: "derived", label: "has_rulespec" },
      { from: "rules-state", to: "navigation", kind: "derived", label: "has_rulespec" },
    ],
  },
  {
    id: "boundaries",
    title: "Repo boundaries",
    description:
      "Three independent code repos plus the rules-* family. Hard rules about who can " +
      "write what — apps never write to corpus; encoder never writes provisions; rules-* " +
      "are observed, never authoritative for legal text.",
    nodes: [
      N("state-sources", 60, 180),
      N("adapters", 360, 180),
      N("provisions", 660, 80),
      N("navigation", 660, 280),
      N("axiom-encode", 660, 480),
      N("rules-us", 960, 360),
      N("rules-state", 960, 480),
      N("rules-other", 960, 600),
      N("axiom-foundation", 1260, 80),
      N("finbot", 1260, 200),
      N("dashboard-builder", 1260, 320),
    ],
    edges: [
      { from: "state-sources", to: "adapters", kind: "solid", label: "ingest" },
      { from: "adapters", to: "provisions", kind: "solid", label: "load" },
      { from: "provisions", to: "navigation", kind: "derived" },
      { from: "provisions", to: "axiom-encode", kind: "read", label: "reads text" },
      { from: "axiom-encode", to: "rules-us", kind: "solid", label: "writes YAML" },
      { from: "axiom-encode", to: "rules-state", kind: "solid", label: "writes YAML" },
      { from: "axiom-encode", to: "rules-other", kind: "solid", label: "writes YAML" },
      { from: "rules-us", to: "navigation", kind: "derived", label: "has_rulespec" },
      { from: "rules-state", to: "navigation", kind: "derived", label: "has_rulespec" },
      { from: "rules-other", to: "navigation", kind: "derived", label: "has_rulespec" },
      { from: "navigation", to: "axiom-foundation", kind: "read", label: "REST" },
      { from: "provisions", to: "axiom-foundation", kind: "read" },
      { from: "provisions", to: "finbot", kind: "read" },
      { from: "provisions", to: "dashboard-builder", kind: "read" },
    ],
  },
];
