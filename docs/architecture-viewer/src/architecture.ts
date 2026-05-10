// Source-of-truth data model for the architecture viewer.
//
// One `NodeSpec` per component, one `EdgeSpec` per relationship.
// `repo` is load-bearing — every node lives somewhere, and the
// "By repository" scene partitions by it. Adding a new component is
// a single entry here; UI / panels / layouts all derive from this file.

export type Layer =
  | "upstream"
  | "ingest"
  | "storage-cold"
  | "storage-hot"
  | "rules"
  | "consumer";

export type Repo =
  | "axiom-corpus"
  | "axiom-encode"
  | "axiom-foundation.org"
  | "rules-us"
  | "rules-us-state"
  | "rules-non-us"
  | "infrastructure"
  | "external";

export interface RepoSpec {
  id: Repo;
  label: string;
  description: string;
}

export const REPOS: RepoSpec[] = [
  {
    id: "axiom-corpus",
    label: "axiom-corpus",
    description: "Source-document ingestion, JSONL artifacts, Supabase loads. This repo.",
  },
  {
    id: "axiom-encode",
    label: "axiom-encode",
    description: "Encoder pipeline. Reads corpus, writes RuleSpec YAML.",
  },
  {
    id: "axiom-foundation.org",
    label: "axiom-foundation.org",
    description: "Public-facing web app. Read-only consumer of the corpus.",
  },
  {
    id: "rules-us",
    label: "rules-us",
    description: "US federal RuleSpec YAML encodings.",
  },
  {
    id: "rules-us-state",
    label: "rules-us-{*}",
    description: "Per-state RuleSpec repos (rules-us-co, rules-us-tx, …).",
  },
  {
    id: "rules-non-us",
    label: "rules-uk · rules-ca",
    description: "Non-US RuleSpec repos.",
  },
  {
    id: "infrastructure",
    label: "Managed infrastructure",
    description: "Cloudflare R2 bucket and Supabase project — not source code.",
  },
  {
    id: "external",
    label: "External publishers",
    description: "Government sources outside our control. We snapshot; we don't change.",
  },
];

export interface NodeSpec {
  id: string;
  label: string;
  layer: Layer;
  repo: Repo;
  summary: string;
  detail: string;
  source?: string;
}

export interface EdgeSpec {
  from: string;
  to: string;
  label?: string;
  kind: "solid" | "derived" | "read";
}

export const NODES: NodeSpec[] = [
  // ── Upstream ──────────────────────────────────────────────────────
  {
    id: "ecfr",
    label: "eCFR",
    layer: "upstream",
    repo: "external",
    summary: "Federal regulations (CFR) in XML",
    detail:
      "ecfr.gov publishes the Code of Federal Regulations as XML. Ingested by " +
      "extract-ecfr.",
  },
  {
    id: "usc",
    label: "USC (USLM)",
    layer: "upstream",
    repo: "external",
    summary: "US Code in USLM XML",
    detail:
      "uscode.house.gov publishes the US Code as USLM XML. Ingested by extract-usc.",
  },
  {
    id: "state-sources",
    label: "State publishers",
    layer: "upstream",
    repo: "external",
    summary: "State legislature / agency sites",
    detail:
      "Per-state HTML, PDF, ZIP downloads. Each state adapter (delaware.py, " +
      "indiana.py, montana.py, …) knows its own source.",
  },
  {
    id: "canada-source",
    label: "laws-lois.justice.gc.ca",
    layer: "upstream",
    repo: "external",
    summary: "Canadian federal acts (LIMS XML)",
    detail:
      "Department of Justice publishes consolidated Acts as LIMS XML. Ingested " +
      "by extract-canada-acts.",
  },
  {
    id: "irs-bulk",
    label: "IRS bulk",
    layer: "upstream",
    repo: "external",
    summary: "Revenue procedures / rulings",
    detail: "Bulk IRS guidance from irs.gov. Ingested via the IRS bulk fetcher path.",
  },

  // ── Ingest ────────────────────────────────────────────────────────
  {
    id: "fetchers",
    label: "Fetchers",
    layer: "ingest",
    repo: "axiom-corpus",
    summary: "HTTP download, rate-limited",
    detail:
      "Thin clients that fetch raw upstream bytes. No parsing, no storage. One " +
      "module per source family.",
    source: "src/axiom_corpus/fetchers/",
  },
  {
    id: "parsers",
    label: "Parsers",
    layer: "ingest",
    repo: "axiom-corpus",
    summary: "Bytes → typed domain models",
    detail:
      "Format-bound (USLM, LIMS, eCFR XML, state HTML). Produce typed Pydantic " +
      "models — CanadaSection, IndianaCodeProvision, etc.",
    source: "src/axiom_corpus/parsers/",
  },
  {
    id: "adapters",
    label: "Source-first adapters",
    layer: "ingest",
    repo: "axiom-corpus",
    summary: "Typed models → ProvisionRecord + JSONL",
    detail:
      "Project parser output into ProvisionRecord (with canonical citation_path). " +
      "Write sources / inventory / provisions / coverage artifacts via " +
      "CorpusArtifactStore.",
    source: "src/axiom_corpus/corpus/",
  },
  {
    id: "artifacts",
    label: "data/corpus/",
    layer: "ingest",
    repo: "axiom-corpus",
    summary: "Local JSONL artifact tree",
    detail:
      "sources/{jur}/{doc}/{run}/  raw upstream bytes\n" +
      "inventory/{jur}/{doc}/{run}.json   expected citations\n" +
      "provisions/{jur}/{doc}/{run}.jsonl  normalized rows\n" +
      "coverage/{jur}/{doc}/{run}.json  inventory ↔ provisions diff",
  },

  // ── Cold storage ──────────────────────────────────────────────────
  {
    id: "r2",
    label: "R2 bucket",
    layer: "storage-cold",
    repo: "infrastructure",
    summary: "Durable provenance store",
    detail:
      "Cloudflare R2 bucket 'axiom-corpus'. Mirror of data/corpus/, same key " +
      "layout. Forensic replay and audit surface.",
  },

  // ── Hot storage (Supabase) ────────────────────────────────────────
  {
    id: "supabase",
    label: "Supabase",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Postgres + PostgREST",
    detail:
      "Live serving database. App reads via REST with Accept-Profile: corpus. " +
      "Schemas: corpus, encodings, telemetry, app.",
  },
  {
    id: "provisions",
    label: "corpus.provisions",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Source of truth for legal text",
    detail:
      "Normalized rows from JSONL. Indexed by citation_path. Holds body text, " +
      "metadata, deterministic UUID5 ids.",
  },
  {
    id: "navigation",
    label: "corpus.navigation_nodes",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Derived tree-navigation index",
    detail:
      "Precomputed parent/child rows rebuilt from corpus.provisions. Replaces " +
      "prefix-LIKE scans. Carries has_rulespec and encoded_descendant_count " +
      "for encoded-only browsing.",
  },
  {
    id: "counts",
    label: "corpus.provision_counts",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Materialized view",
    detail:
      "Per-(jurisdiction, doc_type) totals. Refreshed via RPC at load time.",
  },
  {
    id: "references",
    label: "corpus.provision_references",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Cross-reference graph",
    detail: "Inter-provision citations extracted by extract-references.",
  },

  // ── Rules repos ───────────────────────────────────────────────────
  {
    id: "rules-us",
    label: "rules-us",
    layer: "rules",
    repo: "rules-us",
    summary: "US federal RuleSpec YAML",
    detail:
      "Per-provision YAML encodings of US federal statutes / regulations / " +
      "policies. Local checkout drives has_rulespec at nav rebuild time.",
  },
  {
    id: "rules-state",
    label: "rules-us-{state}",
    layer: "rules",
    repo: "rules-us-state",
    summary: "Per-state RuleSpec",
    detail:
      "rules-us-co, rules-us-tx, … One repo per state. Same convention as " +
      "rules-us.",
  },
  {
    id: "rules-other",
    label: "rules-uk · rules-ca",
    layer: "rules",
    repo: "rules-non-us",
    summary: "Non-US RuleSpec",
    detail: "UK and Canadian encoded rules. Same convention as US repos.",
  },

  // ── Consumers ─────────────────────────────────────────────────────
  {
    id: "axiom-foundation",
    label: "axiom-foundation.org",
    layer: "consumer",
    repo: "axiom-foundation.org",
    summary: "Main web app",
    detail:
      "Public-facing browser of the corpus. Reads corpus.navigation_nodes for " +
      "tree navigation; reads corpus.provisions for body text.",
  },
  {
    id: "finbot",
    label: "finbot",
    layer: "consumer",
    repo: "axiom-foundation.org",
    summary: "Financial advice demo",
    detail:
      "Demo that calls Supabase + RuleSpec to answer benefit / tax questions.",
  },
  {
    id: "dashboard-builder",
    label: "dashboard-builder",
    layer: "consumer",
    repo: "axiom-foundation.org",
    summary: "Dashboard demo",
    detail: "Demo for assembling policy dashboards on top of the corpus.",
  },
  {
    id: "axiom-encode",
    label: "axiom-encode",
    layer: "consumer",
    repo: "axiom-encode",
    summary: "Encoder pipeline",
    detail:
      "Reads corpus.provisions to know what to encode against, writes RuleSpec " +
      "YAML into the rules-* repos. Coupled to corpus only via citation paths.",
  },
];

export const EDGES: EdgeSpec[] = [
  { from: "ecfr", to: "fetchers", kind: "solid" },
  { from: "usc", to: "fetchers", kind: "solid" },
  { from: "state-sources", to: "fetchers", kind: "solid" },
  { from: "canada-source", to: "fetchers", kind: "solid" },
  { from: "irs-bulk", to: "fetchers", kind: "solid" },

  { from: "fetchers", to: "parsers", kind: "solid", label: "bytes" },
  { from: "parsers", to: "adapters", kind: "solid", label: "typed models" },
  { from: "adapters", to: "artifacts", kind: "solid", label: "JSONL" },

  { from: "artifacts", to: "r2", kind: "solid", label: "sync-r2" },
  { from: "artifacts", to: "provisions", kind: "solid", label: "load-supabase" },

  { from: "provisions", to: "navigation", kind: "derived", label: "build-nav-index" },
  { from: "provisions", to: "counts", kind: "derived", label: "RPC refresh" },
  { from: "provisions", to: "references", kind: "derived", label: "extract-references" },

  { from: "rules-us", to: "navigation", kind: "derived", label: "has_rulespec" },
  { from: "rules-state", to: "navigation", kind: "derived", label: "has_rulespec" },
  { from: "rules-other", to: "navigation", kind: "derived", label: "has_rulespec" },

  { from: "provisions", to: "axiom-encode", kind: "read" },
  { from: "axiom-encode", to: "rules-us", kind: "solid", label: "writes YAML" },
  { from: "axiom-encode", to: "rules-state", kind: "solid", label: "writes YAML" },
  { from: "axiom-encode", to: "rules-other", kind: "solid", label: "writes YAML" },

  { from: "navigation", to: "axiom-foundation", kind: "read", label: "REST" },
  { from: "provisions", to: "axiom-foundation", kind: "read", label: "REST" },
  { from: "provisions", to: "finbot", kind: "read", label: "REST" },
  { from: "provisions", to: "dashboard-builder", kind: "read", label: "REST" },
];

export type Layout = {
  id: string;
  title: string;
  eyebrow: string;
  description: string;
  nodes: Array<{ id: string; x: number; y: number }>;
  edges: EdgeSpec[];
};

const N = (id: string, x: number, y: number) => ({ id, x, y });

const edgesAmong = (ids: Set<string>) =>
  EDGES.filter((e) => ids.has(e.from) && ids.has(e.to));

// Sequential story arc. Each scene builds on the previous: start with the
// upstream sources, walk through ingest, then storage, then encoding, then
// the full pipeline including consumers. Repo identity stays present on
// every card eyebrow throughout, so there's no need for a separate
// "by repo" view.
export const LAYOUTS: Layout[] = [
  // ═══════════════════════════════════════════════════════════════
  // § 01 — where the corpus begins. Just the upstream publishers.
  // ═══════════════════════════════════════════════════════════════
  {
    id: "sources",
    title: "Where the corpus begins",
    eyebrow: "§ 01 · Sources",
    description:
      "Five categories of official publishers. We snapshot them — never change " +
      "what we ingest. Hover any source to see what we pull from it.",
    nodes: [
      N("ecfr", 80, 80),
      N("usc", 80, 240),
      N("state-sources", 80, 400),
      N("canada-source", 80, 560),
      N("irs-bulk", 80, 720),
    ],
    edges: [],
  },

  // ═══════════════════════════════════════════════════════════════
  // § 02 — bytes become structured data. Adds ingest layer + JSONL.
  // ═══════════════════════════════════════════════════════════════
  {
    id: "ingest",
    title: "From bytes to JSONL",
    eyebrow: "§ 02 · Ingest",
    description:
      "Each upstream source flows through a fetcher, a parser, and a " +
      "source-first adapter. The adapter projects parser output into " +
      "ProvisionRecord rows and writes them to a local JSONL artifact tree — " +
      "the contract that every downstream stage reads from.",
    nodes: [
      N("ecfr", 60, 80),
      N("canada-source", 60, 240),
      N("state-sources", 60, 400),
      N("fetchers", 460, 240),
      N("parsers", 800, 240),
      N("adapters", 1140, 240),
      N("artifacts", 1480, 240),
    ],
    edges: edgesAmong(
      new Set([
        "ecfr",
        "canada-source",
        "state-sources",
        "fetchers",
        "parsers",
        "adapters",
        "artifacts",
      ]),
    ),
  },

  // ═══════════════════════════════════════════════════════════════
  // § 03 — where the JSONL lands. Cold storage (R2) + live Supabase
  // and its derived tables.
  // ═══════════════════════════════════════════════════════════════
  {
    id: "storage",
    title: "Where it lands",
    eyebrow: "§ 03 · Storage",
    description:
      "The same JSONL produces a durable R2 mirror and a live Supabase " +
      "snapshot. corpus.provisions is the source of truth for legal text; " +
      "corpus.navigation_nodes, provision_counts, and references are derived " +
      "and rebuildable in minutes.",
    nodes: [
      N("artifacts", 80, 320),
      N("r2", 500, 100),
      N("provisions", 500, 380),
      N("navigation", 920, 100),
      N("counts", 920, 280),
      N("references", 920, 460),
    ],
    edges: [
      { from: "artifacts", to: "r2", kind: "solid", label: "sync-r2" },
      { from: "artifacts", to: "provisions", kind: "solid", label: "load-supabase" },
      { from: "provisions", to: "navigation", kind: "derived", label: "build-nav-index" },
      { from: "provisions", to: "counts", kind: "derived", label: "RPC refresh" },
      { from: "provisions", to: "references", kind: "derived", label: "extract-references" },
    ],
  },

  // ═══════════════════════════════════════════════════════════════
  // § 04 — how rules emerge from law. The encoder, the rules-*
  // repos, and the has_rulespec loop back into navigation.
  // ═══════════════════════════════════════════════════════════════
  {
    id: "encoding",
    title: "Law becomes rules",
    eyebrow: "§ 04 · Encoding",
    description:
      "axiom-encode reads the corpus to know what to encode against, writes " +
      "RuleSpec YAML into rules-* repos, and the next navigation rebuild " +
      "observes that coverage via has_rulespec — closing the loop.",
    nodes: [
      N("provisions", 80, 200),
      N("axiom-encode", 480, 200),
      N("rules-us", 880, 60),
      N("rules-state", 880, 220),
      N("rules-other", 880, 380),
      N("navigation", 1280, 220),
    ],
    edges: [
      { from: "provisions", to: "axiom-encode", kind: "read", label: "reads text" },
      { from: "axiom-encode", to: "rules-us", kind: "solid", label: "writes YAML" },
      { from: "axiom-encode", to: "rules-state", kind: "solid", label: "writes YAML" },
      { from: "axiom-encode", to: "rules-other", kind: "solid", label: "writes YAML" },
      { from: "rules-us", to: "navigation", kind: "derived", label: "has_rulespec" },
      { from: "rules-state", to: "navigation", kind: "derived", label: "has_rulespec" },
      { from: "rules-other", to: "navigation", kind: "derived", label: "has_rulespec" },
      { from: "provisions", to: "navigation", kind: "derived", label: "build-nav-index" },
    ],
  },

  // ═══════════════════════════════════════════════════════════════
  // § 05 — everything assembled, end to end. The card eyebrows
  // make every repo boundary visible at a glance.
  // ═══════════════════════════════════════════════════════════════
  {
    id: "end-to-end",
    title: "End to end",
    eyebrow: "§ 05 · Pipeline",
    description:
      "All pieces in one frame. Upstream publishers, ingest, storage, encoder, " +
      "rules, and the read-only consumers. The repo eyebrow on every card " +
      "tells you exactly who owns what.",
    nodes: [
      // Upstream
      N("ecfr", 40, 40),
      N("usc", 40, 200),
      N("state-sources", 40, 360),
      N("canada-source", 40, 520),
      N("irs-bulk", 40, 680),
      // Ingest
      N("fetchers", 460, 200),
      N("parsers", 460, 380),
      N("adapters", 460, 560),
      // Artifacts
      N("artifacts", 880, 380),
      // Storage stack
      N("r2", 1300, 100),
      N("provisions", 1300, 280),
      N("navigation", 1300, 460),
      // Rules below navigation
      N("rules-us", 1300, 680),
      N("rules-state", 1300, 820),
      N("rules-other", 1300, 960),
      // Encoder beside rules
      N("axiom-encode", 880, 820),
      // Consumers
      N("axiom-foundation", 1720, 280),
      N("finbot", 1720, 460),
      N("dashboard-builder", 1720, 640),
    ],
    edges: edgesAmong(
      new Set([
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
        "provisions",
        "navigation",
        "rules-us",
        "rules-state",
        "rules-other",
        "axiom-encode",
        "axiom-foundation",
        "finbot",
        "dashboard-builder",
      ]),
    ),
  },
];

export function neighborsOf(
  nodeId: string,
  edges: EdgeSpec[],
): {
  incoming: { node: NodeSpec; edge: EdgeSpec }[];
  outgoing: { node: NodeSpec; edge: EdgeSpec }[];
} {
  const byId = new Map(NODES.map((n) => [n.id, n]));
  const incoming: { node: NodeSpec; edge: EdgeSpec }[] = [];
  const outgoing: { node: NodeSpec; edge: EdgeSpec }[] = [];
  for (const edge of edges) {
    if (edge.to === nodeId) {
      const node = byId.get(edge.from);
      if (node) incoming.push({ node, edge });
    } else if (edge.from === nodeId) {
      const node = byId.get(edge.to);
      if (node) outgoing.push({ node, edge });
    }
  }
  return { incoming, outgoing };
}
