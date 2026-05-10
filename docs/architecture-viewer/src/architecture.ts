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

const ALL_NODE_IDS = new Set(NODES.map((n) => n.id));
const edgesAmong = (ids: Set<string>) =>
  EDGES.filter((e) => ids.has(e.from) && ids.has(e.to));

const OVERVIEW_IDS = new Set([
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
]);

function byRepoLayout() {
  const ORDER: Repo[] = [
    "external",
    "axiom-corpus",
    "infrastructure",
    "axiom-encode",
    "rules-us",
    "rules-us-state",
    "rules-non-us",
    "axiom-foundation.org",
  ];
  const COLUMN_X = 60;
  const COLUMN_GAP = 290;
  const ROW_Y = 100;
  const ROW_GAP = 130;

  const out: Array<{ id: string; x: number; y: number }> = [];
  ORDER.forEach((repoId, columnIndex) => {
    const nodesInRepo = NODES.filter((n) => n.repo === repoId);
    nodesInRepo.forEach((node, rowIndex) => {
      out.push({
        id: node.id,
        x: COLUMN_X + columnIndex * COLUMN_GAP,
        y: ROW_Y + rowIndex * ROW_GAP,
      });
    });
  });
  return out;
}

export const LAYOUTS: Layout[] = [
  {
    id: "overview",
    title: "End-to-end pipeline",
    eyebrow: "§ 01 · Overview",
    description:
      "Upstream publishers on the left, ingest in the middle, durable + serving " +
      "storage to the right, consumers at the far right.",
    nodes: [
      N("ecfr", 40, 20),
      N("usc", 40, 100),
      N("state-sources", 40, 180),
      N("canada-source", 40, 260),
      N("irs-bulk", 40, 340),
      N("fetchers", 340, 100),
      N("parsers", 340, 200),
      N("adapters", 340, 300),
      N("artifacts", 640, 200),
      N("r2", 940, 80),
      N("supabase", 940, 320),
      N("provisions", 1240, 220),
      N("navigation", 1240, 360),
      N("rules-us", 940, 520),
      N("rules-state", 940, 600),
      N("rules-other", 940, 680),
      N("axiom-foundation", 1540, 200),
      N("finbot", 1540, 340),
      N("dashboard-builder", 1540, 480),
    ],
    edges: edgesAmong(OVERVIEW_IDS),
  },
  {
    id: "ingest",
    title: "Ingest stages",
    eyebrow: "§ 02 · Ingest",
    description:
      "How a single upstream document becomes a row in corpus.provisions. " +
      "JSONL is the contract between adapter and loader.",
    nodes: [
      N("ecfr", 60, 60),
      N("canada-source", 60, 160),
      N("state-sources", 60, 260),
      N("fetchers", 360, 160),
      N("parsers", 640, 160),
      N("adapters", 920, 160),
      N("artifacts", 1200, 160),
      N("provisions", 1500, 60),
      N("r2", 1500, 260),
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
    eyebrow: "§ 03 · Storage",
    description:
      "Source of truth versus derived. Everything below corpus.provisions is " +
      "rebuildable in minutes.",
    nodes: [
      N("artifacts", 80, 220),
      N("r2", 380, 60),
      N("provisions", 380, 260),
      N("navigation", 680, 80),
      N("counts", 680, 220),
      N("references", 680, 360),
      N("rules-us", 380, 500),
      N("rules-state", 680, 500),
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
    title: "Repository boundaries",
    eyebrow: "§ 04 · Boundaries",
    description:
      "Each block lives in a specific repo. Hard rules about who writes what: " +
      "apps never write to corpus; the encoder never writes provisions; rules-* " +
      "are observed, never authoritative for legal text.",
    nodes: [
      N("state-sources", 60, 220),
      N("adapters", 380, 220),
      N("provisions", 700, 120),
      N("navigation", 700, 320),
      N("axiom-encode", 700, 520),
      N("rules-us", 1020, 400),
      N("rules-state", 1020, 520),
      N("rules-other", 1020, 640),
      N("axiom-foundation", 1340, 120),
      N("finbot", 1340, 240),
      N("dashboard-builder", 1340, 360),
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
  {
    id: "by-repo",
    title: "By repository",
    eyebrow: "§ 05 · Repos",
    description:
      "Every component grouped by the repo that owns it. Tells you exactly where " +
      "to make a change.",
    nodes: byRepoLayout(),
    edges: edgesAmong(ALL_NODE_IDS),
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
