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
  // Optional deep-detail fields. Render only when present so trivial
  // nodes (e.g. external publishers) stay terse and important nodes
  // (ingest, storage, encoding) carry the depth a reader needs.
  mechanics?: string;
  rationale?: string;
  important?: string[];
  files?: string[];
  commands?: string[];
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
      "ecfr.gov is the National Archives' live electronic Code of Federal Regulations. " +
      "Publishes the full CFR in bulk XML, refreshed daily as agencies file rule changes.",
    mechanics:
      "extract-ecfr fetches title-level XML bundles, then walks them part-by-part to " +
      "emit one ProvisionRecord per section / paragraph. The XML preserves hierarchy " +
      "(title → chapter → part → subpart → section), and the adapter keeps that hierarchy " +
      "intact in citation_path.",
    important: [
      "Title 7 (USDA / SNAP regs) is our most-encoded slice today — ~10 of the 24 " +
        "rules-us encodings live there.",
      "Federal regulation paths drop the publication suffix: regulations/7-cfr/273/7 " +
        "in rules-us maps to us/regulation/7/273/7 in the corpus.",
    ],
    commands: ["extract-ecfr"],
  },
  {
    id: "usc",
    label: "USC (USLM)",
    layer: "upstream",
    repo: "external",
    summary: "US Code in USLM XML",
    detail:
      "uscode.house.gov publishes the US Code as USLM (United States Legislative " +
      "Markup) XML. Title-by-title bulk downloads with stable structure.",
    mechanics:
      "extract-usc consumes one USLM XML file at a time, walking <title> → <chapter> → " +
      "<section> → <subsection> elements. USLM is the gold standard among US legislative " +
      "formats — every element has a stable identifier and the hierarchy is " +
      "self-describing.",
    important: [
      "Title 26 (Internal Revenue Code) is the most policy-relevant title — most tax " +
        "RuleSpec encoding pulls from here.",
      "USLM ids round-trip cleanly into our citation_paths: /us/usc/t26/s32 becomes " +
        "us/statute/26/32.",
    ],
    commands: ["extract-usc", "inventory-usc"],
  },
  {
    id: "state-sources",
    label: "State publishers",
    layer: "upstream",
    repo: "external",
    summary: "State legislature / agency sites",
    detail:
      "Each US state publishes its statutes (and sometimes regulations and policy) " +
      "differently. Texas Legislature publishes ZIPs of HTML; Indiana ships annual " +
      "code dumps; Colorado serves agency rules through the Secretary of State's CCR " +
      "portal; some smaller states publish PDFs only.",
    mechanics:
      "Per-state adapter modules in src/axiom_corpus/corpus/state_adapters/ each know " +
      "their state's quirks: download method, HTML structure, hierarchy markers. They " +
      "all produce ProvisionRecord with the same citation_path shape " +
      "(us-{state}/{doc_type}/...).",
    rationale:
      "Concentrating state-specific knowledge in one module per state keeps the " +
      "core pipeline format-agnostic. Adding a new state is a new file, not a refactor.",
    important: [
      "Most state corpora are SHALLOW: section-level rows with no chapter or title " +
        "containers. That's a data-quality issue inherited from the upstream publisher, " +
        "not a navigation bug.",
      "About 50 states + DC are represented; coverage varies — some are statute-only, " +
        "some include regulations and policy.",
    ],
    files: ["src/axiom_corpus/corpus/state_adapters/"],
  },
  {
    id: "canada-source",
    label: "laws-lois.justice.gc.ca",
    layer: "upstream",
    repo: "external",
    summary: "Canadian federal acts (LIMS XML)",
    detail:
      "Canada's Department of Justice publishes consolidated federal Acts as LIMS XML. " +
      "~956 acts total. Bilingual (English + French) but the XML files are split per " +
      "language.",
    mechanics:
      "extract-canada-acts fetches per-act XML, parses the LIMS structure (Section → " +
      "Subsection → Paragraph → Subparagraph → Clause), and emits one ProvisionRecord " +
      "per node. Adapter inserts an act-level container row " +
      "(canada/statute/{cn}) so each act has a navigable root.",
    important: [
      "Until May 2026 the legacy ingest left citation_path=null on every Canada row. " +
        "Fixed by switching to the source-first adapter pattern and re-extracting in " +
        "place via load-supabase --replace-scope.",
      "Currently 161 of ~956 acts ingested. The rest exist as XML upstream but haven't " +
        "been pulled yet.",
      "English only for now; French content is in separate XMLs we haven't wired up.",
    ],
    files: ["src/axiom_corpus/corpus/canada.py"],
    commands: ["extract-canada-acts"],
  },
  {
    id: "irs-bulk",
    label: "IRS bulk",
    layer: "upstream",
    repo: "external",
    summary: "Revenue procedures / rulings",
    detail:
      "Internal Revenue Service publishes guidance documents (Revenue Procedures, " +
      "Revenue Rulings, Notices) as PDFs. Hundreds per year.",
    mechanics:
      "Bulk fetcher pulls PDFs, downstream tooling extracts text and section structure.",
    important: [
      "Document classification matters: 'guidance' vs 'rulemaking' is meaningful in " +
        "the corpus and affects how the app renders them.",
    ],
  },

  // ── Ingest ────────────────────────────────────────────────────────
  {
    id: "fetchers",
    label: "Fetchers",
    layer: "ingest",
    repo: "axiom-corpus",
    summary: "HTTP download, rate-limited",
    detail:
      "Thin HTTP clients that fetch raw bytes from upstream publishers. One module per " +
      "source family. Returns bytes only — no parsing, no storage.",
    mechanics:
      "Each fetcher exposes a small surface: typically a list_*() method to enumerate " +
      "available documents and a download_*() method to retrieve one. Rate limits, " +
      "retries, and authentication concerns live here.",
    rationale:
      "Isolating HTTP from parsing lets parsers stay deterministic and trivially " +
      "testable. A flaky upstream doesn't propagate into parser tests, and a parser " +
      "bug doesn't poison the fetcher cache.",
    important: [
      "The Canada fetcher uses requests (not httpx) for downloads — the httpx client " +
        "reliably hung in _ssl__SSLSocket_read on darwin when streaming >10 MB acts. " +
        "Caught in May 2026; fix is a single override on download_act().",
      "Rate limits are baked in (typically 0.5s between requests). Don't bypass — most " +
        "upstreams will block you.",
      "User-Agent strings identify us as 'Axiom/1.0 (legislation archiver; " +
        "contact@axiom-foundation.org)' — keep that intact.",
    ],
    files: ["src/axiom_corpus/fetchers/legislation_canada.py", "src/axiom_corpus/fetchers/irs_bulk.py"],
    source: "src/axiom_corpus/fetchers/",
  },
  {
    id: "parsers",
    label: "Parsers",
    layer: "ingest",
    repo: "axiom-corpus",
    summary: "Bytes → typed domain models",
    detail:
      "Each parser knows exactly one upstream format. USLM (US Code), LIMS (Canada), " +
      "eCFR XML, state-specific HTML. Output is typed Pydantic models — CanadaSection, " +
      "IndianaCodeProvision, never strings or untyped dicts.",
    mechanics:
      "Heavy use of lxml for XML, BeautifulSoup for HTML. Each parser returns model " +
      "instances with strong typing so downstream adapters can refactor without " +
      "guessing what's in a field.",
    rationale:
      "Format complexity lives here so adapters don't have to think about XML namespaces " +
      "or HTML quirks. When a format upstream changes, only one parser breaks.",
    important: [
      "Parsers must be deterministic on the same input bytes. No timestamps, no " +
        "random IDs. Re-running the parser on the same bytes always yields the same " +
        "model objects.",
      "Pydantic v2 throughout for runtime validation — catches malformed upstream data " +
        "before it reaches the adapter.",
    ],
    source: "src/axiom_corpus/parsers/",
  },
  {
    id: "adapters",
    label: "Source-first adapters",
    layer: "ingest",
    repo: "axiom-corpus",
    summary: "Typed models → ProvisionRecord + JSONL",
    detail:
      "The heart of the source-first pipeline. One adapter per jurisdiction, each " +
      "responsible for projecting parser output into the canonical ProvisionRecord " +
      "shape and writing four parallel artifact trees.",
    mechanics:
      "Adapter loops over parser output, builds canonical citation_paths, computes " +
      "deterministic UUID5 ids, and calls CorpusArtifactStore.write_*() to emit: " +
      "sources/ (raw upstream bytes), inventory/ (expected citation list), " +
      "provisions/ (normalized rows as JSONL), coverage/ (inventory ↔ provisions diff). " +
      "Same key structure mirrors to R2.",
    rationale:
      "The 'source-first' contract: JSONL on disk is the boundary between adapters and " +
      "everything downstream. Adapters can change internals freely; consumers stay " +
      "stable. Same JSONL produces both R2 mirror and Supabase rows.",
    important: [
      "Citation path is the canonical id. Format: {jurisdiction}/{doc_type}/{path_segments}. " +
        "First segment must equal jurisdiction. Becomes the input to UUID5.",
      "Deterministic UUID5 means re-runs are upserts in place. Two pipelines processing " +
        "the same source produce the same ids — no drift.",
      "Coverage report compares expected citations to actual rows. Adapter exits non-zero " +
        "if coverage is incomplete unless --allow-incomplete.",
      "Each adapter is ~300-1500 lines depending on the upstream complexity. They share " +
        "the CorpusArtifactStore + ProvisionRecord contract, not parsing logic.",
    ],
    files: [
      "src/axiom_corpus/corpus/canada.py",
      "src/axiom_corpus/corpus/colorado.py",
      "src/axiom_corpus/corpus/ecfr.py",
      "src/axiom_corpus/corpus/usc.py",
      "src/axiom_corpus/corpus/state_adapters/",
    ],
    commands: [
      "extract-ecfr",
      "extract-usc",
      "extract-canada-acts",
      "extract-state-statutes",
      "extract-indiana-code",
      "extract-colorado-ccr",
      "extract-{state}-code",
    ],
    source: "src/axiom_corpus/corpus/",
  },
  {
    id: "artifacts",
    label: "data/corpus/",
    layer: "ingest",
    repo: "axiom-corpus",
    summary: "Local JSONL artifact tree",
    detail:
      "Filesystem layout that holds the intermediate state of every extract. Four " +
      "parallel trees under data/corpus/ — sources, inventory, provisions, coverage. " +
      "Same key shape mirrors to R2.",
    mechanics:
      "sources/{jur}/{doc}/{run_id}/   raw upstream bytes (sha256 tracked)\n" +
      "inventory/{jur}/{doc}/{run_id}.json   expected citation list\n" +
      "provisions/{jur}/{doc}/{run_id}.jsonl   one ProvisionRecord per line\n" +
      "coverage/{jur}/{doc}/{run_id}.json   inventory ↔ provisions diff report",
    rationale:
      "JSONL is the contract between adapter and loader. Append-friendly, streamable, " +
      "grep-friendly, diff-friendly in git. Same line-oriented file produces both R2 " +
      "mirror (sync-r2) and Supabase rows (load-supabase).",
    important: [
      "Each line is one ProvisionRecord.to_mapping(), JSON-encoded with sort_keys=True. " +
        "Two runs on the same input produce byte-identical files — critical for " +
        "deterministic diffs.",
      "Required fields per line: jurisdiction, document_class, citation_path. " +
        "Everything else is optional and emitted only when non-null.",
      "Reader is NOT streaming today — load_provisions reads the whole file then " +
        "splits. Fine for current sizes (<300 MB) but would OOM on >1 GB inputs.",
      "UTF-8 throughout. Tolerates French ligatures, em-dashes, fancy quotes.",
    ],
  },

  // ── Cold storage ──────────────────────────────────────────────────
  {
    id: "r2",
    label: "R2 bucket",
    layer: "storage-cold",
    repo: "infrastructure",
    summary: "Durable provenance store",
    detail:
      "Cloudflare R2 bucket 'axiom-corpus'. Mirror of the local data/corpus/ tree, " +
      "same key layout. Holds raw upstream bytes and JSONL artifacts.",
    mechanics:
      "sync-r2 computes a sha256 for each local file, lists the bucket, uploads only " +
      "missing or changed files. Workers in parallel for big runs.",
    rationale:
      "Provenance / forensics. Lets you replay any historical ingest, prove what was " +
      "ingested when, and serve large assets without hitting Supabase. Pipeline can " +
      "rebuild Supabase from R2 alone.",
    important: [
      "Nothing in production serving reads from R2 today — it's audit/replay only. " +
        "If no downstream consumer ever materializes, this is dead weight worth removing.",
      "Credentials at ~/.config/axiom-foundation/r2-credentials.json.",
      "Bucket size is sub-GB today, well within R2's free tier.",
    ],
    commands: ["sync-r2", "artifact-report", "release-artifact-manifest"],
  },

  // ── Hot storage (Supabase) ────────────────────────────────────────
  {
    id: "supabase",
    label: "Supabase",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Postgres + PostgREST",
    detail:
      "Managed Postgres + PostgREST hosted by Supabase. Live serving database. " +
      "Schemas: corpus (legal text + nav), encodings (encoder run history), telemetry " +
      "(observability), app (frontend state).",
    mechanics:
      "Apps read via REST endpoints with `Accept-Profile: corpus` header to scope to " +
      "the corpus schema. Loader uses POST with `Prefer: resolution=merge-duplicates` " +
      "for idempotent upserts.",
    important: [
      "Project ref: swocpijqqahhuwtuahwc. URL: swocpijqqahhuwtuahwc.supabase.co.",
      "Service-role key needed for writes; anon key suffices for reads.",
      "RLS is enabled on every corpus table. Public SELECT, no public writes.",
    ],
  },
  {
    id: "provisions",
    label: "corpus.provisions",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Source of truth for legal text",
    detail:
      "The primary table in the corpus schema. One row per provision. ~1.75M rows " +
      "across all jurisdictions. Holds body text plus metadata.",
    mechanics:
      "Indexed by (jurisdiction, doc_type, id) for scope queries, citation_path with " +
      "text_pattern_ops for prefix scans, parent_id for tree walks, gin index on " +
      "identifiers for jsonb lookups. Loaded in chunks of 500 rows via PostgREST upsert.",
    rationale:
      "Single source of truth for legal text. Every other corpus.* surface " +
      "(navigation_nodes, provision_counts, references) is derived from this table and " +
      "rebuildable in minutes.",
    important: [
      "IDs are deterministic UUID5 of 'axiom:' + citation_path. Same path → same id, " +
        "forever. Re-runs upsert in place.",
      "Loader projects parent_id from parent_citation_path automatically — adapters " +
        "set the path, the loader sets the id.",
      "Schema is intentionally wide: 20+ columns including source_url, " +
        "source_document_id, expression_date, language, legal_identifier, identifiers " +
        "(jsonb), and the FTS column for full-text search.",
      "citation_path is nullable today — a holdover from the original Canada ingestion. " +
        "Once Canada is fully re-extracted we should ALTER TABLE … SET NOT NULL.",
    ],
    commands: ["load-supabase", "export-supabase", "snapshot-provision-counts"],
  },
  {
    id: "navigation",
    label: "corpus.navigation_nodes",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Derived tree-navigation index",
    detail:
      "Precomputed parent/child rows for fast tree navigation. One row per provision " +
      "(plus synthesized act-level containers for some jurisdictions). ~1.75M rows.",
    mechanics:
      "Each row has path, parent_path, segment, label, sort_key (natural-ordered with " +
      "zero-padded numerics), depth, child_count, has_children, has_rulespec, " +
      "encoded_descendant_count, status, timestamps. Indexed on (parent_path, " +
      "sort_key) for the app's primary lookup; partial index on " +
      "encoded_descendant_count > 0 OR has_rulespec for encoded-only browsing.",
    rationale:
      "The app used to derive tree navigation live via prefix-LIKE scans against " +
      "corpus.provisions. As state corpora grew those queries started hitting " +
      "Supabase's statement timeout. navigation_nodes turns the same lookup into a " +
      "single indexed parent_path query.",
    important: [
      "Disposable. If wrong, rebuild from corpus.provisions with " +
        "build-navigation-index. Cycles in parent_citation_path get broken automatically " +
        "(one node promoted to root, rest reach it).",
      "Auto-rebuilt as a post-step of load-supabase for every loaded scope, since PR #23.",
      "has_rulespec is set when a matching path exists in local rules-* checkouts at " +
        "rebuild time. encoded_descendant_count rolls up bottom-up.",
      "Status field is editorial metadata (e.g. 'deprecated', 'in-review') — preserved " +
        "across rebuilds via fetch_navigation_statuses + _apply_navigation_status_overrides.",
      "Sharp edge: if you run load-supabase in CI without local rules-* checkouts, " +
        "the rebuild silently demotes has_rulespec to false for paths whose encoding " +
        "the checkout-less worker can't see.",
    ],
    commands: ["build-navigation-index"],
  },
  {
    id: "counts",
    label: "corpus.provision_counts",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Materialized view",
    detail:
      "Per-(jurisdiction, doc_type) row counts. Refreshed via SQL RPC at the end of " +
      "every load-supabase run.",
    rationale:
      "Counting 1.75M rows live across all jurisdictions is slow. Materialized view " +
      "gives the analytics dashboard a cheap snapshot to read from.",
    important: [
      "Can drift if the refresh times out and --allow-refresh-failure was passed. " +
        "Re-running load-supabase (or refresh_corpus_analytics RPC directly) fixes it.",
      "Read by the analytics dashboard and the artifact-report command.",
    ],
  },
  {
    id: "references",
    label: "corpus.provision_references",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Cross-reference graph",
    detail:
      "Graph of inter-provision citations. Each row links a citing provision to a " +
      "cited one.",
    rationale:
      "Powers the app's 'cited by' / 'cites' UI. Without this, finding cross-references " +
      "would require scanning every body text.",
    commands: ["extract-references"],
  },

  // ── Rules repos ───────────────────────────────────────────────────
  {
    id: "rules-us",
    label: "rules-us",
    layer: "rules",
    repo: "rules-us",
    summary: "US federal RuleSpec YAML",
    detail:
      "RuleSpec YAML files encoding executable computation for US federal benefit " +
      "programs. Per-provision: one YAML per addressable section / subsection.",
    mechanics:
      "File path encodes the citation: statutes/26/3111/a.yaml ↔ us/statute/26/3111/a. " +
      "Same convention for regulations/ (with -cfr suffix stripped on US-federal) and " +
      "policies/.",
    rationale:
      "Encoding lives in separate repos so the corpus stays purely about source text. " +
      "Encoding cadence and corpus cadence are independent — you can add a new rule " +
      "without touching the corpus, and re-ingest the corpus without breaking rules.",
    important: [
      "Loosely coupled to corpus by citation_path only.",
      "Path mapping mirrored in axiom-corpus/src/axiom_corpus/corpus/rulespec_paths.py.",
      "Test files (.test.yaml) and meta files (.meta.yaml) are skipped at discovery.",
      "Discovered at nav rebuild time by walking the local checkout. No GitHub API " +
        "calls during corpus operations.",
      "~24 encoded paths today, mostly SNAP regulations (CFR Title 7 Part 273).",
    ],
    files: [
      "rules-us/statutes/",
      "rules-us/regulations/",
      "rules-us/policies/",
    ],
  },
  {
    id: "rules-state",
    label: "rules-us-{state}",
    layer: "rules",
    repo: "rules-us-state",
    summary: "Per-state RuleSpec",
    detail:
      "One repo per state (rules-us-co, rules-us-tx, rules-us-ca, …). Same convention " +
      "as rules-us. State-specific regulations and agency policy.",
    important: [
      "rules-us-co is the most-encoded today (~34 paths under " +
        "regulations/10-ccr-2506-1/ for Colorado SNAP).",
      "Other state repos exist but are mostly empty placeholders.",
      "Adding a new state means creating the repo, adding it to repo-map.ts in the " +
        "app, and adding it to JURISDICTION_REPO_MAP in rulespec_paths.py.",
    ],
  },
  {
    id: "rules-other",
    label: "rules-uk · rules-ca",
    layer: "rules",
    repo: "rules-non-us",
    summary: "Non-US RuleSpec",
    detail:
      "UK and Canadian RuleSpec repos. Same convention as the US repos but different " +
      "path conventions per jurisdiction's citation scheme.",
    important: [
      "rules-uk holds 146 has_rulespec rows on the corpus side — most of those came " +
        "from pre-existing has_rulespec flags in corpus.provisions, not from current " +
        "rules-uk YAML files.",
      "rules-ca is the canonical Canadian repo. Maps from canada/* corpus paths.",
    ],
  },

  // ── Consumers ─────────────────────────────────────────────────────
  {
    id: "axiom-foundation",
    label: "axiom-foundation.org",
    layer: "consumer",
    repo: "axiom-foundation.org",
    summary: "Main web app",
    detail:
      "Public-facing browser of the corpus at axiom-foundation.org. Next.js app, " +
      "deployed to Vercel.",
    mechanics:
      "Reads corpus.navigation_nodes for tree navigation (parent_path lookups), " +
      "corpus.provisions for body text, corpus.provision_references for cross-refs. " +
      "PostgREST client with Accept-Profile: corpus header on every call.",
    rationale:
      "Read-only consumer. Should never be the only place a citation lookup happens — " +
      "the API surface is the source of truth.",
    important: [
      "src/lib/axiom/repo-map.ts is authoritative for jurisdiction → rules-* repo " +
        "mapping. axiom-corpus mirrors this in rulespec_paths.py; keep in sync when " +
        "new jurisdictions land.",
      "src/lib/axiom/rulespec/repo-listing.ts converts repo paths to citation paths " +
        "(and vice versa) for the encoded-rules UI surface.",
      "Never writes to Supabase. Read-only RLS suffices.",
    ],
  },
  {
    id: "finbot",
    label: "finbot",
    layer: "consumer",
    repo: "axiom-foundation.org",
    summary: "Financial advice demo",
    detail:
      "Demo that combines corpus citations with RuleSpec computation to answer " +
      "benefit / tax questions in natural language. Local repo.",
    mechanics:
      "Calls Supabase REST + a RuleSpec runtime to compute eligibility / benefit " +
      "amounts, then surfaces the actual source provisions that drove the answer.",
  },
  {
    id: "dashboard-builder",
    label: "dashboard-builder",
    layer: "consumer",
    repo: "axiom-foundation.org",
    summary: "Dashboard demo",
    detail:
      "Demo for assembling policy dashboards on top of the corpus.",
  },
  {
    id: "axiom-encode",
    label: "axiom-encode",
    layer: "consumer",
    repo: "axiom-encode",
    summary: "Encoder pipeline",
    detail:
      "Drives the creation of RuleSpec YAML for provisions in the corpus. Combines " +
      "LLM workflows with structured validation, then opens PRs against rules-* repos.",
    mechanics:
      "Reads corpus.provisions to know what provisions exist. For a target provision, " +
      "drafts a candidate RuleSpec via prompt orchestration, validates against " +
      "machine-readable test cases, iterates until clean, then writes YAML.",
    rationale:
      "Encoding is the bottleneck for downstream usefulness. Only ~60 RuleSpec files " +
      "exist today across 1.75M provisions. Any tool that compounds encoder throughput " +
      "is high-leverage.",
    important: [
      "One-way dependency on corpus. The encoder NEVER writes to corpus.provisions.",
      "Closes the feedback loop indirectly — the next navigation rebuild observes " +
        "newly-authored YAML and sets has_rulespec=true.",
    ],
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
