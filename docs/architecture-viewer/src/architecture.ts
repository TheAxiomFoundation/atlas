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
      "ecfr.gov, run by the National Archives, is the live electronic Code of Federal " +
      "Regulations. Bulk XML downloads, refreshed daily as agencies file rule changes.",
    mechanics:
      "extract-ecfr fetches title-level XML bundles (workers configurable, 600s timeout " +
      "per request), then walks them part-by-part to emit one ProvisionRecord per " +
      "section/paragraph. The XML preserves hierarchy (title → chapter → part → subpart → " +
      "section) and the adapter keeps that hierarchy intact in citation_path.",
    important: [
      "Title 7 (USDA / SNAP regs) is the most-encoded slice today — ~10 of the 24 rules-us " +
        "encodings live there.",
      "Federal regulation paths drop the publication suffix: rules-us stores " +
        "regulations/7-cfr/273/7.yaml but the corpus row is us/regulation/7/273/7. The " +
        "adapter normalises this via _normalize_tail.",
      "Section-by-section iteration via lxml's root.iter() can be slow on Title 26 " +
        "(thousands of sections); no indexed XPath shortcut.",
    ],
    commands: ["inventory-ecfr", "extract-ecfr"],
  },
  {
    id: "usc",
    label: "USC (USLM)",
    layer: "upstream",
    repo: "external",
    summary: "US Code in USLM XML",
    detail:
      "uscode.house.gov publishes the US Code as USLM (United States Legislative Markup) " +
      "XML. Title-by-title bulk downloads with a stable, self-describing structure — every " +
      "element has a hierarchical identifier (/us/usc/t26/s32) and the XML carries its own " +
      "schema metadata.",
    mechanics:
      "extract-usc consumes one USLM XML file at a time, walking <title> → <chapter> → " +
      "<section> → <subsection>. The parser detects USLM namespace dynamically (USLM " +
      "appears from both GPO and House sources with slightly different URIs) via " +
      "root.tag inspection. Section iteration uses namespace-aware XPath: " +
      "root.iter(f\"{{{ns_uri}}}section\"). Identifier extraction parses '/s' splits.",
    important: [
      "Title 26 (Internal Revenue Code) is the most policy-relevant title — most tax " +
        "RuleSpec encoding pulls from here.",
      "USLM ids round-trip cleanly: /us/usc/t26/s32 becomes us/statute/26/32.",
      "Namespace detection by URL inspection is fragile — multiple possible namespaces " +
        "mean unfamiliar variants won't parse without hardcoding.",
    ],
    commands: ["inventory-usc", "extract-usc", "extract-usc-dir"],
  },
  {
    id: "state-sources",
    label: "State publishers",
    layer: "upstream",
    repo: "external",
    summary: "State legislature / agency sites",
    detail:
      "Each state publishes its statutes (and sometimes regulations and policy) " +
      "differently. Texas ships ZIPs of HTML; Indiana publishes annual code dumps; " +
      "Colorado serves agency rules through the Secretary of State's CCR portal; " +
      "smaller states publish PDFs only.",
    mechanics:
      "Per-state adapter modules under src/axiom_corpus/corpus/state_adapters/ each know " +
      "their state's quirks: download method, HTML structure, hierarchy markers. They all " +
      "produce ProvisionRecord with the same canonical citation_path shape " +
      "(us-{state}/{doc_type}/...). Common runtime helpers — BeautifulSoup + lxml, " +
      "ProvisionCoverageReport — live alongside.",
    important: [
      "Most state corpora are SHALLOW: section-level rows with no chapter or title " +
        "containers. That's an upstream-publisher artifact, not a navigation bug.",
      "50 states + DC are represented; coverage varies — statute-only for many, statute + " +
        "regulations + policy for a few.",
      "Each state adapter is 300-1500 lines depending on the upstream complexity. They " +
        "share the CorpusArtifactStore + ProvisionRecord contract, not parsing logic.",
    ],
    files: ["src/axiom_corpus/corpus/state_adapters/"],
    commands: [
      "extract-state-statutes",
      "extract-indiana-code",
      "extract-montana-code",
      "extract-nevada-nrs",
      "extract-oregon-ors",
      "extract-texas-tcas",
      "extract-{state}-…",
    ],
  },
  {
    id: "canada-source",
    label: "laws-lois.justice.gc.ca",
    layer: "upstream",
    repo: "external",
    summary: "Canadian federal acts (LIMS XML)",
    detail:
      "Canada's Department of Justice publishes consolidated federal Acts as LIMS XML " +
      "(Legal Information Management System, http://justice.gc.ca/lims namespace). ~956 " +
      "acts total. Bilingual (English + French) but split across per-language URLs.",
    mechanics:
      "CanadaLegislationFetcher.list_all_acts() scrapes alphabetical index pages " +
      "(/eng/acts/{A-Z}.html) for hrefs matching r'([A-Z][A-Za-z0-9.-]*)/index\\.html'. " +
      "download_act streams /eng/XML/{cn}.xml via requests (64 KB chunks). " +
      "CanadaStatuteParser walks <Section> → <Subsection> → <Paragraph> → <Subparagraph> → " +
      "<Clause> recursively, extracting LIMS namespace attributes (inforce-start-date, " +
      "lastAmendedDate, lims:id). extract-canada-acts inserts an act-level container row " +
      "(canada/statute/{cn}) so each act has a navigable root.",
    rationale:
      "Bringing Canada into the source-first pipeline was the May 2026 structural fix — " +
      "before that, citation_path was null on every Canada row.",
    important: [
      "Until May 2026 the legacy ingest left citation_path=null on every Canada row. " +
        "Fixed by switching to the source-first adapter and re-extracting in place via " +
        "load-supabase --replace-scope.",
      "The fetcher uses requests, not httpx, for the streaming download — httpx hung " +
        "reliably on _ssl__SSLSocket_read for files >10 MB (e.g., I-3.3 at ~13 MB) on " +
        "darwin. Caught in May 2026; fix is one line in download_act.",
      "Currently 161 of ~956 acts ingested. The rest exist as XML upstream but haven't " +
        "been pulled yet.",
      "English only for now; French content is in separate XMLs we haven't wired up.",
      "LIMS XML is non-standard and undocumented — the parser is empirically derived from " +
        "live specimens. Watch for upstream schema drift.",
    ],
    files: [
      "src/axiom_corpus/corpus/canada.py",
      "src/axiom_corpus/fetchers/legislation_canada.py",
      "src/axiom_corpus/parsers/canada/statutes.py",
      "src/axiom_corpus/models_canada.py",
    ],
    commands: ["extract-canada-acts"],
  },
  {
    id: "irs-bulk",
    label: "IRS bulk",
    layer: "upstream",
    repo: "external",
    summary: "Revenue procedures / rulings / notices",
    detail:
      "Internal Revenue Service publishes guidance documents (Revenue Procedures, " +
      "Revenue Rulings, Notices, Announcements) as PDFs at irs.gov/pub/irs-drop/.",
    mechanics:
      "IRSBulkFetcher combines HTML scraping (BeautifulSoup over paginated drop-folder " +
      "listings) with regex pattern matching (GUIDANCE_PATTERN = r'(rp|rr|n|a)-(\\d{2})-" +
      "(\\d+)\\.pdf') to enumerate documents. The pipeline composes download → " +
      "PDFTextExtractor → IRSDocumentParser → IRSParameterExtractor, optionally driven by " +
      "a storage callback for incremental loading.",
    important: [
      "Document classification matters — 'guidance' vs 'rulemaking' is meaningful in the " +
        "corpus and affects how the app renders them.",
      "Yearly filter + type filter applied at the listing stage to bound work.",
      "Listing pagination relies on string-matching ?page= in HTML; fragile if IRS " +
        "changes their template.",
    ],
    files: ["src/axiom_corpus/fetchers/irs_bulk.py"],
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
      "source family. Returns bytes only — no parsing, no storage. Isolates HTTP " +
      "concerns from everything downstream.",
    mechanics:
      "Each fetcher exposes a small surface: typically list_*() to enumerate available " +
      "documents and download_*() to retrieve one. Rate limits, retries, and " +
      "authentication concerns live here. CanadaLegislationFetcher uses a lazy " +
      "httpx.Client (line 69 of legislation_canada.py) with a configurable rate_limit " +
      "(default 0.5s between requests). Downloads stream in 64 KB chunks. IRSBulkFetcher " +
      "uses a coordinated fetch+extract pipeline with optional progress and storage " +
      "callbacks so the pipeline composes cleanly with later stages.",
    rationale:
      "Separating HTTP from parsing lets parsers stay deterministic and trivially " +
      "testable — a flaky upstream doesn't propagate into parser tests, and a parser " +
      "bug doesn't poison the fetcher cache. Fetchers are injectable; adapters accept a " +
      "fetcher param so tests pass fakes.",
    important: [
      "The Canada fetcher uses requests (not httpx) for downloads — the httpx client " +
        "reliably hung in _ssl__SSLSocket_read on darwin when streaming >10 MB acts. " +
        "Switch lives at one line in CanadaLegislationFetcher.download_act.",
      "Rate limits are baked in (typically 0.5s between requests). Don't bypass — most " +
        "upstreams will rate-limit or block.",
      "Manual time.sleep() rate limiting is thread-unsafe. Concurrent fetchers using " +
        "the same instance bypass the limit.",
      "User-Agent strings identify us as 'Axiom/1.0 (legislation archiver; " +
        "contact@axiom-foundation.org)' — keep that intact for upstream operators.",
      "No checkpointing today. If a bulk run fails halfway, you restart from scratch " +
        "unless the artifact store has the prior session's downloads.",
    ],
    files: [
      "src/axiom_corpus/fetchers/legislation_canada.py",
      "src/axiom_corpus/fetchers/ecfr.py",
      "src/axiom_corpus/fetchers/irs_bulk.py",
      "src/axiom_corpus/fetchers/irs_parser.py",
      "src/axiom_corpus/fetchers/legislation_uk.py",
      "src/axiom_corpus/fetchers/pdf_extractor.py",
    ],
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
      "eCFR XML, state-specific HTML, CLML (UK). Output is typed Pydantic / dataclass " +
      "models — CanadaSection, IndianaCodeProvision, RegulationSubsection — never " +
      "strings or untyped dicts.",
    mechanics:
      "Heavy use of lxml for XML and BeautifulSoup for HTML. CanadaStatuteParser " +
      "lazily loads the XML tree (line 49 of parsers/canada/statutes.py) and yields " +
      "CanadaSection via iter_sections(). Each section carries marginal note, full body " +
      "text, parsed-via-_parse_subsections subsection chain, LIMS temporal attributes " +
      "(inforce-start-date, lastAmendedDate), historical notes, and cross-references " +
      "extracted from <XRefExternal> and <XRefInternal>. Subsection labels are stored " +
      "with parentheses ('(1)', '(a)', '(i)'); the adapter strips them when building " +
      "paths.",
    rationale:
      "Format complexity lives here so adapters don't have to think about XML " +
      "namespaces or HTML quirks. When a format upstream changes, only one parser breaks.",
    important: [
      "Parsers must be deterministic on the same input bytes. No timestamps, no random " +
        "IDs. Re-running the parser on the same bytes always yields the same model " +
        "objects.",
      "Lazy tree loading via @property means a 13 MB LIMS XML doesn't sit in memory " +
        "until queried.",
      "USLM namespace detection is dynamic because GPO and House.gov ship slightly " +
        "different URIs — root.tag inspection picks the right one at runtime.",
      "Subsection labels stored with parens ('(1)') but stripped when building citation " +
        "paths ('1'). Consumers must normalise if they want to display labels.",
      "Parsing failures are logged and skipped — bad sections don't kill the run, but " +
        "they silently drop from output.",
    ],
    files: [
      "src/axiom_corpus/parsers/canada/statutes.py",
      "src/axiom_corpus/parsers/us/statutes.py",
      "src/axiom_corpus/parsers/cfr.py",
      "src/axiom_corpus/parsers/clml.py",
      "src/axiom_corpus/models_canada.py",
      "src/axiom_corpus/models_regulation.py",
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
      "responsible for projecting parser output into the canonical ProvisionRecord shape " +
      "and writing four parallel artifact trees: sources/, inventory/, provisions/, " +
      "coverage/.",
    mechanics:
      "Adapter loops over parser output, builds canonical citation_paths " +
      "({jurisdiction}/{doc_type}/{segments…}), computes deterministic UUID5 ids " +
      "(uuid5(NAMESPACE_URL, f'axiom:{citation_path}')), and calls " +
      "CorpusArtifactStore.write_*() to emit artifacts. extract_canada_acts (canada.py) " +
      "is a representative example: per-act, it downloads bytes, sha256-tracks them into " +
      "sources/, parses with CanadaStatuteParser, emits an act-level container row " +
      "(canada/statute/{cn}), then per-section ProvisionRecord (with parent_citation_path " +
      "back to the act), then recursively emits subsections via _emit_subsections. " +
      "Path segments strip parens via _label_segment ((a) → a). Each adapter ends by " +
      "running compare_provision_coverage and writing the diff report.",
    rationale:
      "The 'source-first' contract: JSONL on disk is the boundary between adapters and " +
      "everything downstream. Adapters can change internals freely; consumers stay " +
      "stable. Same JSONL produces both R2 mirror and Supabase rows. Source-first means " +
      "the inventory is the assertion ('we expected these citations'), provisions is " +
      "the result ('we extracted these'), and coverage is the audit trail.",
    important: [
      "Citation path is the canonical id. Format: {jurisdiction}/{doc_type}/{segments}. " +
        "First segment must equal jurisdiction. Path becomes input to UUID5 → row id.",
      "Deterministic UUID5 means re-runs are upserts in place. Two pipelines processing " +
        "the same source produce the same ids — no drift, no duplicates.",
      "Coverage report compares expected citations to actual rows. Adapter exits " +
        "non-zero unless --allow-incomplete is passed.",
      "Path conventions vary subtly by jurisdiction: us-co/regulation/ optionally has " +
        "@variant suffixes; US federal regulation drops -cfr from titles; Indiana uses " +
        "dashes within segments (1-2-3) rather than slashes.",
      "Each adapter is ~300-1500 lines depending on upstream complexity. They share " +
        "the CorpusArtifactStore + ProvisionRecord contract, not parsing logic.",
      "Inventory + provisions are both kept on disk so coverage can be diffed. " +
        "Mismatch counts surface in the coverage JSON and the extract command exit code.",
    ],
    files: [
      "src/axiom_corpus/corpus/canada.py",
      "src/axiom_corpus/corpus/colorado.py",
      "src/axiom_corpus/corpus/ecfr.py",
      "src/axiom_corpus/corpus/usc.py",
      "src/axiom_corpus/corpus/state_adapters/",
      "src/axiom_corpus/corpus/coverage.py",
    ],
    commands: [
      "extract-ecfr",
      "extract-usc",
      "extract-canada-acts",
      "extract-state-statutes",
      "extract-colorado-ccr",
      "coverage",
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
      "Filesystem layout holding the intermediate state of every extract. Four " +
      "parallel trees — sources, inventory, provisions, coverage — keyed identically. " +
      "Same key structure mirrors to R2. CorpusArtifactStore (artifacts.py) is the " +
      "single class that builds paths and writes files.",
    mechanics:
      "Path scheme (per artifacts.py):\n" +
      "  sources/{jur}/{doc}/{run_id}/{name}        raw upstream bytes\n" +
      "  inventory/{jur}/{doc}/{run_id}.json        expected citations (JSON)\n" +
      "  provisions/{jur}/{doc}/{run_id}.jsonl      ProvisionRecord per line\n" +
      "  coverage/{jur}/{doc}/{run_id}.json         inventory ↔ provisions diff\n" +
      "  exports/{format}/{jur}/{doc}/{ver}/        external format conversions\n\n" +
      "All path components run through safe_segment() to reject empty, '.', '..', or " +
      "slash-containing values. write_bytes uses tempfile + atomic rename. write_json " +
      "uses sort_keys=True + 2-space indent for stable diffs. write_provisions writes " +
      "one JSON object per line (sort_keys=True), final newline if non-empty. Every " +
      "write returns the SHA256 of bytes written.",
    rationale:
      "JSONL is the contract between adapter and loader. Append-friendly, line-streamable, " +
      "grep-friendly, diff-friendly in git. Same line-oriented file produces both R2 " +
      "mirror (sync-r2) and Supabase rows (load-supabase). Atomic writes via tempfile " +
      "prevent partial state if the process crashes mid-write.",
    important: [
      "Each JSONL line is one ProvisionRecord.to_mapping() encoded with sort_keys=True. " +
        "Two runs on the same input produce byte-identical files — critical for " +
        "deterministic diffs and content-addressed storage.",
      "Required JSONL fields per line: jurisdiction, document_class, citation_path. " +
        "Everything else is optional and emitted only when non-null.",
      "Reader is NOT streaming today — load_provisions reads the whole file then " +
        "splits. Fine for current sizes (<300 MB) but would OOM on multi-GB inputs.",
      "UTF-8 throughout. Tolerates French ligatures, em-dashes, fancy quotes.",
      "iter_provision_files walks the tree and returns paths matching a filter — used " +
        "by bulk operations like artifact-report.",
    ],
    files: [
      "src/axiom_corpus/corpus/artifacts.py",
      "src/axiom_corpus/corpus/io.py",
      "src/axiom_corpus/corpus/coverage.py",
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
      "same key layout. Holds raw upstream bytes plus all JSONL artifacts and coverage " +
      "reports.",
    mechanics:
      "load_r2_config (r2.py) bootstraps credentials from env vars (R2_ACCESS_KEY_ID + " +
      "R2_SECRET_ACCESS_KEY), AWS_* legacy names, or a JSON file at " +
      "~/.config/axiom-foundation/r2-credentials.json with multiple naming conventions. " +
      "Falls back to a default account id if no endpoint is supplied. sync_artifacts_" +
      "to_r2 walks the local tree via iter_local_artifacts, lists remote via boto3 " +
      "list_objects_v2 paginator, diffs by (key present + size match). Upload candidates " +
      "are sent in parallel via ThreadPoolExecutor with configurable worker count, each " +
      "writing SHA256 into the R2 object metadata. Scope filtering via " +
      "_artifact_matches_scope() parses the key into (artifact_type, jurisdiction, " +
      "document_class, version) and filters before upload.",
    rationale:
      "Provenance / forensics. Lets you replay any historical ingest, prove what was " +
      "ingested when, and serve large assets without hitting Supabase. The full " +
      "pipeline can rebuild Supabase from R2 alone — no upstream re-fetch needed.",
    important: [
      "Diff is size-based, not hash-based. Changed content with identical byte count " +
        "won't retrigger upload unless --force.",
      "Nothing in production serving reads from R2 today — it's audit/replay only. If " +
        "no downstream consumer ever materialises, this is dead weight worth removing.",
      "Credentials at ~/.config/axiom-foundation/r2-credentials.json. Missing creds → " +
        "RuntimeError mentioning both env-var and file paths.",
      "build_artifact_report_with_r2 produces a three-way health check: local files vs " +
        "R2 inventory vs Supabase provision counts.",
      "Bucket size sub-GB today, well within R2's free tier.",
    ],
    files: ["src/axiom_corpus/corpus/r2.py"],
    commands: [
      "sync-r2",
      "artifact-report",
      "release-artifact-manifest",
      "validate-release",
    ],
  },

  // ── Hot storage (Supabase) ────────────────────────────────────────
  {
    id: "supabase",
    label: "Supabase",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Postgres + PostgREST",
    detail:
      "Managed Postgres + PostgREST hosted by Supabase. Live serving database for the " +
      "whole ecosystem. Four schemas exposed: corpus (legal text + navigation), " +
      "encodings (encoder run history), telemetry (observability), app (frontend state).",
    mechanics:
      "Apps read via REST endpoints with `Accept-Profile: corpus` header to scope to " +
      "the corpus schema. Writes go through `Content-Profile: corpus` POST/DELETE with " +
      "`Prefer: resolution=merge-duplicates,return=minimal` for idempotent upserts. " +
      "PostgREST routing is controlled at the role level: " +
      "`ALTER ROLE authenticator SET pgrst.db_schemas = " +
      "'public,graphql_public,corpus,encodings,telemetry,app'` (corpus_schema.sql).",
    important: [
      "Project ref: swocpijqqahhuwtuahwc. URL: swocpijqqahhuwtuahwc.supabase.co.",
      "Service-role key needed for writes; anon key suffices for reads.",
      "RLS is enabled on every corpus table. Public SELECT, no public writes.",
      "Service key resolution: SUPABASE_SERVICE_ROLE_KEY env var first, then " +
        "SUPABASE_ACCESS_TOKEN as fallback (loader fetches the service-role key via " +
        "the Management API at api.supabase.com/v1/projects/{ref}/api-keys).",
      "Every corpus table sets `SET search_path = corpus, public` on its functions so " +
        "SECURITY DEFINER RPCs don't accidentally hit the public schema.",
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
      "across all jurisdictions. Holds body text plus 20+ metadata columns.",
    mechanics:
      "Loaded by load_provisions_to_supabase (supabase.py). The loader chunks records " +
      "(default 500/chunk), projects each through provision_to_supabase_row (which sets " +
      "deterministic UUID5 id from citation_path and derives parent_id from " +
      "parent_citation_path), and POSTs to /rest/v1/provisions?on_conflict=id with " +
      "merge-duplicates resolution. With --preserve-existing-ids, fetch_existing_" +
      "provision_ids batch-queries by citation_path to keep historical ids stable. " +
      "With --replace-scope, delete_supabase_provisions_scope cursor-paginates all rows " +
      "in (jurisdiction, doc_type) and deletes them in chunks first, ordered by " +
      "(-level, id) to delete deepest children first.",
    rationale:
      "Single source of truth for legal text. Every other corpus.* surface " +
      "(navigation_nodes, provision_counts, references) is derived from this table and " +
      "rebuildable in minutes. UUID5(citation_path) means same path → same id, forever — " +
      "no row drift across re-runs.",
    important: [
      "IDs are deterministic UUID5(NAMESPACE_URL, f'axiom:{citation_path}'). Stable " +
        "across re-runs. Re-loading the same JSONL is a no-op.",
      "Loader projects parent_id from parent_citation_path automatically — adapters " +
        "set the path, the loader sets the id.",
      "Schema is intentionally wide: id, jurisdiction, doc_type, parent_id, level, " +
        "ordinal, heading, body, source_url, source_path, citation_path, rulespec_path, " +
        "has_rulespec, source_document_id, source_as_of, expression_date, language, " +
        "legal_identifier, identifiers (jsonb), plus FTS column.",
      "Key indexes: idx_provisions_orphan_citation_prefix_ordinal " +
        "(citation_path text_pattern_ops, ordinal) WHERE parent_id IS NULL — the " +
        "text_pattern_ops opclass enables LIKE 'prefix%' lookups under C-locale; " +
        "without it those queries do a sequential scan.",
      "idx_provisions_jurisdiction_doc_type_id covers scope-replace queries. " +
        "INCLUDE (level) lets the planner avoid a heap fetch for ordering.",
      "Parent FK uses ON DELETE SET NULL — partial tree deletes orphan children rather " +
        "than cascading, which is safer for scope-based replace operations.",
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
      "Precomputed parent/child rows for fast tree navigation. ~1.75M rows. Each row " +
      "carries path, parent_path, segment, label, sort_key, depth, child_count, " +
      "has_children, has_rulespec, encoded_descendant_count, status, plus timestamps.",
    mechanics:
      "build_navigation_nodes (navigation.py) runs five stages: (1) filter & dedupe " +
      "records by citation_path; (2) resolve parent_path per record — explicit " +
      "parent_citation_path wins if it exists in scope, otherwise walk path-prefix " +
      "segments upward until an ancestor is found, otherwise root; (3) _break_parent_" +
      "cycles scans for cycles (A→B→A or longer) and promotes one member to root using " +
      "lexicographic min so the result is deterministic; (4) _resolve_depths walks the " +
      "parent chain with memoised recursion and a cycle-guard stack; (5) build " +
      "NavigationNode per record with deterministic id (uuid5 of 'axiom-navigation:' + " +
      "path), sort_key (8-digit ordinal + lowercased segment with 12-digit zero-padded " +
      "numeric runs so 2 < 10), has_rulespec OR-merged with encoded_paths, and bottom-up " +
      "encoded_descendant_count accumulation. Final sort by (parent_path, sort_key, path). " +
      "write_navigation_nodes_to_supabase materializes, groups by scope, deletes stale " +
      "rows scope-by-scope (existing-paths minus new-paths), upserts in chunks of 500.",
    rationale:
      "The app's tree navigation used to scan corpus.provisions with prefix-LIKE; it " +
      "kept timing out on big scopes. navigation_nodes turns that into a single indexed " +
      "parent_path query. Disposable by design — rebuild any time. Builder is " +
      "deterministic: same input always produces byte-identical output.",
    important: [
      "Auto-rebuilt as a post-step of load-supabase for every loaded scope (since " +
        "PR #23).",
      "Five indexes: unique(path), (parent_path, sort_key), (jurisdiction, doc_type, " +
        "parent_path, sort_key), partial (parent_path, sort_key) WHERE " +
        "encoded_descendant_count > 0 OR has_rulespec — the encoded-only browser query, " +
        "and partial (provision_id) WHERE provision_id IS NOT NULL.",
      "has_rulespec is set at rebuild time by walking local rules-* checkouts. " +
        "encoded_descendant_count rolls up bottom-up.",
      "Status field is editorial metadata (e.g. 'deprecated', 'in-review'). Preserved " +
        "across rebuilds via fetch_navigation_statuses + _apply_navigation_status_" +
        "overrides — fresh overrides only win when non-empty, so None doesn't clobber " +
        "curated state.",
      "Cycle handling: _break_parent_cycles is deterministic — picks the lexicographic " +
        "min of the cycle as the new root every time, so two runs with the same input " +
        "produce the same broken-edge.",
      "Sharp edge: if you run load-supabase in CI without local rules-* checkouts, the " +
        "rebuild silently demotes has_rulespec=false for paths whose encoding the " +
        "checkout-less worker can't see. Mitigate via --rulespec-repo flag or by " +
        "running outside CI.",
      "Sort_key encodes natural order: ordinal slot first (or 'zzzzzzzz' if no " +
        "ordinal), then '|', then segment lowercased with 12-digit zero-padded " +
        "numeric runs. 'Section 10A' becomes 'section 000000000010a' under the hood.",
    ],
    files: [
      "src/axiom_corpus/corpus/navigation.py",
      "src/axiom_corpus/corpus/navigation_supabase.py",
      "src/axiom_corpus/corpus/rulespec_paths.py",
      "supabase/migrations/20260505120000_corpus_navigation_nodes.sql",
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
      "every load-supabase run. Plus a sibling corpus.current_provision_counts that " +
      "filters to the active release.",
    mechanics:
      "MV computes provision_count, body_count, top_level_count (parent_id IS NULL), " +
      "rulespec_count (has_rulespec IS TRUE), and refreshed_at(now()) per scope. " +
      "Refreshed by the refresh_corpus_analytics() RPC (statement_timeout=0). Called " +
      "by load_provisions_to_supabase at the end of every run unless --skip-refresh. " +
      "Failures are caught; with --allow-refresh-failure the upsert succeeds " +
      "regardless.",
    rationale:
      "Counting 1.75M rows live across all jurisdictions is slow. Materialized view " +
      "gives the analytics dashboard a cheap snapshot to read from. Refresh on every " +
      "load keeps the dashboard in sync without manual intervention.",
    important: [
      "Can drift if the refresh times out and --allow-refresh-failure was passed. " +
        "Re-running load-supabase (or the refresh_corpus_analytics RPC directly) fixes it.",
      "Read by the analytics dashboard, artifact-report, and state-statute-completion.",
      "corpus.current_provision_counts (sibling MV, filtered by corpus.release_scopes) " +
        "is what the public app shows; corpus.provision_counts includes legacy rows.",
    ],
    commands: ["snapshot-provision-counts", "analytics"],
  },
  {
    id: "references",
    label: "corpus.provision_references",
    layer: "storage-hot",
    repo: "infrastructure",
    summary: "Cross-reference graph",
    detail:
      "Graph of inter-provision citations. Each row links a citing provision to a " +
      "cited one. Built by extract-references walking provision body text.",
    rationale:
      "Powers the app's 'cited by' / 'cites' UI. Without this, finding cross-references " +
      "would require scanning every body text on every request.",
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
      "programs. Per-provision: one YAML per addressable section/subsection. ~24 " +
      "encoded files today, concentrated in SNAP regulations (CFR Title 7 Part 273).",
    mechanics:
      "Repository layout mirrors citation paths: statutes/26/3101/a.yaml ↔ " +
      "us/statute/26/3101/a. Each YAML begins with format: rulespec/v1 plus a module " +
      "block (summary, source citations, source verification). The rules array contains " +
      "one or more rule definitions, each with a kind (parameter / derived / " +
      "reiteration), dtype (Money, Rate, Judgment), entity scope (Household, TaxUnit, " +
      "Person), period scope (Month, Year), and one or more versions keyed by " +
      "effective_from date. Formulas are Python-like expressions (if/else, match, " +
      "arithmetic, count_where(), table indexing) but not actual Python — they're " +
      "parsed by the axiom-rules Rust engine at compile time.",
    rationale:
      "Encoding lives in separate repos so the corpus stays purely about source text. " +
      "Encoding cadence and corpus cadence are independent — you can add a new rule " +
      "without touching the corpus, and re-ingest the corpus without breaking rules. " +
      "Per-section YAML keeps rules bundled with their source for fine-grained Git " +
      "history.",
    important: [
      "Coupling to corpus is by citation_path only — no foreign keys, no row references.",
      "Path mapping mirrored in axiom-corpus/src/axiom_corpus/corpus/rulespec_paths.py " +
        "and in axiom-foundation.org/src/lib/axiom/rulespec/repo-listing.ts. Keep them " +
        "in sync.",
      ".test.yaml and .meta.yaml files are skipped at navigation discovery time. " +
        "Hidden dirs (.github, etc.) and tests/ subdirs are also skipped.",
      "Discovered at nav rebuild by walking the local checkout — no GitHub API calls " +
        "during corpus operations.",
      "RuleSpec kind: parameter / derived / reiteration. Reiterations are " +
        "coverage markers (state-level provisions that restate federal rules); they're " +
        "no-ops at execution time.",
      "Tests reference rules by durable id ('us:statutes/7/2017/a#snap_regular_" +
        "month_allotment'), not by local name. Tests run against the compiled module " +
        "(after imports are merged), not individual rules.",
      "Imports use canonical paths ('us:statutes/7/2017/a') and resolve cross-repo " +
        "(rules-us-co imports rules-us).",
    ],
    files: [
      "rules-us/statutes/",
      "rules-us/regulations/",
      "rules-us/policies/",
      "rules-us/tests/",
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
    mechanics:
      "Colorado is the deepest example today: rules-us-co/regulations/10-ccr-2506-1/ " +
      "holds ~34 encoded SNAP-administration sections. Other state repos exist mostly " +
      "as placeholders. The Colorado SNAP composition file " +
      "(rules-us-co/policies/cdhs/snap/fy-2026-benefit-calculation.yaml) imports " +
      "23 rules — federal USDA parameters and Colorado overlays — and composes them " +
      "into a single executable module for microsimulation.",
    important: [
      "rules-us-co has ~34 paths under regulations/10-ccr-2506-1/. The Colorado " +
        "Code of Regulations citation '10 CCR 2506-1' becomes '10-ccr-2506-1' in repo " +
        "paths (dash-separated, lowercase).",
      "Many state RuleSpec files use kind: reiteration to declare 'this section " +
        "restates federal X'. Coverage marker, not executable.",
      "Adding a new state: create the repo, add to repo-map.ts in the app, add to " +
        "JURISDICTION_REPO_MAP in rulespec_paths.py.",
      "Composition files (e.g. fy-2026-benefit-calculation.yaml) are how state " +
        "microsimulations work: they import federal rules and overlay state-specific " +
        "values into a single compiled ruleset.",
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
        "rules-uk YAML.",
      "rules-ca maps from canada/* corpus paths. The canada jurisdiction slug is " +
        "non-obvious — JURISDICTION_REPO_MAP['canada'] = 'rules-ca', not 'rules-canada'.",
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
      "Public-facing browser of the corpus at axiom-foundation.org. Next.js app " +
      "deployed to Vercel. URL pattern /axiom/* maps directly to citation_path.",
    mechanics:
      "Four Supabase clients in src/lib/supabase.ts, one per schema (corpus, encodings, " +
      "telemetry, plus the public auth client). Tree navigation queries " +
      "corpus.navigation_nodes by parent_path (the indexed btree query). Body text " +
      "comes from corpus.current_provisions — the release-filtered view of " +
      "corpus.provisions. When an encoded rule exists in a rules-* repo but " +
      "encoding_runs hasn't been backfilled, the app falls back to GitHub raw fetch " +
      "with a 1-hour cache. Path-mapping conventions (REPO_BUCKET_RENAMES, -cfr suffix " +
      "handling) mirror axiom-corpus/rulespec_paths.py.",
    rationale:
      "Read-only consumer. Should never be the only place a citation lookup happens — " +
      "the API surface is the source of truth. Multi-schema client isolation prevents " +
      "heavy telemetry queries from competing with corpus reads.",
    important: [
      "src/lib/axiom/repo-map.ts is authoritative for jurisdiction → rules-* repo " +
        "mapping. axiom-corpus mirrors it in rulespec_paths.py; keep in sync when new " +
        "jurisdictions land.",
      "src/lib/axiom/rulespec/repo-listing.ts handles repo path ↔ citation path " +
        "conversion in both directions. Singular bucket names on the corpus side " +
        "(statute, regulation, policy); plural on the rules-* side.",
      "Parent-path candidate matching: when resolving an encoding for a citation the " +
        "app walks up the path hierarchy and returns the most specific match.",
      "Deep links use ?mark=term URL param to highlight search terms in the rendered " +
        "body.",
      "Encoded-rule listings can come from corpus.encoding_runs OR from a live " +
        "GitHub fetch of rules-* repo trees — useful during rolling backfills.",
      "Never writes to Supabase. Read-only RLS policies suffice.",
    ],
    files: [
      "axiom-foundation.org/src/lib/supabase.ts",
      "axiom-foundation.org/src/lib/axiom/repo-map.ts",
      "axiom-foundation.org/src/lib/axiom/rulespec/repo-listing.ts",
      "axiom-foundation.org/src/lib/axiom/navigation-index/read.ts",
      "axiom-foundation.org/src/app/axiom/[[...segments]]/page.tsx",
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
      "benefit / tax questions in natural language. Local repo, embedded inside " +
      "axiom-foundation.org.",
    mechanics:
      "Calls Supabase REST + a RuleSpec runtime to compute eligibility / benefit " +
      "amounts, then surfaces the actual source provisions that drove the answer. " +
      "Cites corpus paths inline so the user can trace any number back to a real law.",
  },
  {
    id: "dashboard-builder",
    label: "dashboard-builder",
    layer: "consumer",
    repo: "axiom-foundation.org",
    summary: "Dashboard demo",
    detail:
      "Demo for assembling policy dashboards on top of the corpus. Local repo.",
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
      "machine-readable test cases, iterates until clean, then commits YAML. Run " +
      "history (scores, iterations, agent transcripts) lands in corpus.encoding_runs.",
    rationale:
      "Encoding is the bottleneck for downstream usefulness. ~60 RuleSpec files exist " +
      "today across 1.75M provisions. Any tool that compounds encoder throughput is " +
      "high-leverage.",
    important: [
      "One-way dependency on corpus. The encoder NEVER writes to corpus.provisions.",
      "Closes the feedback loop indirectly — the next navigation rebuild observes " +
        "newly-authored YAML and sets has_rulespec=true.",
      "Telemetry lands in encodings.encoding_runs (run id, iterations, scores, " +
        "agent_model, session_id, file_path). telemetry.agent_transcripts holds the " +
        "per-tool-use detail.",
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
