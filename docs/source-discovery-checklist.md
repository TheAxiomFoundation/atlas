# Source Discovery Checklist

This checklist tracks external discovery inputs that can help find primary
policy sources without becoming corpus sources themselves.

The corpus contract remains source-first: ingestion snapshots primary official
documents, builds inventories from those documents, extracts normalized
provisions, and publishes corpus artifacts from that source tree. External
models, datasets, reports, and citation lists are only gap-finding aids.

## PolicyEngine References

PolicyEngine-US contains thousands of claim-level references to statutes,
regulations, agency manuals, tax forms, tables, and guidance documents. These
references are useful for discovering public policy source documents, but they
must not become a runtime dependency or privileged upstream for Axiom.

Use this as an offline checklist only:

- [ ] Export a static URL inventory from PolicyEngine-US references into a
      review artifact, preserving the citing parameter or variable path.
- [x] Normalize URL variants, strip page anchors into separate metadata, and
      deduplicate by canonical URL.
- [x] Classify each URL by source status: primary official, official-but-not-
      current, secondary mirror, analytical report, vendor-only, or unknown.
- [x] Classify each primary official URL by document class: statute,
      regulation, manual, state plan, waiver, agency memo, tax form
      instruction, data table, or other policy document.
- [ ] Prioritize official/open URLs that are not already covered by current
      release artifacts.
- [ ] For each selected URL, create or extend a corpus manifest that points to
      the official source directly. Do not ingest from PolicyEngine files.
- [ ] Preserve the external citation path only as discovery provenance, such as
      `discovered_via: policyengine-us:<parameter-or-variable-path>`, not as a
      legal source.
- [ ] Exclude LexisNexis, Westlaw, Cornell, Justia, Casetext, public.law, and
      similar mirrors from automatic ingestion unless a jurisdiction confirms
      that endpoint as the official publisher or we add an explicit secondary
      audit document class.
- [ ] Re-fetch every selected document from the official source during corpus
      ingestion and store it under `sources/{jurisdiction}/{document_class}/...`
      with Axiom-owned hashes and timestamps.
- [ ] After ingestion, validate that the PE-cited policy point is traceable to
      an Axiom source/provision, then mark that discovery item covered.

Do not:

- import PolicyEngine packages from the corpus pipeline
- read PolicyEngine YAML during production ingestion
- treat PolicyEngine parameter values as source text
- let a PE citation substitute for official source discovery or coverage

Useful initial scale from a local PolicyEngine-US checkout:

- about 6,259 unique URLs across `policyengine_us`
- about 5,175 unique URLs under `policyengine_us/parameters`
- about 4,226 unique state-level parameter URLs
- about 4,115 parameter YAML files with a reference or URL

Those counts are discovery scope, not corpus coverage.

The current ops artifact is
`data/corpus/analytics/source-discovery-current.json`, generated from the
static URL lists under `sources/policyengine-us/`. It powers the Axiom `/ops`
Source Discovery Backlog section and should be refreshed when the offline
inventory changes.

For agent assignment, prefer `ready_for_manifest` rows whose source status is
`primary_official`. Do not assign vendor-only or source-access-blocked items to
ingestion agents until there is an explicit official export, license, or access
path to use.

Use `group_rows` in the report before assigning work from raw `rows`.
`group_rows` collapses uncovered `ready_for_manifest` URLs into actionable
manifest candidates by jurisdiction, document class, and source family. This is
especially important for forms, where the raw inventory often contains multiple
yearly tax booklet, schedule, and instruction URLs that should be scoped as a
coherent current-year manifest before historical years are added.
