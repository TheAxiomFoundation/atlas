"""CLI for the source-first corpus pipeline."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterable
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

from axiom_corpus.corpus.analytics import (
    build_analytics_report,
    load_provision_count_snapshot,
)
from axiom_corpus.corpus.artifacts import CorpusArtifactStore, sha256_bytes
from axiom_corpus.corpus.california_mpp import (
    MppDocxSource,
    extract_california_mpp_calfresh,
)
from axiom_corpus.corpus.colorado import extract_colorado_ccr
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.documents import extract_official_documents
from axiom_corpus.corpus.ecfr import build_ecfr_inventory, ecfr_run_id, extract_ecfr
from axiom_corpus.corpus.io import load_provisions, load_source_inventory
from axiom_corpus.corpus.models import (
    CorpusManifest,
    CorpusSource,
    DocumentClass,
    ProvisionRecord,
)
from axiom_corpus.corpus.navigation import (
    NavigationNode,
    build_navigation_nodes,
)
from axiom_corpus.corpus.navigation_supabase import (
    fetch_navigation_statuses,
    fetch_provisions_for_navigation,
    write_navigation_nodes_to_supabase,
)
from axiom_corpus.corpus.ny_rulemaking import extract_ny_state_register
from axiom_corpus.corpus.nycrr import extract_nycrr
from axiom_corpus.corpus.r2 import (
    DEFAULT_ARTIFACT_PREFIXES,
    DEFAULT_RELEASE_ARTIFACT_PREFIXES,
    build_artifact_report,
    build_artifact_report_with_r2,
    build_release_artifact_manifest,
    load_r2_config,
    sync_artifacts_to_r2,
)
from axiom_corpus.corpus.regulation_completion import (
    build_regulation_completion_report,
)
from axiom_corpus.corpus.release_quality import validate_release
from axiom_corpus.corpus.releases import ReleaseManifest, resolve_release_manifest_path
from axiom_corpus.corpus.rulespec_paths import (
    JURISDICTION_REPO_MAP,
    discover_encoded_paths,
    discover_encoded_paths_for_jurisdictions,
)
from axiom_corpus.corpus.source_discovery import build_source_discovery_report
from axiom_corpus.corpus.state_adapters.alabama import extract_alabama_code
from axiom_corpus.corpus.state_adapters.alaska import (
    ALASKA_STATUTES_DEFAULT_YEAR,
    extract_alaska_statutes,
)
from axiom_corpus.corpus.state_adapters.arizona import extract_arizona_revised_statutes
from axiom_corpus.corpus.state_adapters.connecticut import extract_connecticut_statutes
from axiom_corpus.corpus.state_adapters.delaware import extract_delaware_code
from axiom_corpus.corpus.state_adapters.florida import (
    FLORIDA_STATUTES_DEFAULT_YEAR,
    extract_florida_statutes,
)
from axiom_corpus.corpus.state_adapters.hawaii import extract_hawaii_revised_statutes
from axiom_corpus.corpus.state_adapters.idaho import extract_idaho_statutes
from axiom_corpus.corpus.state_adapters.illinois import extract_illinois_ilcs
from axiom_corpus.corpus.state_adapters.indiana import (
    INDIANA_CODE_DEFAULT_YEAR,
    extract_indiana_code,
)
from axiom_corpus.corpus.state_adapters.iowa import (
    IOWA_CODE_DEFAULT_YEAR,
    extract_iowa_code,
)
from axiom_corpus.corpus.state_adapters.kansas import extract_kansas_statutes
from axiom_corpus.corpus.state_adapters.louisiana import (
    extract_louisiana_revised_statutes,
)
from axiom_corpus.corpus.state_adapters.maine import extract_maine_revised_statutes
from axiom_corpus.corpus.state_adapters.maryland import extract_maryland_code
from axiom_corpus.corpus.state_adapters.massachusetts import (
    extract_massachusetts_general_laws,
)
from axiom_corpus.corpus.state_adapters.michigan import (
    extract_michigan_compiled_laws,
)
from axiom_corpus.corpus.state_adapters.missouri import (
    extract_missouri_revised_statutes,
)
from axiom_corpus.corpus.state_adapters.montana import (
    MONTANA_CODE_DEFAULT_YEAR,
    extract_montana_code,
)
from axiom_corpus.corpus.state_adapters.nevada import (
    NEVADA_NRS_DEFAULT_YEAR,
    extract_nevada_nrs,
)
from axiom_corpus.corpus.state_adapters.new_hampshire import extract_new_hampshire_rsa
from axiom_corpus.corpus.state_adapters.new_jersey import (
    NEW_JERSEY_STATUTES_ZIP_URL,
    extract_new_jersey_statutes,
)
from axiom_corpus.corpus.state_adapters.new_mexico import extract_new_mexico_statutes
from axiom_corpus.corpus.state_adapters.new_york import (
    extract_new_york_consolidated_laws,
    extract_new_york_openleg_api,
)
from axiom_corpus.corpus.state_adapters.oklahoma import extract_oklahoma_statutes
from axiom_corpus.corpus.state_adapters.oregon import (
    OREGON_ORS_DEFAULT_YEAR,
    extract_oregon_ors,
)
from axiom_corpus.corpus.state_adapters.pennsylvania import extract_pennsylvania_statutes
from axiom_corpus.corpus.state_adapters.rhode_island import (
    RHODE_ISLAND_GENERAL_LAWS_DEFAULT_YEAR,
    extract_rhode_island_general_laws,
)
from axiom_corpus.corpus.state_adapters.south_carolina import extract_south_carolina_code
from axiom_corpus.corpus.state_adapters.south_dakota import (
    extract_south_dakota_codified_laws,
)
from axiom_corpus.corpus.state_adapters.utah import (
    UTAH_CODE_SOURCE_URL,
    extract_utah_code,
)
from axiom_corpus.corpus.state_adapters.west_virginia import extract_west_virginia_code
from axiom_corpus.corpus.state_adapters.wisconsin import (
    WISCONSIN_STATUTES_TOC_URL,
    extract_wisconsin_statutes,
)
from axiom_corpus.corpus.state_statute_completion import (
    build_state_statute_completion_report,
    load_source_access_statuses,
)
from axiom_corpus.corpus.states import (
    StateStatuteExtractReport,
    extract_california_codes_bulk,
    extract_cic_html_release,
    extract_cic_odt_release,
    extract_colorado_docx_release,
    extract_dc_code,
    extract_minnesota_statutes,
    extract_nebraska_revised_statutes,
    extract_ohio_revised_code,
    extract_state_html_directory,
    extract_texas_tcas,
    extract_washington_rcw,
)
from axiom_corpus.corpus.supabase import (
    DEFAULT_ACCESS_TOKEN_ENV,
    DEFAULT_AXIOM_SUPABASE_URL,
    DEFAULT_SERVICE_KEY_ENV,
    delete_supabase_provisions_scope,
    fetch_provision_counts,
    load_provisions_to_supabase,
    resolve_service_key,
    sync_release_scopes_to_supabase,
    verify_release_coverage,
    write_supabase_rows_jsonl,
)
from axiom_corpus.corpus.usc import (
    build_usc_inventory_from_xml,
    decode_uslm_bytes,
    extract_usc,
    extract_usc_directory,
    infer_uslm_title,
    usc_run_id,
)


def _cmd_validate_manifest(args: argparse.Namespace) -> int:
    manifest = CorpusManifest.load(args.path)
    manifest.require_unique_sources()
    print(
        json.dumps(
            {
                "ok": True,
                "version": manifest.version,
                "sources": len(manifest.sources),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _cmd_inventory_ecfr(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    run_id = ecfr_run_id(args.version, args.only_title, args.only_part, args.limit)
    inventory = build_ecfr_inventory(
        as_of=args.as_of,
        only_title=args.only_title,
        only_part=args.only_part,
        limit=args.limit,
        run_id=run_id,
    )
    out = store.inventory_path("us", DocumentClass.REGULATION, run_id)
    store.write_inventory(out, inventory.items)
    print(
        json.dumps(
            {
                "jurisdiction": "us",
                "document_class": DocumentClass.REGULATION.value,
                "version": args.version,
                "run_id": run_id,
                "as_of": args.as_of,
                "title_count": inventory.title_count,
                "part_count": inventory.part_count,
                "items_written": len(inventory.items),
                "unique_citation_count": inventory.unique_citation_count,
                "written_to": str(out),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _cmd_inventory_usc(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    source_bytes = args.source_xml.read_bytes()
    xml_content = decode_uslm_bytes(source_bytes)
    title = args.title or infer_uslm_title(xml_content)
    run_id = usc_run_id(args.version, title, args.limit)
    inventory = build_usc_inventory_from_xml(
        xml_content,
        title=title,
        run_id=run_id,
        source_sha256=sha256_bytes(source_bytes),
        source_download_url=args.source_url,
        limit=args.limit,
    )
    out = store.inventory_path("us", DocumentClass.STATUTE, run_id)
    store.write_inventory(out, inventory.items)
    print(
        json.dumps(
            {
                "jurisdiction": "us",
                "document_class": DocumentClass.STATUTE.value,
                "version": args.version,
                "run_id": run_id,
                "title": title,
                "section_count": inventory.section_count,
                "items_written": len(inventory.items),
                "unique_citation_count": inventory.unique_citation_count,
                "written_to": str(out),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _cmd_export_supabase(args: argparse.Namespace) -> int:
    records = load_provisions(args.provisions)
    rows_written = write_supabase_rows_jsonl(args.output, records)
    print(
        json.dumps(
            {
                "rows_written": rows_written,
                "provisions": str(args.provisions),
                "output": str(args.output),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _cmd_load_supabase(args: argparse.Namespace) -> int:
    records = load_provisions(args.provisions)
    service_key = ""
    if not args.dry_run:
        service_key = resolve_service_key(
            args.supabase_url,
            service_key_env=args.service_key_env,
            access_token_env=args.access_token_env,
        )
    replace_report = None
    if args.replace_scope:
        jurisdiction, document_class = _single_provision_scope(records)
        replace_report = delete_supabase_provisions_scope(
            jurisdiction=jurisdiction,
            document_class=document_class,
            service_key=service_key,
            supabase_url=args.supabase_url,
            dry_run=args.dry_run,
            progress_stream=sys.stderr,
        )
    report = load_provisions_to_supabase(
        records,
        service_key=service_key,
        supabase_url=args.supabase_url,
        chunk_size=args.chunk_size,
        refresh=not args.skip_refresh,
        dry_run=args.dry_run,
        allow_refresh_failure=args.allow_refresh_failure,
        preserve_existing_ids=args.preserve_existing_ids and not args.replace_scope,
        progress_stream=sys.stderr,
    )
    payload = report.to_mapping()
    if replace_report is not None:
        payload["replace_scope"] = replace_report.to_mapping()
    payload["provisions"] = str(args.provisions)
    payload["supabase_url"] = args.supabase_url

    if args.build_navigation and not args.dry_run and report.rows_loaded:
        navigation_records: list[ProvisionRecord] = []
        existing_navigation_statuses: dict[str, str] = {}
        for jurisdiction, document_class in _provision_scopes(records):
            navigation_records.extend(
                fetch_provisions_for_navigation(
                    service_key=service_key,
                    supabase_url=args.supabase_url,
                    jurisdiction=jurisdiction,
                    doc_type=document_class,
                )
            )
            if args.preserve_navigation_statuses:
                existing_navigation_statuses.update(
                    fetch_navigation_statuses(
                        service_key=service_key,
                        supabase_url=args.supabase_url,
                        jurisdiction=jurisdiction,
                        doc_type=document_class,
                    )
                )
        encoded_paths = _resolve_encoded_paths(
            args, {jurisdiction for jurisdiction, _ in _provision_scopes(records)}
        )
        nodes = build_navigation_nodes(
            _apply_navigation_status_overrides(
                navigation_records,
                existing_statuses=existing_navigation_statuses,
                overrides=records,
            ),
            encoded_paths=encoded_paths,
        )
        navigation_report = write_navigation_nodes_to_supabase(
            nodes,
            service_key=service_key,
            supabase_url=args.supabase_url,
            chunk_size=args.chunk_size,
            replace_scope=True,
            replace_scopes=_provision_scopes(records),
            dry_run=False,
            progress_stream=sys.stderr,
        )
        payload["navigation"] = navigation_report.to_mapping()
    elif args.build_navigation and args.dry_run:
        payload["navigation"] = {"skipped": "dry-run"}

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_snapshot_provision_counts(args: argparse.Namespace) -> int:
    service_key = resolve_service_key(
        args.supabase_url,
        service_key_env=args.service_key_env,
        access_token_env=args.access_token_env,
    )
    rows = fetch_provision_counts(
        service_key=service_key,
        supabase_url=args.supabase_url,
        include_legacy=args.include_legacy,
    )
    payload: dict[str, object] = {"rows": list(rows)}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        payload["written_to"] = str(args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_sync_release_scopes(args: argparse.Namespace) -> int:
    release_path = resolve_release_manifest_path(args.base, args.release)
    release = ReleaseManifest.load(release_path)
    service_key = resolve_service_key(
        args.supabase_url,
        service_key_env=args.service_key_env,
        access_token_env=args.access_token_env,
    )
    report = sync_release_scopes_to_supabase(
        release,
        service_key=service_key,
        supabase_url=args.supabase_url,
        chunk_size=args.chunk_size,
        refresh=not args.skip_refresh,
        dry_run=args.dry_run,
        allow_refresh_failure=args.allow_refresh_failure,
    )
    payload = report.to_mapping()
    payload["release_path"] = str(release_path)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_verify_release_coverage(args: argparse.Namespace) -> int:
    service_key = resolve_service_key(
        args.supabase_url,
        service_key_env=args.service_key_env,
        access_token_env=args.access_token_env,
    )
    report = verify_release_coverage(
        service_key=service_key,
        supabase_url=args.supabase_url,
    )
    print(json.dumps(report.to_mapping(), indent=2, sort_keys=True))
    return 0 if report.ok else 2


def _cmd_build_navigation_index(args: argparse.Namespace) -> int:
    if args.all and not args.provisions and not args.from_supabase:
        raise SystemExit("build-navigation-index --all requires --provisions or --from-supabase")
    if not args.provisions and not args.from_supabase:
        raise SystemExit("build-navigation-index requires --provisions, --from-supabase, or --all")

    will_write_supabase = not args.skip_supabase and not args.dry_run
    service_key = ""
    if will_write_supabase or args.from_supabase:
        service_key = resolve_service_key(
            args.supabase_url,
            service_key_env=args.service_key_env,
            access_token_env=args.access_token_env,
        )

    records: tuple[ProvisionRecord, ...]
    existing_navigation_statuses: dict[str, str] = {}
    if args.provisions:
        loaded: list[ProvisionRecord] = []
        for path in args.provisions:
            loaded.extend(load_provisions(path))
        records = tuple(loaded)
        sources_used = [str(path) for path in args.provisions]
        # Preserve manually-set statuses from the live nav table when we are
        # going to write back, so a rebuild from a partial JSONL does not wipe
        # them. Fetch is scoped per (jurisdiction, doc_type) to avoid pulling
        # rows for unrelated scopes.
        if will_write_supabase and args.preserve_statuses:
            for jurisdiction, document_class in _provision_scopes(records):
                existing_navigation_statuses.update(
                    fetch_navigation_statuses(
                        service_key=service_key,
                        supabase_url=args.supabase_url,
                        jurisdiction=jurisdiction,
                        doc_type=document_class,
                    )
                )
    else:
        records = fetch_provisions_for_navigation(
            service_key=service_key,
            supabase_url=args.supabase_url,
            jurisdiction=args.jurisdiction,
            doc_type=args.doc_type,
        )
        if args.preserve_statuses:
            existing_navigation_statuses = fetch_navigation_statuses(
                service_key=service_key,
                supabase_url=args.supabase_url,
                jurisdiction=args.jurisdiction,
                doc_type=args.doc_type,
            )
        sources_used = [f"supabase:{args.supabase_url}"]

    encoded_jurisdictions = (
        {args.jurisdiction} if args.jurisdiction else {r.jurisdiction for r in records}
    )
    encoded_paths = _resolve_encoded_paths(args, encoded_jurisdictions)

    nodes: tuple[NavigationNode, ...] = build_navigation_nodes(
        _apply_navigation_status_overrides(
            records,
            existing_statuses=existing_navigation_statuses,
            overrides=records if args.provisions else (),
        ),
        jurisdiction=args.jurisdiction,
        document_class=args.doc_type,
        encoded_paths=encoded_paths,
    )

    payload: dict[str, object] = {
        "nodes_built": len(nodes),
        "provisions_input": len(records),
        "jurisdiction": args.jurisdiction,
        "doc_type": args.doc_type,
        "sources": sources_used,
        "preserved_status_count": len(existing_navigation_statuses),
        "encoded_paths_seen": len(encoded_paths),
        "nodes_with_rulespec": sum(1 for n in nodes if n.has_rulespec),
    }

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            "\n".join(json.dumps(node.to_supabase_row(), sort_keys=True) for node in nodes)
            + ("\n" if nodes else "")
        )
        payload["written_to"] = str(args.output)

    if args.skip_supabase:
        payload["skipped_supabase"] = True
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    replace_scope = _build_navigation_replace_scope(args)
    write_report = write_navigation_nodes_to_supabase(
        nodes,
        service_key=service_key,
        supabase_url=args.supabase_url,
        chunk_size=args.chunk_size,
        replace_scope=replace_scope,
        replace_scopes=_explicit_navigation_replace_scopes(args),
        dry_run=args.dry_run,
        progress_stream=sys.stderr,
    )
    payload["supabase"] = write_report.to_mapping()
    payload["supabase_url"] = args.supabase_url
    payload["replace_scope"] = replace_scope
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _provision_scopes(records: tuple[ProvisionRecord, ...]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted({(record.jurisdiction, record.document_class) for record in records}))


def _explicit_navigation_replace_scopes(args: argparse.Namespace) -> tuple[tuple[str, str], ...]:
    if args.jurisdiction and args.doc_type:
        return ((args.jurisdiction, args.doc_type),)
    return ()


def _build_navigation_replace_scope(args: argparse.Namespace) -> bool:
    if args.replace_scope is not None:
        return bool(args.replace_scope)
    return bool(args.from_supabase)


def _resolve_encoded_paths(
    args: argparse.Namespace,
    jurisdictions: Iterable[str],
) -> set[str]:
    """Combine ``--rulespec-repo`` and ``--rulespec-root`` flags into a set of
    canonical encoded citation paths for the given jurisdictions.

    ``--rulespec-repo`` is a repeatable explicit checkout, paired with the
    jurisdiction it covers. ``--rulespec-root`` points at a directory holding
    sibling ``rulespec-*`` checkouts and discovers each jurisdiction's repo by
    name. ``--rulespec-auto`` (the default) silently looks for
    ``../rulespec-{repo}`` next to this corpus checkout. Empty when no repo is
    on disk for any input jurisdiction.
    """
    encoded: set[str] = set()
    juris_list = sorted({j for j in jurisdictions if j})

    repos: list[tuple[str, Path]] = []
    for repo_arg in args.rulespec_repo or []:
        repo_path = Path(repo_arg)
        # Infer the jurisdiction from the repo dir name (rulespec-us-co -> us-co).
        repo_juris = _jurisdiction_for_repo_dir(repo_path.name)
        if repo_juris is None:
            print(
                f"warning: cannot infer jurisdiction from rulespec repo path {repo_path}",
                file=sys.stderr,
            )
            continue
        repos.append((repo_juris, repo_path))

    if args.rulespec_root:
        root_paths = [Path(p) for p in args.rulespec_root]
        for root in root_paths:
            for j, paths in discover_encoded_paths_for_jurisdictions(root, juris_list).items():
                encoded.update(paths)
                # Mark the repo seen so --rulespec-auto doesn't double-count.
                repo_dir_name = JURISDICTION_REPO_MAP.get(j)
                if repo_dir_name is not None:
                    repos.append((j, root / repo_dir_name))

    if args.rulespec_auto:
        # Auto-discover sibling rulespec-* checkouts next to this corpus repo.
        sibling_root = Path.cwd().parent
        for j in juris_list:
            repo_dir_name = JURISDICTION_REPO_MAP.get(j)
            if repo_dir_name is None:
                continue
            sibling = sibling_root / repo_dir_name
            if any(p.samefile(sibling) for _, p in repos if p.exists()):
                continue
            if sibling.is_dir():
                repos.append((j, sibling))

    for j, repo_path in repos:
        encoded.update(discover_encoded_paths(repo_path, j))
    return encoded


def _jurisdiction_for_repo_dir(repo_dir_name: str) -> str | None:
    for jurisdiction, name in JURISDICTION_REPO_MAP.items():
        if name == repo_dir_name:
            return jurisdiction
    return None


def _apply_navigation_status_overrides(
    records: Iterable[ProvisionRecord],
    *,
    existing_statuses: dict[str, str] | None = None,
    overrides: Iterable[ProvisionRecord],
) -> tuple[ProvisionRecord, ...]:
    """Inject curated navigation statuses onto a stream of provision records.

    Statuses are editorial metadata that don't live in `corpus.provisions`;
    extractors typically leave `metadata.status` unset. To keep manually
    curated statuses across rebuilds we resolve each record's status as:

    * If a record in ``overrides`` has a non-empty ``metadata.status`` it
      wins. Re-extracted source records can therefore introduce or change a
      status without colliding with curated state.
    * Otherwise we fall back to ``existing_statuses`` (typically a snapshot
      of the live `corpus.navigation_nodes.status` column).
    * Otherwise the record's own ``metadata.status`` (if any) is left alone.

    A ``None`` override is treated as "no opinion" rather than "clear" so
    that fresh source data doesn't accidentally wipe curated statuses.
    """
    overrides_with_status: dict[str, str] = {}
    for record in overrides:
        status = _navigation_status(record)
        if status is not None:
            overrides_with_status[record.citation_path] = status
    resolved: dict[str, str] = dict(existing_statuses or {})
    resolved.update(overrides_with_status)
    if not resolved:
        return tuple(records)
    updated: list[ProvisionRecord] = []
    for record in records:
        target = resolved.get(record.citation_path)
        if target is None:
            updated.append(record)
            continue
        if _navigation_status(record) == target:
            updated.append(record)
            continue
        metadata = dict(record.metadata or {})
        metadata["status"] = target
        updated.append(replace(record, metadata=metadata))
    return tuple(updated)


def _navigation_status(record: ProvisionRecord) -> str | None:
    if not record.metadata:
        return None
    status = record.metadata.get("status")
    if isinstance(status, str) and status.strip():
        return status.strip()
    return None


def _single_provision_scope(records: tuple[ProvisionRecord, ...]) -> tuple[str, str]:
    jurisdictions = {record.jurisdiction for record in records}
    document_classes = {record.document_class for record in records}
    if len(jurisdictions) != 1 or len(document_classes) != 1:
        raise ValueError("replace-scope requires one jurisdiction and one document class")
    return str(next(iter(jurisdictions))), str(next(iter(document_classes)))


def _cmd_extract_ecfr(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date or args.as_of)
    report = extract_ecfr(
        store,
        version=args.version,
        as_of=args.as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        only_part=args.only_part,
        limit=args.limit,
        workers=args.workers,
        progress_stream=sys.stderr,
    )
    print(
        json.dumps(
            {
                "jurisdiction": "us",
                "document_class": DocumentClass.REGULATION.value,
                "version": args.version,
                "as_of": args.as_of,
                "title_count": report.title_count,
                "part_count": report.part_count,
                "title_error_count": report.title_error_count,
                "title_errors": list(report.title_errors[:20]),
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_usc(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_usc(
        store,
        version=args.version,
        source_xml=args.source_xml,
        title=args.title,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        source_download_url=args.source_url,
        limit=args.limit,
    )
    print(
        json.dumps(
            {
                "jurisdiction": "us",
                "document_class": DocumentClass.STATUTE.value,
                "version": args.version,
                "title": report.title,
                "title_count": report.title_count,
                "section_count": report.section_count,
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_usc_dir(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_usc_directory(
        store,
        version=args.version,
        source_dir=args.source_dir,
        only_title=args.only_title,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        source_download_url=args.source_url,
        limit=args.limit,
    )
    print(
        json.dumps(
            {
                "jurisdiction": "us",
                "document_class": DocumentClass.STATUTE.value,
                "version": args.version,
                "run_title": report.title,
                "title_count": report.title_count,
                "section_count": report.section_count,
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_dc_code(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_dc_code(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": DocumentClass.STATUTE.value,
                "version": args.version,
                "title_count": report.title_count,
                "container_count": report.container_count,
                "section_count": report.section_count,
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_cic_html(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_cic_html_release(
        store,
        jurisdiction=args.jurisdiction,
        version=args.version,
        release_dir=args.release_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": DocumentClass.STATUTE.value,
                "version": args.version,
                "title_count": report.title_count,
                "container_count": report.container_count,
                "section_count": report.section_count,
                "skipped_source_count": report.skipped_source_count,
                "error_count": len(report.errors),
                "errors": list(report.errors[:20]),
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_cic_odt(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_cic_odt_release(
        store,
        jurisdiction=args.jurisdiction,
        version=args.version,
        release_dir=args.release_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": DocumentClass.STATUTE.value,
                "version": args.version,
                "title_count": report.title_count,
                "container_count": report.container_count,
                "section_count": report.section_count,
                "skipped_source_count": report.skipped_source_count,
                "error_count": len(report.errors),
                "errors": list(report.errors[:20]),
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_colorado_docx(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_colorado_docx_release(
        store,
        version=args.version,
        release_dir=args.release_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": DocumentClass.STATUTE.value,
                "version": args.version,
                "title_count": report.title_count,
                "container_count": report.container_count,
                "section_count": report.section_count,
                "skipped_source_count": report.skipped_source_count,
                "error_count": len(report.errors),
                "errors": list(report.errors[:20]),
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_texas_tcas(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_texas_tcas(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-tx-statutes",
                adapter="texas-tcas",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_ohio_revised_code(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_ohio_revised_code(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-oh-revised-code",
                adapter="ohio-revised-code",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_minnesota_statutes(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_minnesota_statutes(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-mn-statutes",
                adapter="minnesota-statutes",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_nebraska_revised_statutes(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_nebraska_revised_statutes(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-ne-revised-statutes",
                adapter="nebraska-revised-statutes",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_washington_rcw(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_washington_rcw(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-wa-rcw",
                adapter="washington-rcw",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_illinois_ilcs(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_illinois_ilcs(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_chapter=args.only_chapter,
        only_act=args.only_act,
        limit=args.limit,
        workers=args.workers,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-il-ilcs",
                adapter="illinois-ilcs",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_indiana_code(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_indiana_code(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_zip=args.source_zip,
        source_year=args.source_year,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-in-code",
                adapter="indiana-code",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_canada_acts(args: argparse.Namespace) -> int:
    from axiom_corpus.corpus.canada import extract_canada_acts

    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    only_acts = tuple(args.only_act) if args.only_act else None
    report = extract_canada_acts(
        store,
        version=args.version,
        only_acts=only_acts,
        limit_acts=args.limit_acts,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        progress_stream=sys.stderr,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": report.document_class,
                "version": args.version,
                "act_count": report.act_count,
                "section_count": report.section_count,
                "subsection_count": report.subsection_count,
                "skipped_act_count": report.skipped_act_count,
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
                "errors": list(report.errors[:20]),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_montana_code(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_montana_code(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_year=args.source_year,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-mt-code",
                adapter="montana-code",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_nevada_nrs(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_nevada_nrs(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_year=args.source_year,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        only_chapter=args.only_chapter,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-nv-nrs",
                adapter="nevada-nrs",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_new_york_consolidated_laws(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_new_york_consolidated_laws(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
        request_delay_seconds=args.request_delay_seconds,
        timeout_seconds=args.timeout_seconds,
        request_attempts=args.request_attempts,
        progress_stream=sys.stderr,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-ny-consolidated-laws",
                adapter="new-york-consolidated-laws",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if _state_statute_report_success(report) or args.allow_incomplete else 2


def _cmd_extract_new_york_openleg_api(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    api_key = os.environ.get(args.api_key_env) if args.api_key_env else None
    report = extract_new_york_openleg_api(
        store,
        version=args.version,
        api_key=api_key,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        download_dir=args.download_dir,
        api_base_url=args.api_base_url,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-ny-openleg-api",
                adapter="new-york-openleg-api",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if _state_statute_report_success(report) or args.allow_incomplete else 2


def _cmd_extract_delaware_code(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_delaware_code(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        only_chapter=args.only_chapter,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-de-code",
                adapter="delaware-code",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_oregon_ors(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_oregon_ors(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_year=args.source_year,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        only_chapter=args.only_chapter,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-or-ors",
                adapter="oregon-ors",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_rhode_island_general_laws(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_rhode_island_general_laws(
        store,
        version=args.version,
        source_dir=args.source_dir,
        source_year=args.source_year,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        only_chapter=args.only_chapter,
        limit=args.limit,
        workers=args.workers,
        download_dir=args.download_dir,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-ri-general-laws",
                adapter="rhode-island-general-laws",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_california_codes_bulk(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_california_codes_bulk(
        store,
        version=args.version,
        source_zip=args.source_zip,
        source_url=args.source_url,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        download_dir=args.download_dir,
        include_inactive=args.include_inactive,
    )
    print(
        json.dumps(
            _state_statute_report_payload(
                report,
                source_id="us-ca-codes",
                adapter="california-codes-bulk",
                version=args.version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_state_statutes(args: argparse.Namespace) -> int:
    manifest_path = args.manifest
    manifest = CorpusManifest.load(manifest_path)
    manifest.require_unique_sources()
    store = CorpusArtifactStore(args.base)
    selected = [
        source
        for source in manifest.sources
        if source.document_class == DocumentClass.STATUTE.value
        and (not args.only_jurisdiction or source.jurisdiction in args.only_jurisdiction)
        and (not args.only_source_id or source.source_id in args.only_source_id)
    ]
    if not selected:
        print(
            json.dumps(
                {
                    "ok": False,
                    "version": manifest.version,
                    "source_count": 0,
                    "error": "no matching statute sources",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    if args.dry_run:
        plan_rows = [
            _state_statute_plan_payload(
                source,
                manifest_path=manifest_path,
                manifest_version=manifest.version,
                limit_override=args.limit_per_source,
            )
            for source in selected
        ]
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "version": manifest.version,
                    "source_count": len(plan_rows),
                    "rows": plan_rows,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if all(row["source_path_exists"] for row in plan_rows) else 1

    rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for source in selected:
        try:
            report = _extract_state_statute_source(
                store,
                manifest_path=manifest_path,
                manifest_version=manifest.version,
                source=source,
                limit_override=args.limit_per_source,
            )
        except Exception as exc:
            failures.append(
                {
                    "source_id": source.source_id,
                    "jurisdiction": source.jurisdiction,
                    "adapter": source.adapter,
                    "error": str(exc),
                }
            )
            continue
        rows.append(
            _state_statute_report_payload(
                report,
                source_id=source.source_id,
                adapter=source.adapter,
                version=source.version or manifest.version,
            )
        )

    coverage_complete = bool(rows) and all(row["coverage_complete"] for row in rows)
    successful = bool(rows) and all(_state_statute_row_success(row) for row in rows)
    payload = {
        "version": manifest.version,
        "source_count": len(selected),
        "completed_count": len(rows),
        "failed_count": len(failures),
        "coverage_complete": coverage_complete,
        "successful": successful,
        "provisions_written": sum(int(row["provisions_written"]) for row in rows),
        "rows": rows,
        "failures": failures,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if failures:
        return 1
    return 0 if successful or args.allow_incomplete else 2


def _extract_state_statute_source(
    store: CorpusArtifactStore,
    *,
    manifest_path: Path,
    manifest_version: str,
    source: CorpusSource,
    limit_override: int | None,
) -> StateStatuteExtractReport:
    options = _state_source_options(source)
    adapter = _canonical_state_statute_adapter(source.adapter)
    version = source.version or manifest_version
    source_as_of = _optional_text(options.get("source_as_of"))
    expression_date = _optional_text(options.get("expression_date"))
    only_title = _optional_text(options.get("only_title"))
    limit = limit_override if limit_override is not None else _optional_int(options.get("limit"))
    if adapter == "alabama-code":
        return extract_alabama_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            graphql_url=_optional_text(options.get("graphql_url"))
            or "https://alison.legislature.state.al.us/graphql",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.05,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 90.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            page_size=_optional_int(options.get("page_size")) or 1000,
        )
    if adapter == "alaska-statutes":
        return extract_alaska_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_year=_optional_int(options.get("source_year"))
            or ALASKA_STATUTES_DEFAULT_YEAR,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://www.akleg.gov/basis/statutes.asp",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.05,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 30.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 1,
        )
    if adapter == "arizona-revised-statutes":
        return extract_arizona_revised_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url")) or "https://www.azleg.gov",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.03,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 30.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 8,
        )
    if adapter == "connecticut-statutes":
        return extract_connecticut_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            current_base_url=_optional_text(options.get("current_base_url"))
            or "https://www.cga.ct.gov/current/pub/",
            supplement_base_url=_optional_text(options.get("supplement_base_url"))
            or "https://www.cga.ct.gov/2026/sup/",
            include_supplement=_optional_bool(options.get("include_supplement"), default=True),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.05,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            verify_ssl=_optional_bool(options.get("verify_ssl"), default=True),
        )
    if adapter == "florida-statutes":
        return extract_florida_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_year=_optional_int(options.get("source_year"))
            or FLORIDA_STATUTES_DEFAULT_YEAR,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            source_zip=_optional_manifest_path(manifest_path, options, "source_zip"),
            source_zip_url=_optional_text(options.get("source_zip_url")),
            base_url=_optional_text(options.get("base_url"))
            or "https://www.leg.state.fl.us/statutes/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.05,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "hawaii-revised-statutes":
        return extract_hawaii_revised_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://data.capitol.hawaii.gov/hrscurrent/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 8,
        )
    if adapter == "kansas-statutes":
        return extract_kansas_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url")) or "https://ksrevisor.gov/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.03,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 8,
        )
    if adapter == "louisiana-revised-statutes":
        return extract_louisiana_revised_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://www.legis.la.gov/Legis/",
            root_folder=_optional_text(options.get("root_folder")) or "75",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 8,
        )
    if adapter == "dc-code":
        return extract_dc_code(
            store,
            version=version,
            source_dir=_required_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
        )
    if adapter == "cic-html":
        return extract_cic_html_release(
            store,
            jurisdiction=source.jurisdiction,
            version=version,
            release_dir=_required_manifest_path(manifest_path, options, "release_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
        )
    if adapter == "cic-odt":
        return extract_cic_odt_release(
            store,
            jurisdiction=source.jurisdiction,
            version=version,
            release_dir=_required_manifest_path(manifest_path, options, "release_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
        )
    if adapter == "colorado-docx":
        return extract_colorado_docx_release(
            store,
            version=version,
            release_dir=_required_manifest_path(manifest_path, options, "release_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
        )
    if adapter == "local-state-html":
        return extract_state_html_directory(
            store,
            jurisdiction=source.jurisdiction,
            version=version,
            source_dir=_required_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
        )
    if adapter == "texas-tcas":
        return extract_texas_tcas(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 4,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "ohio-revised-code":
        return extract_ohio_revised_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "minnesota-statutes":
        return extract_minnesota_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 4,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "nebraska-revised-statutes":
        return extract_nebraska_revised_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 4,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "washington-rcw":
        return extract_washington_rcw(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 4,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "illinois-ilcs":
        return extract_illinois_ilcs(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_chapter=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
        )
    if adapter == "indiana-code":
        return extract_indiana_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_zip=_optional_manifest_path(manifest_path, options, "source_zip"),
            source_year=_optional_int(options.get("source_year")) or INDIANA_CODE_DEFAULT_YEAR,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "iowa-code":
        return extract_iowa_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_year=_optional_int(options.get("source_year")) or IOWA_CODE_DEFAULT_YEAR,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.05,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 1,
        )
    if adapter == "idaho-statutes":
        return extract_idaho_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://legislature.idaho.gov/statutesrules/idstat/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.05,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 1,
        )
    if adapter == "maine-revised-statutes":
        return extract_maine_revised_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://legislature.maine.gov/statutes/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "maryland-code":
        return extract_maryland_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_article=_optional_text(options.get("only_article")) or only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://mgaleg.maryland.gov/mgawebsite",
            include_constitution=_optional_bool(options.get("include_constitution"), default=False),
            enactments=_optional_bool(options.get("enactments"), default=False),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "massachusetts-general-laws":
        return extract_massachusetts_general_laws(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_part=_optional_text(options.get("only_part")),
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url")) or "https://malegislature.gov",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "michigan-compiled-laws":
        return extract_michigan_compiled_laws(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://legislature.mi.gov/documents/mcl/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 120.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "missouri-revised-statutes":
        return extract_missouri_revised_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://revisor.mo.gov/main/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "new-hampshire-rsa":
        return extract_new_hampshire_rsa(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 1,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://gc.nh.gov/rsa/html/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.25,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 30.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 2,
        )
    if adapter == "new-jersey-statutes":
        return extract_new_jersey_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_zip=_optional_manifest_path(manifest_path, options, "source_zip"),
            source_url=_optional_text(options.get("source_url"))
            or source.source_url
            or NEW_JERSEY_STATUTES_ZIP_URL,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 180.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "oklahoma-statutes":
        return extract_oklahoma_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 4,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url"))
            or "https://www.oklegislature.gov/OK_Statutes/CompleteTitles/",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.05,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "south-dakota-codified-laws":
        return extract_south_dakota_codified_laws(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            base_url=_optional_text(options.get("base_url")) or "https://sdlegislature.gov",
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "utah-code":
        return extract_utah_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_url=_optional_text(options.get("source_url"))
            or source.source_url
            or UTAH_CODE_SOURCE_URL,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 60.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 8,
        )
    if adapter == "wisconsin-statutes":
        return extract_wisconsin_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_url=_optional_text(options.get("source_url"))
            or source.source_url
            or WISCONSIN_STATUTES_TOC_URL,
            base_url=_optional_text(options.get("base_url"))
            or "https://docs.legis.wisconsin.gov",
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.02,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 90.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
            workers=_optional_int(options.get("workers")) or 8,
        )
    if adapter == "montana-code":
        return extract_montana_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_year=_optional_int(options.get("source_year")) or MONTANA_CODE_DEFAULT_YEAR,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "nevada-nrs":
        return extract_nevada_nrs(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_year=_optional_int(options.get("source_year")) or NEVADA_NRS_DEFAULT_YEAR,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "new-york-consolidated-laws":
        return extract_new_york_consolidated_laws(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            workers=_optional_int(options.get("workers")) or 1,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.35,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 15.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 2,
        )
    if adapter == "new-york-openleg-api":
        api_key_env = _optional_text(options.get("api_key_env")) or "NYSENATE_OPENLEG_API_KEY"
        return extract_new_york_openleg_api(
            store,
            version=version,
            api_key=os.environ.get(api_key_env),
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            api_base_url=_optional_text(options.get("api_base_url"))
            or "https://legislation.nysenate.gov",
        )
    if adapter == "delaware-code":
        return extract_delaware_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            workers=_optional_int(options.get("workers")) or 1,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "oregon-ors":
        return extract_oregon_ors(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_year=_optional_int(options.get("source_year")) or OREGON_ORS_DEFAULT_YEAR,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "pennsylvania-statutes":
        return extract_pennsylvania_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.2,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 120.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "south-carolina-code":
        return extract_south_carolina_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.15,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 90.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "west-virginia-code":
        return extract_west_virginia_code(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_chapter=_optional_text(options.get("only_chapter")) or only_title,
            only_article=_optional_text(options.get("only_article")),
            limit=limit,
            workers=_optional_int(options.get("workers")) or 1,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.05,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 90.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "new-mexico-statutes":
        return extract_new_mexico_statutes(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            request_delay_seconds=_optional_float(options.get("request_delay_seconds")) or 0.1,
            timeout_seconds=_optional_float(options.get("timeout_seconds")) or 90.0,
            request_attempts=_optional_int(options.get("request_attempts")) or 3,
        )
    if adapter == "rhode-island-general-laws":
        return extract_rhode_island_general_laws(
            store,
            version=version,
            source_dir=_optional_manifest_path(manifest_path, options, "source_dir"),
            source_year=_optional_int(options.get("source_year"))
            or RHODE_ISLAND_GENERAL_LAWS_DEFAULT_YEAR,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            only_chapter=_optional_text(options.get("only_chapter")),
            limit=limit,
            workers=_optional_int(options.get("workers")) or 8,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
        )
    if adapter == "california-codes-bulk":
        return extract_california_codes_bulk(
            store,
            version=version,
            source_zip=_optional_manifest_path(manifest_path, options, "source_zip"),
            source_url=source.source_url,
            source_as_of=source_as_of,
            expression_date=expression_date,
            only_title=only_title,
            limit=limit,
            download_dir=_optional_manifest_path(manifest_path, options, "download_dir"),
            include_inactive=bool(options.get("include_inactive", False)),
        )
    raise ValueError(f"unsupported state statute adapter: {source.adapter}")


def _state_statute_plan_payload(
    source: CorpusSource,
    *,
    manifest_path: Path,
    manifest_version: str,
    limit_override: int | None,
) -> dict[str, Any]:
    options = _state_source_options(source)
    adapter = _canonical_state_statute_adapter(source.adapter)
    path_key = "source_dir" if adapter in {"dc-code", "local-state-html"} else "release_dir"
    source_path = _state_statute_source_path_for_plan(
        adapter,
        manifest_path=manifest_path,
        options=options,
        path_key=path_key,
    )
    return {
        "source_id": source.source_id,
        "jurisdiction": source.jurisdiction,
        "document_class": source.document_class,
        "adapter": adapter,
        "version": source.version or manifest_version,
        "source_path": str(source_path) if source_path is not None else None,
        "source_path_exists": True if source_path is None else source_path.exists(),
        "only_title": _optional_text(options.get("only_title")),
        "limit": (
            limit_override if limit_override is not None else _optional_int(options.get("limit"))
        ),
    }


def _state_statute_report_payload(
    report: StateStatuteExtractReport,
    *,
    source_id: str,
    adapter: str,
    version: str,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "adapter": _canonical_state_statute_adapter(adapter),
        "jurisdiction": report.jurisdiction,
        "document_class": DocumentClass.STATUTE.value,
        "version": version,
        "title_count": report.title_count,
        "container_count": report.container_count,
        "section_count": report.section_count,
        "skipped_source_count": report.skipped_source_count,
        "error_count": len(report.errors),
        "errors": list(report.errors[:20]),
        "source_file_count": len(report.source_paths),
        "provisions_written": report.provisions_written,
        "inventory_path": str(report.inventory_path),
        "provisions_path": str(report.provisions_path),
        "coverage_path": str(report.coverage_path),
        "coverage_complete": report.coverage.complete,
        "source_count": report.coverage.source_count,
        "provision_count": report.coverage.provision_count,
        "matched_count": report.coverage.matched_count,
        "missing_count": len(report.coverage.missing_from_provisions),
        "extra_count": len(report.coverage.extra_provisions),
    }


def _state_statute_row_success(row: dict[str, Any]) -> bool:
    return (
        bool(row["coverage_complete"])
        and int(row["skipped_source_count"]) == 0
        and int(row["error_count"]) == 0
    )


def _state_statute_report_success(report: StateStatuteExtractReport) -> bool:
    return (
        report.coverage.complete
        and report.skipped_source_count == 0
        and len(report.errors) == 0
    )


def _state_source_options(source: CorpusSource) -> dict[str, Any]:
    if source.options is None:
        return {}
    return dict(source.options)


def _canonical_state_statute_adapter(adapter: str) -> str:
    normalized = adapter.lower().replace("_", "-")
    aliases = {
        "dc": "dc-code",
        "dc-code": "dc-code",
        "dc-law-xml": "dc-code",
        "cic-html": "cic-html",
        "cic-state-code-html": "cic-html",
        "cic-odt": "cic-odt",
        "cic-state-code-odt": "cic-odt",
        "state-html": "local-state-html",
        "state-html-directory": "local-state-html",
        "local-state-html": "local-state-html",
        "legacy-state-html": "local-state-html",
        "al": "alabama-code",
        "alabama": "alabama-code",
        "alabama-code": "alabama-code",
        "alabama-code-graphql": "alabama-code",
        "ak": "alaska-statutes",
        "alaska": "alaska-statutes",
        "alaska-statutes": "alaska-statutes",
        "alaska-statutes-html": "alaska-statutes",
        "az": "arizona-revised-statutes",
        "arizona": "arizona-revised-statutes",
        "arizona-revised-statutes": "arizona-revised-statutes",
        "arizona-ars": "arizona-revised-statutes",
        "ars": "arizona-revised-statutes",
        "ct": "connecticut-statutes",
        "connecticut": "connecticut-statutes",
        "connecticut-statutes": "connecticut-statutes",
        "connecticut-general-statutes": "connecticut-statutes",
        "cga-ct": "connecticut-statutes",
        "fl": "florida-statutes",
        "florida": "florida-statutes",
        "florida-statutes": "florida-statutes",
        "florida-statutes-html": "florida-statutes",
        "hi": "hawaii-revised-statutes",
        "hawaii": "hawaii-revised-statutes",
        "hawaii-revised-statutes": "hawaii-revised-statutes",
        "hawaii-hrs": "hawaii-revised-statutes",
        "hrs": "hawaii-revised-statutes",
        "ks": "kansas-statutes",
        "kansas": "kansas-statutes",
        "kansas-statutes": "kansas-statutes",
        "kansas-statutes-html": "kansas-statutes",
        "kansas-ksa": "kansas-statutes",
        "ksa": "kansas-statutes",
        "la": "louisiana-revised-statutes",
        "louisiana": "louisiana-revised-statutes",
        "louisiana-revised-statutes": "louisiana-revised-statutes",
        "louisiana-rs": "louisiana-revised-statutes",
        "la-rs": "louisiana-revised-statutes",
        "colorado-docx": "colorado-docx",
        "colorado-crs-docx": "colorado-docx",
        "ohio": "ohio-revised-code",
        "ohio-revised-code": "ohio-revised-code",
        "orc": "ohio-revised-code",
        "minnesota": "minnesota-statutes",
        "minnesota-statutes": "minnesota-statutes",
        "mn": "minnesota-statutes",
        "nebraska": "nebraska-revised-statutes",
        "nebraska-revised-statutes": "nebraska-revised-statutes",
        "neb-rev-stat": "nebraska-revised-statutes",
        "ne": "nebraska-revised-statutes",
        "washington": "washington-rcw",
        "washington-rcw": "washington-rcw",
        "rcw": "washington-rcw",
        "wa": "washington-rcw",
        "il": "illinois-ilcs",
        "ilcs": "illinois-ilcs",
        "illinois": "illinois-ilcs",
        "illinois-ilcs": "illinois-ilcs",
        "in": "indiana-code",
        "indiana": "indiana-code",
        "indiana-code": "indiana-code",
        "indiana-code-html": "indiana-code",
        "ia": "iowa-code",
        "iowa": "iowa-code",
        "iowa-code": "iowa-code",
        "iowa-code-html": "iowa-code",
        "id": "idaho-statutes",
        "idaho": "idaho-statutes",
        "idaho-statutes": "idaho-statutes",
        "idaho-code": "idaho-statutes",
        "idaho-statutes-html": "idaho-statutes",
        "me": "maine-revised-statutes",
        "maine": "maine-revised-statutes",
        "maine-revised-statutes": "maine-revised-statutes",
        "maine-statutes": "maine-revised-statutes",
        "mrs": "maine-revised-statutes",
        "maine-revised-statutes-html": "maine-revised-statutes",
        "md": "maryland-code",
        "maryland": "maryland-code",
        "maryland-code": "maryland-code",
        "maryland-statutes": "maryland-code",
        "maryland-code-html": "maryland-code",
        "maryland-mga": "maryland-code",
        "ma": "massachusetts-general-laws",
        "massachusetts": "massachusetts-general-laws",
        "massachusetts-general-laws": "massachusetts-general-laws",
        "massachusetts-statutes": "massachusetts-general-laws",
        "mass-general-laws": "massachusetts-general-laws",
        "massachusetts-general-laws-html": "massachusetts-general-laws",
        "mgl": "massachusetts-general-laws",
        "mi": "michigan-compiled-laws",
        "michigan": "michigan-compiled-laws",
        "michigan-compiled-laws": "michigan-compiled-laws",
        "michigan-mcl": "michigan-compiled-laws",
        "mcl": "michigan-compiled-laws",
        "mo": "missouri-revised-statutes",
        "missouri": "missouri-revised-statutes",
        "missouri-revised-statutes": "missouri-revised-statutes",
        "missouri-rs": "missouri-revised-statutes",
        "rsmo": "missouri-revised-statutes",
        "mt": "montana-code",
        "montana": "montana-code",
        "montana-code": "montana-code",
        "montana-code-html": "montana-code",
        "mca": "montana-code",
        "nv": "nevada-nrs",
        "nevada": "nevada-nrs",
        "nevada-nrs": "nevada-nrs",
        "nrs": "nevada-nrs",
        "nevada-nrs-html": "nevada-nrs",
        "nh": "new-hampshire-rsa",
        "new-hampshire": "new-hampshire-rsa",
        "new-hampshire-rsa": "new-hampshire-rsa",
        "new-hampshire-statutes": "new-hampshire-rsa",
        "nh-rsa": "new-hampshire-rsa",
        "rsa-nh": "new-hampshire-rsa",
        "nj": "new-jersey-statutes",
        "new-jersey": "new-jersey-statutes",
        "new-jersey-statutes": "new-jersey-statutes",
        "new-jersey-statutes-text": "new-jersey-statutes",
        "nj-statutes": "new-jersey-statutes",
        "njsa": "new-jersey-statutes",
        "ok": "oklahoma-statutes",
        "oklahoma": "oklahoma-statutes",
        "oklahoma-statutes": "oklahoma-statutes",
        "ok-statutes": "oklahoma-statutes",
        "sd": "south-dakota-codified-laws",
        "south-dakota": "south-dakota-codified-laws",
        "south-dakota-codified-laws": "south-dakota-codified-laws",
        "south-dakota-statutes": "south-dakota-codified-laws",
        "sdcl": "south-dakota-codified-laws",
        "ut": "utah-code",
        "utah": "utah-code",
        "utah-code": "utah-code",
        "utah-code-xml": "utah-code",
        "ut-code": "utah-code",
        "wi": "wisconsin-statutes",
        "wisconsin": "wisconsin-statutes",
        "wisconsin-statutes": "wisconsin-statutes",
        "wisconsin-code": "wisconsin-statutes",
        "wi-statutes": "wisconsin-statutes",
        "ny": "new-york-openleg-api",
        "new-york": "new-york-openleg-api",
        "new-york-openleg-api": "new-york-openleg-api",
        "ny-openleg": "new-york-openleg-api",
        "openleg": "new-york-openleg-api",
        "new-york-consolidated-laws": "new-york-consolidated-laws",
        "nysenate": "new-york-consolidated-laws",
        "ny-senate": "new-york-consolidated-laws",
        "de": "delaware-code",
        "delaware": "delaware-code",
        "delaware-code": "delaware-code",
        "delaware-code-html": "delaware-code",
        "or": "oregon-ors",
        "oregon": "oregon-ors",
        "oregon-ors": "oregon-ors",
        "oregon-ors-html": "oregon-ors",
        "ors": "oregon-ors",
        "pa": "pennsylvania-statutes",
        "pennsylvania": "pennsylvania-statutes",
        "pennsylvania-statutes": "pennsylvania-statutes",
        "pennsylvania-consolidated-statutes": "pennsylvania-statutes",
        "pennsylvania-consolidated-statutes-html": "pennsylvania-statutes",
        "pacode": "pennsylvania-statutes",
        "pa-consolidated-statutes": "pennsylvania-statutes",
        "sc": "south-carolina-code",
        "south-carolina": "south-carolina-code",
        "south-carolina-code": "south-carolina-code",
        "south-carolina-code-html": "south-carolina-code",
        "sc-code": "south-carolina-code",
        "wv": "west-virginia-code",
        "west-virginia": "west-virginia-code",
        "west-virginia-code": "west-virginia-code",
        "west-virginia-code-html": "west-virginia-code",
        "wv-code": "west-virginia-code",
        "nm": "new-mexico-statutes",
        "new-mexico": "new-mexico-statutes",
        "new-mexico-statutes": "new-mexico-statutes",
        "new-mexico-nmsa": "new-mexico-statutes",
        "nmone": "new-mexico-statutes",
        "nmonesource": "new-mexico-statutes",
        "ri": "rhode-island-general-laws",
        "rhode-island": "rhode-island-general-laws",
        "rhode-island-general-laws": "rhode-island-general-laws",
        "rhode-island-general-laws-html": "rhode-island-general-laws",
        "rigl": "rhode-island-general-laws",
        "ca": "california-codes-bulk",
        "california": "california-codes-bulk",
        "california-codes": "california-codes-bulk",
        "california-codes-bulk": "california-codes-bulk",
        "california-leginfo": "california-codes-bulk",
        "ca-leginfo": "california-codes-bulk",
        "texas-tcas": "texas-tcas",
        "texas-api": "texas-tcas",
        "tcas": "texas-tcas",
    }
    if normalized not in aliases:
        raise ValueError(f"unsupported state statute adapter: {adapter}")
    return aliases[normalized]


def _required_manifest_path(
    manifest_path: Path,
    options: dict[str, Any],
    key: str,
) -> Path:
    value = options.get(key)
    if value is None:
        raise ValueError(f"missing required option: {key}")
    path = Path(str(value))
    if not path.is_absolute():
        path = manifest_path.parent / path
    return path


def _state_statute_source_path_for_plan(
    adapter: str,
    *,
    manifest_path: Path,
    options: dict[str, Any],
    path_key: str,
) -> Path | None:
    if adapter in {
        "alabama-code",
        "alaska-statutes",
        "arizona-revised-statutes",
        "connecticut-statutes",
        "florida-statutes",
        "hawaii-revised-statutes",
        "kansas-statutes",
        "louisiana-revised-statutes",
        "minnesota-statutes",
        "nebraska-revised-statutes",
        "ohio-revised-code",
        "texas-tcas",
        "washington-rcw",
        "illinois-ilcs",
        "indiana-code",
        "iowa-code",
        "idaho-statutes",
        "maine-revised-statutes",
        "maryland-code",
        "massachusetts-general-laws",
        "michigan-compiled-laws",
        "missouri-revised-statutes",
        "montana-code",
        "nevada-nrs",
        "new-hampshire-rsa",
        "new-jersey-statutes",
        "oklahoma-statutes",
        "south-dakota-codified-laws",
        "utah-code",
        "wisconsin-statutes",
        "new-york-consolidated-laws",
        "new-york-openleg-api",
        "delaware-code",
        "oregon-ors",
        "pennsylvania-statutes",
        "south-carolina-code",
        "west-virginia-code",
        "new-mexico-statutes",
        "rhode-island-general-laws",
    }:
        return _optional_manifest_path(manifest_path, options, "source_dir") or (
            _optional_manifest_path(manifest_path, options, "source_zip")
            if adapter == "indiana-code"
            else None
        )
    if adapter == "california-codes-bulk":
        return _optional_manifest_path(manifest_path, options, "source_zip")
    if adapter == "local-state-html":
        return _required_manifest_path(manifest_path, options, "source_dir")
    return _required_manifest_path(manifest_path, options, path_key)


def _optional_manifest_path(
    manifest_path: Path,
    options: dict[str, Any],
    key: str,
) -> Path | None:
    value = options.get(key)
    if value is None:
        return None
    path = Path(str(value))
    if not path.is_absolute():
        path = manifest_path.parent / path
    return path


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _optional_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def _cmd_extract_colorado_ccr(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_colorado_ccr(
        store,
        version=args.version,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_series=args.only_series,
        limit=args.limit,
        workers=args.workers,
        release_dir=args.release_dir,
        download_dir=args.download_dir,
        progress_stream=sys.stderr,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": report.document_class,
                "version": args.version,
                "document_count": report.document_count,
                "section_count": report.section_count,
                "skipped_source_count": report.skipped_source_count,
                "error_count": len(report.errors),
                "errors": list(report.errors[:20]),
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_nycrr(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_nycrr(
        store,
        version=args.version,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_title=args.only_title,
        limit=args.limit,
        delay_seconds=args.delay_seconds,
        retry_attempts=args.retry_attempts,
        refresh=args.refresh,
        progress_stream=sys.stderr,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": report.document_class,
                "version": args.version,
                "page_count": report.page_count,
                "browse_page_count": report.browse_page_count,
                "document_page_count": report.document_page_count,
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_california_mpp_calfresh(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    docx_sources = _load_california_mpp_docx_sources(args.manifest)
    report = extract_california_mpp_calfresh(
        store,
        version=args.version,
        docx_sources=docx_sources,
        download_dir=args.download_dir,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        request_delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        request_attempts=args.request_attempts,
    )
    print(
        json.dumps(
            {
                "adapter": "california-mpp-calfresh",
                "jurisdiction": report.jurisdiction,
                "document_class": report.document_class,
                "version": args.version,
                "source_file_count": len(report.source_paths),
                "section_count": report.section_count,
                "subsection_count": report.subsection_count,
                "container_count": report.container_count,
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _load_california_mpp_docx_sources(manifest_path: Path) -> tuple[MppDocxSource, ...]:
    """Load DOCX source declarations from the CA MPP manifest."""
    import yaml  # local import — cli.py already pulls yaml via other paths

    manifest = yaml.safe_load(manifest_path.read_text())
    sources = manifest.get("sources") or []
    if not sources:
        raise ValueError(f"manifest has no sources entry: {manifest_path}")
    # MVP: take the first source block. Multi-source manifests can grow later.
    options = sources[0].get("options") or {}
    docx_sources_raw = options.get("docx_sources") or []
    if not docx_sources_raw:
        raise ValueError(f"manifest has no docx_sources under options: {manifest_path}")
    return tuple(
        MppDocxSource(
            file=str(entry["file"]),
            url=str(entry["url"]),
            chapter=str(entry["chapter"]),
            sections=tuple(str(s) for s in entry.get("sections", ())),
            summary=str(entry.get("summary", "")),
        )
        for entry in docx_sources_raw
    )


def _cmd_extract_ny_state_register(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_ny_state_register(
        store,
        version=args.version,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        limit=args.limit,
        progress_stream=sys.stderr,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": report.document_class,
                "version": args.version,
                "issue_count": report.issue_count,
                "notice_count": report.notice_count,
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_extract_official_documents(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    expression_date = date.fromisoformat(args.expression_date) if args.expression_date else None
    report = extract_official_documents(
        store,
        manifest_path=args.manifest,
        version=args.version,
        source_as_of=args.source_as_of,
        expression_date=expression_date,
        only_source_id=args.only_source_id,
        limit=args.limit,
        progress_stream=sys.stderr,
    )
    print(
        json.dumps(
            {
                "jurisdiction": report.jurisdiction,
                "document_class": report.document_class,
                "version": args.version,
                "document_count": report.document_count,
                "block_count": report.block_count,
                "source_file_count": len(report.source_paths),
                "provisions_written": report.provisions_written,
                "inventory_path": str(report.inventory_path),
                "provisions_path": str(report.provisions_path),
                "coverage_path": str(report.coverage_path),
                "coverage_complete": report.coverage.complete,
                "source_count": report.coverage.source_count,
                "provision_count": report.coverage.provision_count,
                "matched_count": report.coverage.matched_count,
                "missing_count": len(report.coverage.missing_from_provisions),
                "extra_count": len(report.coverage.extra_provisions),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.coverage.complete or args.allow_incomplete else 2


def _cmd_coverage(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    source_inventory = load_source_inventory(args.source_inventory)
    provisions = load_provisions(args.provisions)
    report = compare_provision_coverage(
        source_inventory,
        provisions,
        jurisdiction=args.jurisdiction,
        document_class=args.document_class,
        version=args.version,
    )
    payload = report.to_mapping()
    if args.write:
        out = store.coverage_path(args.jurisdiction, args.document_class, args.version)
        store.write_json(out, payload)
        payload["written_to"] = str(out)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if report.complete or args.allow_incomplete else 2


def _cmd_analytics(args: argparse.Namespace) -> int:
    store = CorpusArtifactStore(args.base)
    report = build_analytics_report(
        store,
        version=args.version,
        provision_counts=load_provision_count_snapshot(
            args.supabase_counts,
            default_document_class=args.default_count_document_class,
        ),
        jurisdictions=tuple(args.jurisdiction),
        document_classes=tuple(args.document_class),
    )
    payload = report.to_mapping()
    if args.write or args.output:
        out = args.output or (store.root / "analytics" / f"{args.version}.json")
        store.write_json(out, payload)
        payload["written_to"] = str(out)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_sync_r2(args: argparse.Namespace) -> int:
    config = load_r2_config(
        credential_path=args.credentials_file,
        bucket=args.bucket,
        endpoint_url=args.endpoint_url,
    )
    report = sync_artifacts_to_r2(
        args.base,
        config=config,
        prefixes=tuple(args.prefix or DEFAULT_ARTIFACT_PREFIXES),
        jurisdiction=args.jurisdiction,
        document_class=args.document_class,
        version=args.version,
        dry_run=not args.apply,
        limit=args.limit,
        workers=args.workers,
        force=args.force,
        progress_stream=sys.stderr,
    )
    print(json.dumps(report.to_mapping(), indent=2, sort_keys=True))
    return 0


def _cmd_artifact_report(args: argparse.Namespace) -> int:
    prefixes = tuple(args.prefix or DEFAULT_ARTIFACT_PREFIXES)
    release = None
    release_path = None
    release_name = args.release
    if release_name is None and not args.all_scopes and not _artifact_scope_filter_supplied(args):
        current_release = resolve_release_manifest_path(args.base, "current")
        if current_release.exists():
            release_name = "current"
    if release_name:
        release_path = resolve_release_manifest_path(args.base, release_name)
        release = ReleaseManifest.load(release_path)
    if args.include_r2:
        config = load_r2_config(
            credential_path=args.credentials_file,
            bucket=args.bucket,
            endpoint_url=args.endpoint_url,
        )
        report = build_artifact_report_with_r2(
            args.base,
            config=config,
            prefixes=prefixes,
            version=args.version,
            jurisdiction=args.jurisdiction,
            document_class=args.document_class,
            supabase_counts_path=args.supabase_counts,
            release_name=release.name if release else None,
            release_scopes=release.scope_keys if release else None,
        )
    else:
        report = build_artifact_report(
            args.base,
            prefixes=prefixes,
            version=args.version,
            jurisdiction=args.jurisdiction,
            document_class=args.document_class,
            supabase_counts_path=args.supabase_counts,
            release_name=release.name if release else None,
            release_scopes=release.scope_keys if release else None,
        )
    payload = report.to_mapping()
    if release_path:
        payload["release_path"] = str(release_path)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        payload["written_to"] = str(args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_release_artifact_manifest(args: argparse.Namespace) -> int:
    release_path = resolve_release_manifest_path(args.base, args.release)
    release = ReleaseManifest.load(release_path)
    manifest = build_release_artifact_manifest(
        args.base,
        release_name=release.name,
        release_scopes=release.scope_keys,
        prefixes=tuple(args.prefix or DEFAULT_RELEASE_ARTIFACT_PREFIXES),
    )
    payload = manifest.to_mapping()
    payload["release_path"] = str(release_path)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        payload["written_to"] = str(args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _cmd_validate_release(args: argparse.Namespace) -> int:
    release_path = resolve_release_manifest_path(args.base, args.release)
    release = ReleaseManifest.load(release_path)
    prefixes = tuple(args.prefix or DEFAULT_RELEASE_ARTIFACT_PREFIXES)
    if args.include_r2:
        config = load_r2_config(
            credential_path=args.credentials_file,
            bucket=args.bucket,
            endpoint_url=args.endpoint_url,
        )
        artifact_report = build_artifact_report_with_r2(
            args.base,
            config=config,
            prefixes=prefixes,
            supabase_counts_path=args.supabase_counts,
            release_name=release.name,
            release_scopes=release.scope_keys,
        )
    else:
        artifact_report = build_artifact_report(
            args.base,
            prefixes=prefixes,
            supabase_counts_path=args.supabase_counts,
            release_name=release.name,
            release_scopes=release.scope_keys,
        )
    report = validate_release(
        args.base,
        release,
        artifact_report=artifact_report,
        max_issues=args.max_issues,
        strict_warnings=args.strict_warnings,
    )
    payload = report.to_mapping()
    payload["release_path"] = str(release_path)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        payload["written_to"] = str(args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if report.ok else 2


def _cmd_state_statute_completion(args: argparse.Namespace) -> int:
    release_path = resolve_release_manifest_path(args.base, args.release)
    release = ReleaseManifest.load(release_path)
    prefixes = tuple(args.prefix or DEFAULT_RELEASE_ARTIFACT_PREFIXES)
    source_access_queue = _resolve_state_source_access_queue(
        args.base, args.source_access_queue
    )
    validation_report_path = args.validation_report
    if validation_report_path is None:
        candidate = args.base / "analytics" / f"validate-release-{release.name}.json"
        if candidate.exists():
            validation_report_path = candidate
    if args.include_r2:
        config = load_r2_config(
            credential_path=args.credentials_file,
            bucket=args.bucket,
            endpoint_url=args.endpoint_url,
        )
        artifact_report = build_artifact_report_with_r2(
            args.base,
            config=config,
            prefixes=prefixes,
            document_class=DocumentClass.STATUTE.value,
            supabase_counts_path=args.supabase_counts,
        )
    else:
        artifact_report = build_artifact_report(
            args.base,
            prefixes=prefixes,
            document_class=DocumentClass.STATUTE.value,
            supabase_counts_path=args.supabase_counts,
        )
    report = build_state_statute_completion_report(
        args.base,
        release=release,
        artifact_report=artifact_report,
        supabase_counts_path=args.supabase_counts,
        validation_report_path=validation_report_path,
        source_access_statuses=load_source_access_statuses(source_access_queue),
    )
    payload = report.to_mapping()
    payload["release_path"] = str(release_path)
    if source_access_queue is not None:
        payload["source_access_queue_path"] = str(source_access_queue)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        payload["written_to"] = str(args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.require_complete and not report.complete:
        return 2
    return 0


def _cmd_regulation_completion(args: argparse.Namespace) -> int:
    release_path = resolve_release_manifest_path(args.base, args.release)
    release = ReleaseManifest.load(release_path)
    prefixes = tuple(args.prefix or DEFAULT_RELEASE_ARTIFACT_PREFIXES)
    validation_report_path = args.validation_report
    if validation_report_path is None:
        candidate = args.base / "analytics" / f"validate-release-{release.name}.json"
        if candidate.exists():
            validation_report_path = candidate
    if args.include_r2:
        config = load_r2_config(
            credential_path=args.credentials_file,
            bucket=args.bucket,
            endpoint_url=args.endpoint_url,
        )
        artifact_report = build_artifact_report_with_r2(
            args.base,
            config=config,
            prefixes=prefixes,
            document_class=DocumentClass.REGULATION.value,
            supabase_counts_path=args.supabase_counts,
        )
    else:
        artifact_report = build_artifact_report(
            args.base,
            prefixes=prefixes,
            document_class=DocumentClass.REGULATION.value,
            supabase_counts_path=args.supabase_counts,
        )
    report = build_regulation_completion_report(
        args.base,
        release=release,
        artifact_report=artifact_report,
        supabase_counts_path=args.supabase_counts,
        validation_report_path=validation_report_path,
    )
    payload = report.to_mapping()
    payload["release_path"] = str(release_path)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        payload["written_to"] = str(args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.require_complete and not report.complete:
        return 2
    return 0


def _cmd_source_discovery(args: argparse.Namespace) -> int:
    release = None
    release_path = None
    covered_source_urls = None
    if args.release:
        release_path = resolve_release_manifest_path(args.base, args.release)
        release = ReleaseManifest.load(release_path)
        covered_source_urls = _release_inventory_source_urls(
            CorpusArtifactStore(args.base),
            release,
        )
    report = build_source_discovery_report(
        tuple(args.input),
        release=release,
        covered_source_urls=covered_source_urls,
        source_name=args.source_name,
    )
    payload = report.to_mapping()
    if release_path is not None:
        payload["release_path"] = str(release_path)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        payload["written_to"] = str(args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _release_inventory_source_urls(
    store: CorpusArtifactStore,
    release: ReleaseManifest,
) -> tuple[str, ...]:
    """Collect official source URLs that have already been materialized."""

    urls: set[str] = set()
    for scope in release.scopes:
        inventory_path = store.inventory_path(
            scope.jurisdiction,
            scope.document_class,
            scope.version,
        )
        if not inventory_path.exists():
            continue
        for item in load_source_inventory(inventory_path):
            if item.source_url:
                urls.add(item.source_url)
            metadata = item.metadata or {}
            for key in ("download_url", "final_url", "source_url"):
                value = metadata.get(key)
                if isinstance(value, str) and value:
                    urls.add(value)
    return tuple(sorted(urls))


def _resolve_state_source_access_queue(base: Path, value: Path | None) -> Path | None:
    if value is not None:
        return value if str(value) else None
    candidates = (
        Path("manifests/state-statute-agent-queue.yaml"),
        base.parent.parent / "manifests" / "state-statute-agent-queue.yaml",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _artifact_scope_filter_supplied(args: argparse.Namespace) -> bool:
    return any((args.version, args.jurisdiction, args.document_class))


def _add_rulespec_args(sub_parser: argparse.ArgumentParser) -> None:
    """Attach the shared --rulespec-* flags so build-navigation-index and
    load-supabase can both pull encoded paths from local rulespec-* checkouts."""
    sub_parser.add_argument(
        "--rulespec-repo",
        action="append",
        default=[],
        help=(
            "Path to a local rulespec-* checkout (e.g. /path/to/rulespec-us). "
            "Repeatable. Jurisdiction is inferred from the directory name."
        ),
    )
    sub_parser.add_argument(
        "--rulespec-root",
        action="append",
        default=[],
        help=(
            "Path to a directory holding sibling rulespec-* checkouts. The "
            "builder discovers each jurisdiction's repo by name (rulespec-us, "
            "rulespec-us-co, …). Repeatable."
        ),
    )
    sub_parser.add_argument(
        "--rulespec-auto",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Automatically check ../rulespec-{repo} next to the corpus checkout "
            "for each input jurisdiction. Disable with --no-rulespec-auto."
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Source-first corpus pipeline tools.")
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate-manifest", help="Validate a corpus manifest.")
    validate.add_argument("path", type=Path)
    validate.set_defaults(func=_cmd_validate_manifest)

    inventory_ecfr = sub.add_parser(
        "inventory-ecfr",
        help="Build a source inventory from eCFR structure JSON.",
    )
    inventory_ecfr.add_argument("--base", type=Path, required=True)
    inventory_ecfr.add_argument("--version", "--run-id", dest="version", required=True)
    inventory_ecfr.add_argument("--as-of", required=True)
    inventory_ecfr.add_argument("--only-title", type=int)
    inventory_ecfr.add_argument("--only-part")
    inventory_ecfr.add_argument("--limit", type=int)
    inventory_ecfr.set_defaults(func=_cmd_inventory_ecfr)

    inventory_usc = sub.add_parser(
        "inventory-usc",
        help="Build a source inventory from official USLM XML for one US Code title.",
    )
    inventory_usc.add_argument("--base", type=Path, required=True)
    inventory_usc.add_argument("--version", "--run-id", dest="version", required=True)
    inventory_usc.add_argument("--source-xml", type=Path, required=True)
    inventory_usc.add_argument("--title")
    inventory_usc.add_argument("--source-url")
    inventory_usc.add_argument("--limit", type=int)
    inventory_usc.set_defaults(func=_cmd_inventory_usc)

    extract_ecfr_cmd = sub.add_parser(
        "extract-ecfr",
        help="Snapshot eCFR source XML and extract normalized provision JSONL.",
    )
    extract_ecfr_cmd.add_argument("--base", type=Path, required=True)
    extract_ecfr_cmd.add_argument("--version", required=True)
    extract_ecfr_cmd.add_argument("--as-of", required=True)
    extract_ecfr_cmd.add_argument("--expression-date")
    extract_ecfr_cmd.add_argument("--only-title", type=int)
    extract_ecfr_cmd.add_argument("--only-part")
    extract_ecfr_cmd.add_argument("--limit", type=int)
    extract_ecfr_cmd.add_argument("--workers", type=int, default=2)
    extract_ecfr_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_ecfr_cmd.set_defaults(func=_cmd_extract_ecfr)

    extract_usc_cmd = sub.add_parser(
        "extract-usc",
        help="Snapshot USLM XML and extract normalized US Code provision JSONL.",
    )
    extract_usc_cmd.add_argument("--base", type=Path, required=True)
    extract_usc_cmd.add_argument("--version", required=True)
    extract_usc_cmd.add_argument("--source-xml", type=Path, required=True)
    extract_usc_cmd.add_argument("--title")
    extract_usc_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_usc_cmd.add_argument("--expression-date")
    extract_usc_cmd.add_argument("--source-url")
    extract_usc_cmd.add_argument("--limit", type=int)
    extract_usc_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_usc_cmd.set_defaults(func=_cmd_extract_usc)

    extract_usc_dir_cmd = sub.add_parser(
        "extract-usc-dir",
        help="Snapshot a directory of USLM XML files and extract combined US Code provision JSONL.",
    )
    extract_usc_dir_cmd.add_argument("--base", type=Path, required=True)
    extract_usc_dir_cmd.add_argument("--version", required=True)
    extract_usc_dir_cmd.add_argument("--source-dir", type=Path, required=True)
    extract_usc_dir_cmd.add_argument("--only-title")
    extract_usc_dir_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_usc_dir_cmd.add_argument("--expression-date")
    extract_usc_dir_cmd.add_argument("--source-url")
    extract_usc_dir_cmd.add_argument("--limit", type=int)
    extract_usc_dir_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_usc_dir_cmd.set_defaults(func=_cmd_extract_usc_dir)

    extract_dc_cmd = sub.add_parser(
        "extract-dc-code",
        help="Snapshot local DC Code XML and extract normalized provision JSONL.",
    )
    extract_dc_cmd.add_argument("--base", type=Path, required=True)
    extract_dc_cmd.add_argument("--version", required=True)
    extract_dc_cmd.add_argument("--source-dir", type=Path, required=True)
    extract_dc_cmd.add_argument("--only-title")
    extract_dc_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_dc_cmd.add_argument("--expression-date")
    extract_dc_cmd.add_argument("--limit", type=int)
    extract_dc_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_dc_cmd.set_defaults(func=_cmd_extract_dc_code)

    extract_cic_html_cmd = sub.add_parser(
        "extract-cic-state-html",
        help="Snapshot a Public.Resource.org CIC state-code HTML release.",
    )
    extract_cic_html_cmd.add_argument("--base", type=Path, required=True)
    extract_cic_html_cmd.add_argument("--version", required=True)
    extract_cic_html_cmd.add_argument("--jurisdiction", required=True)
    extract_cic_html_cmd.add_argument("--release-dir", type=Path, required=True)
    extract_cic_html_cmd.add_argument("--only-title")
    extract_cic_html_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_cic_html_cmd.add_argument("--expression-date")
    extract_cic_html_cmd.add_argument("--limit", type=int)
    extract_cic_html_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_cic_html_cmd.set_defaults(func=_cmd_extract_cic_html)

    extract_cic_odt_cmd = sub.add_parser(
        "extract-cic-state-odt",
        help="Snapshot a Public.Resource.org CIC state-code ODT release.",
    )
    extract_cic_odt_cmd.add_argument("--base", type=Path, required=True)
    extract_cic_odt_cmd.add_argument("--version", required=True)
    extract_cic_odt_cmd.add_argument("--jurisdiction", required=True)
    extract_cic_odt_cmd.add_argument("--release-dir", type=Path, required=True)
    extract_cic_odt_cmd.add_argument("--only-title")
    extract_cic_odt_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_cic_odt_cmd.add_argument("--expression-date")
    extract_cic_odt_cmd.add_argument("--limit", type=int)
    extract_cic_odt_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_cic_odt_cmd.set_defaults(func=_cmd_extract_cic_odt)

    extract_colorado_docx_cmd = sub.add_parser(
        "extract-colorado-docx",
        help="Snapshot the official Colorado CRS DOCX release.",
    )
    extract_colorado_docx_cmd.add_argument("--base", type=Path, required=True)
    extract_colorado_docx_cmd.add_argument("--version", required=True)
    extract_colorado_docx_cmd.add_argument("--release-dir", type=Path, required=True)
    extract_colorado_docx_cmd.add_argument("--only-title")
    extract_colorado_docx_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_colorado_docx_cmd.add_argument("--expression-date")
    extract_colorado_docx_cmd.add_argument("--limit", type=int)
    extract_colorado_docx_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_colorado_docx_cmd.set_defaults(func=_cmd_extract_colorado_docx)

    extract_ohio_revised_code_cmd = sub.add_parser(
        "extract-ohio-revised-code",
        help="Snapshot official Ohio Revised Code HTML.",
    )
    extract_ohio_revised_code_cmd.add_argument("--base", type=Path, required=True)
    extract_ohio_revised_code_cmd.add_argument("--version", required=True)
    extract_ohio_revised_code_cmd.add_argument("--source-dir", type=Path)
    extract_ohio_revised_code_cmd.add_argument("--download-dir", type=Path)
    extract_ohio_revised_code_cmd.add_argument("--only-title")
    extract_ohio_revised_code_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_ohio_revised_code_cmd.add_argument("--expression-date")
    extract_ohio_revised_code_cmd.add_argument("--limit", type=int)
    extract_ohio_revised_code_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_ohio_revised_code_cmd.set_defaults(func=_cmd_extract_ohio_revised_code)

    extract_minnesota_statutes_cmd = sub.add_parser(
        "extract-minnesota-statutes",
        help="Snapshot official Minnesota Statutes HTML.",
    )
    extract_minnesota_statutes_cmd.add_argument("--base", type=Path, required=True)
    extract_minnesota_statutes_cmd.add_argument("--version", required=True)
    extract_minnesota_statutes_cmd.add_argument("--source-dir", type=Path)
    extract_minnesota_statutes_cmd.add_argument("--download-dir", type=Path)
    extract_minnesota_statutes_cmd.add_argument("--only-title")
    extract_minnesota_statutes_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_minnesota_statutes_cmd.add_argument("--expression-date")
    extract_minnesota_statutes_cmd.add_argument("--limit", type=int)
    extract_minnesota_statutes_cmd.add_argument("--workers", type=int, default=4)
    extract_minnesota_statutes_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_minnesota_statutes_cmd.set_defaults(func=_cmd_extract_minnesota_statutes)

    extract_nebraska_statutes_cmd = sub.add_parser(
        "extract-nebraska-revised-statutes",
        help="Snapshot official Nebraska Revised Statutes HTML.",
    )
    extract_nebraska_statutes_cmd.add_argument("--base", type=Path, required=True)
    extract_nebraska_statutes_cmd.add_argument("--version", required=True)
    extract_nebraska_statutes_cmd.add_argument("--source-dir", type=Path)
    extract_nebraska_statutes_cmd.add_argument("--download-dir", type=Path)
    extract_nebraska_statutes_cmd.add_argument("--only-title")
    extract_nebraska_statutes_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_nebraska_statutes_cmd.add_argument("--expression-date")
    extract_nebraska_statutes_cmd.add_argument("--limit", type=int)
    extract_nebraska_statutes_cmd.add_argument("--workers", type=int, default=4)
    extract_nebraska_statutes_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_nebraska_statutes_cmd.set_defaults(func=_cmd_extract_nebraska_revised_statutes)

    extract_washington_rcw_cmd = sub.add_parser(
        "extract-washington-rcw",
        help="Snapshot official Revised Code of Washington HTML.",
    )
    extract_washington_rcw_cmd.add_argument("--base", type=Path, required=True)
    extract_washington_rcw_cmd.add_argument("--version", required=True)
    extract_washington_rcw_cmd.add_argument("--source-dir", type=Path)
    extract_washington_rcw_cmd.add_argument("--download-dir", type=Path)
    extract_washington_rcw_cmd.add_argument("--only-title")
    extract_washington_rcw_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_washington_rcw_cmd.add_argument("--expression-date")
    extract_washington_rcw_cmd.add_argument("--limit", type=int)
    extract_washington_rcw_cmd.add_argument("--workers", type=int, default=4)
    extract_washington_rcw_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_washington_rcw_cmd.set_defaults(func=_cmd_extract_washington_rcw)

    extract_illinois_ilcs_cmd = sub.add_parser(
        "extract-illinois-ilcs",
        help="Snapshot official Illinois ILCS FTP HTML.",
    )
    extract_illinois_ilcs_cmd.add_argument("--base", type=Path, required=True)
    extract_illinois_ilcs_cmd.add_argument("--version", required=True)
    extract_illinois_ilcs_cmd.add_argument("--source-dir", type=Path)
    extract_illinois_ilcs_cmd.add_argument("--only-chapter")
    extract_illinois_ilcs_cmd.add_argument("--only-act")
    extract_illinois_ilcs_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_illinois_ilcs_cmd.add_argument("--expression-date")
    extract_illinois_ilcs_cmd.add_argument("--limit", type=int)
    extract_illinois_ilcs_cmd.add_argument("--workers", type=int, default=8)
    extract_illinois_ilcs_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_illinois_ilcs_cmd.set_defaults(func=_cmd_extract_illinois_ilcs)

    extract_canada_acts_cmd = sub.add_parser(
        "extract-canada-acts",
        help="Snapshot Canadian federal acts from laws-lois.justice.gc.ca.",
    )
    extract_canada_acts_cmd.add_argument("--base", type=Path, required=True)
    extract_canada_acts_cmd.add_argument("--version", required=True)
    extract_canada_acts_cmd.add_argument(
        "--only-act",
        action="append",
        default=[],
        help=("Restrict the extract to a specific consolidated number (e.g. I-3.3). Repeatable."),
    )
    extract_canada_acts_cmd.add_argument(
        "--limit-acts",
        type=int,
        help="Stop after this many acts (after enumerating).",
    )
    extract_canada_acts_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_canada_acts_cmd.add_argument("--expression-date")
    extract_canada_acts_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_canada_acts_cmd.set_defaults(func=_cmd_extract_canada_acts)

    extract_indiana_code_cmd = sub.add_parser(
        "extract-indiana-code",
        help="Snapshot official Indiana Code HTML.",
    )
    extract_indiana_code_cmd.add_argument("--base", type=Path, required=True)
    extract_indiana_code_cmd.add_argument("--version", required=True)
    extract_indiana_code_cmd.add_argument("--source-dir", type=Path)
    extract_indiana_code_cmd.add_argument("--source-zip", type=Path)
    extract_indiana_code_cmd.add_argument(
        "--source-year",
        type=int,
        default=INDIANA_CODE_DEFAULT_YEAR,
    )
    extract_indiana_code_cmd.add_argument("--download-dir", type=Path)
    extract_indiana_code_cmd.add_argument("--only-title")
    extract_indiana_code_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_indiana_code_cmd.add_argument("--expression-date")
    extract_indiana_code_cmd.add_argument("--limit", type=int)
    extract_indiana_code_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_indiana_code_cmd.set_defaults(func=_cmd_extract_indiana_code)

    extract_montana_code_cmd = sub.add_parser(
        "extract-montana-code",
        help="Snapshot official Montana Code Annotated HTML.",
    )
    extract_montana_code_cmd.add_argument("--base", type=Path, required=True)
    extract_montana_code_cmd.add_argument("--version", required=True)
    extract_montana_code_cmd.add_argument("--source-dir", type=Path)
    extract_montana_code_cmd.add_argument(
        "--source-year",
        type=int,
        default=MONTANA_CODE_DEFAULT_YEAR,
    )
    extract_montana_code_cmd.add_argument("--download-dir", type=Path)
    extract_montana_code_cmd.add_argument("--only-title")
    extract_montana_code_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_montana_code_cmd.add_argument("--expression-date")
    extract_montana_code_cmd.add_argument("--limit", type=int)
    extract_montana_code_cmd.add_argument("--workers", type=int, default=8)
    extract_montana_code_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_montana_code_cmd.set_defaults(func=_cmd_extract_montana_code)

    extract_nevada_nrs_cmd = sub.add_parser(
        "extract-nevada-nrs",
        help="Snapshot official Nevada Revised Statutes HTML.",
    )
    extract_nevada_nrs_cmd.add_argument("--base", type=Path, required=True)
    extract_nevada_nrs_cmd.add_argument("--version", required=True)
    extract_nevada_nrs_cmd.add_argument("--source-dir", type=Path)
    extract_nevada_nrs_cmd.add_argument(
        "--source-year",
        type=int,
        default=NEVADA_NRS_DEFAULT_YEAR,
    )
    extract_nevada_nrs_cmd.add_argument("--download-dir", type=Path)
    extract_nevada_nrs_cmd.add_argument("--only-title")
    extract_nevada_nrs_cmd.add_argument("--only-chapter")
    extract_nevada_nrs_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_nevada_nrs_cmd.add_argument("--expression-date")
    extract_nevada_nrs_cmd.add_argument("--limit", type=int)
    extract_nevada_nrs_cmd.add_argument("--workers", type=int, default=8)
    extract_nevada_nrs_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_nevada_nrs_cmd.set_defaults(func=_cmd_extract_nevada_nrs)

    extract_new_york_cmd = sub.add_parser(
        "extract-new-york-consolidated-laws",
        help="Snapshot official New York Senate OpenLegislation HTML.",
    )
    extract_new_york_cmd.add_argument("--base", type=Path, required=True)
    extract_new_york_cmd.add_argument("--version", required=True)
    extract_new_york_cmd.add_argument("--source-dir", type=Path)
    extract_new_york_cmd.add_argument("--download-dir", type=Path)
    extract_new_york_cmd.add_argument("--only-title")
    extract_new_york_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_new_york_cmd.add_argument("--expression-date")
    extract_new_york_cmd.add_argument("--limit", type=int)
    extract_new_york_cmd.add_argument("--workers", type=int, default=1)
    extract_new_york_cmd.add_argument("--request-delay-seconds", type=float, default=0.35)
    extract_new_york_cmd.add_argument("--timeout-seconds", type=float, default=15.0)
    extract_new_york_cmd.add_argument("--request-attempts", type=int, default=2)
    extract_new_york_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_new_york_cmd.set_defaults(func=_cmd_extract_new_york_consolidated_laws)

    extract_new_york_api_cmd = sub.add_parser(
        "extract-new-york-openleg-api",
        help="Snapshot official New York OpenLegislation law JSON.",
    )
    extract_new_york_api_cmd.add_argument("--base", type=Path, required=True)
    extract_new_york_api_cmd.add_argument("--version", required=True)
    extract_new_york_api_cmd.add_argument("--source-dir", type=Path)
    extract_new_york_api_cmd.add_argument("--download-dir", type=Path)
    extract_new_york_api_cmd.add_argument("--only-title")
    extract_new_york_api_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_new_york_api_cmd.add_argument("--expression-date")
    extract_new_york_api_cmd.add_argument("--limit", type=int)
    extract_new_york_api_cmd.add_argument(
        "--api-key-env",
        default="NYSENATE_OPENLEG_API_KEY",
    )
    extract_new_york_api_cmd.add_argument(
        "--api-base-url",
        default="https://legislation.nysenate.gov",
    )
    extract_new_york_api_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_new_york_api_cmd.set_defaults(func=_cmd_extract_new_york_openleg_api)

    extract_delaware_code_cmd = sub.add_parser(
        "extract-delaware-code",
        help="Snapshot official Delaware Code HTML.",
    )
    extract_delaware_code_cmd.add_argument("--base", type=Path, required=True)
    extract_delaware_code_cmd.add_argument("--version", required=True)
    extract_delaware_code_cmd.add_argument("--source-dir", type=Path)
    extract_delaware_code_cmd.add_argument("--download-dir", type=Path)
    extract_delaware_code_cmd.add_argument("--only-title")
    extract_delaware_code_cmd.add_argument("--only-chapter")
    extract_delaware_code_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_delaware_code_cmd.add_argument("--expression-date")
    extract_delaware_code_cmd.add_argument("--limit", type=int)
    extract_delaware_code_cmd.add_argument("--workers", type=int, default=1)
    extract_delaware_code_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_delaware_code_cmd.set_defaults(func=_cmd_extract_delaware_code)

    extract_oregon_ors_cmd = sub.add_parser(
        "extract-oregon-ors",
        help="Snapshot official Oregon Revised Statutes HTML.",
    )
    extract_oregon_ors_cmd.add_argument("--base", type=Path, required=True)
    extract_oregon_ors_cmd.add_argument("--version", required=True)
    extract_oregon_ors_cmd.add_argument("--source-dir", type=Path)
    extract_oregon_ors_cmd.add_argument(
        "--source-year",
        type=int,
        default=OREGON_ORS_DEFAULT_YEAR,
    )
    extract_oregon_ors_cmd.add_argument("--download-dir", type=Path)
    extract_oregon_ors_cmd.add_argument("--only-title")
    extract_oregon_ors_cmd.add_argument("--only-chapter")
    extract_oregon_ors_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_oregon_ors_cmd.add_argument("--expression-date")
    extract_oregon_ors_cmd.add_argument("--limit", type=int)
    extract_oregon_ors_cmd.add_argument("--workers", type=int, default=8)
    extract_oregon_ors_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_oregon_ors_cmd.set_defaults(func=_cmd_extract_oregon_ors)

    extract_rhode_island_cmd = sub.add_parser(
        "extract-rhode-island-general-laws",
        help="Snapshot official Rhode Island General Laws HTML.",
    )
    extract_rhode_island_cmd.add_argument("--base", type=Path, required=True)
    extract_rhode_island_cmd.add_argument("--version", required=True)
    extract_rhode_island_cmd.add_argument("--source-dir", type=Path)
    extract_rhode_island_cmd.add_argument(
        "--source-year",
        type=int,
        default=RHODE_ISLAND_GENERAL_LAWS_DEFAULT_YEAR,
    )
    extract_rhode_island_cmd.add_argument("--download-dir", type=Path)
    extract_rhode_island_cmd.add_argument("--only-title")
    extract_rhode_island_cmd.add_argument("--only-chapter")
    extract_rhode_island_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_rhode_island_cmd.add_argument("--expression-date")
    extract_rhode_island_cmd.add_argument("--limit", type=int)
    extract_rhode_island_cmd.add_argument("--workers", type=int, default=8)
    extract_rhode_island_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_rhode_island_cmd.set_defaults(func=_cmd_extract_rhode_island_general_laws)

    extract_california_codes_cmd = sub.add_parser(
        "extract-california-codes",
        help="Snapshot official California Legislative Counsel bulk code data.",
    )
    extract_california_codes_cmd.add_argument("--base", type=Path, required=True)
    extract_california_codes_cmd.add_argument("--version", required=True)
    extract_california_codes_cmd.add_argument("--source-zip", type=Path)
    extract_california_codes_cmd.add_argument(
        "--source-url",
        default="https://downloads.leginfo.legislature.ca.gov/pubinfo_2025.zip",
    )
    extract_california_codes_cmd.add_argument("--download-dir", type=Path)
    extract_california_codes_cmd.add_argument("--only-title")
    extract_california_codes_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_california_codes_cmd.add_argument("--expression-date")
    extract_california_codes_cmd.add_argument("--limit", type=int)
    extract_california_codes_cmd.add_argument("--include-inactive", action="store_true")
    extract_california_codes_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_california_codes_cmd.set_defaults(func=_cmd_extract_california_codes_bulk)

    extract_texas_tcas_cmd = sub.add_parser(
        "extract-texas-tcas",
        help="Snapshot official Texas statutes from the TCSS API/resources.",
    )
    extract_texas_tcas_cmd.add_argument("--base", type=Path, required=True)
    extract_texas_tcas_cmd.add_argument("--version", required=True)
    extract_texas_tcas_cmd.add_argument("--source-dir", type=Path)
    extract_texas_tcas_cmd.add_argument("--download-dir", type=Path)
    extract_texas_tcas_cmd.add_argument("--only-title")
    extract_texas_tcas_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_texas_tcas_cmd.add_argument("--expression-date")
    extract_texas_tcas_cmd.add_argument("--limit", type=int)
    extract_texas_tcas_cmd.add_argument("--workers", type=int, default=4)
    extract_texas_tcas_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_texas_tcas_cmd.set_defaults(func=_cmd_extract_texas_tcas)

    extract_state_statutes_cmd = sub.add_parser(
        "extract-state-statutes",
        help="Run state statute extract adapters from a corpus manifest.",
    )
    extract_state_statutes_cmd.add_argument("--base", type=Path, required=True)
    extract_state_statutes_cmd.add_argument("--manifest", type=Path, required=True)
    extract_state_statutes_cmd.add_argument("--only-jurisdiction", action="append", default=[])
    extract_state_statutes_cmd.add_argument("--only-source-id", action="append", default=[])
    extract_state_statutes_cmd.add_argument("--limit-per-source", type=int)
    extract_state_statutes_cmd.add_argument("--dry-run", action="store_true")
    extract_state_statutes_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_state_statutes_cmd.set_defaults(func=_cmd_extract_state_statutes)

    extract_colorado_ccr_cmd = sub.add_parser(
        "extract-colorado-ccr",
        help="Snapshot current Colorado Code of Regulations PDFs.",
    )
    extract_colorado_ccr_cmd.add_argument("--base", type=Path, required=True)
    extract_colorado_ccr_cmd.add_argument("--version", required=True)
    extract_colorado_ccr_cmd.add_argument("--only-series")
    extract_colorado_ccr_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_colorado_ccr_cmd.add_argument("--expression-date")
    extract_colorado_ccr_cmd.add_argument("--limit", type=int)
    extract_colorado_ccr_cmd.add_argument("--workers", type=int, default=4)
    extract_colorado_ccr_cmd.add_argument("--release-dir", type=Path)
    extract_colorado_ccr_cmd.add_argument("--download-dir", type=Path)
    extract_colorado_ccr_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_colorado_ccr_cmd.set_defaults(func=_cmd_extract_colorado_ccr)

    extract_nycrr_cmd = sub.add_parser(
        "extract-nycrr",
        help="Snapshot the public New York Codes, Rules and Regulations tree.",
    )
    extract_nycrr_cmd.add_argument("--base", type=Path, required=True)
    extract_nycrr_cmd.add_argument("--version", required=True)
    extract_nycrr_cmd.add_argument("--only-title", type=int)
    extract_nycrr_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_nycrr_cmd.add_argument("--expression-date")
    extract_nycrr_cmd.add_argument("--limit", type=int)
    extract_nycrr_cmd.add_argument("--delay-seconds", type=float, default=0.25)
    extract_nycrr_cmd.add_argument("--retry-attempts", type=int, default=4)
    extract_nycrr_cmd.add_argument("--refresh", action="store_true")
    extract_nycrr_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_nycrr_cmd.set_defaults(func=_cmd_extract_nycrr)

    extract_california_mpp_cmd = sub.add_parser(
        "extract-california-mpp-calfresh",
        help="Snapshot CDSS MPP Division 63 (CalFresh) DOCX files.",
    )
    extract_california_mpp_cmd.add_argument("--base", type=Path, required=True)
    extract_california_mpp_cmd.add_argument("--version", required=True)
    extract_california_mpp_cmd.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Path to manifests/us-ca-cdss-mpp-calfresh.yaml (declares the DOCX source set).",
    )
    extract_california_mpp_cmd.add_argument("--download-dir", type=Path)
    extract_california_mpp_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_california_mpp_cmd.add_argument("--expression-date")
    extract_california_mpp_cmd.add_argument("--delay-seconds", type=float, default=0.25)
    extract_california_mpp_cmd.add_argument("--timeout-seconds", type=float, default=60.0)
    extract_california_mpp_cmd.add_argument("--request-attempts", type=int, default=3)
    extract_california_mpp_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_california_mpp_cmd.set_defaults(func=_cmd_extract_california_mpp_calfresh)

    extract_ny_state_register_cmd = sub.add_parser(
        "extract-ny-state-register",
        help="Snapshot NY Department of State Register issue PDFs.",
    )
    extract_ny_state_register_cmd.add_argument("--base", type=Path, required=True)
    extract_ny_state_register_cmd.add_argument("--version", required=True)
    extract_ny_state_register_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_ny_state_register_cmd.add_argument("--expression-date")
    extract_ny_state_register_cmd.add_argument("--limit", type=int)
    extract_ny_state_register_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_ny_state_register_cmd.set_defaults(func=_cmd_extract_ny_state_register)

    extract_documents_cmd = sub.add_parser(
        "extract-official-documents",
        help="Snapshot official HTML/PDF policy documents from a manifest.",
    )
    extract_documents_cmd.add_argument("--base", type=Path, required=True)
    extract_documents_cmd.add_argument("--version", required=True)
    extract_documents_cmd.add_argument("--manifest", type=Path, required=True)
    extract_documents_cmd.add_argument("--only-source-id")
    extract_documents_cmd.add_argument("--source-as-of", "--as-of", dest="source_as_of")
    extract_documents_cmd.add_argument("--expression-date")
    extract_documents_cmd.add_argument("--limit", type=int)
    extract_documents_cmd.add_argument("--allow-incomplete", action="store_true")
    extract_documents_cmd.set_defaults(func=_cmd_extract_official_documents)

    coverage = sub.add_parser(
        "coverage",
        help="Compare source inventory with normalized provision records.",
    )
    coverage.add_argument("--base", type=Path, required=True)
    coverage.add_argument("--source-inventory", type=Path, required=True)
    coverage.add_argument("--provisions", type=Path, required=True)
    coverage.add_argument("--jurisdiction", required=True)
    coverage.add_argument(
        "--document-class",
        choices=[document_class.value for document_class in DocumentClass],
        default=DocumentClass.STATUTE.value,
    )
    coverage.add_argument("--version", required=True)
    coverage.add_argument("--write", action="store_true")
    coverage.add_argument("--allow-incomplete", action="store_true")
    coverage.set_defaults(func=_cmd_coverage)

    export_supabase = sub.add_parser(
        "export-supabase",
        help="Project normalized provision JSONL into corpus.provisions JSONL.",
    )
    export_supabase.add_argument("--provisions", type=Path, required=True)
    export_supabase.add_argument("--output", type=Path, required=True)
    export_supabase.set_defaults(func=_cmd_export_supabase)

    load_supabase = sub.add_parser(
        "load-supabase",
        help="Upsert normalized provision JSONL into corpus.provisions.",
    )
    load_supabase.add_argument("--provisions", type=Path, required=True)
    load_supabase.add_argument(
        "--supabase-url",
        default=os.environ.get("AXIOM_SUPABASE_URL", DEFAULT_AXIOM_SUPABASE_URL),
    )
    load_supabase.add_argument("--chunk-size", type=int, default=500)
    load_supabase.add_argument("--dry-run", action="store_true")
    load_supabase.add_argument("--skip-refresh", action="store_true")
    load_supabase.add_argument("--allow-refresh-failure", action="store_true")
    load_supabase.add_argument(
        "--replace-scope",
        action="store_true",
        help=(
            "Delete existing rows for the JSONL's single jurisdiction/document class "
            "before loading."
        ),
    )
    load_supabase.add_argument(
        "--preserve-existing-ids",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reuse existing corpus.provisions IDs for matching citation paths before upsert.",
    )
    load_supabase.add_argument("--service-key-env", default=DEFAULT_SERVICE_KEY_ENV)
    load_supabase.add_argument("--access-token-env", default=DEFAULT_ACCESS_TOKEN_ENV)
    load_supabase.add_argument(
        "--build-navigation",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Rebuild corpus.navigation_nodes for the loaded scope after the "
            "provisions upsert succeeds. Disabled with --no-build-navigation."
        ),
    )
    load_supabase.add_argument(
        "--preserve-navigation-statuses",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Carry curated corpus.navigation_nodes.status values across the "
            "post-load nav rebuild. Disable with --no-preserve-navigation-statuses."
        ),
    )
    _add_rulespec_args(load_supabase)
    load_supabase.set_defaults(func=_cmd_load_supabase)

    build_navigation = sub.add_parser(
        "build-navigation-index",
        help=(
            "Build (and optionally upsert) corpus.navigation_nodes from a "
            "provisions JSONL or directly from corpus.provisions in Supabase."
        ),
    )
    build_navigation.add_argument(
        "--provisions",
        type=Path,
        action="append",
        default=[],
        help="Path to a provisions JSONL file. Repeatable.",
    )
    build_navigation.add_argument(
        "--from-supabase",
        action="store_true",
        help="Fetch provisions to navigate from Supabase instead of local JSONL.",
    )
    build_navigation.add_argument(
        "--all",
        action="store_true",
        help=(
            "Rebuild every (jurisdiction, doc_type) scope. Requires --from-supabase "
            "or --provisions input."
        ),
    )
    build_navigation.add_argument(
        "--jurisdiction",
        help="Filter provisions/scope to one jurisdiction (e.g. us-co).",
    )
    build_navigation.add_argument(
        "--doc-type",
        choices=[document_class.value for document_class in DocumentClass],
        help="Filter to one document class (e.g. statute, regulation).",
    )
    build_navigation.add_argument(
        "--output",
        type=Path,
        help="Optionally write the built navigation rows to JSONL for inspection.",
    )
    build_navigation.add_argument(
        "--replace-scope",
        dest="replace_scope",
        action="store_true",
        default=None,
        help=(
            "Prune stale rows for rebuilt scopes. Defaults to on with "
            "--from-supabase and off with --provisions."
        ),
    )
    build_navigation.add_argument(
        "--no-replace-scope",
        dest="replace_scope",
        action="store_false",
        help="Skip pruning stale rows for rebuilt scopes.",
    )
    build_navigation.add_argument("--chunk-size", type=int, default=500)
    build_navigation.add_argument("--dry-run", action="store_true")
    build_navigation.add_argument(
        "--skip-supabase",
        action="store_true",
        help="Build rows locally without contacting Supabase.",
    )
    build_navigation.add_argument(
        "--supabase-url",
        default=os.environ.get("AXIOM_SUPABASE_URL", DEFAULT_AXIOM_SUPABASE_URL),
    )
    build_navigation.add_argument("--service-key-env", default=DEFAULT_SERVICE_KEY_ENV)
    build_navigation.add_argument("--access-token-env", default=DEFAULT_ACCESS_TOKEN_ENV)
    build_navigation.add_argument(
        "--preserve-statuses",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Carry curated corpus.navigation_nodes.status values across the "
            "rebuild. Disable with --no-preserve-statuses to wipe and rederive."
        ),
    )
    _add_rulespec_args(build_navigation)
    build_navigation.set_defaults(func=_cmd_build_navigation_index)

    snapshot_counts = sub.add_parser(
        "snapshot-provision-counts",
        aliases=["snapshot-supabase-counts"],
        help="Snapshot current-release provision counts from Supabase.",
    )
    snapshot_counts.add_argument(
        "--supabase-url",
        default=os.environ.get("AXIOM_SUPABASE_URL", DEFAULT_AXIOM_SUPABASE_URL),
    )
    snapshot_counts.add_argument("--output", type=Path)
    snapshot_counts.add_argument(
        "--include-legacy",
        action="store_true",
        help="Read corpus.provision_counts instead of the current-release count view.",
    )
    snapshot_counts.add_argument("--service-key-env", default=DEFAULT_SERVICE_KEY_ENV)
    snapshot_counts.add_argument("--access-token-env", default=DEFAULT_ACCESS_TOKEN_ENV)
    snapshot_counts.set_defaults(func=_cmd_snapshot_provision_counts)

    sync_release_scopes = sub.add_parser(
        "sync-release-scopes",
        help="Sync a release manifest's active scopes into Supabase.",
    )
    sync_release_scopes.add_argument("--base", type=Path, required=True)
    sync_release_scopes.add_argument("--release", default="current")
    sync_release_scopes.add_argument(
        "--supabase-url",
        default=os.environ.get("AXIOM_SUPABASE_URL", DEFAULT_AXIOM_SUPABASE_URL),
    )
    sync_release_scopes.add_argument("--chunk-size", type=int, default=500)
    sync_release_scopes.add_argument("--dry-run", action="store_true")
    sync_release_scopes.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip refreshing corpus analytics after syncing release scopes.",
    )
    sync_release_scopes.add_argument(
        "--allow-refresh-failure",
        action="store_true",
        help="Return a report even if the post-sync analytics refresh fails.",
    )
    sync_release_scopes.add_argument("--service-key-env", default=DEFAULT_SERVICE_KEY_ENV)
    sync_release_scopes.add_argument("--access-token-env", default=DEFAULT_ACCESS_TOKEN_ENV)
    sync_release_scopes.set_defaults(func=_cmd_sync_release_scopes)

    verify_release_coverage_cmd = sub.add_parser(
        "verify-release-coverage",
        help=(
            "Check that every jurisdiction × document_class with navigation "
            "rows also has rows in corpus.current_provisions. Exits non-zero "
            "if any jurisdiction is missing — the historical UK regression "
            "(rows in corpus.provisions and navigation_nodes but zero in "
            "current_provisions because release_scopes had no matching row)."
        ),
    )
    verify_release_coverage_cmd.add_argument(
        "--supabase-url",
        default=os.environ.get("AXIOM_SUPABASE_URL", DEFAULT_AXIOM_SUPABASE_URL),
    )
    verify_release_coverage_cmd.add_argument(
        "--service-key-env", default=DEFAULT_SERVICE_KEY_ENV
    )
    verify_release_coverage_cmd.add_argument(
        "--access-token-env", default=DEFAULT_ACCESS_TOKEN_ENV
    )
    verify_release_coverage_cmd.set_defaults(func=_cmd_verify_release_coverage)

    analytics = sub.add_parser(
        "analytics",
        help="Summarize source, provision, and Supabase count coverage.",
    )
    analytics.add_argument("--base", type=Path, required=True)
    analytics.add_argument("--version", required=True)
    analytics.add_argument("--supabase-counts", type=Path)
    analytics.add_argument("--jurisdiction", action="append", default=[])
    analytics.add_argument(
        "--document-class",
        action="append",
        choices=[document_class.value for document_class in DocumentClass],
        default=[],
    )
    analytics.add_argument(
        "--default-count-document-class",
        default=DocumentClass.STATUTE.value,
        choices=[document_class.value for document_class in DocumentClass],
    )
    analytics.add_argument("--output", type=Path)
    analytics.add_argument("--write", action="store_true")
    analytics.set_defaults(func=_cmd_analytics)

    sync_r2 = sub.add_parser(
        "sync-r2",
        help="Plan or upload local corpus artifacts to the configured R2 bucket.",
    )
    sync_r2.add_argument("--base", type=Path, required=True)
    sync_r2.add_argument(
        "--prefix",
        action="append",
        choices=list(DEFAULT_ARTIFACT_PREFIXES),
        default=[],
        help="Top-level artifact prefix to include. Repeatable; defaults to all artifact prefixes.",
    )
    sync_r2.add_argument("--bucket")
    sync_r2.add_argument("--endpoint-url")
    sync_r2.add_argument("--credentials-file", type=Path)
    sync_r2.add_argument("--jurisdiction")
    sync_r2.add_argument(
        "--document-class",
        choices=[document_class.value for document_class in DocumentClass],
    )
    sync_r2.add_argument("--version")
    sync_r2.add_argument("--limit", type=int)
    sync_r2.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Concurrent upload workers to use when --apply is set.",
    )
    sync_r2.add_argument(
        "--apply",
        action="store_true",
        help="Upload files. Without this flag the command only prints a dry-run plan.",
    )
    sync_r2.add_argument(
        "--force",
        action="store_true",
        help="Upload matching-size files too.",
    )
    sync_r2.set_defaults(func=_cmd_sync_r2)

    artifact_report = sub.add_parser(
        "artifact-report",
        help="Report local/R2/Supabase artifact coverage by jurisdiction and document class.",
    )
    artifact_report.add_argument("--base", type=Path, required=True)
    artifact_report.add_argument("--version")
    artifact_report.add_argument("--jurisdiction")
    release_group = artifact_report.add_mutually_exclusive_group()
    release_group.add_argument(
        "--release",
        help=(
            "Release manifest name or path. Names resolve to "
            "<base>/releases/<name>.json or manifests/releases/<name>.json."
        ),
    )
    release_group.add_argument(
        "--all-scopes",
        action="store_true",
        help="Report every discovered scope instead of auto-using the current release.",
    )
    artifact_report.add_argument(
        "--document-class",
        choices=[document_class.value for document_class in DocumentClass],
    )
    artifact_report.add_argument("--supabase-counts", type=Path)
    artifact_report.add_argument(
        "--prefix",
        action="append",
        choices=list(DEFAULT_ARTIFACT_PREFIXES),
        default=[],
        help="Top-level artifact prefix to include. Repeatable; defaults to all artifact prefixes.",
    )
    artifact_report.add_argument("--include-r2", action="store_true")
    artifact_report.add_argument("--bucket")
    artifact_report.add_argument("--endpoint-url")
    artifact_report.add_argument("--credentials-file", type=Path)
    artifact_report.add_argument("--output", type=Path)
    artifact_report.set_defaults(func=_cmd_artifact_report)

    release_artifact_manifest = sub.add_parser(
        "release-artifact-manifest",
        help="Write a digest manifest for the concrete artifacts in a named release.",
    )
    release_artifact_manifest.add_argument("--base", type=Path, required=True)
    release_artifact_manifest.add_argument("--release", default="current")
    release_artifact_manifest.add_argument(
        "--prefix",
        action="append",
        choices=list(DEFAULT_RELEASE_ARTIFACT_PREFIXES),
        default=[],
        help="Top-level artifact prefix to include. Defaults to release content prefixes.",
    )
    release_artifact_manifest.add_argument("--output", type=Path)
    release_artifact_manifest.set_defaults(func=_cmd_release_artifact_manifest)

    validate_release_cmd = sub.add_parser(
        "validate-release",
        help="Validate release artifacts, coverage, provision invariants, and optional R2/Supabase state.",
    )
    validate_release_cmd.add_argument("--base", type=Path, required=True)
    validate_release_cmd.add_argument("--release", default="current")
    validate_release_cmd.add_argument("--supabase-counts", type=Path)
    validate_release_cmd.add_argument(
        "--prefix",
        action="append",
        choices=list(DEFAULT_RELEASE_ARTIFACT_PREFIXES),
        default=[],
        help="Top-level artifact prefix to include in the artifact report.",
    )
    validate_release_cmd.add_argument("--include-r2", action="store_true")
    validate_release_cmd.add_argument("--bucket")
    validate_release_cmd.add_argument("--endpoint-url")
    validate_release_cmd.add_argument("--credentials-file", type=Path)
    validate_release_cmd.add_argument("--strict-warnings", action="store_true")
    validate_release_cmd.add_argument("--max-issues", type=int, default=200)
    validate_release_cmd.add_argument("--output", type=Path)
    validate_release_cmd.set_defaults(func=_cmd_validate_release)

    state_statute_completion = sub.add_parser(
        "state-statute-completion",
        help="Classify 50-state plus DC statute ingestion against the current release.",
    )
    state_statute_completion.add_argument("--base", type=Path, required=True)
    state_statute_completion.add_argument("--release", default="current")
    state_statute_completion.add_argument("--supabase-counts", type=Path)
    state_statute_completion.add_argument(
        "--validation-report",
        type=Path,
        help=(
            "validate-release JSON output. Defaults to "
            "<base>/analytics/validate-release-<release>.json when present."
        ),
    )
    state_statute_completion.add_argument(
        "--prefix",
        action="append",
        choices=list(DEFAULT_RELEASE_ARTIFACT_PREFIXES),
        default=[],
        help="Top-level artifact prefix to inspect. Defaults to release content prefixes.",
    )
    state_statute_completion.add_argument("--include-r2", action="store_true")
    state_statute_completion.add_argument("--bucket")
    state_statute_completion.add_argument("--endpoint-url")
    state_statute_completion.add_argument("--credentials-file", type=Path)
    state_statute_completion.add_argument(
        "--source-access-queue",
        type=Path,
        help=(
            "State statute agent queue with blocked source-access statuses. "
            "Defaults to manifests/state-statute-agent-queue.yaml when present."
        ),
    )
    state_statute_completion.add_argument("--output", type=Path)
    state_statute_completion.add_argument(
        "--require-complete",
        action="store_true",
        help="Exit nonzero unless every expected state/DC statute is productionized and validated.",
    )
    state_statute_completion.set_defaults(func=_cmd_state_statute_completion)

    regulation_completion = sub.add_parser(
        "regulation-completion",
        help="Classify federal plus state regulation ingestion against the current release.",
    )
    regulation_completion.add_argument("--base", type=Path, required=True)
    regulation_completion.add_argument("--release", default="current")
    regulation_completion.add_argument("--supabase-counts", type=Path)
    regulation_completion.add_argument(
        "--validation-report",
        type=Path,
        help=(
            "validate-release JSON output. Defaults to "
            "<base>/analytics/validate-release-<release>.json when present."
        ),
    )
    regulation_completion.add_argument(
        "--prefix",
        action="append",
        choices=list(DEFAULT_RELEASE_ARTIFACT_PREFIXES),
        default=[],
        help="Top-level artifact prefix to inspect. Defaults to release content prefixes.",
    )
    regulation_completion.add_argument("--include-r2", action="store_true")
    regulation_completion.add_argument("--bucket")
    regulation_completion.add_argument("--endpoint-url")
    regulation_completion.add_argument("--credentials-file", type=Path)
    regulation_completion.add_argument("--output", type=Path)
    regulation_completion.add_argument(
        "--require-complete",
        action="store_true",
        help=(
            "Exit nonzero unless every expected federal/state regulation corpus is "
            "productionized and validated."
        ),
    )
    regulation_completion.set_defaults(func=_cmd_regulation_completion)

    source_discovery = sub.add_parser(
        "source-discovery",
        help="Classify static external URL inventories for source-discovery operations.",
    )
    source_discovery.add_argument("--base", type=Path, required=True)
    source_discovery.add_argument(
        "--input",
        type=Path,
        action="append",
        required=True,
        help="Static URL-list file to classify. Repeatable.",
    )
    source_discovery.add_argument(
        "--source-name",
        default="policyengine-us",
        help="External discovery source label to write into the report.",
    )
    source_discovery.add_argument(
        "--release",
        default="current",
        help="Release manifest to compare for matching jurisdiction/class scopes. Use empty string to skip.",
    )
    source_discovery.add_argument("--output", type=Path)
    source_discovery.set_defaults(func=_cmd_source_discovery)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
