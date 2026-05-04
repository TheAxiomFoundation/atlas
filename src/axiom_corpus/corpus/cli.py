"""CLI for the source-first corpus pipeline."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

from axiom_corpus.corpus.analytics import (
    build_analytics_report,
    load_provision_count_snapshot,
)
from axiom_corpus.corpus.artifacts import CorpusArtifactStore, sha256_bytes
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
from axiom_corpus.corpus.r2 import (
    DEFAULT_ARTIFACT_PREFIXES,
    DEFAULT_RELEASE_ARTIFACT_PREFIXES,
    build_artifact_report,
    build_artifact_report_with_r2,
    build_release_artifact_manifest,
    load_r2_config,
    sync_artifacts_to_r2,
)
from axiom_corpus.corpus.release_quality import validate_release
from axiom_corpus.corpus.releases import ReleaseManifest, resolve_release_manifest_path
from axiom_corpus.corpus.state_statute_completion import (
    build_state_statute_completion_report,
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
)
from axiom_corpus.corpus.supabase import (
    DEFAULT_ACCESS_TOKEN_ENV,
    DEFAULT_AXIOM_SUPABASE_URL,
    DEFAULT_SERVICE_KEY_ENV,
    delete_supabase_provisions_scope,
    load_provisions_to_supabase,
    resolve_service_key,
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
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


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
    payload = {
        "version": manifest.version,
        "source_count": len(selected),
        "completed_count": len(rows),
        "failed_count": len(failures),
        "coverage_complete": coverage_complete,
        "provisions_written": sum(int(row["provisions_written"]) for row in rows),
        "rows": rows,
        "failures": failures,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if failures:
        return 1
    return 0 if coverage_complete or args.allow_incomplete else 2


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
        "minnesota-statutes",
        "nebraska-revised-statutes",
        "ohio-revised-code",
        "texas-tcas",
    }:
        return _optional_manifest_path(manifest_path, options, "source_dir")
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


def _artifact_scope_filter_supplied(args: argparse.Namespace) -> bool:
    return any((args.version, args.jurisdiction, args.document_class))


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
    load_supabase.set_defaults(func=_cmd_load_supabase)

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
    state_statute_completion.add_argument("--output", type=Path)
    state_statute_completion.add_argument(
        "--require-complete",
        action="store_true",
        help="Exit nonzero unless every expected state/DC statute is productionized and validated.",
    )
    state_statute_completion.set_defaults(func=_cmd_state_statute_completion)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
