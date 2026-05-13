"""Supabase row projection for normalized provision JSONL."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO
from uuid import NAMESPACE_URL, UUID, uuid5

from axiom_corpus.corpus.models import ProvisionRecord
from axiom_corpus.corpus.releases import ReleaseManifest, ReleaseScope

DEFAULT_AXIOM_SUPABASE_URL = "https://swocpijqqahhuwtuahwc.supabase.co"
DEFAULT_SERVICE_KEY_ENV = "SUPABASE_SERVICE_ROLE_KEY"
DEFAULT_ACCESS_TOKEN_ENV = "SUPABASE_ACCESS_TOKEN"
USER_AGENT = "axiom-corpus/0.1"

SUPABASE_PROVISIONS_COLUMNS = (
    "id",
    "jurisdiction",
    "doc_type",
    "parent_id",
    "level",
    "ordinal",
    "heading",
    "body",
    "source_url",
    "source_path",
    "citation_path",
    "version",
    "rulespec_path",
    "has_rulespec",
    "source_document_id",
    "source_as_of",
    "expression_date",
    "language",
    "legal_identifier",
    "identifiers",
)


@dataclass(frozen=True)
class SupabaseLoadReport:
    rows_total: int
    rows_loaded: int
    chunk_count: int
    dry_run: bool = False
    existing_id_count: int = 0
    refreshed: bool = False
    refresh_error: str | None = None
    auto_registered_scopes: tuple[dict[str, object], ...] = ()

    def to_mapping(self) -> dict[str, object]:
        return {
            "rows_total": self.rows_total,
            "rows_loaded": self.rows_loaded,
            "chunk_count": self.chunk_count,
            "dry_run": self.dry_run,
            "existing_id_count": self.existing_id_count,
            "refreshed": self.refreshed,
            "refresh_error": self.refresh_error,
            "auto_registered_scopes": [dict(s) for s in self.auto_registered_scopes],
        }


@dataclass(frozen=True)
class SupabaseDeleteReport:
    intended_rows_deleted: int
    delete_chunk_count: int
    dry_run: bool = False

    def to_mapping(self) -> dict[str, object]:
        return {
            "intended_rows_deleted": self.intended_rows_deleted,
            "delete_chunk_count": self.delete_chunk_count,
            "dry_run": self.dry_run,
        }


@dataclass(frozen=True)
class SupabaseReleaseScopeSyncReport:
    release_name: str
    rows_total: int
    rows_loaded: int
    chunk_count: int
    dry_run: bool = False
    refreshed: bool = False
    refresh_error: str | None = None

    def to_mapping(self) -> dict[str, object]:
        return {
            "release_name": self.release_name,
            "rows_total": self.rows_total,
            "rows_loaded": self.rows_loaded,
            "chunk_count": self.chunk_count,
            "dry_run": self.dry_run,
            "refreshed": self.refreshed,
            "refresh_error": self.refresh_error,
        }


@dataclass(frozen=True)
class ReleaseCoverageFinding:
    """A jurisdiction × document_class with navigation rows but no current provisions."""

    jurisdiction: str
    document_class: str
    navigation_node_count: int
    current_provision_count: int

    def to_mapping(self) -> dict[str, object]:
        return {
            "jurisdiction": self.jurisdiction,
            "document_class": self.document_class,
            "navigation_node_count": self.navigation_node_count,
            "current_provision_count": self.current_provision_count,
        }


@dataclass(frozen=True)
class ReleaseCoverageReport:
    """Result of verifying the navigation → current_provisions join.

    The view ``corpus.current_provisions`` exists if and only if there is a
    matching row in ``corpus.release_scopes`` (release_name='current',
    active=true). A jurisdiction with navigation rows but no matching release
    scope row produces ``current_provision_count == 0`` here — the historical
    UK failure mode that left rows unreachable to consumers.
    """

    checked_at: str
    missing_current_provisions: tuple[ReleaseCoverageFinding, ...]

    @property
    def ok(self) -> bool:
        return not self.missing_current_provisions

    def to_mapping(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "checked_at": self.checked_at,
            "missing_current_provisions": [
                f.to_mapping() for f in self.missing_current_provisions
            ],
        }


def verify_release_coverage(
    *,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
) -> ReleaseCoverageReport:
    """Check the invariant: any jurisdiction with navigation rows must also
    have rows in ``corpus.current_provisions``.

    Reads two PostgREST views:
      * ``navigation_node_counts`` — per (jurisdiction, doc_type) row counts
        derived from active-release navigation rows
      * ``current_provision_counts`` — per (jurisdiction, document_class)
        row counts from corpus.current_provisions

    Reports any (jurisdiction, doc_type) pair where navigation has rows and
    current_provisions has zero. The historical UK regression (4,705 nav rows,
    0 current_provisions) is exactly this shape.
    """
    rest_url = _rest_url(supabase_url)
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Accept": "application/json",
        "Accept-Profile": "corpus",
        "User-Agent": USER_AGENT,
    }

    nav_counts = _fetch_navigation_node_counts(rest_url, headers)
    current_counts = _fetch_current_provision_counts(rest_url, headers)
    current_keys = {
        (row["jurisdiction"], row["document_class"]) for row in current_counts
    }

    missing: list[ReleaseCoverageFinding] = []
    for nav in nav_counts:
        jurisdiction = str(nav["jurisdiction"])
        document_class = str(nav["document_class"])
        count_value = nav["count"]
        count = int(count_value) if isinstance(count_value, int | str) else 0
        if count > 0 and (jurisdiction, document_class) not in current_keys:
            missing.append(
                ReleaseCoverageFinding(
                    jurisdiction=jurisdiction,
                    document_class=document_class,
                    navigation_node_count=count,
                    current_provision_count=0,
                )
            )

    return ReleaseCoverageReport(
        checked_at=datetime.now(UTC).isoformat(),
        missing_current_provisions=tuple(sorted(
            missing, key=lambda f: (f.jurisdiction, f.document_class)
        )),
    )


def _fetch_navigation_node_counts(
    rest_url: str, headers: dict[str, str]
) -> tuple[dict[str, object], ...]:
    """Get GROUP BY (jurisdiction, doc_type) counts via the corpus RPC.

    We do this server-side because corpus.navigation_nodes is ~2.4M rows
    and PostgREST caps responses at 1000 — paginating the whole table
    would take thousands of round trips. The RPC
    ``corpus.get_navigation_node_counts`` returns ~70 current-release rows
    in one call. See migrations 20260512170000_navigation_node_counts_rpc.sql
    and 20260513101000_version_aware_navigation_nodes.sql.
    """
    # POST RPC resolves the schema from Content-Profile, not Accept-Profile.
    # Without it PostgREST defaults to `public` and returns 404 for the
    # corpus.* function.
    req = urllib.request.Request(
        f"{rest_url}/rpc/get_navigation_node_counts",
        data=b"{}",
        method="POST",
        headers={
            **headers,
            "Content-Type": "application/json",
            "Content-Profile": "corpus",
        },
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        rows = json.loads(resp.read())
    if not isinstance(rows, list):
        return ()
    out: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append({
            "jurisdiction": str(row.get("jurisdiction") or ""),
            "document_class": str(row.get("document_class") or "unknown"),
            "count": int(row.get("node_count") or 0),
        })
    return tuple(out)


def _fetch_current_provision_counts(
    rest_url: str, headers: dict[str, str]
) -> tuple[dict[str, object], ...]:
    query = urllib.parse.urlencode({
        "select": "jurisdiction,document_class,provision_count",
    })
    req = urllib.request.Request(
        f"{rest_url}/current_provision_counts?{query}",
        headers=headers,
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        rows = json.loads(resp.read())
    if not isinstance(rows, list):
        return ()
    return tuple(
        {
            "jurisdiction": str(row.get("jurisdiction") or ""),
            "document_class": str(row.get("document_class") or "unknown"),
            "count": int(row.get("provision_count") or 0),
        }
        for row in rows
        if isinstance(row, dict)
    )


def deterministic_provision_id(citation_path: str, version: str | None = None) -> str:
    """Return the stable UUID used by `corpus.provisions` ingests.

    Historical callers passed only a citation path, so that form intentionally
    keeps the old UUID. Supabase loads now pass a release version as well so
    the same citation can exist in multiple staged/published versions.
    """
    normalized_version = _normalize_version(version)
    if normalized_version is None:
        return str(uuid5(NAMESPACE_URL, f"axiom:{citation_path}"))
    identity = json.dumps(
        ["axiom", normalized_version, citation_path],
        separators=(",", ":"),
    )
    return str(uuid5(NAMESPACE_URL, identity))


def _normalize_version(version: str | None) -> str | None:
    if version is None:
        return None
    normalized = str(version).strip()
    return normalized or None


def _release_document_class(document_class: str | None) -> str:
    normalized = str(document_class or "").strip()
    return normalized or "unknown"


def _uuid_or_none(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return str(UUID(str(value)))
    except ValueError:
        return None


def provision_to_supabase_row(
    record: ProvisionRecord,
    *,
    versioned_ids: bool = True,
) -> dict[str, object]:
    """Project a normalized provision record into the `corpus.provisions` shape."""
    version = _normalize_version(record.version)
    legacy_provision_id = deterministic_provision_id(record.citation_path)
    if versioned_ids and version is not None and (
        record.id is None or record.id == legacy_provision_id
    ):
        provision_id = deterministic_provision_id(record.citation_path, version)
    else:
        provision_id = record.id or legacy_provision_id
    parent_id = record.parent_id
    if parent_id is None and record.parent_citation_path:
        parent_id = deterministic_provision_id(
            record.parent_citation_path,
            version if versioned_ids else None,
        )
    elif versioned_ids and version is not None and record.parent_citation_path:
        legacy_parent_id = deterministic_provision_id(record.parent_citation_path)
        if parent_id == legacy_parent_id:
            parent_id = deterministic_provision_id(record.parent_citation_path, version)
    source_document_id = _uuid_or_none(record.source_document_id)
    identifiers = dict(record.identifiers or {})
    if record.source_document_id is not None and source_document_id is None:
        identifiers.setdefault("source:document_id", record.source_document_id)

    row: dict[str, object] = {
        "id": provision_id,
        "jurisdiction": record.jurisdiction,
        "doc_type": record.document_class,
        "parent_id": parent_id,
        "level": record.level,
        "ordinal": record.ordinal,
        "heading": record.heading,
        "body": record.body,
        "source_url": record.source_url,
        "source_path": record.source_path,
        "citation_path": record.citation_path,
        "version": version,
        "rulespec_path": record.rulespec_path,
        "has_rulespec": bool(record.has_rulespec) if record.has_rulespec is not None else False,
        "source_document_id": source_document_id,
        "source_as_of": record.source_as_of,
        "expression_date": record.expression_date,
        "language": record.language,
        "legal_identifier": record.legal_identifier,
        "identifiers": identifiers,
    }
    return row


def iter_supabase_rows(
    records: Iterable[ProvisionRecord],
    *,
    versioned_ids: bool = True,
) -> Iterator[dict[str, object]]:
    for record in records:
        yield provision_to_supabase_row(record, versioned_ids=versioned_ids)


def write_supabase_rows_jsonl(path: str | Path, records: Iterable[ProvisionRecord]) -> int:
    """Write rows ready for a Supabase REST upsert payload as JSONL."""
    rows = tuple(iter_supabase_rows(records))
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else "")
    )
    return len(rows)


def fetch_provision_counts(
    *,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
    include_legacy: bool = False,
) -> tuple[dict[str, object], ...]:
    """Fetch production provision-count rows.

    By default this reads the current release boundary. Set
    ``include_legacy=True`` for a full table snapshot that includes scopes not
    present in the current release manifest.
    """
    table_name = "provision_counts" if include_legacy else "current_provision_counts"
    query = urllib.parse.urlencode(
        {
            "select": (
                "jurisdiction,document_class,provision_count,body_count,"
                "top_level_count,rulespec_count,refreshed_at"
            ),
            "order": "jurisdiction.asc,document_class.asc",
        }
    )
    req = urllib.request.Request(
        f"{_rest_url(supabase_url)}/{table_name}?{query}",
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Accept": "application/json",
            "Accept-Profile": "corpus",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        rows = json.loads(resp.read())
    if not isinstance(rows, list):
        raise RuntimeError("unexpected Supabase provision-count response")
    return tuple(_normalize_count_row(row) for row in rows if isinstance(row, dict))


def sync_release_scopes_to_supabase(
    release: ReleaseManifest,
    *,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
    chunk_size: int = 500,
    refresh: bool = True,
    dry_run: bool = False,
    allow_refresh_failure: bool = False,
    exclusive: bool = False,
) -> SupabaseReleaseScopeSyncReport:
    """Sync the Supabase release-scope set from a release manifest.

    Default behavior (``exclusive=False``) is **upsert-incremental**: each
    scope in the manifest is upserted (insert-or-update) into
    ``corpus.release_scopes`` with ``active=true``. Scopes already in the
    table but NOT in the manifest are left untouched. Safe to run from a
    feature branch whose manifest is a subset of production state.

    ``exclusive=True`` opts into the older "deactivate all then reinsert"
    semantics: every existing active row for ``release.name`` is marked
    inactive first, then the manifest's rows are inserted active. Use only
    when you specifically want to enforce that the manifest is the complete
    set of active scopes for the release — typically not what you want from
    a branch that does not have full coverage of production.

    The historical default was ``exclusive=True``. That behavior caused a
    silent unpromotion of ``us-wa/regulation`` on 2026-05-12 when a feature
    branch's manifest was used to sync (the WAC scope existed on a
    different branch). The new default eliminates this class of regression.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    rest_url = _rest_url(supabase_url)
    synced_at = datetime.now(UTC).isoformat()
    rows = [
        release_scope_to_supabase_row(
            scope,
            release_name=release.name,
            synced_at=synced_at,
        )
        for scope in release.scopes
    ]
    chunk_count = 0
    if rows:
        chunk_count = (len(rows) + chunk_size - 1) // chunk_size

    if not dry_run:
        if exclusive:
            deactivate_release_scope_rows(
                release_name=release.name,
                service_key=service_key,
                rest_url=rest_url,
            )
        for chunk in _chunked(iter(rows), chunk_size):
            upsert_release_scope_rows(chunk, service_key=service_key, rest_url=rest_url)

    refreshed = False
    refresh_error = None
    if refresh and not dry_run:
        try:
            refresh_corpus_analytics(service_key=service_key, rest_url=rest_url)
            refreshed = True
        except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError, RuntimeError) as exc:
            refresh_error = str(exc)
            if not allow_refresh_failure:
                raise RuntimeError(f"corpus analytics refresh failed: {exc}") from exc

    return SupabaseReleaseScopeSyncReport(
        release_name=release.name,
        rows_total=len(rows),
        rows_loaded=0 if dry_run else len(rows),
        chunk_count=chunk_count,
        dry_run=dry_run,
        refreshed=refreshed,
        refresh_error=refresh_error,
    )


def release_scope_to_supabase_row(
    scope: ReleaseScope,
    *,
    release_name: str,
    synced_at: str,
) -> dict[str, object]:
    return {
        "release_name": release_name,
        "jurisdiction": scope.jurisdiction,
        "document_class": scope.document_class,
        "version": scope.version,
        "active": True,
        "synced_at": synced_at,
    }

def _normalize_count_row(row: Mapping[str, object]) -> dict[str, object]:
    jurisdiction = row.get("jurisdiction")
    document_class = row.get("document_class")
    provision_count = row.get("provision_count")
    if jurisdiction is None or document_class is None or provision_count is None:
        raise RuntimeError("Supabase provision-count row is missing required fields")
    normalized: dict[str, object] = {
        "jurisdiction": str(jurisdiction),
        "document_class": str(document_class),
        "provision_count": _count_value(provision_count),
    }
    for key in ("body_count", "top_level_count", "rulespec_count"):
        value = row.get(key)
        if value is not None:
            normalized[key] = _count_value(value)
    if row.get("refreshed_at") is not None:
        normalized["refreshed_at"] = str(row["refreshed_at"])
    return normalized


def _count_value(value: object) -> int:
    if isinstance(value, bool):
        raise RuntimeError("Supabase count value must be numeric")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise RuntimeError("Supabase count value must be numeric")


def resolve_service_key(
    supabase_url: str,
    *,
    service_key: str | None = None,
    environ: Mapping[str, str] = os.environ,
    service_key_env: str = DEFAULT_SERVICE_KEY_ENV,
    access_token_env: str = DEFAULT_ACCESS_TOKEN_ENV,
) -> str:
    """Resolve the Supabase service role key without persisting credentials."""
    if service_key:
        return service_key
    env_service_key = environ.get(service_key_env)
    if env_service_key:
        return env_service_key
    access_token = environ.get(access_token_env)
    if not access_token:
        raise RuntimeError(
            f"{service_key_env} or {access_token_env} env var required for Supabase load"
        )

    project_ref = _project_ref_from_url(supabase_url)
    req = urllib.request.Request(
        f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
        headers={
            "Authorization": f"Bearer {access_token}",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        keys = json.loads(resp.read())
    for entry in keys:
        if entry.get("name") == "service_role" and entry.get("api_key"):
            return str(entry["api_key"])
    raise RuntimeError("service_role key not found")


def load_provisions_to_supabase(
    records: Iterable[ProvisionRecord],
    *,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
    chunk_size: int = 500,
    refresh: bool = True,
    dry_run: bool = False,
    allow_refresh_failure: bool = False,
    preserve_existing_ids: bool = False,
    progress_stream: TextIO | None = None,
    auto_register_scopes: bool = True,
    auto_publish: bool = True,
    release_name: str = "current",
) -> SupabaseLoadReport:
    """Upsert normalized provision records into `corpus.provisions`.

    By default, also ensures a row in ``corpus.release_scopes`` exists for
    each distinct ``(jurisdiction, document_class, version)`` triple in the
    loaded records, with ``active=True`` so the data is immediately visible
    via ``corpus.current_provisions``. This eliminates the silent-invisibility
    bug class where data was loaded but invisible because nobody added a
    matching release row.

    Pass ``auto_publish=False`` to stage the load (rows created but
    invisible — flip later via ``axiom-corpus-ingest publish``). Pass
    ``auto_register_scopes=False`` to skip release_scopes management
    entirely (legacy behavior; not recommended).
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    existing_id_count = 0
    records_iter = records
    total_records: int | None = None
    if preserve_existing_ids and not dry_run:
        materialized_records = tuple(records)
        total_records = len(materialized_records)
        existing_ids = fetch_existing_provision_ids(
            (record.citation_path for record in materialized_records),
            service_key=service_key,
            rest_url=_rest_url(supabase_url),
            chunk_size=100,
        )
        existing_id_count = len(existing_ids)
        if progress_stream is not None:
            print(
                f"resolved {existing_id_count} existing Supabase IDs "
                f"for {total_records} provisions",
                file=progress_stream,
                flush=True,
            )
        records_iter = (
            _record_with_existing_ids(record, existing_ids) for record in materialized_records
        )

    release_scope_keys: set[tuple[str, str, str]] = set()

    def _capture_release_scope_keys(
        source: Iterable[ProvisionRecord],
    ) -> Iterator[ProvisionRecord]:
        for record in source:
            version = _normalize_version(record.version)
            if auto_register_scopes and version is None:
                raise ValueError(
                    "ProvisionRecord.version is required when auto-registering "
                    "Supabase release scopes; pass --no-auto-register only for "
                    "explicit legacy/migration loads."
                )
            if version is not None:
                release_scope_keys.add(
                    (
                        record.jurisdiction,
                        _release_document_class(record.document_class),
                        version,
                    )
                )
            yield record

    records_iter = _capture_release_scope_keys(records_iter)

    rows_loaded = 0
    chunk_count = 0
    rest_url = _rest_url(supabase_url)
    row_iter = iter_supabase_rows(records_iter, versioned_ids=not preserve_existing_ids)
    for chunk in _chunked(row_iter, chunk_size):
        chunk_count += 1
        if not dry_run:
            upsert_supabase_rows(chunk, service_key=service_key, rest_url=rest_url)
        rows_loaded += len(chunk)
        if progress_stream is not None and (chunk_count == 1 or chunk_count % 10 == 0):
            total_text = f"/{total_records}" if total_records is not None else ""
            print(
                f"processed Supabase chunk {chunk_count} ({rows_loaded}{total_text} rows)",
                file=progress_stream,
                flush=True,
            )

    # Auto-register release_scopes rows after the provisions upsert succeeds.
    auto_registered: tuple[dict[str, object], ...] = ()
    if auto_register_scopes and not dry_run and release_scope_keys:
        auto_registered = ensure_release_scopes_for_loaded_data(
            release_scope_keys,
            release_name=release_name,
            active=auto_publish,
            service_key=service_key,
            rest_url=rest_url,
        )
        if progress_stream is not None:
            for row in auto_registered:
                state = "published" if row.get("active") is True else "staged (unpublished)"
                print(
                    f"release scope ready: {row['jurisdiction']}/"
                    f"{row['document_class']} v{row['version']} → {state}",
                    file=progress_stream,
                    flush=True,
                )

    refreshed = False
    refresh_error = None
    if refresh and not dry_run:
        try:
            refresh_corpus_analytics(service_key=service_key, rest_url=rest_url)
            refreshed = True
        except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError, RuntimeError) as exc:
            refresh_error = str(exc)
            if not allow_refresh_failure:
                raise RuntimeError(f"corpus analytics refresh failed: {exc}") from exc

    return SupabaseLoadReport(
        rows_total=rows_loaded,
        rows_loaded=0 if dry_run else rows_loaded,
        chunk_count=chunk_count,
        dry_run=dry_run,
        existing_id_count=existing_id_count,
        refreshed=refreshed,
        refresh_error=refresh_error,
        auto_registered_scopes=auto_registered,
    )


def delete_supabase_provisions_scope(
    *,
    jurisdiction: str,
    document_class: str,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
    delete_chunk_size: int = 100,
    fetch_page_size: int = 1_000,
    dry_run: bool = False,
    progress_stream: TextIO | None = None,
) -> SupabaseDeleteReport:
    """Delete all `corpus.provisions` rows for one jurisdiction/document class."""
    if delete_chunk_size <= 0:
        raise ValueError("delete_chunk_size must be positive")
    if fetch_page_size <= 0:
        raise ValueError("fetch_page_size must be positive")
    if dry_run:
        return SupabaseDeleteReport(
            intended_rows_deleted=0,
            delete_chunk_count=0,
            dry_run=True,
        )

    rest_url = _rest_url(supabase_url)
    provision_ids = fetch_provision_ids_for_scope(
        jurisdiction=jurisdiction,
        document_class=document_class,
        service_key=service_key,
        rest_url=rest_url,
        page_size=fetch_page_size,
    )
    intended_rows_deleted = 0
    delete_chunk_count = 0
    for chunk in _chunked_values(provision_ids, delete_chunk_size):
        delete_chunk_count += 1
        delete_supabase_provision_ids(chunk, service_key=service_key, rest_url=rest_url)
        intended_rows_deleted += len(chunk)
        if progress_stream is not None and (
            delete_chunk_count == 1 or delete_chunk_count % 10 == 0
        ):
            print(
                f"deleted Supabase chunk {delete_chunk_count} "
                f"({intended_rows_deleted}/{len(provision_ids)} scoped rows)",
                file=progress_stream,
                flush=True,
            )
    return SupabaseDeleteReport(
        intended_rows_deleted=intended_rows_deleted,
        delete_chunk_count=delete_chunk_count,
    )


def fetch_existing_provision_ids(
    citation_paths: Iterable[str],
    *,
    service_key: str,
    rest_url: str,
    chunk_size: int = 100,
) -> dict[str, str]:
    """Fetch current provision IDs keyed by citation path for in-place migrations."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    existing: dict[str, str] = {}
    unique_paths = sorted(set(citation_paths))
    for chunk in _chunked_values(unique_paths, chunk_size):
        if not chunk:
            continue
        filter_value = "in.(" + ",".join(_postgrest_in_value(value) for value in chunk) + ")"
        query = urllib.parse.urlencode(
            {
                "select": "id,citation_path",
                "citation_path": filter_value,
            }
        )
        req = urllib.request.Request(
            f"{rest_url}/provisions?{query}",
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
                "Accept": "application/json",
                "Accept-Profile": "corpus",
                "User-Agent": USER_AGENT,
            },
        )
        with urllib.request.urlopen(req, timeout=180) as resp:
            rows = json.loads(resp.read())
        if not isinstance(rows, list):
            raise RuntimeError("unexpected Supabase existing-id response")
        for row in rows:
            if not isinstance(row, dict):
                continue
            citation_path = row.get("citation_path")
            provision_id = row.get("id")
            if citation_path and provision_id:
                existing[str(citation_path)] = str(provision_id)
    return existing


def fetch_provision_ids_for_scope(
    *,
    jurisdiction: str,
    document_class: str,
    service_key: str,
    rest_url: str,
    page_size: int = 1_000,
) -> tuple[str, ...]:
    scoped_rows: list[tuple[str, int]] = []
    last_id: str | None = None
    while True:
        query_params = {
            "select": "id,level",
            "jurisdiction": f"eq.{jurisdiction}",
            "doc_type": f"eq.{document_class}",
            "order": "id.asc",
            "limit": str(page_size),
        }
        if last_id is not None:
            query_params["id"] = f"gt.{last_id}"
        query = urllib.parse.urlencode(query_params)
        req = urllib.request.Request(
            f"{rest_url}/provisions?{query}",
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
                "Accept": "application/json",
                "Accept-Profile": "corpus",
                "User-Agent": USER_AGENT,
            },
        )
        with urllib.request.urlopen(req, timeout=180) as resp:
            rows = json.loads(resp.read())
        if not isinstance(rows, list):
            raise RuntimeError("unexpected Supabase scope-id response")
        page_rows = tuple(
            (
                str(row["id"]),
                int(row.get("level") or 0),
            )
            for row in rows
            if isinstance(row, dict) and row.get("id") is not None
        )
        scoped_rows.extend(page_rows)
        if len(page_rows) < page_size:
            break
        last_id = page_rows[-1][0]
    scoped_rows.sort(key=lambda row: (-row[1], row[0]))
    return tuple(row[0] for row in scoped_rows)


def delete_supabase_provision_ids(
    provision_ids: list[str],
    *,
    service_key: str,
    rest_url: str,
) -> None:
    if not provision_ids:
        return
    query = urllib.parse.urlencode(
        {"id": "in.(" + ",".join(_postgrest_in_value(value) for value in provision_ids) + ")"}
    )
    req = urllib.request.Request(
        f"{rest_url}/provisions?{query}",
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Accept-Profile": "corpus",
            "Content-Profile": "corpus",
            "Prefer": "return=minimal",
            "User-Agent": USER_AGENT,
        },
        method="DELETE",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        resp.read()


def _record_with_existing_ids(
    record: ProvisionRecord,
    existing_ids: Mapping[str, str],
) -> ProvisionRecord:
    provision_id = existing_ids.get(record.citation_path, record.id)
    parent_id = record.parent_id
    if record.parent_citation_path and record.parent_citation_path in existing_ids:
        parent_id = existing_ids[record.parent_citation_path]
    if provision_id == record.id and parent_id == record.parent_id:
        return record
    return replace(record, id=provision_id, parent_id=parent_id)


def upsert_supabase_rows(
    rows: list[dict[str, object]],
    *,
    service_key: str,
    rest_url: str,
) -> None:
    if not rows:
        return
    req = urllib.request.Request(
        f"{rest_url}/provisions?on_conflict=id",
        data=json.dumps(rows).encode("utf-8"),
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
            "Content-Profile": "corpus",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"upsert failed {exc.code}: {body}") from exc


def set_release_scope_active(
    *,
    jurisdiction: str,
    document_class: str,
    active: bool,
    release_name: str = "current",
    version: str | None = None,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
    refresh: bool = True,
) -> dict[str, object]:
    """Flip the ``active`` flag on a single release_scopes row.

    If ``version`` is None, picks the most recent row for (release_name,
    jurisdiction, document_class) by ``synced_at`` descending. Raises if no
    matching row exists.

    Returns the affected row's contents as a dict, plus the refresh result.
    """
    rest_url = _rest_url(supabase_url)
    auth = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Accept-Profile": "corpus",
        "User-Agent": USER_AGENT,
    }

    if version is None:
        # Find the most recent row for this (release_name, jurisdiction, doc_class).
        query = urllib.parse.urlencode({
            "release_name": f"eq.{release_name}",
            "jurisdiction": f"eq.{jurisdiction}",
            "document_class": f"eq.{document_class}",
            "select": "version,synced_at,active",
            "order": "synced_at.desc",
            "limit": 1,
        })
        req = urllib.request.Request(
            f"{rest_url}/release_scopes?{query}",
            headers=auth,
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            rows = json.loads(resp.read())
        if not rows:
            raise RuntimeError(
                f"no release_scopes row found for "
                f"({release_name}, {jurisdiction}, {document_class}); "
                f"load data first or pass --version to disambiguate"
            )
        version = rows[0]["version"]

    # PATCH the target row.
    query = urllib.parse.urlencode({
        "release_name": f"eq.{release_name}",
        "jurisdiction": f"eq.{jurisdiction}",
        "document_class": f"eq.{document_class}",
        "version": f"eq.{version}",
    })
    req = urllib.request.Request(
        f"{rest_url}/release_scopes?{query}",
        data=json.dumps({"active": active}).encode(),
        method="PATCH",
        headers={
            **auth,
            "Content-Type": "application/json",
            "Content-Profile": "corpus",
            "Prefer": "return=representation",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
    if not result:
        raise RuntimeError(
            f"PATCH affected zero rows: ({release_name}, {jurisdiction}, "
            f"{document_class}, {version})"
        )

    refreshed = False
    refresh_error: str | None = None
    if refresh:
        try:
            refresh_corpus_analytics(service_key=service_key, rest_url=rest_url)
            refreshed = True
        except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError, RuntimeError) as exc:
            refresh_error = str(exc)

    return {
        "scope": result[0],
        "refreshed": refreshed,
        "refresh_error": refresh_error,
    }


def list_release_scopes(
    *,
    release_name: str = "current",
    active: bool | None = None,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
) -> tuple[dict[str, object], ...]:
    """List release_scopes rows, optionally filtered by active state."""
    rest_url = _rest_url(supabase_url)
    params: dict[str, str] = {
        "release_name": f"eq.{release_name}",
        "select": "release_name,jurisdiction,document_class,version,active,synced_at",
        "order": "jurisdiction.asc,document_class.asc,synced_at.desc",
    }
    if active is not None:
        params["active"] = f"is.{str(active).lower()}"

    req = urllib.request.Request(
        f"{rest_url}/release_scopes?{urllib.parse.urlencode(params)}",
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Accept-Profile": "corpus",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        rows = json.loads(resp.read())
    if not isinstance(rows, list):
        return ()
    return tuple(row for row in rows if isinstance(row, dict))


def ensure_release_scopes_for_loaded_data(
    release_scope_keys: Iterable[tuple[str, str, str]],
    *,
    release_name: str = "current",
    active: bool = True,
    service_key: str,
    rest_url: str,
) -> tuple[dict[str, object], ...]:
    """Idempotently ensure a release_scopes row exists for each loaded scope.

    Uses ``Prefer: resolution=ignore-duplicates``: if a row already exists
    for ``(release_name, jurisdiction, document_class, version)``, this
    function leaves it alone — including its ``active`` flag. State
    changes always require an explicit ``axiom-corpus-ingest publish`` /
    ``unpublish`` invocation, never a re-load.

    This protects against the surprise that a re-load with ``--stage``
    would silently demote a previously-published scope (or that a re-load
    without ``--stage`` would silently undo an explicit unpublish).
    Re-loads of existing scopes become no-ops at the release_scopes layer.

    Default ``active=True`` for newly-inserted rows matches the design
    choice that loading new data should publish it by default. Callers
    wanting to stage explicitly pass ``active=False``.
    """
    keys = tuple(sorted(set(release_scope_keys)))
    if not keys:
        return ()

    synced_at = datetime.now(UTC).isoformat()
    rows = [
        {
            "release_name": release_name,
            "jurisdiction": jurisdiction,
            "document_class": document_class,
            "version": version,
            "active": active,
            "synced_at": synced_at,
        }
        for jurisdiction, document_class, version in keys
    ]
    insert_release_scope_rows_ignore_duplicates(
        rows, service_key=service_key, rest_url=rest_url
    )
    return tuple(
        fetch_release_scope_row(
            release_name=release_name,
            jurisdiction=jurisdiction,
            document_class=document_class,
            version=version,
            service_key=service_key,
            rest_url=rest_url,
        )
        for jurisdiction, document_class, version in keys
    )


def fetch_release_scope_row(
    *,
    release_name: str,
    jurisdiction: str,
    document_class: str,
    version: str,
    service_key: str,
    rest_url: str,
) -> dict[str, object]:
    """Fetch the persisted release_scopes row after an ignore-duplicates insert."""
    query = urllib.parse.urlencode(
        {
            "release_name": f"eq.{release_name}",
            "jurisdiction": f"eq.{jurisdiction}",
            "document_class": f"eq.{document_class}",
            "version": f"eq.{version}",
            "select": "release_name,jurisdiction,document_class,version,active,synced_at",
            "limit": "1",
        }
    )
    req = urllib.request.Request(
        f"{rest_url}/release_scopes?{query}",
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Accept": "application/json",
            "Accept-Profile": "corpus",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        rows = json.loads(resp.read())
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise RuntimeError(
            "release-scope insert/readback failed for "
            f"({release_name}, {jurisdiction}, {document_class}, {version})"
        )
    return dict(rows[0])


def insert_release_scope_rows_ignore_duplicates(
    rows: list[dict[str, object]],
    *,
    service_key: str,
    rest_url: str,
) -> None:
    """Insert release_scopes rows; skip any row that conflicts on the unique
    key (release_name, jurisdiction, document_class, version).

    Sister function to ``upsert_release_scope_rows`` which uses
    ``resolution=merge-duplicates``. The auto-register-on-load flow uses
    this ignore-duplicates variant so that re-loads don't silently flip
    the ``active`` flag on existing rows.
    """
    if not rows:
        return
    req = urllib.request.Request(
        (
            f"{rest_url}/release_scopes?"
            "on_conflict=release_name,jurisdiction,document_class,version"
        ),
        data=json.dumps(rows).encode("utf-8"),
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=ignore-duplicates,return=minimal",
            "Content-Profile": "corpus",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(
            f"release-scope ignore-duplicates insert failed {exc.code}: {body}"
        ) from exc


def deactivate_release_scope_rows(
    *,
    release_name: str,
    service_key: str,
    rest_url: str,
) -> None:
    query = urllib.parse.urlencode(
        {
            "release_name": f"eq.{release_name}",
            "active": "eq.true",
        }
    )
    req = urllib.request.Request(
        f"{rest_url}/release_scopes?{query}",
        data=json.dumps({"active": False}).encode("utf-8"),
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
            "Content-Profile": "corpus",
            "User-Agent": USER_AGENT,
        },
        method="PATCH",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        resp.read()


def upsert_release_scope_rows(
    rows: list[dict[str, object]],
    *,
    service_key: str,
    rest_url: str,
) -> None:
    if not rows:
        return
    req = urllib.request.Request(
        (
            f"{rest_url}/release_scopes?"
            "on_conflict=release_name,jurisdiction,document_class,version"
        ),
        data=json.dumps(rows).encode("utf-8"),
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
            "Content-Profile": "corpus",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"release-scope upsert failed {exc.code}: {body}") from exc


def refresh_corpus_analytics(*, service_key: str, rest_url: str) -> None:
    """Refresh corpus analytics after loading provision rows."""
    _post_refresh_rpc(
        service_key=service_key, rest_url=rest_url, rpc_name="refresh_corpus_analytics"
    )


def _post_refresh_rpc(*, service_key: str, rest_url: str, rpc_name: str) -> None:
    req = urllib.request.Request(
        f"{rest_url}/rpc/{rpc_name}",
        data=b"{}",
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Content-Profile": "corpus",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        resp.read()


def _chunked(
    rows: Iterable[dict[str, object]],
    size: int,
) -> Iterator[list[dict[str, object]]]:
    chunk: list[dict[str, object]] = []
    for row in rows:
        chunk.append(row)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _chunked_values(values: Iterable[str], size: int) -> Iterator[list[str]]:
    chunk: list[str] = []
    for value in values:
        chunk.append(value)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _postgrest_in_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _rest_url(supabase_url: str) -> str:
    return f"{supabase_url.rstrip('/')}/rest/v1"


def _project_ref_from_url(supabase_url: str) -> str:
    parsed = urllib.parse.urlparse(supabase_url)
    host = parsed.netloc or parsed.path
    if not host:
        raise ValueError(f"invalid Supabase URL: {supabase_url}")
    return host.split(".", 1)[0]
