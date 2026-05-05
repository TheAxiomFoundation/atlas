"""Supabase row projection for normalized provision JSONL."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TextIO
from uuid import NAMESPACE_URL, uuid5

from axiom_corpus.corpus.models import ProvisionRecord

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

    def to_mapping(self) -> dict[str, object]:
        return {
            "rows_total": self.rows_total,
            "rows_loaded": self.rows_loaded,
            "chunk_count": self.chunk_count,
            "dry_run": self.dry_run,
            "existing_id_count": self.existing_id_count,
            "refreshed": self.refreshed,
            "refresh_error": self.refresh_error,
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


def deterministic_provision_id(citation_path: str) -> str:
    """Return the stable UUID used by existing `corpus.provisions` ingests."""
    return str(uuid5(NAMESPACE_URL, f"axiom:{citation_path}"))


def provision_to_supabase_row(record: ProvisionRecord) -> dict[str, object]:
    """Project a normalized provision record into the `corpus.provisions` shape."""
    provision_id = record.id or deterministic_provision_id(record.citation_path)
    parent_id = record.parent_id
    if parent_id is None and record.parent_citation_path:
        parent_id = deterministic_provision_id(record.parent_citation_path)

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
        "rulespec_path": record.rulespec_path,
        "has_rulespec": bool(record.has_rulespec) if record.has_rulespec is not None else False,
        "source_document_id": record.source_document_id,
        "source_as_of": record.source_as_of,
        "expression_date": record.expression_date,
        "language": record.language,
        "legal_identifier": record.legal_identifier,
        "identifiers": record.identifiers or {},
    }
    return row


def iter_supabase_rows(records: Iterable[ProvisionRecord]) -> Iterator[dict[str, object]]:
    for record in records:
        yield provision_to_supabase_row(record)


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
) -> tuple[dict[str, object], ...]:
    """Fetch the production `corpus.provision_counts` materialized-view rows."""
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
        f"{_rest_url(supabase_url)}/provision_counts?{query}",
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
) -> SupabaseLoadReport:
    """Upsert normalized provision records into `corpus.provisions`."""
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

    rows_loaded = 0
    chunk_count = 0
    rest_url = _rest_url(supabase_url)
    row_iter = iter_supabase_rows(records_iter)
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
