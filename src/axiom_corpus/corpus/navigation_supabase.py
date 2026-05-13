"""Supabase REST writer for the precomputed `corpus.navigation_nodes` index.

Mirrors the chunked upsert / scoped-delete pattern used by
`axiom_corpus.corpus.supabase` for `corpus.provisions`. Rebuilding scope-by-
scope deletes only rows for the (jurisdiction, doc_type) being rebuilt, so
unrelated scopes are untouched. Repeated runs are idempotent because the
deterministic IDs and column projections are stable.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TextIO

from axiom_corpus.corpus.models import ProvisionRecord
from axiom_corpus.corpus.navigation import NavigationNode, group_nodes_by_scope
from axiom_corpus.corpus.supabase import (
    DEFAULT_AXIOM_SUPABASE_URL,
    USER_AGENT,
    _postgrest_in_value,
    _rest_url,
)

NavigationScope = tuple[str, str, str | None]

_PROVISION_FIELDS = (
    "id",
    "jurisdiction",
    "doc_type",
    "parent_id",
    "level",
    "ordinal",
    "heading",
    "citation_path",
    "version",
    "rulespec_path",
    "has_rulespec",
    "language",
    "legal_identifier",
    "identifiers",
)


@dataclass(frozen=True)
class NavigationSupabaseWriteReport:
    rows_total: int
    rows_loaded: int
    chunk_count: int
    scopes_replaced: tuple[NavigationScope, ...]
    rows_deleted: int
    delete_chunk_count: int
    dry_run: bool = False

    def to_mapping(self) -> dict[str, object]:
        return {
            "rows_total": self.rows_total,
            "rows_loaded": self.rows_loaded,
            "chunk_count": self.chunk_count,
            "scopes_replaced": [list(scope) for scope in self.scopes_replaced],
            "rows_deleted": self.rows_deleted,
            "delete_chunk_count": self.delete_chunk_count,
            "dry_run": self.dry_run,
        }


def write_navigation_nodes_to_supabase(
    nodes: Iterable[NavigationNode],
    *,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
    chunk_size: int = 500,
    delete_chunk_size: int = 200,
    replace_scope: bool = True,
    replace_scopes: Iterable[NavigationScope] | None = None,
    dry_run: bool = False,
    progress_stream: TextIO | None = None,
) -> NavigationSupabaseWriteReport:
    """Upsert navigation rows into `corpus.navigation_nodes`.

    When ``replace_scope`` is true the writer first deletes existing rows for
    every ``(jurisdiction, doc_type, version)`` represented in the input,
    plus any explicit ``replace_scopes``. Scopes not in those sets are never
    touched.
    The loader is otherwise idempotent: rerun with the same input and the
    table converges on the same rows because IDs are deterministic and
    unscoped rows would only arise if a provision was deleted upstream —
    which is exactly what a scoped rebuild also fixes.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if delete_chunk_size <= 0:
        raise ValueError("delete_chunk_size must be positive")

    materialized = tuple(nodes)
    grouped = group_nodes_by_scope(materialized)
    scope_set = set(grouped)
    scope_set.update(replace_scopes or ())
    scopes_to_replace = (
        tuple(sorted(scope_set, key=lambda scope: (scope[0], scope[1], scope[2] or "")))
        if replace_scope
        else ()
    )
    rest_url = _rest_url(supabase_url)

    rows_deleted = 0
    delete_chunk_count = 0
    if replace_scope and not dry_run:
        for jurisdiction, doc_type, version in scopes_to_replace:
            scope_nodes = grouped.get((jurisdiction, doc_type, version), ())
            scope_rows_deleted, scope_chunks = _delete_scope_excluding_paths(
                jurisdiction=jurisdiction,
                doc_type=doc_type,
                version=version,
                keep_paths={node.path for node in scope_nodes},
                service_key=service_key,
                rest_url=rest_url,
                delete_chunk_size=delete_chunk_size,
            )
            rows_deleted += scope_rows_deleted
            delete_chunk_count += scope_chunks
            if progress_stream is not None:
                print(
                    f"navigation: pruned {scope_rows_deleted} stale rows in "
                    f"({jurisdiction}, {doc_type}, {version or 'legacy'})",
                    file=progress_stream,
                    flush=True,
                )

    rows_loaded = 0
    chunk_count = 0
    rows = [node.to_supabase_row() for node in materialized]
    for chunk in _chunked_rows(rows, chunk_size):
        chunk_count += 1
        if not dry_run:
            _upsert_navigation_rows(
                chunk,
                service_key=service_key,
                rest_url=rest_url,
            )
        rows_loaded += len(chunk)
        if progress_stream is not None and (chunk_count == 1 or chunk_count % 10 == 0):
            print(
                f"navigation: upserted chunk {chunk_count} ({rows_loaded}/{len(rows)} rows)",
                file=progress_stream,
                flush=True,
            )

    return NavigationSupabaseWriteReport(
        rows_total=len(rows),
        rows_loaded=0 if dry_run else rows_loaded,
        chunk_count=chunk_count,
        scopes_replaced=scopes_to_replace,
        rows_deleted=rows_deleted,
        delete_chunk_count=delete_chunk_count,
        dry_run=dry_run,
    )


def _delete_scope_excluding_paths(
    *,
    jurisdiction: str,
    doc_type: str,
    version: str | None,
    keep_paths: set[str],
    service_key: str,
    rest_url: str,
    delete_chunk_size: int,
) -> tuple[int, int]:
    existing = _fetch_scope_paths(
        jurisdiction=jurisdiction,
        doc_type=doc_type,
        version=version,
        service_key=service_key,
        rest_url=rest_url,
    )
    stale = [path for path in existing if path not in keep_paths]
    if not stale:
        return 0, 0
    rows_deleted = 0
    chunk_count = 0
    for chunk in _chunked_strings(stale, delete_chunk_size):
        chunk_count += 1
        _delete_navigation_paths(
            chunk,
            jurisdiction=jurisdiction,
            doc_type=doc_type,
            version=version,
            service_key=service_key,
            rest_url=rest_url,
        )
        rows_deleted += len(chunk)
    return rows_deleted, chunk_count


def _fetch_scope_paths(
    *,
    jurisdiction: str,
    doc_type: str,
    version: str | None,
    service_key: str,
    rest_url: str,
    page_size: int = 1_000,
) -> tuple[str, ...]:
    paths: list[str] = []
    last_path: str | None = None
    while True:
        params = {
            "select": "path",
            "jurisdiction": f"eq.{jurisdiction}",
            "doc_type": f"eq.{doc_type}",
            "version": f"eq.{version}" if version is not None else "is.null",
            "order": "path.asc",
            "limit": str(page_size),
        }
        if last_path is not None:
            params["path"] = f"gt.{last_path}"
        query = urllib.parse.urlencode(params)
        req = urllib.request.Request(
            f"{rest_url}/navigation_nodes?{query}",
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
            raise RuntimeError("unexpected Supabase navigation_nodes response")
        page_paths = [str(row["path"]) for row in rows if isinstance(row, dict) and row.get("path")]
        paths.extend(page_paths)
        if len(page_paths) < page_size:
            break
        last_path = page_paths[-1]
    return tuple(paths)


def _delete_navigation_paths(
    paths: list[str],
    *,
    jurisdiction: str,
    doc_type: str,
    version: str | None,
    service_key: str,
    rest_url: str,
) -> None:
    if not paths:
        return
    in_clause = "in.(" + ",".join(_postgrest_in_value(value) for value in paths) + ")"
    query = urllib.parse.urlencode(
        {
            "jurisdiction": f"eq.{jurisdiction}",
            "doc_type": f"eq.{doc_type}",
            "version": f"eq.{version}" if version is not None else "is.null",
            "path": in_clause,
        }
    )
    req = urllib.request.Request(
        f"{rest_url}/navigation_nodes?{query}",
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


def _upsert_navigation_rows(
    rows: list[dict[str, object]],
    *,
    service_key: str,
    rest_url: str,
) -> None:
    if not rows:
        return
    req = urllib.request.Request(
        f"{rest_url}/navigation_nodes?on_conflict=id",
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
        raise RuntimeError(f"navigation upsert failed {exc.code}: {body}") from exc


def fetch_provisions_for_navigation(
    *,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
    jurisdiction: str | None = None,
    doc_type: str | None = None,
    version: str | None = None,
    page_size: int = 1_000,
) -> tuple[ProvisionRecord, ...]:
    """Page through `corpus.provisions` and return the slim records the
    navigation builder needs.

    The body text is intentionally not selected: navigation rows do not store
    legal text and pulling it across the wire just to drop it would be wasted
    bandwidth on large scopes.
    """
    rest_url = _rest_url(supabase_url)
    raw_rows: list[dict[str, object]] = []
    last_id: str | None = None
    while True:
        params: dict[str, str] = {
            "select": ",".join(_PROVISION_FIELDS),
            "order": "id.asc",
            "limit": str(page_size),
        }
        if jurisdiction is not None:
            params["jurisdiction"] = f"eq.{jurisdiction}"
        if doc_type is not None:
            params["doc_type"] = f"eq.{doc_type}"
        if version is not None:
            params["version"] = f"eq.{version}"
        if last_id is not None:
            params["id"] = f"gt.{last_id}"
        query = urllib.parse.urlencode(params)
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
        page = _fetch_json_with_retries(req, timeout=180)
        if not isinstance(page, list):
            raise RuntimeError("unexpected Supabase provisions response")
        if not page:
            break
        for raw in page:
            if isinstance(raw, dict) and raw.get("citation_path") and raw.get("jurisdiction"):
                raw_rows.append(raw)
        if len(page) < page_size:
            break
        last_id = str(page[-1].get("id")) if isinstance(page[-1], dict) else None
        if not last_id:
            break

    id_to_path: dict[str, str] = {
        str(raw["id"]): str(raw["citation_path"]) for raw in raw_rows if raw.get("id")
    }
    records: list[ProvisionRecord] = []
    for raw in raw_rows:
        parent_id = raw.get("parent_id")
        parent_path = id_to_path.get(str(parent_id)) if parent_id else None
        records.append(
            ProvisionRecord.from_mapping(
                {
                    "jurisdiction": raw.get("jurisdiction"),
                    "document_class": raw.get("doc_type"),
                    "citation_path": raw.get("citation_path"),
                    "id": raw.get("id"),
                    "parent_id": parent_id,
                    "parent_citation_path": parent_path,
                    "level": raw.get("level"),
                    "ordinal": raw.get("ordinal"),
                    "heading": raw.get("heading"),
                    "version": raw.get("version"),
                    "rulespec_path": raw.get("rulespec_path"),
                    "has_rulespec": raw.get("has_rulespec"),
                    "language": raw.get("language"),
                    "legal_identifier": raw.get("legal_identifier"),
                    "identifiers": raw.get("identifiers"),
                }
            )
        )
    return tuple(records)


def fetch_navigation_statuses(
    *,
    service_key: str,
    supabase_url: str = DEFAULT_AXIOM_SUPABASE_URL,
    jurisdiction: str | None = None,
    doc_type: str | None = None,
    version: str | None = None,
    page_size: int = 1_000,
) -> dict[str, str]:
    """Fetch existing non-empty navigation statuses keyed by path."""
    rest_url = _rest_url(supabase_url)
    statuses: dict[str, str] = {}
    last_path: str | None = None
    while True:
        params: dict[str, str] = {
            "select": "path,status",
            "order": "path.asc",
            "limit": str(page_size),
        }
        if jurisdiction is not None:
            params["jurisdiction"] = f"eq.{jurisdiction}"
        if doc_type is not None:
            params["doc_type"] = f"eq.{doc_type}"
        if version is not None:
            params["version"] = f"eq.{version}"
        if last_path is not None:
            params["path"] = f"gt.{last_path}"
        query = urllib.parse.urlencode(params)
        req = urllib.request.Request(
            f"{rest_url}/navigation_nodes?{query}",
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
                "Accept": "application/json",
                "Accept-Profile": "corpus",
                "User-Agent": USER_AGENT,
            },
        )
        page = _fetch_json_with_retries(req, timeout=180)
        if not isinstance(page, list):
            raise RuntimeError("unexpected Supabase navigation status response")
        page_paths: list[str] = []
        for raw in page:
            if not isinstance(raw, dict) or not raw.get("path"):
                continue
            path = str(raw["path"])
            page_paths.append(path)
            status = raw.get("status")
            if isinstance(status, str) and status.strip():
                statuses[path] = status.strip()
        if len(page_paths) < page_size:
            break
        last_path = page_paths[-1]
    return statuses


def _fetch_json_with_retries(
    req: urllib.request.Request,
    *,
    timeout: float,
    attempts: int = 3,
) -> object:
    last_error: BaseException | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code < 500 or attempt >= attempts:
                raise
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt >= attempts:
                raise
        time.sleep(0.5 * attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError("Supabase JSON fetch failed")


def _chunked_rows(
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


def _chunked_strings(values: Iterable[str], size: int) -> Iterator[list[str]]:
    chunk: list[str] = []
    for value in values:
        chunk.append(value)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
