"""Backfill the citation graph in ``akn.rule_references``.

For every rule with a non-empty body, run the citation extractors and
upsert one row per extracted ref. Target rule ids are resolved against
``akn.rules.citation_path`` — unresolved targets are stored anyway so a
later ingestion (or a later re-run) activates the link.

Semantics
---------
* Idempotent per source rule. Before inserting, we DELETE all existing
  refs for each source rule in this batch, so changing the extractor
  and re-running produces a clean result rather than stacking duplicates
  next to stale offsets.
* Batched by source rule. 100 source rules per delete + bulk insert
  round trip — tuned for Supabase's REST API throughput.
* Resumable. The ``--since-citation-path`` flag lets you pick up where a
  prior run left off (lexicographic order).

Usage
-----
::

    SUPABASE_ACCESS_TOKEN=... uv run python scripts/extract_references.py

    # Re-extract only the regulation lane
    SUPABASE_ACCESS_TOKEN=... uv run python scripts/extract_references.py \\
        --doc-type regulation

    # Dry-run — print what would be inserted without writing.
    uv run python scripts/extract_references.py --dry-run --limit 50
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingest_cfr_parts import (  # noqa: E402
    REST_URL,
    USER_AGENT,
    chunked,
    get_service_key,
)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from atlas.citations import extract_all  # noqa: E402

PAGE_SIZE = 500


def _auth_headers(service_key: str) -> dict[str, str]:
    return {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "User-Agent": USER_AGENT,
    }


def _prefix_upper_bound(prefix: str) -> str:
    """Exclusive upper bound for a prefix range, in the default btree ordering.

    The range ``[prefix, prefix_upper_bound)`` exactly covers every string
    that begins with ``prefix`` under PostgreSQL's default text collation.
    Incrementing the last character avoids LIKE's dependency on
    ``text_pattern_ops`` indexes — a plain btree range scan suffices.

    Examples:
        'us-ny/' -> 'us-ny0'      (slash 0x2F + 1 = '0' 0x30)
        'us-dc/' -> 'us-dc0'
        'us-nz'  -> 'us-n{'       (z + 1 = '{')
    """
    if not prefix:
        raise ValueError("prefix must be non-empty")
    return prefix[:-1] + chr(ord(prefix[-1]) + 1)


def fetch_rules_page(
    service_key: str,
    offset: int,
    doc_type: str | None,
    since_citation_path: str | None,
    prefix: str | None = None,
) -> list[dict]:
    """Fetch a page of rules with non-empty body, ordered by citation_path.

    When ``prefix`` is set, rows are restricted to the half-open range
    ``[prefix, _prefix_upper_bound(prefix))`` so we can target one
    jurisdiction (``us-ny/``) without rescanning the whole corpus.
    ``since_citation_path`` still acts as an intra-prefix cursor for
    resumption.
    """
    params = [
        "select=id,citation_path,body,jurisdiction",
        "body=not.is.null",
        "order=citation_path.asc",
        f"limit={PAGE_SIZE}",
        f"offset={offset}",
    ]
    if doc_type:
        params.append(f"doc_type=eq.{doc_type}")
    if prefix:
        # First page: inclusive lower bound at the prefix. Resumption pages:
        # strict greater-than the last path, same as the no-prefix path.
        if since_citation_path:
            params.append(
                f"citation_path=gt.{urllib.parse.quote(since_citation_path)}"
            )
        else:
            params.append(f"citation_path=gte.{urllib.parse.quote(prefix)}")
        params.append(
            f"citation_path=lt.{urllib.parse.quote(_prefix_upper_bound(prefix))}"
        )
    elif since_citation_path:
        params.append(f"citation_path=gt.{urllib.parse.quote(since_citation_path)}")
    url = f"{REST_URL}/rules?{'&'.join(params)}"
    req = urllib.request.Request(
        url,
        headers={
            **_auth_headers(service_key),
            "Accept-Profile": "akn",
        },
    )
    return json.loads(_retrying_urlopen(req, timeout=60))


def resolve_target_ids(service_key: str, citation_paths: set[str]) -> dict[str, str]:
    """Batch-resolve target_citation_paths → rule IDs.

    Unknown paths simply don't appear in the returned dict.
    """
    if not citation_paths:
        return {}

    # PostgREST supports in.(a,b,c) filters; 100 paths per call is a
    # reasonable batch size given URL length limits.
    out: dict[str, str] = {}
    paths = list(citation_paths)
    for i in range(0, len(paths), 100):
        batch = paths[i : i + 100]
        # Commas in values must be quoted in PostgREST's in.() filter.
        quoted = ",".join(f'"{p}"' for p in batch)
        url = (
            f"{REST_URL}/rules?select=id,citation_path"
            f"&citation_path=in.({urllib.parse.quote(quoted)})"
            f"&limit={len(batch)}"
        )
        req = urllib.request.Request(
            url,
            headers={
                **_auth_headers(service_key),
                "Accept-Profile": "akn",
            },
        )
        try:
            data = json.loads(_retrying_urlopen(req, timeout=60))
        except RuntimeError as exc:
            print(
                f"  WARN: target resolution failed, batch skipped: {exc}",
                file=sys.stderr,
            )
            continue
        for row in data:
            out[row["citation_path"]] = row["id"]
    return out


def _retrying_urlopen(req: urllib.request.Request, timeout: int = 60) -> bytes:
    """Open ``req`` with retries on transient upstream errors.

    Supabase / PostgREST occasionally returns 500 / 502 / 503 under
    contention (e.g. when a parallel ingest is saturating the same
    pool). These are almost always transient; retry with exponential
    backoff up to a short cap before bubbling the error to the caller.
    """
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode()[:300]
            if exc.code in (500, 502, 503, 504, 429) and attempt < max_attempts:
                sleep_s = min(30.0, 2.0**attempt)
                print(
                    f"  transient {exc.code} (attempt {attempt}), "
                    f"retrying in {sleep_s:.1f}s: {body[:100]}",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt < max_attempts:
                sleep_s = min(30.0, 2.0**attempt)
                print(
                    f"  network error (attempt {attempt}), retrying in {sleep_s:.1f}s: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(sleep_s)
                continue
            raise
    raise RuntimeError("exhausted retries")


def delete_existing(service_key: str, source_ids: list[str]) -> None:
    if not source_ids:
        return
    quoted = ",".join(f'"{sid}"' for sid in source_ids)
    url = f"{REST_URL}/rule_references?source_rule_id=in.({urllib.parse.quote(quoted)})"
    req = urllib.request.Request(
        url,
        headers={
            **_auth_headers(service_key),
            "Content-Profile": "akn",
        },
        method="DELETE",
    )
    _retrying_urlopen(req, timeout=60)


def insert_rows(service_key: str, rows: list[dict]) -> None:
    if not rows:
        return
    url = f"{REST_URL}/rule_references"
    req = urllib.request.Request(
        url,
        data=json.dumps(rows).encode(),
        headers={
            **_auth_headers(service_key),
            "Content-Type": "application/json",
            "Content-Profile": "akn",
            "Prefer": "return=minimal",
        },
        method="POST",
    )
    _retrying_urlopen(req, timeout=120)


def process_batch(
    service_key: str | None,
    rules: list[dict],
    dry_run: bool,
) -> tuple[int, int, int]:
    """Returns (source_rules_processed, refs_extracted, refs_resolved)."""
    all_refs: list[tuple[dict, list]] = []  # (rule, refs)
    all_targets: set[str] = set()
    for rule in rules:
        body = rule.get("body") or ""
        refs = extract_all(
            body,
            jurisdiction=rule.get("jurisdiction"),
            source_citation_path=rule.get("citation_path"),
        )
        if refs:
            all_refs.append((rule, refs))
            all_targets.update(r.target_citation_path for r in refs)

    extracted = sum(len(r) for _, r in all_refs)
    if not extracted:
        return (len(rules), 0, 0)

    if dry_run:
        for rule, refs in all_refs[:3]:
            print(f"  {rule['citation_path']}:")
            for ref in refs[:5]:
                print(f"    → {ref.target_citation_path} [{ref.pattern_kind}]")
        return (len(rules), extracted, 0)

    assert service_key is not None
    target_map = resolve_target_ids(service_key, all_targets)
    resolved = sum(1 for _, refs in all_refs for r in refs if r.target_citation_path in target_map)

    rows: list[dict] = []
    for rule, refs in all_refs:
        for ref in refs:
            rows.append(
                {
                    "source_rule_id": rule["id"],
                    "target_citation_path": ref.target_citation_path,
                    "target_rule_id": target_map.get(ref.target_citation_path),
                    "citation_text": ref.raw_text,
                    "pattern_kind": ref.pattern_kind,
                    "start_offset": ref.start_offset,
                    "end_offset": ref.end_offset,
                    "confidence": ref.confidence,
                }
            )

    # DELETE then INSERT per batch (idempotent for rules seen in this run).
    source_ids = [rule["id"] for rule, _ in all_refs]
    delete_existing(service_key, source_ids)
    for chunk in chunked(rows, size=500):
        insert_rows(service_key, chunk)

    return (len(rules), extracted, resolved)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--doc-type", choices=["statute", "regulation"], default=None)
    parser.add_argument("--since-citation-path", default=None)
    parser.add_argument(
        "--prefix",
        default=None,
        help=(
            "Limit extraction to citation_paths starting with this prefix "
            "(e.g. 'us-ny/'). Useful for per-jurisdiction backfills."
        ),
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Stop after N source rules (for testing)"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    # Need urllib.parse for query-string construction.
    import urllib.parse as _parse  # noqa: F401

    service_key = None if args.dry_run else get_service_key()

    started = time.time()
    total_rules = 0
    total_refs = 0
    total_resolved = 0
    last_path = args.since_citation_path

    while True:
        page = (
            fetch_rules_page(
                service_key
                or "",  # for dry-run we need read; anon would suffice but service_key is fine
                offset=0,
                doc_type=args.doc_type,
                since_citation_path=last_path,
                prefix=args.prefix,
            )
            if service_key
            else _dry_run_page(args)
        )
        if not page:
            break

        rules_processed, refs_extracted, refs_resolved = process_batch(
            service_key, page, args.dry_run
        )
        total_rules += rules_processed
        total_refs += refs_extracted
        total_resolved += refs_resolved

        last_path = page[-1]["citation_path"]
        elapsed = time.time() - started
        print(
            f"  through {last_path}: {total_rules} rules, "
            f"{total_refs} refs extracted, {total_resolved} resolved, "
            f"{elapsed / 60:.1f} min",
            flush=True,
        )

        if args.limit and total_rules >= args.limit:
            break

    print(
        f"\nDONE — {total_rules} rules processed, {total_refs} refs extracted, "
        f"{total_resolved} resolved, {(time.time() - started) / 60:.1f} min",
        flush=True,
    )
    return 0


def _dry_run_page(args: argparse.Namespace) -> list[dict]:
    """Anonymous read for --dry-run mode, so callers without an access
    token can still see what the extractor produces.
    """

    anon_key = os.environ.get("SUPABASE_ANON_KEY") or _fallback_anon_key()
    params = [
        "select=id,citation_path,body,jurisdiction",
        "body=not.is.null",
        "order=citation_path.asc",
        f"limit={PAGE_SIZE}",
    ]
    if args.doc_type:
        params.append(f"doc_type=eq.{args.doc_type}")
    if args.prefix:
        params.append(f"citation_path=gte.{urllib.parse.quote(args.prefix)}")
        params.append(
            f"citation_path=lt.{urllib.parse.quote(_prefix_upper_bound(args.prefix))}"
        )
    url = f"{REST_URL}/rules?{'&'.join(params)}"
    req = urllib.request.Request(
        url,
        headers={
            "apikey": anon_key,
            "Authorization": f"Bearer {anon_key}",
            "User-Agent": USER_AGENT,
            "Accept-Profile": "akn",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def _fallback_anon_key() -> str:
    """The public anon key for the shared Supabase project.

    Hard-coded to keep ``--dry-run`` usable without env setup. This key
    is already exposed in the website bundle, so documenting it here
    adds no attack surface.
    """
    return (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5zdXBxaGZjaGR0cWNsb21scmdzIiwi"
        "cm9sZSI6ImFub24iLCJpYXQiOjE3NjY5MzExMDgsImV4cCI6MjA4MjUwNzEwOH0."
        "BPdUadtBCdKfWZrKbfxpBQUqSGZ4hd34Dlor8kMBrVI"
    )


if __name__ == "__main__":
    raise SystemExit(main())
