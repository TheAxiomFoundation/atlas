"""Backfill federal US Code title containers in ``corpus.provisions``.

The Axiom app resolves pages by exact ``citation_path``. Historical US Code
ingest made sections like ``us/statute/7/2011`` top-level rows, so title pages
like ``us/statute/7`` had no row to resolve. This script inserts title rows
such as ``us/statute/7`` and relinks existing section roots under them.

Default mode is a dry run:

    uv run python scripts/backfill_us_statute_title_containers.py

Apply to Supabase:

    uv run python scripts/backfill_us_statute_title_containers.py --apply

Limit scope for testing:

    uv run python scripts/backfill_us_statute_title_containers.py --title 7 --apply
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from axiom_corpus.ingest.rule_converter import _deterministic_id  # noqa: E402
from axiom_corpus.ingest.rule_uploader import (  # noqa: E402
    DEFAULT_AXIOM_SUPABASE_URL,
    RuleUploader,
)
from axiom_corpus.sources.uslm import US_CODE_TITLES  # noqa: E402

USER_AGENT = "axiom-corpus-backfill/0.1"
TIMEOUT = httpx.Timeout(180.0, connect=30.0, read=180.0, write=180.0)


def load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE pairs without printing secrets."""
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def headers(service_key: str, *, write: bool = False) -> dict[str, str]:
    profile_header = "Content-Profile" if write else "Accept-Profile"
    return {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        profile_header: "corpus",
        "User-Agent": USER_AGENT,
    }


def parse_titles(raw_titles: list[str] | None) -> set[str] | None:
    if not raw_titles:
        return None
    titles: set[str] = set()
    for raw in raw_titles:
        for part in raw.split(","):
            title = part.strip()
            if title:
                titles.add(title)
    return titles


def citation_depth(citation_path: str) -> int:
    return citation_path.count("/") + 1


def title_ordinal(title: str) -> int | None:
    digits = ""
    for char in title:
        if char.isdigit():
            digits += char
        else:
            break
    return int(digits) if digits else None


def build_title_row(title: str) -> dict:
    citation_path = f"us/statute/{title}"
    title_name = US_CODE_TITLES.get(title, f"Title {title}")
    return {
        "id": _deterministic_id(citation_path),
        "jurisdiction": "us",
        "doc_type": "statute",
        "parent_id": None,
        "level": 0,
        "ordinal": title_ordinal(title),
        "heading": title_name,
        "body": None,
        "effective_date": None,
        "source_url": None,
        "source_path": None,
        "citation_path": citation_path,
        "rulespec_path": None,
        "has_rulespec": False,
        "line_count": 1,
    }


def fetch_orphan_sections(
    client: httpx.Client,
    rest_url: str,
    service_key: str,
    *,
    titles: set[str] | None,
    page_size: int,
) -> dict[str, list[str]]:
    def get_rows(params: list[tuple[str, str]]) -> list[dict]:
        for attempt in range(5):
            try:
                response = client.get(
                    f"{rest_url}/provisions",
                    headers=headers(service_key),
                    params=params,
                )
                response.raise_for_status()
                return response.json()
            except (httpx.ReadTimeout, httpx.HTTPStatusError) as exc:
                retryable_status = (
                    isinstance(exc, httpx.HTTPStatusError)
                    and exc.response.status_code >= 500
                )
                if (retryable_status or isinstance(exc, httpx.ReadTimeout)) and attempt < 4:
                    time.sleep(2**attempt)
                    continue
                raise
        return []

    by_title: dict[str, list[str]] = defaultdict(list)

    target_titles = titles or set(US_CODE_TITLES)
    for title in sorted(target_titles, key=lambda t: (title_ordinal(t) or 0, t)):
        prefix = f"us/statute/{title}"
        offset = 0
        while True:
            rows = get_rows(
                [
                    ("select", "id,citation_path"),
                    ("jurisdiction", "eq.us"),
                    ("doc_type", "eq.statute"),
                    ("parent_id", "is.null"),
                    ("citation_path", f"gte.{prefix}/"),
                    ("citation_path", f"lt.{prefix}~"),
                    ("order", "citation_path"),
                    ("limit", str(page_size)),
                    ("offset", str(offset)),
                ]
            )
            if not rows:
                break

            for row in rows:
                path = row.get("citation_path")
                row_id = row.get("id")
                if not path or not row_id or citation_depth(path) != 4:
                    continue
                by_title[title].append(row_id)

            if len(rows) < page_size:
                break
            offset += page_size

        by_title.setdefault(title, [])
    return dict(by_title)


def upsert_title_rows(
    client: httpx.Client,
    rest_url: str,
    service_key: str,
    titles: Iterable[str],
) -> int:
    rows = [build_title_row(title) for title in sorted(titles, key=lambda t: (title_ordinal(t) or 0, t))]
    if not rows:
        return 0
    response = client.post(
        f"{rest_url}/provisions?on_conflict=id",
        headers={
            **headers(service_key, write=True),
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        json=rows,
    )
    response.raise_for_status()
    return len(rows)


def chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def link_sections(
    client: httpx.Client,
    rest_url: str,
    service_key: str,
    by_title: dict[str, list[str]],
    *,
    chunk_size: int,
) -> int:
    linked = 0
    for title, ids in sorted(by_title.items(), key=lambda item: (title_ordinal(item[0]) or 0, item[0])):
        if not ids:
            continue
        parent_id = _deterministic_id(f"us/statute/{title}")
        for id_chunk in chunks(ids, chunk_size):
            id_filter = ",".join(id_chunk)
            response = client.patch(
                f"{rest_url}/provisions",
                headers={
                    **headers(service_key, write=True),
                    "Prefer": "return=minimal",
                },
                params=[("id", f"in.({id_filter})")],
                json={"parent_id": parent_id, "level": 1},
            )
            response.raise_for_status()
            linked += len(id_chunk)
    return linked


def _post_refresh_rpc(
    client: httpx.Client,
    rest_url: str,
    service_key: str,
    rpc_name: str,
) -> httpx.Response:
    return client.post(
        f"{rest_url}/rpc/{rpc_name}",
        headers=headers(service_key, write=True),
        json={},
    )


def refresh_corpus_analytics(client: httpx.Client, rest_url: str, service_key: str) -> None:
    response = _post_refresh_rpc(client, rest_url, service_key, "refresh_corpus_analytics")
    if response.status_code == 404:
        response = _post_refresh_rpc(client, rest_url, service_key, "refresh_jurisdiction_counts")
    response.raise_for_status()


def refresh_jurisdiction_counts(client: httpx.Client, rest_url: str, service_key: str) -> None:
    refresh_corpus_analytics(client, rest_url, service_key)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="Write changes to Supabase.")
    parser.add_argument(
        "--title",
        action="append",
        help="Limit to one title. May be repeated or comma-separated.",
    )
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--patch-chunk-size", type=int, default=100)
    args = parser.parse_args(argv)

    load_env_file(REPO_ROOT / ".env")

    supabase_url = os.environ.get("AXIOM_SUPABASE_URL", DEFAULT_AXIOM_SUPABASE_URL)
    uploader = RuleUploader(url=supabase_url)
    rest_url = uploader.rest_url
    selected_titles = parse_titles(args.title)

    with httpx.Client(timeout=TIMEOUT) as client:
        by_title = fetch_orphan_sections(
            client,
            rest_url,
            uploader.key,
            titles=selected_titles,
            page_size=args.page_size,
        )
        titles = set(by_title)
        orphan_count = sum(len(ids) for ids in by_title.values())

        print(
            f"Found {orphan_count:,} orphan federal section roots across {len(titles)} title(s)."
        )
        for title in sorted(titles, key=lambda t: (title_ordinal(t) or 0, t)):
            print(f"  Title {title}: {len(by_title[title]):,} section root(s)")

        if not args.apply:
            print("\nDry run only. Re-run with --apply to insert title rows and relink sections.")
            return 0

        inserted = upsert_title_rows(client, rest_url, uploader.key, titles)
        linked = link_sections(
            client,
            rest_url,
            uploader.key,
            by_title,
            chunk_size=args.patch_chunk_size,
        )
        refresh_corpus_analytics(client, rest_url, uploader.key)
        print(f"\nUpserted {inserted:,} title container(s); linked {linked:,} section row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
