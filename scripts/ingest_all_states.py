"""Orchestrate ingest_state_laws.py over every TheAxiomFoundation/rules-us-* repo.

Lists the state-code repos via ``gh repo list`` and runs the generic
state ingester against each one that actually has a ``statutes/``
directory. Jumps over states whose repo is empty or has only
regulations. Optionally runs the refs extractor (``--with-refs``)
after each successful state so inline refs populate right away.

Usage
-----
::

    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_all_states.py

    # Preview which states would run, without cloning or uploading
    uv run python scripts/ingest_all_states.py --dry-run

    # Skip states that already have rows (comma list)
    uv run python scripts/ingest_all_states.py --skip ny,ca,dc

    # After each state, backfill refs too
    uv run python scripts/ingest_all_states.py --with-refs
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent


def list_state_repos() -> list[str]:
    """Return sorted 2-letter codes for every rules-us-xx repo in the org.

    Uses the ``gh`` CLI so the caller's auth is honoured; shells out
    because ``gh`` handles pagination/rate limits better than urllib.
    """
    out = subprocess.run(
        [
            "gh",
            "repo",
            "list",
            "TheAxiomFoundation",
            "--limit",
            "200",
            "--json",
            "name",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    repos = json.loads(out.stdout)
    codes: list[str] = []
    for r in repos:
        name = r["name"]
        if name.startswith("rules-us-"):
            state = name[len("rules-us-") :]
            if len(state) == 2 and state.isalpha():
                codes.append(state.lower())
    return sorted(set(codes))


def has_statutes(state: str) -> bool:
    """Return True if ``rules-us-{state}/statutes`` exists and contains dirs."""
    out = subprocess.run(
        [
            "gh",
            "api",
            f"repos/TheAxiomFoundation/rules-us-{state}/contents/statutes",
            "--jq",
            '[.[] | select(.type=="dir")] | length',
        ],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        return False
    try:
        return int(out.stdout.strip() or "0") > 0
    except ValueError:
        return False


def ingest_state(
    state: str, dry_run: bool, with_refs: bool, env: dict[str, str]
) -> tuple[int, int]:
    """Run the state ingester; return (parsed, uploaded)."""
    cmd = [
        "uv",
        "run",
        "python",
        str(SCRIPTS_DIR / "ingest_state_laws.py"),
        "--state",
        state,
    ]
    if dry_run:
        cmd.append("--dry-run")
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    # Echo output so a long run's progress is visible.
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if result.returncode != 0:
        print(f"  WARN: {state} ingest exited {result.returncode}", file=sys.stderr)
        return (0, 0)

    # Parse the DONE line for stats.
    parsed = uploaded = 0
    for line in result.stdout.splitlines():
        if line.startswith(f"DONE us-{state}"):
            # "DONE us-xx — N parsed, M skipped, K rows uploaded, ..."
            import re

            m = re.search(
                r"(\d+)\s+parsed,\s+\d+\s+skipped,\s+(\d+)\s+rows\s+(?:uploaded|would upload)",
                line,
            )
            if m:
                parsed = int(m.group(1))
                uploaded = int(m.group(2))
            break

    if with_refs and not dry_run and uploaded > 0:
        extract_cmd = [
            "uv",
            "run",
            "python",
            str(SCRIPTS_DIR / "extract_references.py"),
            "--prefix",
            f"us-{state}/",
        ]
        subprocess.run(extract_cmd, env=env, check=False)

    return (parsed, uploaded)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--skip",
        default="",
        help="Comma-separated state codes to skip (e.g. 'ny,ca,dc').",
    )
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated state codes to run (defaults to all).",
    )
    parser.add_argument(
        "--with-refs",
        action="store_true",
        help="After each state's ingest, run extract_references --prefix us-xx/.",
    )
    args = parser.parse_args(argv)

    skip = {s.strip().lower() for s in args.skip.split(",") if s.strip()}
    only = {s.strip().lower() for s in args.only.split(",") if s.strip()}
    env = os.environ.copy()

    candidates = list_state_repos()
    if only:
        candidates = [s for s in candidates if s in only]
    candidates = [s for s in candidates if s not in skip]

    print(f"Candidate states: {', '.join(candidates)}", flush=True)

    started = time.time()
    total_parsed = 0
    total_uploaded = 0
    ingested = 0

    for state in candidates:
        if not has_statutes(state):
            print(f"  {state}: skipping — no statutes/ dir")
            continue
        print(f"\n--- us-{state} ---", flush=True)
        parsed, uploaded = ingest_state(state, args.dry_run, args.with_refs, env)
        total_parsed += parsed
        total_uploaded += uploaded
        if uploaded > 0:
            ingested += 1

    elapsed = (time.time() - started) / 60
    verb = "would upload" if args.dry_run else "uploaded"
    print(
        f"\nALL DONE — {ingested} states ingested, "
        f"{total_parsed} sections parsed, "
        f"{total_uploaded} rows {verb}, "
        f"{elapsed:.1f} min total",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
