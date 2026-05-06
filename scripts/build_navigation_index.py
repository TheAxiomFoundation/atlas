#!/usr/bin/env python
"""Build the precomputed `corpus.navigation_nodes` index.

Examples:
    uv run python scripts/build_navigation_index.py --jurisdiction us-co --from-supabase
    uv run python scripts/build_navigation_index.py --jurisdiction us-co --doc-type regulation --from-supabase
    uv run python scripts/build_navigation_index.py --all --from-supabase
    uv run python scripts/build_navigation_index.py --provisions data/corpus/provisions/us-co/regulation-2026.jsonl

Thin wrapper around `axiom-corpus-ingest build-navigation-index` for callers
that prefer a script entrypoint matching existing repo style.
"""

from __future__ import annotations

import sys

from axiom_corpus.corpus.cli import main as cli_main


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    return cli_main(["build-navigation-index", *args])


if __name__ == "__main__":
    raise SystemExit(main())
