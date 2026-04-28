#!/usr/bin/env python
"""Ingest US Code Titles 27-40 into Supabase."""

import sys

sys.path.insert(0, "/Users/maxghenis/TheAxiomFoundation/axiom-corpus/src")

from axiom.ingest.supabase import SupabaseIngestor


def main():
    ingestor = SupabaseIngestor()
    titles = list(range(27, 41))
    total = 0

    for t in titles:
        try:
            count = ingestor.ingest_usc_title(t)
            total += count
            print(f"Title {t}: {count} rules")
        except Exception as e:
            print(f"Title {t} error: {e}")

    print(f"\n{'=' * 50}")
    print(f"Total rules inserted: {total}")
    print(f"{'=' * 50}")
    return total


if __name__ == "__main__":
    main()
