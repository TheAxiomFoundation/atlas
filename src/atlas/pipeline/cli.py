"""CLI for the statute pipeline."""

import argparse

from atlas.pipeline.runner import STATE_CONVERTERS, StatePipeline


def main():
    """Run the statute pipeline CLI."""
    parser = argparse.ArgumentParser(
        description="Process state statutes: fetch → archive → convert → upload"
    )
    parser.add_argument("--state", help="State code (e.g., ak, ny)")
    parser.add_argument("--all-states", action="store_true", help="Process all states")
    parser.add_argument("--dry-run", action="store_true", help="Don't upload anything")
    args = parser.parse_args()

    if args.all_states:
        states = list(STATE_CONVERTERS.keys())
    elif args.state:
        states = [args.state.lower()]
    else:
        print("Specify --state or --all-states")
        return

    total_stats = {
        "sections_found": 0,
        "raw_uploaded": 0,
        "xml_generated": 0,
        "errors": 0,
    }

    for state in states:
        pipeline = StatePipeline(state, dry_run=args.dry_run)
        stats = pipeline.run()

        for k, v in stats.items():
            total_stats[k] += v

        print(f"\n  {state.upper()} Stats:")
        print(f"    Sections found: {stats['sections_found']}")
        print(f"    Raw uploaded:   {stats['raw_uploaded']}")
        print(f"    XML generated:  {stats['xml_generated']}")
        print(f"    Errors:         {stats['errors']}")

    print(f"\n{'='*60}")
    print("TOTAL STATS:")
    print(f"  Sections found: {total_stats['sections_found']}")
    print(f"  Raw uploaded:   {total_stats['raw_uploaded']}")
    print(f"  XML generated:  {total_stats['xml_generated']}")
    print(f"  Errors:         {total_stats['errors']}")


if __name__ == "__main__":
    main()
