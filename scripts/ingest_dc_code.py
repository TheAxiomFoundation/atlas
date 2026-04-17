"""Ingest the DC Code from local ``sources/dc/dc-law-xml`` into Supabase.

The DC Code is checked into this repo as ~21,700 Akoma-Ntoso-derived XML
files under ``sources/dc/dc-law-xml/us/dc/council/code/titles/*/sections``.
The existing :class:`atlas.converters.us_states.dc.DCConverter` knows how
to parse those XML bytes into a :class:`Section`; this script walks the
local tree and funnels every section through that converter + the shared
:class:`RuleUploader`.

Why not reuse the converter's ``fetch_section``? That path assumes
GitHub-hosted XML. We already have the XML locally — no reason to
round-trip 21k HTTP fetches. ``_parse_xml`` is called directly with the
on-disk bytes.

Usage
-----
::

    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_dc_code.py

    # Dry-run — parse, count, but don't upload
    uv run python scripts/ingest_dc_code.py --dry-run

    # Limit (testing)
    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_dc_code.py --limit 200
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from atlas.converters.us_states import dc as dc_module  # noqa: E402
from atlas.converters.us_states.dc import DCConverter, DCConverterError  # noqa: E402
from atlas.ingest.rule_converter import section_to_rules  # noqa: E402
from atlas.ingest.rule_uploader import RuleUploader  # noqa: E402


DC_ROOT = Path(__file__).resolve().parent.parent / (
    "sources/dc/dc-law-xml/us/dc/council/code/titles"
)


# Patch DCConverter._parse_section_number to accept DC's alpha-suffixed
# title numbers like "29A-1001". The upstream converter is strict because
# it needs an int for URL building when fetching from GitHub; this local
# ingest already has the XML bytes, so the title-number precision only
# affects display metadata. Strip any trailing letters for the int cast,
# return the original string so downstream can keep the full "29A" label.
def _lenient_parse_section_number(self, section_number: str) -> tuple[int, str]:
    parts = section_number.split("-", 1)
    if len(parts) != 2:
        raise DCConverterError(f"Invalid section number format: {section_number}")
    import re as _re

    digits_match = _re.match(r"\d+", parts[0])
    if not digits_match:
        raise DCConverterError(f"Invalid title number in section: {section_number}")
    return int(digits_match.group(0)), section_number


DCConverter._parse_section_number = _lenient_parse_section_number


def iter_section_files(root: Path):
    """Yield ``(section_number, xml_bytes, file_path)`` tuples.

    Walks ``{root}/{title}/sections/{section}.xml`` lexicographically so
    progress reports stay stable.
    """
    for title_dir in sorted(root.iterdir(), key=lambda p: int(p.name) if p.name.isdigit() else 0):
        if not title_dir.is_dir():
            continue
        sections_dir = title_dir / "sections"
        if not sections_dir.exists():
            continue
        for xml_file in sorted(sections_dir.glob("*.xml")):
            section_number = xml_file.stem  # "47-1801.04"
            try:
                yield section_number, xml_file.read_bytes(), xml_file
            except OSError as exc:
                print(f"  SKIP {xml_file}: {exc}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--batch",
        type=int,
        default=500,
        help="Upload batch size (default 500).",
    )
    args = parser.parse_args(argv)

    if not DC_ROOT.exists():
        print(f"FATAL: DC source tree not found at {DC_ROOT}", file=sys.stderr)
        return 2

    converter = DCConverter()
    uploader = None if args.dry_run else RuleUploader()

    started = time.time()
    parsed = 0
    failed = 0
    rule_buffer: list[dict] = []
    rows_uploaded = 0

    def flush() -> None:
        nonlocal rule_buffer, rows_uploaded
        if not rule_buffer:
            return
        if uploader is not None:
            uploader.upsert_all(rule_buffer)
        rows_uploaded += len(rule_buffer)
        rule_buffer = []

    for section_number, xml_bytes, path in iter_section_files(DC_ROOT):
        if args.limit is not None and parsed + failed >= args.limit:
            break
        try:
            content = xml_bytes.decode("utf-8")
            parsed_section = converter._parse_xml(content, section_number, f"file://{path}")
            section = converter._to_section(parsed_section)
        except (DCConverterError, ValueError, SyntaxError) as exc:
            failed += 1
            if failed <= 5:
                print(f"  parse fail {section_number}: {exc}", file=sys.stderr)
            continue
        except Exception as exc:  # broad catch per-file: continue the walk
            failed += 1
            if failed <= 5:
                print(f"  parse fail {section_number}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue

        parsed += 1
        rule_buffer.extend(section_to_rules(section, jurisdiction="us-dc"))
        if len(rule_buffer) >= args.batch:
            flush()
            elapsed = time.time() - started
            print(
                f"  through {section_number}: {parsed} sections parsed, "
                f"{failed} failed, {rows_uploaded} rows uploaded, "
                f"{elapsed / 60:.1f} min",
                flush=True,
            )

    flush()
    elapsed = time.time() - started
    verb = "would upload" if args.dry_run else "uploaded"
    print(
        f"\nDONE — {parsed} sections parsed, {failed} failed, "
        f"{rows_uploaded} rows {verb}, {elapsed / 60:.1f} min",
        flush=True,
    )
    return 0 if failed == 0 or parsed > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
