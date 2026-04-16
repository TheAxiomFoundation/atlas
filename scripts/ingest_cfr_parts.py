"""Ingest CFR parts from eCFR into Supabase ``arch.rules``.

Downloads the eCFR Versioner XML for a given (title, part), parses the
DIV5/DIV6/DIV8 hierarchy, and upserts Part / Subpart / Section rows with
deterministic UUIDs keyed off ``citation_path``.

Usage
-----
::

    # One part (SNAP -- 7 CFR Chapter II, Subchapter C)
    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_cfr_parts.py \\
        --title 7 --chapter II --subchapter C --part 273

    # Multiple parts
    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_cfr_parts.py \\
        --title 7 --chapter II --subchapter C \\
        --parts 271,272,273,274,275,276,277,278,279,281,282,283

When ``--chapter`` / ``--subchapter`` are omitted the script falls back to
a generic eCFR URL (``/current/title-{title}/part-{part}``); eCFR handles
the indirect lookup. Provide the flags to produce the deep-linked URLs
the viewer prefers.

Environment
-----------
* ``SUPABASE_ACCESS_TOKEN`` — personal access token used to retrieve the
  project's ``service_role`` API key via the Management API. The script does
  not persist credentials; the ``service_role`` key is held only for the
  lifetime of the process.

Design notes
------------
* Deterministic IDs (``uuid5(NAMESPACE_URL, "atlas:" + citation_path)``) match
  :mod:`atlas.ingest.supabase` so re-runs upsert cleanly instead of producing
  duplicates.
* ``doc_type`` is ``"regulation"`` for all rows.
* Not every CFR part uses subparts; when none are present, sections become
  direct children of the part with ``level == 1`` rather than ``2``.
* ``source_url`` points at the current eCFR reading view (not the raw XML),
  matching the pattern the USC ingestion uses.
* This script intentionally does not ingest sub-paragraphs (``(a)(1)(i)``) —
  the viewer's leaf-section body render already handles those inline from the
  section body. Sub-paragraph structuring can be a later migration.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Iterable, Iterator
from uuid import NAMESPACE_URL, uuid5
from xml.etree import ElementTree as ET

SUPABASE_URL = "https://nsupqhfchdtqclomlrgs.supabase.co"
REST_URL = f"{SUPABASE_URL}/rest/v1"
DEFAULT_AS_OF = "2024-04-16"
USER_AGENT = "atlas-ingest/0.1"


# --- Helpers ---------------------------------------------------------------


def deterministic_id(citation_path: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def collect_full_text(elem: ET.Element) -> str:
    """Gather heading + paragraph text in document order."""
    out: list[str] = []

    def visit(node: ET.Element) -> None:
        if node.tag == "HEAD" or node.tag == "P":
            t = clean_text("".join(node.itertext()))
            if t:
                out.append(t)
        else:
            for child in node:
                visit(child)

    visit(elem)
    return "\n\n".join(out)


# --- Fetch -----------------------------------------------------------------


def fetch_part_xml(title: int, part: int, as_of: str) -> ET.Element:
    url = f"https://www.ecfr.gov/api/versioner/v1/full/{as_of}/title-{title}.xml?part={part}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return ET.fromstring(resp.read())


# --- Row builder -----------------------------------------------------------


def _ecfr_part_url(title: int, part_num: int, chapter: str | None, subchapter: str | None) -> str:
    """Build an eCFR URL for a CFR part.

    If ``chapter`` and ``subchapter`` are supplied, the deep-linked
    ``/title-{t}/chapter-{c}/subchapter-{s}/part-{p}`` form is returned.
    Otherwise we fall back to ``/title-{t}/part-{p}`` — eCFR resolves the
    correct chapter/subchapter on its side, so the link still works; it
    just doesn't preserve the hierarchy in the URL.
    """
    base = f"https://www.ecfr.gov/current/title-{title}"
    if chapter and subchapter:
        return f"{base}/chapter-{chapter}/subchapter-{subchapter}/part-{part_num}"
    return f"{base}/part-{part_num}"


def _ecfr_subpart_url(
    title: int, part_num: int, subpart_letter: str, chapter: str | None, subchapter: str | None
) -> str:
    base = f"https://www.ecfr.gov/current/title-{title}"
    if chapter and subchapter:
        return (
            f"{base}/chapter-{chapter}/subchapter-{subchapter}"
            f"/part-{part_num}/subpart-{subpart_letter}"
        )
    return f"{base}/part-{part_num}/subpart-{subpart_letter}"


def _ecfr_section_url(
    title: int,
    part_num: int,
    sec_num: str,
    chapter: str | None,
    subchapter: str | None,
) -> str:
    base = f"https://www.ecfr.gov/current/title-{title}"
    anchor = f"#p-{part_num}.{sec_num}"
    if chapter and subchapter:
        return f"{base}/chapter-{chapter}/subchapter-{subchapter}/part-{part_num}{anchor}"
    return f"{base}/part-{part_num}{anchor}"


def build_rows(
    title: int,
    part_num: int,
    part_root: ET.Element,
    chapter: str | None = None,
    subchapter: str | None = None,
) -> list[dict]:
    rows: list[dict] = []

    part_elem = None
    for div5 in part_root.iter("DIV5"):
        if div5.get("TYPE") == "PART" and div5.get("N") == str(part_num):
            part_elem = div5
            break
    if part_elem is None:
        print(
            f"  WARN: no DIV5 PART element for {title} CFR {part_num}",
            file=sys.stderr,
        )
        return rows

    part_path = f"us/regulation/{title}/{part_num}"
    part_id = deterministic_id(part_path)
    part_head = part_elem.find("HEAD")
    part_heading = (
        clean_text("".join(part_head.itertext())) if part_head is not None else f"Part {part_num}"
    )
    part_heading = re.sub(rf"^PART\s+{part_num}\s*[—–-]\s*", "", part_heading, flags=re.I)
    part_source = _ecfr_part_url(title, part_num, chapter, subchapter)
    rows.append(
        {
            "id": part_id,
            "jurisdiction": "us",
            "doc_type": "regulation",
            "parent_id": None,
            "level": 0,
            "ordinal": part_num,
            "heading": part_heading,
            "body": None,
            "source_url": part_source,
            "citation_path": part_path,
        }
    )

    def emit_section(div8: ET.Element, parent_id: str, level: int) -> None:
        n_attr = div8.get("N", "")
        m = re.search(r"(\d+)\.(\d+[a-z]?)", n_attr)
        if not m:
            return
        sec_num = m.group(2)
        sec_head = div8.find("HEAD")
        sec_heading = (
            clean_text("".join(sec_head.itertext()))
            if sec_head is not None
            else f"§ {part_num}.{sec_num}"
        )
        sec_heading = re.sub(rf"^§\s*{part_num}\.{sec_num}\s*", "", sec_heading).strip(" .")
        body = collect_full_text(div8)
        sec_path = f"{part_path}/{sec_num}"
        digits = re.match(r"(\d+)", sec_num).group(1)
        sec_source = _ecfr_section_url(title, part_num, sec_num, chapter, subchapter)
        rows.append(
            {
                "id": deterministic_id(sec_path),
                "jurisdiction": "us",
                "doc_type": "regulation",
                "parent_id": parent_id,
                "level": level,
                # Ordinal: digit-section x 10 (so section 273.1 -> 10, section 273.10 -> 100),
                # + 1 for lettered suffixes (section 273.1a -> 11) so they sort after the plain form.
                "ordinal": int(digits) * 10 + (0 if sec_num.isdigit() else 1),
                "heading": sec_heading,
                "body": body,
                "source_url": sec_source,
                "citation_path": sec_path,
            }
        )

    has_subparts = False
    for div6 in part_elem.iter("DIV6"):
        if div6.get("TYPE") != "SUBPART":
            continue
        has_subparts = True
        subpart_letter = div6.get("N", "")
        subpart_head = div6.find("HEAD")
        subpart_heading = (
            clean_text("".join(subpart_head.itertext())) if subpart_head is not None else ""
        )
        subpart_heading = re.sub(r"^Subpart\s+[A-Z]+\s*[—–-]\s*", "", subpart_heading, flags=re.I)
        subpart_path = f"{part_path}/subpart-{subpart_letter.lower()}"
        subpart_id = deterministic_id(subpart_path)
        subpart_source = _ecfr_subpart_url(title, part_num, subpart_letter, chapter, subchapter)
        rows.append(
            {
                "id": subpart_id,
                "jurisdiction": "us",
                "doc_type": "regulation",
                "parent_id": part_id,
                "level": 1,
                "ordinal": ord(subpart_letter) if len(subpart_letter) == 1 else 0,
                "heading": f"Subpart {subpart_letter} — {subpart_heading}",
                "body": None,
                "source_url": subpart_source,
                "citation_path": subpart_path,
            }
        )
        for div8 in div6.iter("DIV8"):
            if div8.get("TYPE") == "SECTION":
                emit_section(div8, subpart_id, 2)

    if not has_subparts:
        for div8 in part_elem.iter("DIV8"):
            if div8.get("TYPE") == "SECTION":
                emit_section(div8, part_id, 1)

    return rows


# --- Upsert ----------------------------------------------------------------


def chunked(rows: list[dict], size: int = 100) -> Iterator[list[dict]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def upsert_rows(rows: list[dict], service_key: str) -> None:
    if not rows:
        return
    url = f"{REST_URL}/rules?on_conflict=id"
    payload = json.dumps(rows).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
            "Content-Profile": "arch",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            print(
                f"    upserted {len(rows):4d} rows (status {resp.status})",
                file=sys.stderr,
            )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()[:500]
        raise RuntimeError(f"upsert failed {exc.code}: {body}") from exc


# --- Service-key retrieval -------------------------------------------------


def get_service_key() -> str:
    token = os.environ.get("SUPABASE_ACCESS_TOKEN")
    if not token:
        raise SystemExit("SUPABASE_ACCESS_TOKEN env var required")
    project_ref = SUPABASE_URL.split("//", 1)[1].split(".", 1)[0]
    req = urllib.request.Request(
        f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
        headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        keys = json.loads(resp.read())
    for entry in keys:
        if entry.get("name") == "service_role" and entry.get("api_key"):
            return entry["api_key"]
    raise SystemExit("service_role key not found")


# --- Entry point -----------------------------------------------------------


def parse_part_list(raw: str) -> list[int]:
    return [int(p.strip()) for p in raw.split(",") if p.strip()]


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--title", type=int, required=True, help="CFR title")
    parts_group = parser.add_mutually_exclusive_group(required=True)
    parts_group.add_argument("--part", type=int, help="Single part number")
    parts_group.add_argument(
        "--parts",
        type=parse_part_list,
        help="Comma-separated part numbers",
    )
    parser.add_argument(
        "--as-of",
        default=DEFAULT_AS_OF,
        help=f"eCFR point-in-time date (YYYY-MM-DD, default {DEFAULT_AS_OF})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse, but do not upsert (requires no SUPABASE_ACCESS_TOKEN)",
    )

    parser.add_argument(
        "--chapter",
        type=str,
        default=None,
        help=(
            "CFR chapter (e.g. 'II'). Optional -- used only to produce "
            "deep-linked eCFR URLs. When omitted, generic /title-T/part-P "
            "URLs are emitted."
        ),
    )
    parser.add_argument(
        "--subchapter",
        type=str,
        default=None,
        help=(
            "CFR subchapter (e.g. 'C'). Optional -- used only for "
            "deep-linked eCFR URLs. Must be paired with --chapter."
        ),
    )

    args = parser.parse_args(list(argv) if argv is not None else None)
    # Use ``is not None`` so --part 0 doesn't fall through to args.parts.
    parts = [args.part] if args.part is not None else args.parts

    if bool(args.chapter) != bool(args.subchapter):
        parser.error("--chapter and --subchapter must be supplied together")

    service_key = None if args.dry_run else get_service_key()

    total = 0
    for part_num in parts:
        print(f"Title {args.title} CFR Part {part_num}:", file=sys.stderr)
        try:
            xml_root = fetch_part_xml(args.title, part_num, args.as_of)
        except Exception as exc:  # noqa: BLE001
            print(f"  FETCH FAILED: {exc}", file=sys.stderr)
            continue
        rows = build_rows(
            args.title,
            part_num,
            xml_root,
            chapter=args.chapter,
            subchapter=args.subchapter,
        )
        print(f"  built {len(rows):4d} rows", file=sys.stderr)
        if args.dry_run:
            total += len(rows)
            continue
        assert service_key is not None
        for batch in chunked(rows):
            upsert_rows(batch, service_key)
        total += len(rows)

    verb = "would upsert" if args.dry_run else "upserted"
    print(f"\n{verb} {total} rows total")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
