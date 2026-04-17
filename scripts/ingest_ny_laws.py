"""Ingest the NY consolidated laws from the ``rules-us-ny`` repo into Supabase.

The ``TheAxiomFoundation/rules-us-ny`` GitHub repo carries ~37k section
XML files in Akoma Ntoso 3.0 format, one per NY-law-code section. This
script clones the repo to a scratch dir (if not already cached), walks
``{law_code}/{section}.xml``, and uploads one row per section to
``akn.rules`` with ``jurisdiction='us-ny'``.

Design
------
* Self-contained parser. AKN 3.0 is well-specified enough that we don't
  need the state_orchestrator plumbing — just ElementTree.
* ``citation_path`` = ``us-ny/statute/{law_code}/{section_number}``.
  Matches the Atlas viewer's expected shape for a state jurisdiction
  with doc_type = "statute".
* Body text = concatenated ``<content>`` inner text in document order,
  preserving paragraph breaks. Good enough for search + reading; the
  fine-grained ``<subsection>`` / ``<paragraph>`` hierarchy stays in the
  raw XML (accessible later via a raw.fetched_documents FK once we wire
  provenance).
* Idempotent per section. Deterministic UUID seeded by citation_path;
  re-runs upsert cleanly.

Usage
-----
::

    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_ny_laws.py

    # Point at a pre-cloned checkout instead of using /tmp
    uv run python scripts/ingest_ny_laws.py --repo-dir /path/to/rules-us-ny

    # Dry-run: no credentials needed
    uv run python scripts/ingest_ny_laws.py --dry-run --limit 100
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingest_cfr_parts import chunked, get_service_key  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from atlas.ingest.rule_uploader import RuleUploader  # noqa: E402


AKN_NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"}
REPO_URL = "https://github.com/TheAxiomFoundation/rules-us-ny.git"
DEFAULT_CACHE = Path("/tmp/rules-us-ny-ingest")


# Law codes present in the repo that don't correspond to a section-XML
# bundle (README, top-level aggregators). Skip them at scan time.
NON_LAW_DIRS = {"README.md", "regulations", "statutes"}


def deterministic_id(citation_path: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


def ensure_repo(target: Path, update: bool) -> Path:
    """Clone ``rules-us-ny`` into ``target`` if missing; pull if present + update."""
    if target.exists() and (target / ".git").exists():
        if update:
            subprocess.run(["git", "-C", str(target), "pull", "--ff-only"], check=False)
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "clone", "--depth", "1", REPO_URL, str(target)], check=True)
    return target


def extract_text(elem: ET.Element) -> str:
    """Collect paragraph text inside an AKN element in document order.

    Uses ``itertext()`` for each ``<p>`` so inline elements like
    ``<ref>``, ``<i>``, etc. contribute their plain text without
    structural markers leaking into the body column. Paragraph breaks
    become blank lines; that's how search + the viewer's leaf render
    expect the body to be shaped.

    The upstream ``rules-us-ny`` repo emits ``<content>`` text with
    LITERAL ``\\n`` sequences (two chars: backslash + n) rather than
    real newlines. Downstream this breaks citation extraction
    (``\\s+`` won't match literal backslash-n) and renders as visible
    ``\\n`` in the viewer. Decode at ingest so the body column always
    carries real line breaks.
    """
    out: list[str] = []
    for p in elem.iter("{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}p"):
        raw = "".join(p.itertext()).replace("\\n", "\n")
        t = " ".join(raw.split())
        if t:
            out.append(t)
    return "\n\n".join(out)


def build_row(xml_path: Path, law_code: str) -> dict | None:
    """Parse one section XML file into a rules-table row.

    Returns ``None`` if the file doesn't match the expected shape so
    bulk ingests continue past isolated weirdness without aborting.
    """
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return None
    root = tree.getroot()
    section = root.find(".//akn:body/akn:section", AKN_NS)
    if section is None:
        return None

    # <num> and <heading> are the canonical identity of the section.
    num_elem = section.find("akn:num", AKN_NS)
    heading_elem = section.find("akn:heading", AKN_NS)
    num = (num_elem.text or "").strip() if num_elem is not None else xml_path.stem
    heading = (heading_elem.text or "").strip() if heading_elem is not None else ""

    section_id = re.sub(r"\s+", "-", num.strip().strip(".")) or xml_path.stem
    citation_path = f"us-ny/statute/{law_code}/{section_id}"
    body = extract_text(section)

    # Ordinal: try to parse the numeric prefix of the section id so
    # sorting inside the viewer is natural. Falls back to 0 if the
    # id is alpha-only (rare; e.g. some drafting holdovers).
    m = re.match(r"(\d+)", section_id)
    ordinal = int(m.group(1)) if m else 0

    return {
        "id": deterministic_id(citation_path),
        "jurisdiction": "us-ny",
        "doc_type": "statute",
        "parent_id": None,
        "level": 0,
        "ordinal": ordinal,
        "heading": heading or None,
        "body": body or None,
        "source_url": f"https://github.com/TheAxiomFoundation/rules-us-ny/blob/main/{law_code}/{xml_path.name}",
        "citation_path": citation_path,
    }


def iter_sections(repo_dir: Path):
    """Yield ``(law_code, xml_path)`` for every section file in the repo."""
    for law_dir in sorted(p for p in repo_dir.iterdir() if p.is_dir()):
        if law_dir.name in NON_LAW_DIRS or law_dir.name.startswith("."):
            continue
        for xml_path in sorted(law_dir.glob("*.xml")):
            yield law_dir.name, xml_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo-dir", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--update", action="store_true", help="git pull before ingesting")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch", type=int, default=500)
    args = parser.parse_args(argv)

    repo_dir = ensure_repo(args.repo_dir, args.update)
    uploader = None if args.dry_run else RuleUploader()

    started = time.time()
    parsed = 0
    skipped = 0
    uploaded = 0
    buffer: list[dict] = []

    def flush() -> None:
        nonlocal buffer, uploaded
        if not buffer:
            return
        if uploader is not None:
            for chunk in chunked(buffer, size=args.batch):
                uploader.upsert_all(chunk)
        uploaded += len(buffer)
        buffer = []

    for law_code, xml_path in iter_sections(repo_dir):
        if args.limit is not None and parsed + skipped >= args.limit:
            break
        row = build_row(xml_path, law_code)
        if row is None:
            skipped += 1
            continue
        buffer.append(row)
        parsed += 1
        if len(buffer) >= args.batch:
            flush()
            elapsed = time.time() - started
            print(
                f"  {law_code}/{xml_path.stem}: "
                f"{parsed} parsed, {skipped} skipped, "
                f"{uploaded} rows uploaded, {elapsed / 60:.1f} min",
                flush=True,
            )

    flush()
    elapsed = time.time() - started
    verb = "would upload" if args.dry_run else "uploaded"
    print(
        f"\nDONE — {parsed} parsed, {skipped} skipped, "
        f"{uploaded} rows {verb}, {elapsed / 60:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
