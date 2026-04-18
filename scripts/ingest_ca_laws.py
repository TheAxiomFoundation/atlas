"""Ingest the California consolidated laws from ``rules-us-ca`` into Supabase.

The ``TheAxiomFoundation/rules-us-ca`` GitHub repo carries section XML
files in Akoma Ntoso 3.0 format under ``statutes/{code}/{code}-{section}.xml``.
This script clones the repo to a scratch dir (if not already cached),
walks those files, and uploads one row per section to ``akn.rules``
with ``jurisdiction='us-ca'``.

Design
------
* Same AKN-3.0 parser shape as ``ingest_ny_laws.py`` — just different
  jurisdiction, filename convention, and base paths.
* ``citation_path`` = ``us-ca/statute/{law_code}/{section_number}``.
* Section number taken from ``<FRBRnumber>`` when present, else stripped
  filename (drop the leading ``{code}-`` prefix).
* Idempotent: deterministic UUID seeded by citation_path.

Usage
-----
::

    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_ca_laws.py

    # Point at a pre-cloned checkout
    uv run python scripts/ingest_ca_laws.py --repo-dir /path/to/rules-us-ca

    # Dry-run: no credentials needed
    uv run python scripts/ingest_ca_laws.py --dry-run --limit 50
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
from ingest_cfr_parts import chunked, get_service_key  # noqa: E402, F401

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from atlas.ingest.rule_uploader import RuleUploader  # noqa: E402


AKN_NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"}
REPO_URL = "https://github.com/TheAxiomFoundation/rules-us-ca.git"
DEFAULT_CACHE = Path("/tmp/rules-us-ca-ingest")


def deterministic_id(citation_path: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


def ensure_repo(target: Path, update: bool) -> Path:
    """Clone ``rules-us-ca`` into ``target`` if missing; pull if present + update."""
    if target.exists() and (target / ".git").exists():
        if update:
            subprocess.run(["git", "-C", str(target), "pull", "--ff-only"], check=False)
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "clone", "--depth", "1", REPO_URL, str(target)], check=True)
    return target


def extract_text(elem: ET.Element) -> str:
    """Collect paragraph text inside an AKN element in document order.

    Same shape as the NY ingest — concatenate ``<p>`` inner text with
    blank-line separators. The CA repo uses proper newlines (no literal
    ``\\n`` escaping) so no extra decoding is needed, but the replace
    stays for defensive symmetry with NY.
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
    section = root.find(".//akn:body//akn:section", AKN_NS)
    if section is None:
        return None

    # Canonical section number: FRBRnumber is the authoritative source
    # when present; fall back to the <num> under <section>; finally the
    # filename stem with the law-code prefix stripped.
    frbr_number = root.find(".//akn:FRBRWork/akn:FRBRnumber", AKN_NS)
    num_elem = section.find("akn:num", AKN_NS)
    heading_elem = section.find("akn:heading", AKN_NS)

    section_id: str
    if frbr_number is not None and frbr_number.get("value"):
        section_id = frbr_number.get("value", "").strip()
    elif num_elem is not None and num_elem.text:
        section_id = num_elem.text.strip()
    else:
        # Strip the "{code}-" prefix from e.g. "rtc-17041" → "17041".
        stem = xml_path.stem
        prefix = f"{law_code}-"
        section_id = stem[len(prefix):] if stem.startswith(prefix) else stem

    # Normalize whitespace / trailing dots.
    section_id = re.sub(r"\s+", "-", section_id.strip().strip(".")) or xml_path.stem
    heading = (heading_elem.text or "").strip() if heading_elem is not None else ""

    citation_path = f"us-ca/statute/{law_code}/{section_id}"
    body = extract_text(section)

    m = re.match(r"(\d+)", section_id)
    ordinal = int(m.group(1)) if m else 0

    return {
        "id": deterministic_id(citation_path),
        "jurisdiction": "us-ca",
        "doc_type": "statute",
        "parent_id": None,
        "level": 0,
        "ordinal": ordinal,
        "heading": heading or None,
        "body": body or None,
        "source_url": (
            "https://github.com/TheAxiomFoundation/rules-us-ca/blob/main/"
            f"statutes/{law_code}/{xml_path.name}"
        ),
        "citation_path": citation_path,
    }


def iter_sections(repo_dir: Path):
    """Yield ``(law_code, xml_path)`` for every section file in the repo."""
    statutes = repo_dir / "statutes"
    if not statutes.is_dir():
        return
    for law_dir in sorted(p for p in statutes.iterdir() if p.is_dir()):
        if law_dir.name.startswith("."):
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
