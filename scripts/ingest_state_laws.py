"""Generic ingest for any US state ``rules-us-xx`` repo in AKN 3.0.

The per-state drivers (``ingest_ny_laws.py``, ``ingest_ca_laws.py``)
were 90% the same code with different slugs. This consolidated driver
replaces them for any state that uses the standard layout::

    rules-us-{state}/statutes/{code}/{file}.xml

where ``{file}.xml`` contains an AKN-3.0 ``<section>`` with ``<num>``,
``<heading>``, and ``<p>`` content. Section numbers are resolved in
priority order: ``<FRBRnumber value="...">`` from the meta block →
``<num>`` inside the section → filename stem with the law-code prefix
stripped.

Usage
-----
::

    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_state_laws.py --state nm

    # Dry-run, no creds needed
    uv run python scripts/ingest_state_laws.py --state nm --dry-run --limit 50

    # Point at a pre-cloned checkout
    uv run python scripts/ingest_state_laws.py --state nm --repo-dir /path/to/rules-us-nm
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
from ingest_cfr_parts import (  # noqa: E402
    chunked,
    get_service_key,
    refresh_jurisdiction_counts,
)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from atlas.ingest.rule_uploader import RuleUploader  # noqa: E402


AKN_NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"}
REPO_URL_TEMPLATE = "https://github.com/TheAxiomFoundation/rules-us-{state}.git"


def deterministic_id(citation_path: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


def ensure_repo(state: str, target: Path, update: bool) -> Path:
    """Clone ``rules-us-{state}`` into ``target`` if missing; pull if present + update."""
    if target.exists() and (target / ".git").exists():
        if update:
            subprocess.run(["git", "-C", str(target), "pull", "--ff-only"], check=False)
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "clone", "--depth", "1", REPO_URL_TEMPLATE.format(state=state), str(target)],
        check=True,
    )
    return target


def extract_text(elem: ET.Element) -> str:
    """Collect paragraph text inside an AKN element in document order.

    Decodes literal ``\\n`` escapes (some upstream repos emit them
    instead of real newlines) before collapsing whitespace. Paragraph
    boundaries become blank lines via the join.
    """
    out: list[str] = []
    for p in elem.iter("{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}p"):
        raw = "".join(p.itertext()).replace("\\n", "\n")
        t = " ".join(raw.split())
        if t:
            out.append(t)
    return "\n\n".join(out)


def build_row(xml_path: Path, law_code: str, state: str) -> dict | None:
    """Parse one section XML file into a rules-table row."""
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return None
    root = tree.getroot()
    section = root.find(".//akn:body//akn:section", AKN_NS)
    if section is None:
        return None

    frbr_number = root.find(".//akn:FRBRWork/akn:FRBRnumber", AKN_NS)
    num_elem = section.find("akn:num", AKN_NS)
    heading_elem = section.find("akn:heading", AKN_NS)

    if frbr_number is not None and frbr_number.get("value"):
        section_id = frbr_number.get("value", "").strip()
    elif num_elem is not None and num_elem.text:
        section_id = num_elem.text.strip()
    else:
        # Strip the "{code}-" prefix from filename stem if present.
        stem = xml_path.stem
        prefix = f"{law_code}-"
        section_id = stem[len(prefix) :] if stem.startswith(prefix) else stem

    section_id = re.sub(r"\s+", "-", section_id.strip().strip(".")) or xml_path.stem
    heading = (heading_elem.text or "").strip() if heading_elem is not None else ""

    jurisdiction = f"us-{state}"
    citation_path = f"{jurisdiction}/statute/{law_code}/{section_id}"
    body = extract_text(section)

    m = re.match(r"(\d+)", section_id)
    ordinal = int(m.group(1)) if m else 0

    return {
        "id": deterministic_id(citation_path),
        "jurisdiction": jurisdiction,
        "doc_type": "statute",
        "parent_id": None,
        "level": 0,
        "ordinal": ordinal,
        "heading": heading or None,
        "body": body or None,
        "source_url": (
            f"https://github.com/TheAxiomFoundation/rules-us-{state}/blob/main/"
            f"statutes/{law_code}/{xml_path.name}"
        ),
        "citation_path": citation_path,
    }


def iter_sections(repo_dir: Path):
    """Yield ``(law_code, xml_path)`` for every section file in the repo.

    Walks ``statutes/{law}/**/*.xml`` recursively: some upstream repos
    use ``statutes/chapter-X/article-Y/sec-Z.xml`` rather than the
    flat ``statutes/{code}/*.xml`` layout. The immediate child of
    ``statutes/`` is always used as the law_code so the citation-path
    shape stays stable across states.

    Regulations (``regulations/`` at repo root) are intentionally
    skipped — they have a different schema story and we haven't
    picked a path layout for them yet.
    """
    statutes = repo_dir / "statutes"
    if not statutes.is_dir():
        return
    for law_dir in sorted(p for p in statutes.iterdir() if p.is_dir()):
        if law_dir.name.startswith("."):
            continue
        for xml_path in sorted(law_dir.rglob("*.xml")):
            yield law_dir.name, xml_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--state",
        required=True,
        help="Two-letter state code (e.g. 'nm', 'ct'). Selects the rules-us-{state} repo.",
    )
    parser.add_argument(
        "--repo-dir",
        type=Path,
        default=None,
        help="Use an already-cloned checkout instead of cloning to /tmp.",
    )
    parser.add_argument("--update", action="store_true", help="git pull before ingesting")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch", type=int, default=500)
    args = parser.parse_args(argv)

    state = args.state.lower()
    repo_dir = args.repo_dir or Path(f"/tmp/rules-us-{state}-ingest")
    repo_dir = ensure_repo(state, repo_dir, args.update)
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
        row = build_row(xml_path, law_code, state)
        if row is None:
            skipped += 1
            continue
        buffer.append(row)
        parsed += 1
        if len(buffer) >= args.batch:
            flush()
            elapsed = time.time() - started
            print(
                f"  {state}/{law_code}/{xml_path.stem}: "
                f"{parsed} parsed, {skipped} skipped, "
                f"{uploaded} rows uploaded, {elapsed / 60:.1f} min",
                flush=True,
            )

    flush()
    if not args.dry_run and uploaded > 0:
        refresh_jurisdiction_counts(get_service_key())
    elapsed = time.time() - started
    verb = "would upload" if args.dry_run else "uploaded"
    print(
        f"\nDONE us-{state} — {parsed} parsed, {skipped} skipped, "
        f"{uploaded} rows {verb}, {elapsed / 60:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
