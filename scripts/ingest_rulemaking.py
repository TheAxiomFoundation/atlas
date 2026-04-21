"""Ingest axiom-scrapers ``rulemaking/`` trees into Atlas.

Rulemaking documents (Federal Register Rules + Proposed Rules at
present) follow a different layout and doc_type from codified
statutes, so they get their own ingester rather than generalizing
``ingest_state_laws.py``.

Expected input layout, produced by
``axiom-scrape --jurisdiction us-federal --doc-type rulemaking``::

    {tree}/us-federal/rulemaking/{YYYY}/{document_number}.xml

Each file is an Akoma Ntoso 3.0 document with one ``<section>`` whose
``<FRBRnumber>`` carries the FR document number, ``<num>`` holds the
citation (``91 FR 20899`` or ``FR Doc. …`` fallback), ``<heading>``
holds the title (prefixed with ``[Proposed] `` for PRORULE), and
``<content>/<p>`` holds the plain-text body.

Usage
-----
::

    uv run python scripts/ingest_rulemaking.py --tree /tmp/fr-out --dry-run --limit 20
    SUPABASE_ACCESS_TOKEN=... uv run python scripts/ingest_rulemaking.py --tree /tmp/fr-out
"""

from __future__ import annotations

import argparse
import os
import re
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

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)
from atlas.ingest.rule_uploader import RuleUploader  # noqa: E402


AKN_NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"}


def deterministic_id(citation_path: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


def extract_text(elem: ET.Element) -> str:
    """Collect paragraph text inside an AKN element in document order.

    Identical to the statutes ingester's helper — FR body is plain text
    wrapped in a single ``<p>`` today, but we keep the multi-paragraph
    join so a future switch to structured paragraphs costs nothing.
    """
    out: list[str] = []
    for p in elem.iter("{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}p"):
        raw = "".join(p.itertext()).replace("\\n", "\n")
        t = " ".join(raw.split())
        if t:
            out.append(t)
    return "\n\n".join(out)


def build_row(xml_path: Path, jurisdiction: str, year: str) -> dict | None:
    """Parse one rulemaking XML file into a rules-table row.

    Returns ``None`` when the file doesn't have an AKN ``<section>`` we
    can project (corrupt write, non-AKN content that landed in the
    tree by accident, etc.).
    """
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
        document_number = frbr_number.get("value", "").strip()
    elif num_elem is not None and num_elem.text:
        # Fallback to the citation (``91 FR 20899``). Less ideal because
        # it's not unique across document revisions, but sufficient for
        # dedup if FRBRnumber is missing.
        document_number = num_elem.text.strip()
    else:
        document_number = xml_path.stem

    document_number = re.sub(r"\s+", "-", document_number.strip())
    heading = (heading_elem.text or "").strip() if heading_elem is not None else ""
    citation = (num_elem.text or "").strip() if num_elem is not None else ""

    # citation_path mirrors the on-disk layout so collisions across
    # years can't happen. The stable primary key is the UUIDv5 derived
    # from this path.
    citation_path = f"{jurisdiction}/rulemaking/{year}/{document_number}"
    body = extract_text(section)

    # Ordinal sorting on a bare FR document number (e.g. ``2026-07681``)
    # would go by the leading year, which is useless. Drop the year
    # prefix if present and parse the rest as the ordinal.
    stem = document_number.split("-", 1)[-1] if "-" in document_number else document_number
    m = re.match(r"(\d+)", stem)
    ordinal = int(m.group(1)) if m else 0

    # Public Federal Register viewer URL.
    # Format: /documents/{YYYY}/{MM}/{DD}/{document_number}/{slug}
    # The exact slug is not reconstructible from the FRBR metadata, so
    # we link to the API-stable document_number endpoint instead.
    source_url = f"https://www.federalregister.gov/d/{document_number}"

    # The rules table has no ``citation`` column today — the AKN <num>
    # element preserves the FR citation in the raw XML in R2, and
    # downstream consumers can parse it out if they need a rendered
    # short-cite. Keep the local variable for logging but don't emit
    # it as a row field.
    _ = citation  # retained for future schema addition

    return {
        "id": deterministic_id(citation_path),
        "jurisdiction": jurisdiction,
        "doc_type": "rulemaking",
        "parent_id": None,
        "level": 0,
        "ordinal": ordinal,
        "heading": heading or None,
        "body": body or None,
        "source_url": source_url,
        "citation_path": citation_path,
    }


def iter_rulemaking_xml(tree_dir: Path, jurisdiction: str):
    """Yield ``(year, xml_path)`` for every rulemaking XML under ``tree_dir``.

    Walks ``{tree_dir}/{jurisdiction}/rulemaking/{year}/*.xml``.
    """
    root = tree_dir / jurisdiction / "rulemaking"
    if not root.is_dir():
        return
    for year_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        if year_dir.name.startswith("."):
            continue
        for xml_path in sorted(year_dir.glob("*.xml")):
            yield year_dir.name, xml_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--tree",
        type=Path,
        required=True,
        help="Path to the axiom-scrapers output tree (the directory containing"
        " ``us-federal/rulemaking/...``).",
    )
    parser.add_argument(
        "--jurisdiction",
        default="us-federal",
        help="Top-level jurisdiction slug under the tree. Defaults to"
        " 'us-federal' since the Federal Register is the only rulemaking"
        " source today.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch", type=int, default=500)
    args = parser.parse_args(argv)

    if not args.tree.is_dir():
        raise SystemExit(f"--tree {args.tree} does not exist or is not a directory")

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

    for year, xml_path in iter_rulemaking_xml(args.tree, args.jurisdiction):
        if args.limit is not None and parsed + skipped >= args.limit:
            break
        row = build_row(xml_path, args.jurisdiction, year)
        if row is None:
            skipped += 1
            continue
        buffer.append(row)
        parsed += 1
        if len(buffer) >= args.batch:
            flush()
            elapsed = time.time() - started
            print(
                f"  {args.jurisdiction}/rulemaking/{year}/{xml_path.stem}: "
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
        f"\nDONE {args.jurisdiction}/rulemaking — {parsed} parsed, "
        f"{skipped} skipped, {uploaded} rows {verb}, {elapsed / 60:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
