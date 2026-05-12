"""California CDSS Manual of Policies and Procedures (MPP) adapter.

Snapshots and normalizes the CalFresh slice of MPP — Division 63, the CDSS
operational manual for CalFresh (CA SNAP). This adapter covers the
encoding-essential subset:

- §63-300 Application Process
- §63-301 Application Processing Time Standards
- §63-401–63-405 Eligibility Standards (citizenship through resources)
- §63-406–63-411 Eligibility Standards (income through work registration)
- §63-501–63-502 Eligibility Determination and Benefit Issuance
- §63-503 Allotment Computation

Full §63 (~2,000 pages) is out of scope for v1; see the encoding playbook
§ 14 and tracking issue for the broader effort.

Source format: each chapter group is published as a Microsoft Word DOCX
file on cdss.ca.gov. Parsing is delegated to
``axiom_corpus.parsers.us_ca.regulations``.

Citation path layout::

    us-ca/regulation/mpp                           — root
    us-ca/regulation/mpp/63                        — Division 63
    us-ca/regulation/mpp/63-300                    — Section
    us-ca/regulation/mpp/63-300.1                  — Subsection

Variant resolution (MR vs QR — Monthly Reporting / Quarterly Reporting),
deeper nesting beyond first-level subsections, ACL/ACIN overlay handling,
and table-aware parsing are explicit follow-on work captured in the
encoding playbook. The v1 adapter intentionally accepts that some MPP
content will appear concatenated into a parent subsection body rather
than as separate rows.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import requests

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import (
    ProvisionCoverageReport,
    compare_provision_coverage,
)
from axiom_corpus.corpus.models import (
    DocumentClass,
    ProvisionRecord,
    SourceInventoryItem,
)
from axiom_corpus.parsers.us_ca.regulations import (
    MppSection,
    MppSubsection,
    extract_paragraphs,
    parse_mpp_sections,
)

JURISDICTION = "us-ca"
DOC_CLASS = DocumentClass.REGULATION
SOURCE_FORMAT = "docx"
ROOT_CITATION_PATH = "us-ca/regulation/mpp"
DIVISION_CITATION_PATH = "us-ca/regulation/mpp/63"
DIVISION_HEADING = "Division 63 — CalFresh (Food Stamps)"

@dataclass(frozen=True)
class MppDocxSource:
    """One DOCX file in the MPP source set."""

    file: str
    url: str
    chapter: str
    sections: tuple[str, ...]
    summary: str


# Default MVP scope. Caller can override via run options.
DEFAULT_DOCX_SOURCES: tuple[MppDocxSource, ...] = ()


@dataclass(frozen=True)
class CaliforniaMppExtractReport:
    """Result from a CA MPP extraction run."""

    jurisdiction: str
    document_class: str
    source_count: int
    section_count: int
    subsection_count: int
    container_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]


def extract_california_mpp_calfresh(
    store: CorpusArtifactStore,
    *,
    version: str,
    docx_sources: tuple[MppDocxSource, ...],
    download_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    request_delay_seconds: float = 0.25,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
    session: requests.Session | None = None,
) -> CaliforniaMppExtractReport:
    """Snapshot CDSS MPP DOCX files and extract CalFresh provisions.

    ``docx_sources`` must be non-empty; the caller is responsible for
    declaring the MVP subset (typically via manifests/us-ca-cdss-mpp-calfresh.yaml).
    """
    if not docx_sources:
        raise ValueError("extract_california_mpp_calfresh: docx_sources must be non-empty")

    client = session or requests.Session()
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    run_id = version
    download_root = Path(download_dir) if download_dir is not None else None

    inventory_items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    section_count = 0
    subsection_count = 0
    container_count = 0

    # Root + division container rows. One inventory entry per container, so
    # the coverage report exercises the same paths the provisions emit.
    inventory_items.append(SourceInventoryItem(citation_path=ROOT_CITATION_PATH))
    inventory_items.append(SourceInventoryItem(citation_path=DIVISION_CITATION_PATH))

    records.append(
        _container_provision(
            citation_path=ROOT_CITATION_PATH,
            heading="CDSS Manual of Policies and Procedures",
            kind="manual",
            parent_citation_path=None,
            level=0,
            ordinal=0,
            run_id=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            source_url=None,
            source_path=None,
            source_id=None,
        )
    )
    records.append(
        _container_provision(
            citation_path=DIVISION_CITATION_PATH,
            heading=DIVISION_HEADING,
            kind="division",
            parent_citation_path=ROOT_CITATION_PATH,
            level=1,
            ordinal=63,
            run_id=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            source_url=None,
            source_path=None,
            source_id=None,
        )
    )
    container_count += 2

    # MPP uses "chapter" loosely — "Chapter 63-300" is the same as "§63-300"
    # at the encoding-relevant level of detail. Parent sections directly
    # to the division; don't emit redundant chapter containers.
    for src in docx_sources:
        docx_bytes = _fetch_docx(
            client,
            url=src.url,
            timeout=timeout_seconds,
            attempts=request_attempts,
            cache_path=(download_root / src.file) if download_root else None,
            delay_seconds=request_delay_seconds,
        )
        relative = f"cdss-mpp/{src.file}"
        artifact_path = store.source_path(JURISDICTION, DOC_CLASS, run_id, relative)
        store.write_bytes(artifact_path, docx_bytes)
        source_paths.append(artifact_path)

        paragraphs = extract_paragraphs(docx_bytes)
        parsed_sections = parse_mpp_sections(
            paragraphs,
            source_file=src.file,
            expected_sections=src.sections,
        )
        # Filter to the expected sections declared in the manifest. Other
        # sections in the file (e.g., continuation pages from neighboring
        # chapters) are skipped; they're covered by their declaring file.
        wanted = set(src.sections)
        for section in parsed_sections:
            if wanted and section.num not in wanted:
                continue
            section_path = f"us-ca/regulation/mpp/{section.num}"
            inventory_items.append(SourceInventoryItem(citation_path=section_path))
            records.append(
                _section_provision(
                    section,
                    parent_citation_path=DIVISION_CITATION_PATH,
                    run_id=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    source_url=src.url,
                    source_path=str(artifact_path),
                    source_id=src.file,
                )
            )
            section_count += 1

            for ordinal, sub in enumerate(section.subsections, start=1):
                sub_path = f"{section_path}.{sub.num}"
                inventory_items.append(SourceInventoryItem(citation_path=sub_path))
                records.append(
                    _subsection_provision(
                        sub,
                        parent_citation_path=section_path,
                        ordinal=ordinal,
                        run_id=run_id,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                        source_url=src.url,
                        source_path=str(artifact_path),
                        source_id=src.file,
                    )
                )
                subsection_count += 1

    inventory_path = store.inventory_path(JURISDICTION, DOC_CLASS, run_id)
    store.write_inventory(inventory_path, inventory_items)
    provisions_path = store.provisions_path(JURISDICTION, DOC_CLASS, run_id)
    store.write_provisions(provisions_path, records)

    coverage = compare_provision_coverage(
        tuple(inventory_items),
        tuple(records),
        jurisdiction=JURISDICTION,
        document_class=DOC_CLASS.value,
        version=run_id,
    )
    coverage_path = store.coverage_path(JURISDICTION, DOC_CLASS, run_id)
    store.write_json(coverage_path, coverage.to_mapping())

    return CaliforniaMppExtractReport(
        jurisdiction=JURISDICTION,
        document_class=DOC_CLASS.value,
        source_count=len(docx_sources),
        section_count=section_count,
        subsection_count=subsection_count,
        container_count=container_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def _fetch_docx(
    client: requests.Session,
    *,
    url: str,
    timeout: float,
    attempts: int,
    cache_path: Path | None,
    delay_seconds: float,
) -> bytes:
    """Download DOCX bytes, with optional local cache and retry."""
    if cache_path is not None and cache_path.exists():
        return cache_path.read_bytes()
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = client.get(url, timeout=timeout)
            data = resp.content
            if not data:
                raise ValueError(f"empty body for {url}")
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_bytes(data)
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            return data
        except Exception as exc:  # noqa: BLE001 — retry on any transport error
            last_error = exc
            if attempt < attempts:
                time.sleep(delay_seconds * attempt)
    raise RuntimeError(f"failed to fetch {url} after {attempts} attempts: {last_error}")


def _section_provision(
    section: MppSection,
    *,
    parent_citation_path: str,
    run_id: str,
    source_as_of: str,
    expression_date: str,
    source_url: str | None,
    source_path: str | None,
    source_id: str | None,
) -> ProvisionRecord:
    citation_path = f"us-ca/regulation/mpp/{section.num}"
    return ProvisionRecord(
        jurisdiction=JURISDICTION,
        document_class=DOC_CLASS.value,
        citation_path=citation_path,
        body=None,
        heading=f"{section.num} {section.title}".strip(),
        citation_label=f"MPP {section.num}",
        version=run_id,
        source_url=source_url,
        source_path=source_path,
        source_id=source_id,
        source_format=SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=parent_citation_path,
        level=2,
        ordinal=_ordinal_for(section.num),
        kind="section",
        identifiers={"state:mpp_section": section.num},
        legal_identifier=f"CA MPP §{section.num}",
        metadata={"source_file": section.source_file},
    )


def _subsection_provision(
    sub: MppSubsection,
    *,
    parent_citation_path: str,
    ordinal: int,
    run_id: str,
    source_as_of: str,
    expression_date: str,
    source_url: str | None,
    source_path: str | None,
    source_id: str | None,
) -> ProvisionRecord:
    citation_path = f"{parent_citation_path}.{sub.num}"
    return ProvisionRecord(
        jurisdiction=JURISDICTION,
        document_class=DOC_CLASS.value,
        citation_path=citation_path,
        body=sub.body or None,
        heading=f".{sub.num} {sub.title}".strip(),
        citation_label=f"MPP {sub.parent_num}.{sub.num}",
        version=run_id,
        source_url=source_url,
        source_path=source_path,
        source_id=source_id,
        source_format=SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=parent_citation_path,
        level=3,
        ordinal=ordinal,
        kind="subsection",
        identifiers={
            "state:mpp_section": sub.parent_num,
            "state:mpp_subsection": sub.num,
        },
        legal_identifier=f"CA MPP §{sub.parent_num}.{sub.num}",
    )


def _container_provision(
    *,
    citation_path: str,
    heading: str,
    kind: str,
    parent_citation_path: str | None,
    level: int,
    ordinal: int,
    run_id: str,
    source_as_of: str,
    expression_date: str,
    source_url: str | None,
    source_path: str | None,
    source_id: str | None,
) -> ProvisionRecord:
    return ProvisionRecord(
        jurisdiction=JURISDICTION,
        document_class=DOC_CLASS.value,
        citation_path=citation_path,
        body=None,
        heading=heading,
        version=run_id,
        source_url=source_url,
        source_path=source_path,
        source_id=source_id,
        source_format=SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=parent_citation_path,
        level=level,
        ordinal=ordinal,
        kind=kind,
    )


def _ordinal_for(section_num: str) -> int:
    """Stable sort key derived from the §63-XXX number."""
    # "63-301" → 301; "63-401.5" → 401
    try:
        tail = section_num.split("-", 1)[1]
        return int(tail.split(".", 1)[0])
    except (IndexError, ValueError):
        return 0


def _date_text(value: date | str | None, fallback: str) -> str:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value:
        return value
    return fallback
