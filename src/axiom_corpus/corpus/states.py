"""State statute source adapters for source-first corpus ingestion."""

from __future__ import annotations

import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

DC_CODE_WEB_BASE = "https://code.dccouncil.gov/us/dc/council/code"
DC_CODE_REPO_BASE = "https://github.com/dccouncil/law-xml-codified"
DC_XML_SOURCE_FORMAT = "dc-law-xml"
CIC_HTML_SOURCE_FORMAT = "cic-state-code-html"
CIC_ODT_SOURCE_FORMAT = "cic-state-code-odt"
COLORADO_DOCX_SOURCE_FORMAT = "colorado-crs-docx"
ODT_TEXT_NS = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"
WORD_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
PRIMARY_CIC_ODT_PREFIXES = {
    "us-ga": "gov.ga.ocga",
    "us-ky": "gov.ky.krs",
    "us-nc": "gov.nc.stat",
    "us-nd": "gov.nd.code",
    "us-va": "gov.va.code",
    "us-vt": "gov.vt.vsa",
    "us-wy": "gov.wy.code",
}


@dataclass(frozen=True)
class StateStatuteExtractReport:
    """Result from a state statute extraction run."""

    jurisdiction: str
    title_count: int
    container_count: int
    section_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]
    skipped_source_count: int = 0
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class _StateContainer:
    jurisdiction: str
    title: str
    kind: str
    num: str
    heading: str | None
    citation_path: str
    parent_citation_path: str | None
    level: int
    ordinal: int | None
    source_path: str
    source_url: str | None
    source_id: str | None
    source_format: str
    sha256: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class _DcSectionTarget:
    section: str
    title: str
    parent_citation_path: str
    level: int
    ordinal: int | None


@dataclass(frozen=True)
class _DcSectionDocument:
    section: str
    title: str
    heading: str | None
    body: str | None
    source_id: str | None
    references_to: tuple[str, ...]
    annotations: tuple[dict[str, str], ...]

    @property
    def citation_path(self) -> str:
        return f"us-dc/statute/{self.title}/{self.section}"


@dataclass(frozen=True)
class _CicSection:
    title: str
    section: str
    heading: str | None
    body: str | None
    source_id: str | None
    parent_citation_path: str
    level: int
    ordinal: int | None
    references_to: tuple[str, ...]

    @property
    def citation_path(self) -> str:
        return f"{self.parent_citation_path.split('/statute/', 1)[0]}/statute/{self.title}/{self.section}"


@dataclass(frozen=True)
class _ColoradoSection:
    title: str
    section: str
    variant: str | None
    heading: str | None
    body: str | None
    source_id: str | None
    parent_citation_path: str
    level: int
    ordinal: int | None
    references_to: tuple[str, ...]
    supplement_pdf_files: tuple[str, ...]
    supplement_source_paths: tuple[str, ...]
    missing_supplement_pdf_files: tuple[str, ...]

    @property
    def base_citation_path(self) -> str:
        return f"us-co/statute/{self.title}/{self.section}"

    @property
    def citation_path(self) -> str:
        if self.variant:
            return f"{self.base_citation_path}@{self.variant}"
        return self.base_citation_path


@dataclass(frozen=True)
class _OdtParagraph:
    style: str | None
    text: str
    source_id: str


@dataclass(frozen=True)
class _DocxParagraph:
    text: str
    source_id: str


@dataclass(frozen=True)
class _ColoradoSupplementPdf:
    file_name: str
    source_path: str
    sha256: str
    text: str


def state_run_id(
    version: str,
    *,
    jurisdiction: str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
) -> str:
    """Return a scoped state ingest run id."""
    parts = [version]
    if jurisdiction:
        parts.append(jurisdiction)
    if only_title is not None:
        parts.append(f"title-{_clean_title_token(only_title)}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def extract_colorado_docx_release(
    store: CorpusArtifactStore,
    *,
    version: str,
    release_dir: str | Path,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
) -> StateStatuteExtractReport:
    """Snapshot the official Colorado CRS DOCX release and extract provisions."""
    jurisdiction = "us-co"
    source_root = Path(release_dir)
    docx_root = source_root / "docx"
    if not docx_root.exists():
        raise ValueError(f"Colorado CRS DOCX directory does not exist: {docx_root}")
    only_title_token = _clean_title_token(only_title) if only_title is not None else None
    run_id = (
        state_run_id(version, jurisdiction=jurisdiction, only_title=only_title_token, limit=limit)
        if only_title_token or limit is not None
        else version
    )
    source_as_of_text = source_as_of or _release_date_from_name(source_root.name) or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    supplement_map, supplement_source_paths = _load_colorado_supplement_pdfs(
        store,
        source_root=source_root,
        run_id=run_id,
        only_title=only_title_token,
    )

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = list(supplement_source_paths)
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    errors: list[str] = []
    remaining = limit

    for docx_path in _iter_colorado_title_docx_files(docx_root, only_title_token):
        if remaining is not None and remaining <= 0:
            break
        title = _title_from_colorado_docx_filename(docx_path)
        if title is None:
            skipped_source_count += 1
            continue
        docx_bytes = docx_path.read_bytes()
        relative = f"colorado-crs-docx/{source_root.name}/docx/{docx_path.name}"
        artifact_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, relative)
        source_sha256 = store.write_bytes(artifact_path, docx_bytes)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, relative)
        title_count += 1

        try:
            paragraphs = _docx_paragraphs(docx_bytes)
            containers, sections = _parse_colorado_title_docx(
                paragraphs,
                title=title,
                supplements=supplement_map,
            )
        except (ValueError, ET.ParseError, zipfile.BadZipFile, KeyError) as exc:
            errors.append(f"{docx_path.name}: {exc}")
            continue

        for container in containers:
            if remaining is not None and remaining <= 0:
                break
            container = _replace_container_source(
                container,
                source_path=source_key,
                source_format=COLORADO_DOCX_SOURCE_FORMAT,
                sha256=source_sha256,
                metadata_extra={"release": source_root.name, "file_name": docx_path.name},
            )
            item = _container_inventory_item(container)
            record = _container_provision(
                container,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
            items.append(item)
            records.append(record)
            container_count += 1
            if remaining is not None:
                remaining -= 1
        if remaining is not None and remaining <= 0:
            break

        for section in sections:
            if remaining is not None and remaining <= 0:
                break
            metadata = _colorado_section_metadata(
                section,
                release=source_root.name,
                file_name=docx_path.name,
            )
            item = SourceInventoryItem(
                citation_path=section.citation_path,
                source_url=None,
                source_path=source_key,
                source_format=COLORADO_DOCX_SOURCE_FORMAT,
                sha256=source_sha256,
                metadata=metadata,
            )
            record = _colorado_section_provision(
                section,
                version=run_id,
                source_path=source_key,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
            items.append(item)
            records.append(record)
            section_count += 1
            if remaining is not None:
                remaining -= 1

    if not items:
        raise ValueError(f"no Colorado CRS provisions extracted from {source_root}")

    inventory_path = store.inventory_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
    coverage_path = store.coverage_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return StateStatuteExtractReport(
        jurisdiction=jurisdiction,
        title_count=title_count,
        container_count=container_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        skipped_source_count=skipped_source_count,
        errors=tuple(errors),
    )


def extract_cic_odt_release(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    version: str,
    release_dir: str | Path,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
) -> StateStatuteExtractReport:
    """Snapshot a Public.Resource.org CIC ODT release and extract state provisions."""
    source_root = Path(release_dir)
    if not source_root.exists():
        raise ValueError(f"CIC ODT release directory does not exist: {source_root}")
    only_title_token = _clean_title_token(only_title) if only_title is not None else None
    run_id = (
        state_run_id(version, jurisdiction=jurisdiction, only_title=only_title_token, limit=limit)
        if only_title_token or limit is not None
        else version
    )
    source_as_of_text = source_as_of or _release_date_from_name(source_root.name) or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    errors: list[str] = []
    remaining = limit

    for odt_path in _iter_cic_title_odt_files(source_root, jurisdiction, only_title_token):
        if remaining is not None and remaining <= 0:
            break
        title = _title_from_cic_odt_filename(odt_path)
        if title is None:
            skipped_source_count += 1
            continue
        odt_bytes = odt_path.read_bytes()
        relative = f"cic-odt/{source_root.name}/{odt_path.name}"
        artifact_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, relative)
        source_sha256 = store.write_bytes(artifact_path, odt_bytes)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, relative)
        title_count += 1

        try:
            paragraphs = _odt_paragraphs(odt_bytes)
            containers, sections = _parse_cic_title_odt(
                paragraphs,
                jurisdiction=jurisdiction,
                title=title,
            )
        except (ValueError, ET.ParseError, zipfile.BadZipFile, KeyError) as exc:
            errors.append(f"{odt_path.name}: {exc}")
            continue

        for container in containers:
            if remaining is not None and remaining <= 0:
                break
            container = _replace_container_source(
                container,
                source_path=source_key,
                source_format=CIC_ODT_SOURCE_FORMAT,
                sha256=source_sha256,
                metadata_extra={"release": source_root.name, "file_name": odt_path.name},
            )
            item = _container_inventory_item(container)
            record = _container_provision(
                container,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
            items.append(item)
            records.append(record)
            container_count += 1
            if remaining is not None:
                remaining -= 1
        if remaining is not None and remaining <= 0:
            break

        for section in sections:
            if remaining is not None and remaining <= 0:
                break
            item = SourceInventoryItem(
                citation_path=section.citation_path,
                source_url=None,
                source_path=source_key,
                source_format=CIC_ODT_SOURCE_FORMAT,
                sha256=source_sha256,
                metadata={
                    "kind": "section",
                    "title": section.title,
                    "section": section.section,
                    "heading": section.heading,
                    "parent_citation_path": section.parent_citation_path,
                    "source_id": section.source_id,
                    "references_to": list(section.references_to),
                    "release": source_root.name,
                    "file_name": odt_path.name,
                },
            )
            record = _cic_section_provision(
                section,
                jurisdiction=jurisdiction,
                version=run_id,
                source_path=source_key,
                source_format=CIC_ODT_SOURCE_FORMAT,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
            items.append(item)
            records.append(record)
            section_count += 1
            if remaining is not None:
                remaining -= 1

    if not items:
        raise ValueError(f"no CIC ODT provisions extracted from {source_root}")

    inventory_path = store.inventory_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
    coverage_path = store.coverage_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return StateStatuteExtractReport(
        jurisdiction=jurisdiction,
        title_count=title_count,
        container_count=container_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        skipped_source_count=skipped_source_count,
        errors=tuple(errors),
    )


def extract_dc_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
) -> StateStatuteExtractReport:
    """Snapshot local DC Code XML and extract normalized provisions."""
    title_root = Path(source_dir)
    if not title_root.exists():
        raise ValueError(f"DC Code source directory does not exist: {title_root}")
    only_title_token = _clean_title_token(only_title) if only_title is not None else None
    run_id = (
        state_run_id(version, only_title=only_title_token, limit=limit)
        if only_title_token or limit is not None
        else version
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    title_count = 0
    container_count = 0
    section_count = 0
    remaining = limit

    for title_dir in _iter_dc_title_dirs(title_root, only_title_token):
        if remaining is not None and remaining <= 0:
            break
        title = title_dir.name
        index_path = title_dir / "index.xml"
        index_bytes = index_path.read_bytes()
        index_relative = f"dc-law-xml/titles/{title}/index.xml"
        index_artifact_path = store.source_path(
            "us-dc", DocumentClass.STATUTE, run_id, index_relative
        )
        index_sha256 = store.write_bytes(index_artifact_path, index_bytes)
        source_paths.append(index_artifact_path)
        index_key = _state_source_key("us-dc", run_id, index_relative)
        root = ET.fromstring(index_bytes)

        title_count += 1
        containers, targets = _dc_index_items(
            root,
            title=title,
            source_path=index_key,
            source_sha256=index_sha256,
        )
        for container in containers:
            if remaining is not None and remaining <= 0:
                break
            item = _container_inventory_item(container)
            record = _container_provision(
                container,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
            items.append(item)
            records.append(record)
            container_count += 1
            if remaining is not None:
                remaining -= 1
        if remaining is not None and remaining <= 0:
            break

        for target in targets:
            if remaining is not None and remaining <= 0:
                break
            section_path = title_dir / "sections" / f"{target.section}.xml"
            if not section_path.exists():
                continue
            section_bytes = section_path.read_bytes()
            section_relative = f"dc-law-xml/titles/{title}/sections/{target.section}.xml"
            section_artifact_path = store.source_path(
                "us-dc",
                DocumentClass.STATUTE,
                run_id,
                section_relative,
            )
            section_sha256 = store.write_bytes(section_artifact_path, section_bytes)
            source_paths.append(section_artifact_path)
            section_source_key = _state_source_key("us-dc", run_id, section_relative)
            document = _parse_dc_section_xml(section_bytes)
            item = SourceInventoryItem(
                citation_path=document.citation_path,
                source_url=_dc_section_url(document.section),
                source_path=section_source_key,
                source_format=DC_XML_SOURCE_FORMAT,
                sha256=section_sha256,
                metadata={
                    "kind": "section",
                    "title": document.title,
                    "section": document.section,
                    "heading": document.heading,
                    "parent_citation_path": target.parent_citation_path,
                    "source_id": document.source_id,
                    "references_to": list(document.references_to),
                    "annotations": list(document.annotations),
                },
            )
            record = _dc_section_provision(
                document,
                version=run_id,
                source_path=section_source_key,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                parent_citation_path=target.parent_citation_path,
                level=target.level,
                ordinal=target.ordinal,
            )
            items.append(item)
            records.append(record)
            section_count += 1
            if remaining is not None:
                remaining -= 1

    if not items:
        raise ValueError(f"no DC Code provisions extracted from {title_root}")

    inventory_path = store.inventory_path("us-dc", DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path("us-dc", DocumentClass.STATUTE, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction="us-dc",
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
    coverage_path = store.coverage_path("us-dc", DocumentClass.STATUTE, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return StateStatuteExtractReport(
        jurisdiction="us-dc",
        title_count=title_count,
        container_count=container_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def extract_cic_html_release(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    version: str,
    release_dir: str | Path,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
) -> StateStatuteExtractReport:
    """Snapshot a Public.Resource.org CIC HTML release and extract state provisions."""
    source_root = Path(release_dir)
    if not source_root.exists():
        raise ValueError(f"CIC HTML release directory does not exist: {source_root}")
    only_title_token = _clean_title_token(only_title) if only_title is not None else None
    run_id = (
        state_run_id(version, jurisdiction=jurisdiction, only_title=only_title_token, limit=limit)
        if only_title_token or limit is not None
        else version
    )
    source_as_of_text = source_as_of or _release_date_from_name(source_root.name) or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    errors: list[str] = []
    remaining = limit

    for html_path in _iter_cic_title_html_files(source_root, only_title_token):
        if remaining is not None and remaining <= 0:
            break
        title = _title_from_cic_filename(html_path)
        if title is None:
            skipped_source_count += 1
            continue
        html_bytes = html_path.read_bytes()
        relative = f"cic-html/{source_root.name}/{html_path.name}"
        artifact_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, relative)
        source_sha256 = store.write_bytes(artifact_path, html_bytes)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, relative)
        soup = BeautifulSoup(html_bytes.decode("utf-8", errors="replace"), "html.parser")
        title_count += 1

        try:
            containers, sections = _parse_cic_title_html(
                soup,
                jurisdiction=jurisdiction,
                title=title,
            )
        except ValueError as exc:
            errors.append(f"{html_path.name}: {exc}")
            continue

        for container in containers:
            if remaining is not None and remaining <= 0:
                break
            container = _replace_container_source(
                container,
                source_path=source_key,
                source_format=CIC_HTML_SOURCE_FORMAT,
                sha256=source_sha256,
                metadata_extra={"release": source_root.name, "file_name": html_path.name},
            )
            item = _container_inventory_item(container)
            record = _container_provision(
                container,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
            items.append(item)
            records.append(record)
            container_count += 1
            if remaining is not None:
                remaining -= 1
        if remaining is not None and remaining <= 0:
            break

        for section in sections:
            if remaining is not None and remaining <= 0:
                break
            item = SourceInventoryItem(
                citation_path=section.citation_path,
                source_url=None,
                source_path=source_key,
                source_format=CIC_HTML_SOURCE_FORMAT,
                sha256=source_sha256,
                metadata={
                    "kind": "section",
                    "title": section.title,
                    "section": section.section,
                    "heading": section.heading,
                    "parent_citation_path": section.parent_citation_path,
                    "source_id": section.source_id,
                    "references_to": list(section.references_to),
                    "release": source_root.name,
                    "file_name": html_path.name,
                },
            )
            record = _cic_section_provision(
                section,
                jurisdiction=jurisdiction,
                version=run_id,
                source_path=source_key,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
            items.append(item)
            records.append(record)
            section_count += 1
            if remaining is not None:
                remaining -= 1

    if not items:
        raise ValueError(f"no CIC HTML provisions extracted from {source_root}")

    inventory_path = store.inventory_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
    coverage_path = store.coverage_path(jurisdiction, DocumentClass.STATUTE, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return StateStatuteExtractReport(
        jurisdiction=jurisdiction,
        title_count=title_count,
        container_count=container_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        skipped_source_count=skipped_source_count,
        errors=tuple(errors),
    )


def _iter_dc_title_dirs(title_root: Path, only_title: str | None) -> Iterator[Path]:
    dirs = [
        path for path in title_root.iterdir() if path.is_dir() and (path / "index.xml").exists()
    ]
    for title_dir in sorted(dirs, key=lambda path: _title_sort_key(path.name)):
        title = _clean_title_token(title_dir.name)
        if only_title is None or title == only_title:
            yield title_dir


def _dc_index_items(
    root: ET.Element,
    *,
    title: str,
    source_path: str,
    source_sha256: str,
) -> tuple[tuple[_StateContainer, ...], tuple[_DcSectionTarget, ...]]:
    containers: list[_StateContainer] = []
    sections: list[_DcSectionTarget] = []
    title_path = f"us-dc/statute/{title}"
    root_heading = _direct_local_text(root, "heading") or f"Title {title}"
    root_num = _direct_local_text(root, "num") or title
    containers.append(
        _StateContainer(
            jurisdiction="us-dc",
            title=title,
            kind="title",
            num=root_num,
            heading=root_heading,
            citation_path=title_path,
            parent_citation_path=None,
            level=0,
            ordinal=_ordinal(root_num),
            source_path=source_path,
            source_url=_dc_title_url(title),
            source_id=root.get("id"),
            source_format=DC_XML_SOURCE_FORMAT,
            sha256=source_sha256,
            metadata={
                "title": title,
                "prefix": _direct_local_text(root, "prefix") or "Title",
                "enacted": root.get("enacted"),
            },
        )
    )
    _walk_dc_index_children(
        root,
        title=title,
        parent_path=title_path,
        level=1,
        containers=containers,
        sections=sections,
        source_path=source_path,
        source_sha256=source_sha256,
    )
    return tuple(containers), tuple(sections)


def _walk_dc_index_children(
    elem: ET.Element,
    *,
    title: str,
    parent_path: str,
    level: int,
    containers: list[_StateContainer],
    sections: list[_DcSectionTarget],
    source_path: str,
    source_sha256: str,
) -> None:
    child_container_index = 0
    section_index = 0
    for child in elem:
        name = _local_name(child.tag)
        if name == "container":
            prefix = _direct_local_text(child, "prefix") or "container"
            num = _direct_local_text(child, "num") or str(child_container_index + 1)
            kind = _clean_kind(prefix)
            citation_path = f"{parent_path}/{kind}-{_clean_path_token(num)}"
            heading = _direct_local_text(child, "heading")
            containers.append(
                _StateContainer(
                    jurisdiction="us-dc",
                    title=title,
                    kind=kind,
                    num=num,
                    heading=heading,
                    citation_path=citation_path,
                    parent_citation_path=parent_path,
                    level=level,
                    ordinal=_ordinal(num) or child_container_index,
                    source_path=source_path,
                    source_url=None,
                    source_id=child.get("id"),
                    source_format=DC_XML_SOURCE_FORMAT,
                    sha256=source_sha256,
                    metadata={
                        "title": title,
                        "prefix": prefix,
                        "num": num,
                        "enacted": child.get("enacted"),
                    },
                )
            )
            _walk_dc_index_children(
                child,
                title=title,
                parent_path=citation_path,
                level=level + 1,
                containers=containers,
                sections=sections,
                source_path=source_path,
                source_sha256=source_sha256,
            )
            child_container_index += 1
        elif name == "include":
            href = child.get("href") or ""
            section = _section_from_include_href(href)
            if section is None:
                continue
            sections.append(
                _DcSectionTarget(
                    section=section,
                    title=title,
                    parent_citation_path=parent_path,
                    level=level,
                    ordinal=_section_ordinal(section) or section_index,
                )
            )
            section_index += 1


def _parse_dc_section_xml(data: bytes) -> _DcSectionDocument:
    root = ET.fromstring(data)
    section = _direct_local_text(root, "num")
    if not section:
        raise ValueError("DC section XML has no num")
    title = _title_from_state_section(section)
    heading = _direct_local_text(root, "heading")
    body = _dc_section_body(root)
    references_to = _dc_references(root)
    annotations = _dc_annotations(root)
    return _DcSectionDocument(
        section=section,
        title=title,
        heading=heading,
        body=body,
        source_id=root.get("id") or root.get("identifier"),
        references_to=references_to,
        annotations=annotations,
    )


def _dc_section_body(root: ET.Element) -> str | None:
    lines: list[str] = []
    text = _direct_local_text(root, "text")
    if text:
        lines.append(text)
    for child in root:
        if _local_name(child.tag) == "para":
            para = _dc_para_text(child, indent=0)
            if para:
                lines.append(para)
    body = "\n".join(line for line in lines if line).strip()
    return body or None


def _dc_para_text(para: ET.Element, indent: int) -> str:
    prefix = "  " * indent
    num = _direct_local_text(para, "num")
    heading = _direct_local_text(para, "heading")
    text = _direct_local_text(para, "text")
    first_parts = [part for part in (num, heading, text) if part]
    lines = [prefix + " ".join(first_parts)] if first_parts else []
    for child in para:
        if _local_name(child.tag) == "para":
            child_text = _dc_para_text(child, indent + 1)
            if child_text:
                lines.append(child_text)
    return "\n".join(lines)


def _dc_references(root: ET.Element) -> tuple[str, ...]:
    refs: set[str] = set()
    for elem in root.iter():
        if _local_name(elem.tag) != "cite":
            continue
        ref = _dc_cite_to_citation_path(elem.get("path") or "")
        if ref:
            refs.add(ref)
    return tuple(sorted(refs))


def _dc_annotations(root: ET.Element) -> tuple[dict[str, str], ...]:
    annotations: list[dict[str, str]] = []
    for annotations_elem in root:
        if _local_name(annotations_elem.tag) != "annotations":
            continue
        for child in annotations_elem:
            if _local_name(child.tag) != "text":
                continue
            text = _element_text(child)
            if text:
                annotation: dict[str, str] = {"text": text}
                annotation_type = child.get("type")
                if annotation_type:
                    annotation["type"] = annotation_type
                annotations.append(annotation)
    return tuple(annotations)


def _dc_cite_to_citation_path(path: str) -> str | None:
    match = re.search(
        r"§\s*(?P<section>[0-9A-Za-z]+(?:[:~-][0-9A-Za-z]+)?-[0-9A-Za-z][0-9A-Za-z.]*[a-zA-Z]?)",
        path,
    )
    if not match:
        return None
    section = match.group("section")
    title = _title_from_state_section(section)
    return f"us-dc/statute/{title}/{section}"


def _dc_section_provision(
    document: _DcSectionDocument,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    parent_citation_path: str,
    level: int,
    ordinal: int | None,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(document.citation_path),
        jurisdiction="us-dc",
        document_class=DocumentClass.STATUTE.value,
        citation_path=document.citation_path,
        citation_label=f"D.C. Code § {document.section}",
        heading=document.heading,
        body=document.body,
        version=version,
        source_url=_dc_section_url(document.section),
        source_path=source_path,
        source_id=document.source_id,
        source_format=DC_XML_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=parent_citation_path,
        parent_id=deterministic_provision_id(parent_citation_path),
        level=level,
        ordinal=ordinal,
        kind="section",
        legal_identifier=f"D.C. Code § {document.section}",
        identifiers={"dc:section": document.section, "dc:title": document.title},
        metadata={
            "title": document.title,
            "section": document.section,
            "references_to": list(document.references_to),
            "annotations": list(document.annotations),
        },
    )


def _load_colorado_supplement_pdfs(
    store: CorpusArtifactStore,
    *,
    source_root: Path,
    run_id: str,
    only_title: str | None,
) -> tuple[dict[str, _ColoradoSupplementPdf], tuple[Path, ...]]:
    supplement_root = source_root / "supplement-pdfs"
    if not supplement_root.exists():
        supplement_root = source_root / "docx" / "01 Statute PDFs"
    if not supplement_root.exists():
        return {}, ()

    supplements: dict[str, _ColoradoSupplementPdf] = {}
    source_paths: list[Path] = []
    for pdf_path in sorted(supplement_root.rglob("*.pdf")):
        title = _title_from_colorado_supplement_path(pdf_path)
        if only_title is not None and title != only_title:
            continue
        data = pdf_path.read_bytes()
        relative_pdf_path = pdf_path.relative_to(supplement_root).as_posix()
        relative = f"colorado-crs-docx/{source_root.name}/supplement-pdfs/{relative_pdf_path}"
        artifact_path = store.source_path("us-co", DocumentClass.STATUTE, run_id, relative)
        sha256 = store.write_bytes(artifact_path, data)
        source_paths.append(artifact_path)
        source_key = _state_source_key("us-co", run_id, relative)
        file_name = pdf_path.name
        supplements[file_name] = _ColoradoSupplementPdf(
            file_name=file_name,
            source_path=source_key,
            sha256=sha256,
            text=_pdf_text(data),
        )
    return supplements, tuple(source_paths)


def _title_from_colorado_supplement_path(path: Path) -> str | None:
    for part in reversed(path.parts):
        match = re.fullmatch(r"Title\s+(?P<title>\d+(?:\.\d+)?)", part, flags=re.I)
        if match:
            return _clean_title_token(match.group("title"))
    match = re.match(r"(?P<title>\d+(?:\.\d+)?)-", path.name)
    if match:
        return _clean_title_token(match.group("title"))
    return None


def _pdf_text(data: bytes) -> str:
    import fitz  # type: ignore[import-untyped]

    with fitz.open(stream=data, filetype="pdf") as document:
        text = "\n".join(page.get_text("text", sort=True) for page in document)
    return _clean_multiline_text(text)


def _iter_colorado_title_docx_files(docx_root: Path, only_title: str | None) -> Iterator[Path]:
    candidates: list[Path] = []
    for path in docx_root.glob("crs*-title-*.docx"):
        title = _title_from_colorado_docx_filename(path)
        if title is None or title == "0":
            continue
        if only_title is not None and title != only_title:
            continue
        candidates.append(path)
    yield from sorted(
        candidates,
        key=lambda path: _title_sort_key(_title_from_colorado_docx_filename(path) or ""),
    )


def _title_from_colorado_docx_filename(path: Path) -> str | None:
    match = re.search(r"-title-(?P<title>\d+(?:\.\d+)?)\.docx$", path.name, flags=re.I)
    if not match:
        return None
    return _clean_title_token(match.group("title"))


def _docx_paragraphs(data: bytes) -> tuple[_DocxParagraph, ...]:
    with zipfile.ZipFile(BytesIO(data)) as archive:
        root = ET.fromstring(archive.read("word/document.xml"))
    paragraphs: list[_DocxParagraph] = []
    index = 0
    for elem in root.iter(f"{{{WORD_NS}}}p"):
        text = _docx_paragraph_text(elem)
        if not text:
            continue
        index += 1
        paragraphs.append(_DocxParagraph(text=text, source_id=f"docx-p-{index}"))
    if not paragraphs:
        raise ValueError("DOCX word/document.xml has no text paragraphs")
    return tuple(paragraphs)


def _docx_paragraph_text(elem: ET.Element) -> str:
    parts: list[str] = []
    for node in elem.iter():
        if node.tag == f"{{{WORD_NS}}}t" and node.text:
            parts.append(node.text)
        elif node.tag in {f"{{{WORD_NS}}}tab", f"{{{WORD_NS}}}br"}:
            parts.append(" ")
    return _clean_text("".join(parts))


def _parse_colorado_title_docx(
    paragraphs: tuple[_DocxParagraph, ...],
    *,
    title: str,
    supplements: dict[str, _ColoradoSupplementPdf],
) -> tuple[tuple[_StateContainer, ...], tuple[_ColoradoSection, ...]]:
    title_path = f"us-co/statute/{title}"
    title_container = _StateContainer(
        jurisdiction="us-co",
        title=title,
        kind="title",
        num=title,
        heading=_colorado_title_heading(paragraphs, title) or f"Title {title}",
        citation_path=title_path,
        parent_citation_path=None,
        level=0,
        ordinal=_ordinal(title),
        source_path="",
        source_url=None,
        source_id=None,
        source_format=COLORADO_DOCX_SOURCE_FORMAT,
        sha256="",
        metadata={"title": title},
    )
    containers: list[_StateContainer] = [title_container]
    sections: list[_ColoradoSection] = []
    current_by_kind: dict[str, _StateContainer] = {"title": title_container}
    seen_containers: set[str] = {title_path}
    seen_sections: set[str] = set()
    section_occurrences: dict[str, int] = {}
    current_section: _ColoradoSection | None = None
    current_body: list[str] = []
    current_supplement_files: list[str] = []
    current_supplement_paths: list[str] = []
    current_missing_supplements: list[str] = []

    def finish_section() -> None:
        nonlocal current_section, current_body
        nonlocal current_supplement_files, current_supplement_paths, current_missing_supplements
        if current_section is None:
            return
        body = "\n".join(current_body).strip() or None
        base_citation_path = current_section.base_citation_path
        occurrence = section_occurrences.get(base_citation_path, 0) + 1
        section_occurrences[base_citation_path] = occurrence
        variant = (
            _colorado_section_variant(current_section.heading, "\n".join(current_body), occurrence)
            if occurrence > 1
            else None
        )
        section = _ColoradoSection(
            title=current_section.title,
            section=current_section.section,
            variant=variant,
            heading=current_section.heading,
            body=body,
            source_id=current_section.source_id,
            parent_citation_path=current_section.parent_citation_path,
            level=current_section.level,
            ordinal=current_section.ordinal,
            references_to=current_section.references_to,
            supplement_pdf_files=tuple(dict.fromkeys(current_supplement_files)),
            supplement_source_paths=tuple(dict.fromkeys(current_supplement_paths)),
            missing_supplement_pdf_files=tuple(dict.fromkeys(current_missing_supplements)),
        )
        if section.citation_path in seen_sections:
            section = _replace_colorado_variant(section, f"{variant or 'version'}-{occurrence}")
        seen_sections.add(section.citation_path)
        sections.append(section)
        current_section = None
        current_body = []
        current_supplement_files = []
        current_supplement_paths = []
        current_missing_supplements = []

    index = 0
    while index < len(paragraphs):
        paragraph = paragraphs[index]
        text = paragraph.text
        container_parsed = _parse_colorado_container_heading(text)
        if container_parsed is not None:
            finish_section()
            prefix, kind, num = container_parsed
            label: str | None = None
            next_index = index + 1
            if next_index < len(paragraphs) and _is_colorado_container_label(
                paragraphs[next_index].text
            ):
                label = paragraphs[next_index].text
            parent = _colorado_container_parent(kind, current_by_kind)
            citation_path = f"{parent.citation_path}/{kind}-{_clean_path_token(num)}"
            container = _StateContainer(
                jurisdiction="us-co",
                title=title,
                kind=kind,
                num=num,
                heading=label or f"{prefix} {num}",
                citation_path=citation_path,
                parent_citation_path=parent.citation_path,
                level=parent.level + 1,
                ordinal=_ordinal(num),
                source_path="",
                source_url=None,
                source_id=paragraph.source_id,
                source_format=COLORADO_DOCX_SOURCE_FORMAT,
                sha256="",
                metadata={"title": title, "prefix": prefix, "num": num},
            )
            if citation_path not in seen_containers:
                containers.append(container)
                seen_containers.add(citation_path)
            _set_colorado_current_container(current_by_kind, container)
            index += 2 if label else 1
            continue

        section_parsed = _parse_colorado_section_heading(text, paragraph.source_id)
        if section_parsed is not None:
            finish_section()
            section, heading, first_body = section_parsed
            parent = _deepest_colorado_parent(current_by_kind)
            current_section = _ColoradoSection(
                title=title,
                section=section,
                variant=None,
                heading=heading,
                body=None,
                source_id=paragraph.source_id,
                parent_citation_path=parent.citation_path,
                level=parent.level + 1,
                ordinal=_section_ordinal(section),
                references_to=(),
                supplement_pdf_files=(),
                supplement_source_paths=(),
                missing_supplement_pdf_files=(),
            )
            current_body = []
            current_supplement_files = []
            current_supplement_paths = []
            current_missing_supplements = []
            if first_body:
                resolved = _replace_colorado_pdf_inserts(
                    first_body,
                    supplements,
                    context=first_body,
                )
                current_body.append(resolved.text)
                current_supplement_files.extend(resolved.files)
                current_supplement_paths.extend(resolved.source_paths)
                current_missing_supplements.extend(resolved.missing)
            index += 1
            continue

        if current_section is not None:
            context = "\n".join(
                part for part in (current_body[-1] if current_body else "", text) if part
            )
            resolved = _replace_colorado_pdf_inserts(
                text,
                supplements,
                context=context,
            )
            current_body.append(resolved.text)
            current_supplement_files.extend(resolved.files)
            current_supplement_paths.extend(resolved.source_paths)
            current_missing_supplements.extend(resolved.missing)
        index += 1

    finish_section()
    return tuple(containers), tuple(sections)


def _colorado_title_heading(paragraphs: tuple[_DocxParagraph, ...], title: str) -> str | None:
    title_pattern = re.compile(rf"^TITLE\s+{re.escape(title)}$", flags=re.I)
    for index, paragraph in enumerate(paragraphs):
        if not title_pattern.match(paragraph.text):
            continue
        heading_parts: list[str] = []
        for candidate in paragraphs[index + 1 :]:
            text = candidate.text
            if _is_colorado_preface_note(text) or _parse_colorado_container_heading(text):
                break
            if _parse_colorado_section_heading(text, None):
                break
            if not heading_parts:
                heading_parts.append(text)
                continue
            if _is_upper_heading_fragment(heading_parts[0]) and _is_upper_heading_fragment(text):
                heading_parts.append(text)
                continue
            break
        heading = _clean_text(" ".join(heading_parts))
        return heading or None
    return None


def _parse_colorado_container_heading(text: str) -> tuple[str, str, str] | None:
    match = re.fullmatch(
        r"(?P<prefix>ARTICLE|PART)\s+(?P<num>[0-9A-Z]+(?:\.[0-9A-Z]+)?)",
        text,
        flags=re.I,
    )
    if not match:
        return None
    prefix = match.group("prefix").title()
    return prefix, _clean_kind(prefix), match.group("num")


def _is_colorado_container_label(text: str) -> bool:
    if _is_colorado_preface_note(text):
        return False
    if _parse_colorado_container_heading(text) is not None:
        return False
    return _parse_colorado_section_heading(text, None) is None


def _is_colorado_preface_note(text: str) -> bool:
    lower = text.lower()
    return lower.startswith(
        (
            "editor's note:",
            "cross references:",
            "law reviews:",
            "am. jur.",
            "c.j.s.",
            "research references:",
        )
    )


def _is_upper_heading_fragment(text: str) -> bool:
    letters = [char for char in text if char.isalpha()]
    return bool(letters) and all(not char.islower() for char in letters)


def _colorado_container_parent(
    kind: str,
    current_by_kind: dict[str, _StateContainer],
) -> _StateContainer:
    if kind == "part":
        return current_by_kind.get("article") or current_by_kind["title"]
    return current_by_kind["title"]


def _deepest_colorado_parent(current_by_kind: dict[str, _StateContainer]) -> _StateContainer:
    return current_by_kind.get("part") or current_by_kind.get("article") or current_by_kind["title"]


def _set_colorado_current_container(
    current_by_kind: dict[str, _StateContainer],
    container: _StateContainer,
) -> None:
    current_by_kind[container.kind] = container
    if container.kind == "title":
        current_by_kind.pop("article", None)
        current_by_kind.pop("part", None)
    elif container.kind == "article":
        current_by_kind.pop("part", None)


def _parse_colorado_section_heading(
    text: str,
    source_id: str | None,
) -> tuple[str, str | None, str | None] | None:
    del source_id
    section_pattern = r"\d+(?:\.\d+)?-\d+(?:\.\d+)?-\d+(?:\.\d+)?[A-Za-z]?"
    match = re.match(
        rf"^(?P<section>{section_pattern})"
        r"(?!\.\d)"
        rf"(?:\s+to\s+{section_pattern})?"
        r"\.\s*(?P<rest>.*)$",
        text,
    )
    if not match:
        return None
    rest = _clean_text(match.group("rest"))
    if not rest:
        return match.group("section"), None, None
    split = re.search(r"\.\s+", rest)
    if split:
        heading = rest[: split.start() + 1].strip()
        body = rest[split.end() :].strip() or None
        return match.group("section"), heading or None, body
    return match.group("section"), rest, None


def _colorado_section_variant(heading: str | None, body: str | None, occurrence: int) -> str:
    text = " ".join(part for part in (heading, body) if part)
    note = re.search(r"\[Editor's note:\s*(?P<note>[^\]]+)\]", text, flags=re.I)
    if note:
        token = _clean_path_token(note.group("note"))[:120].strip("-.")
        if token:
            return token
    return f"version-{occurrence}"


def _replace_colorado_variant(section: _ColoradoSection, variant: str) -> _ColoradoSection:
    return _ColoradoSection(
        title=section.title,
        section=section.section,
        variant=variant,
        heading=section.heading,
        body=section.body,
        source_id=section.source_id,
        parent_citation_path=section.parent_citation_path,
        level=section.level,
        ordinal=section.ordinal,
        references_to=section.references_to,
        supplement_pdf_files=section.supplement_pdf_files,
        supplement_source_paths=section.supplement_source_paths,
        missing_supplement_pdf_files=section.missing_supplement_pdf_files,
    )


@dataclass(frozen=True)
class _ColoradoInsertResolution:
    text: str
    files: tuple[str, ...]
    source_paths: tuple[str, ...]
    missing: tuple[str, ...]


def _replace_colorado_pdf_inserts(
    text: str,
    supplements: dict[str, _ColoradoSupplementPdf],
    *,
    context: str,
) -> _ColoradoInsertResolution:
    files: list[str] = []
    source_paths: list[str] = []
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        file_name = match.group("file")
        supplement = _match_colorado_supplement(file_name, supplements, context)
        if supplement is None:
            missing.append(file_name)
            return match.group(0)
        files.append(supplement.file_name)
        source_paths.append(supplement.source_path)
        return supplement.text

    resolved = re.sub(r"\[Insert (?P<file>[^\]]+\.pdf) here\]", replace, text)
    return _ColoradoInsertResolution(
        text=_clean_multiline_text(resolved),
        files=tuple(files),
        source_paths=tuple(source_paths),
        missing=tuple(missing),
    )


def _match_colorado_supplement(
    file_name: str,
    supplements: dict[str, _ColoradoSupplementPdf],
    context: str,
) -> _ColoradoSupplementPdf | None:
    supplement = supplements.get(file_name)
    if supplement is not None:
        return supplement
    stem = file_name.removesuffix(".pdf")
    candidates = [
        candidate
        for name, candidate in supplements.items()
        if name.startswith(f"{stem} ") and name.endswith(".pdf")
    ]
    if len(candidates) == 1:
        return candidates[0]
    hint = _colorado_effective_pdf_hint(context)
    if hint:
        for candidate in candidates:
            if hint in candidate.file_name.lower():
                return candidate
    return None


def _colorado_effective_pdf_hint(context: str) -> str | None:
    match = re.search(
        r"effective\s+(?P<until>until\s+)?(?P<month>[A-Z][a-z]+)\s+\d{1,2},\s+"
        r"(?P<year>\d{4})",
        context,
    )
    if not match:
        return None
    until = "until " if match.group("until") else ""
    return f"effective {until}{match.group('month').lower()} {match.group('year')}"


def _colorado_section_metadata(
    section: _ColoradoSection,
    *,
    release: str,
    file_name: str,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": section.title,
        "section": section.section,
        "base_citation_path": section.base_citation_path,
        "heading": section.heading,
        "parent_citation_path": section.parent_citation_path,
        "source_id": section.source_id,
        "references_to": list(section.references_to),
        "release": release,
        "file_name": file_name,
    }
    if section.variant:
        metadata["variant"] = section.variant
    if section.supplement_pdf_files:
        metadata["supplement_pdf_files"] = list(section.supplement_pdf_files)
        metadata["supplement_source_paths"] = list(section.supplement_source_paths)
    if section.missing_supplement_pdf_files:
        metadata["missing_supplement_pdf_files"] = list(section.missing_supplement_pdf_files)
    return metadata


def _colorado_section_provision(
    section: _ColoradoSection,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    metadata = _colorado_section_metadata(section, release="", file_name="")
    metadata.pop("release")
    metadata.pop("file_name")
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-co",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        citation_label=(
            f"{section.section} ({section.variant})" if section.variant else section.section
        ),
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=None,
        source_path=source_path,
        source_id=section.source_id,
        source_format=COLORADO_DOCX_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.parent_citation_path,
        parent_id=deterministic_provision_id(section.parent_citation_path),
        level=section.level,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=section.section,
        identifiers={"state:title": section.title, "state:section": section.section},
        metadata=metadata,
    )


def _iter_cic_title_html_files(release_dir: Path, only_title: str | None) -> Iterator[Path]:
    candidates: list[Path] = []
    for path in release_dir.glob("*.title.*.html"):
        title = _title_from_cic_filename(path)
        if title is None:
            continue
        if only_title is not None and title != only_title:
            continue
        candidates.append(path)
    yield from sorted(
        candidates, key=lambda path: _title_sort_key(_title_from_cic_filename(path) or "")
    )


def _title_from_cic_filename(path: Path) -> str | None:
    match = re.search(r"\.title\.(?P<title>[0-9A-Za-z.]+)\.html$", path.name)
    if not match:
        return None
    return _clean_title_token(match.group("title"))


def _iter_cic_title_odt_files(
    release_dir: Path,
    jurisdiction: str,
    only_title: str | None,
) -> Iterator[Path]:
    candidates: list[Path] = []
    primary_prefix = PRIMARY_CIC_ODT_PREFIXES.get(jurisdiction)
    for path in release_dir.glob("*.title.*.odt"):
        if primary_prefix is not None and not path.name.startswith(f"{primary_prefix}.title."):
            continue
        title = _title_from_cic_odt_filename(path)
        if title is None:
            continue
        if only_title is not None and title != only_title:
            continue
        candidates.append(path)
    yield from sorted(
        candidates,
        key=lambda path: _title_sort_key(_title_from_cic_odt_filename(path) or ""),
    )


def _title_from_cic_odt_filename(path: Path) -> str | None:
    match = re.search(r"\.title\.(?P<title>[0-9A-Za-z.]+)\.odt$", path.name)
    if not match:
        return None
    return _clean_title_token(match.group("title"))


def _parse_cic_title_html(
    soup: BeautifulSoup,
    *,
    jurisdiction: str,
    title: str,
) -> tuple[tuple[_StateContainer, ...], tuple[_CicSection, ...]]:
    title_path = f"{jurisdiction}/statute/{title}"
    title_heading = _clean_text(_first_tag_text(soup, "h1")) or f"Title {title}"
    containers: list[_StateContainer] = [
        _StateContainer(
            jurisdiction=jurisdiction,
            title=title,
            kind="title",
            num=title,
            heading=title_heading,
            citation_path=title_path,
            parent_citation_path=None,
            level=0,
            ordinal=_ordinal(title),
            source_path="",
            source_url=None,
            source_id=None,
            source_format=CIC_HTML_SOURCE_FORMAT,
            sha256="",
            metadata={"title": title},
        )
    ]
    sections: list[_CicSection] = []
    current_by_kind: dict[str, _StateContainer] = {"title": containers[0]}
    seen_containers: set[str] = {title_path}
    seen_sections: set[str] = set()
    main = soup.find("main") or soup.find("body")
    if not isinstance(main, Tag):
        raise ValueError("HTML has no main/body content")

    for heading in main.find_all(["h2", "h3"]):
        if not isinstance(heading, Tag):
            continue
        if heading.name == "h2":
            container = _cic_container_from_heading(
                heading,
                jurisdiction=jurisdiction,
                title=title,
                current_by_kind=current_by_kind,
            )
            if container is None:
                continue
            if container.citation_path not in seen_containers:
                containers.append(container)
                seen_containers.add(container.citation_path)
            _set_current_container(current_by_kind, container)
        elif heading.name == "h3":
            section = _cic_section_from_heading(
                heading,
                jurisdiction=jurisdiction,
                title=title,
                current_by_kind=current_by_kind,
            )
            if section is None or section.citation_path in seen_sections:
                continue
            seen_sections.add(section.citation_path)
            sections.append(section)

    return tuple(containers), tuple(sections)


def _parse_cic_title_odt(
    paragraphs: tuple[_OdtParagraph, ...],
    *,
    jurisdiction: str,
    title: str,
) -> tuple[tuple[_StateContainer, ...], tuple[_CicSection, ...]]:
    title_path = f"{jurisdiction}/statute/{title}"
    title_heading = _odt_title_heading(paragraphs) or f"Title {title}"
    title_container = _StateContainer(
        jurisdiction=jurisdiction,
        title=title,
        kind="title",
        num=title,
        heading=title_heading,
        citation_path=title_path,
        parent_citation_path=None,
        level=0,
        ordinal=_ordinal(title),
        source_path="",
        source_url=None,
        source_id=None,
        source_format=CIC_ODT_SOURCE_FORMAT,
        sha256="",
        metadata={"title": title},
    )
    containers: list[_StateContainer] = [title_container]
    sections: list[_CicSection] = []
    current_by_kind: dict[str, _StateContainer] = {"title": title_container}
    seen_containers: set[str] = {title_path}
    seen_sections: set[str] = set()
    section_styles = _odt_section_heading_styles(paragraphs)
    container_styles = _odt_container_heading_styles(paragraphs, section_styles)
    current_section: _CicSection | None = None
    current_body: list[str] = []

    def finish_section() -> None:
        nonlocal current_section, current_body
        if current_section is None:
            return
        body = "\n".join(current_body).strip() or None
        section = _CicSection(
            title=current_section.title,
            section=current_section.section,
            heading=current_section.heading,
            body=body,
            source_id=current_section.source_id,
            parent_citation_path=current_section.parent_citation_path,
            level=current_section.level,
            ordinal=current_section.ordinal,
            references_to=current_section.references_to,
        )
        if section.citation_path not in seen_sections:
            seen_sections.add(section.citation_path)
            sections.append(section)
        current_section = None
        current_body = []

    for paragraph in paragraphs:
        if not paragraph.text:
            continue
        container_parsed = (
            _parse_cic_container_heading(paragraph.text)
            if paragraph.style in container_styles
            else None
        )
        if container_parsed is not None:
            finish_section()
            prefix, kind, num, label = container_parsed
            if kind == "title":
                continue
            parent = _cic_container_parent(kind, current_by_kind)
            citation_path = f"{parent.citation_path}/{kind}-{_clean_path_token(num)}"
            container = _StateContainer(
                jurisdiction=jurisdiction,
                title=title,
                kind=kind,
                num=num,
                heading=label or f"{prefix} {num}",
                citation_path=citation_path,
                parent_citation_path=parent.citation_path,
                level=parent.level + 1,
                ordinal=_ordinal(num),
                source_path="",
                source_url=None,
                source_id=paragraph.source_id,
                source_format=CIC_ODT_SOURCE_FORMAT,
                sha256="",
                metadata={"title": title, "prefix": prefix, "num": num},
            )
            if citation_path not in seen_containers:
                containers.append(container)
                seen_containers.add(citation_path)
            _set_current_container(current_by_kind, container)
            continue

        section_parsed = (
            _parse_cic_section_heading(paragraph.text, paragraph.source_id)
            if paragraph.style in section_styles
            else None
        )
        if section_parsed is not None:
            finish_section()
            section, label = section_parsed
            parent = _deepest_cic_parent(current_by_kind)
            current_section = _CicSection(
                title=title,
                section=section,
                heading=label,
                body=None,
                source_id=paragraph.source_id,
                parent_citation_path=parent.citation_path,
                level=parent.level + 1,
                ordinal=_section_ordinal(section),
                references_to=(),
            )
            current_body = []
            continue

        if current_section is not None and not _is_odt_section_label(paragraph.text):
            current_body.append(paragraph.text)

    finish_section()
    return tuple(containers), tuple(sections)


def _odt_paragraphs(data: bytes) -> tuple[_OdtParagraph, ...]:
    with zipfile.ZipFile(BytesIO(data)) as archive:
        root = ET.fromstring(archive.read("content.xml"))
    paragraphs: list[_OdtParagraph] = []
    index = 0
    for elem in root.iter():
        if _local_name(elem.tag) not in {"p", "h"}:
            continue
        text = _odt_element_text(elem)
        if not text:
            continue
        index += 1
        paragraphs.append(
            _OdtParagraph(
                style=elem.get(f"{{{ODT_TEXT_NS}}}style-name"),
                text=text,
                source_id=f"odt-p-{index}",
            )
        )
    if not paragraphs:
        raise ValueError("ODT content.xml has no text paragraphs")
    return tuple(paragraphs)


def _odt_element_text(elem: ET.Element) -> str:
    parts: list[str] = []

    def walk(node: ET.Element) -> None:
        if node.text:
            parts.append(node.text)
        for child in node:
            local_name = _local_name(child.tag)
            if local_name == "s":
                count = int(child.get(f"{{{ODT_TEXT_NS}}}c") or "1")
                parts.append(" " * count)
            elif local_name in {"tab", "line-break"}:
                parts.append(" ")
            else:
                walk(child)
            if child.tail:
                parts.append(child.tail)

    walk(elem)
    return _clean_text("".join(parts))


def _odt_title_heading(paragraphs: tuple[_OdtParagraph, ...]) -> str | None:
    for paragraph in paragraphs:
        if paragraph.style == "P1":
            return paragraph.text
    return paragraphs[0].text if paragraphs else None


def _odt_section_heading_styles(paragraphs: tuple[_OdtParagraph, ...]) -> set[str | None]:
    styles: dict[str | None, int] = {}
    for paragraph in paragraphs:
        if _parse_cic_section_heading(paragraph.text, None) is None:
            continue
        styles[paragraph.style] = styles.get(paragraph.style, 0) + 1
    non_toc_styles = {style for style in styles if style != "P2"}
    return non_toc_styles or set(styles)


def _odt_container_heading_styles(
    paragraphs: tuple[_OdtParagraph, ...],
    section_styles: set[str | None],
) -> set[str | None]:
    styles: dict[str | None, int] = {}
    first_section_index = next(
        (
            index
            for index, paragraph in enumerate(paragraphs)
            if paragraph.style in section_styles
            and _parse_cic_section_heading(paragraph.text, None) is not None
        ),
        None,
    )
    for index, paragraph in enumerate(paragraphs):
        if first_section_index is not None and index >= first_section_index:
            break
        if paragraph.style in section_styles:
            continue
        parsed = _parse_cic_container_heading(paragraph.text)
        if parsed is None or parsed[1] == "title":
            continue
        styles[paragraph.style] = styles.get(paragraph.style, 0) + 1
    non_toc_styles = {style for style in styles if style not in {"P1", "P2"}}
    if non_toc_styles:
        return non_toc_styles

    for paragraph in paragraphs:
        if paragraph.style in section_styles:
            continue
        parsed = _parse_cic_container_heading(paragraph.text)
        if parsed is None or parsed[1] == "title":
            continue
        styles[paragraph.style] = styles.get(paragraph.style, 0) + 1
    return {style for style in styles if style not in {"P1", "P2"}} or set(styles)


def _is_odt_section_label(text: str) -> bool:
    return text.lower() in {"text", "history", "annotations", "analysis"}


def _cic_container_from_heading(
    heading: Tag,
    *,
    jurisdiction: str,
    title: str,
    current_by_kind: dict[str, _StateContainer],
) -> _StateContainer | None:
    text = _clean_text(heading.get_text(" ", strip=True))
    parsed = _parse_cic_container_heading(text)
    if parsed is None:
        return None
    prefix, kind, num, label = parsed
    parent = _cic_container_parent(kind, current_by_kind)
    citation_path = f"{parent.citation_path}/{kind}-{_clean_path_token(num)}"
    return _StateContainer(
        jurisdiction=jurisdiction,
        title=title,
        kind=kind,
        num=num,
        heading=label or f"{prefix} {num}",
        citation_path=citation_path,
        parent_citation_path=parent.citation_path,
        level=parent.level + 1,
        ordinal=_ordinal(num),
        source_path="",
        source_url=None,
        source_id=_tag_id(heading),
        source_format=CIC_HTML_SOURCE_FORMAT,
        sha256="",
        metadata={"title": title, "prefix": prefix, "num": num},
    )


def _parse_cic_container_heading(text: str) -> tuple[str, str, str, str | None] | None:
    match = re.match(
        r"(?P<prefix>Title|Chapter|Part|Article|Subtitle|Subchapter|Subpart|Division)"
        r"\s+"
        r"(?P<num>[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*\.?)"
        r"\s*(?P<heading>.*)$",
        text,
        flags=re.I,
    )
    if not match:
        return None
    prefix = match.group("prefix").title()
    kind = _clean_kind(prefix)
    num = match.group("num").rstrip(".")
    label = _clean_text(match.group("heading")) or None
    return prefix, kind, num, label


def _cic_container_parent(
    kind: str,
    current_by_kind: dict[str, _StateContainer],
) -> _StateContainer:
    parent_order = {
        "chapter": ("title",),
        "part": ("chapter", "title"),
        "article": ("part", "chapter", "title"),
        "subchapter": ("chapter", "title"),
        "subpart": ("part", "article", "chapter", "title"),
        "subtitle": ("title",),
        "division": ("subtitle", "title"),
    }
    for parent_kind in parent_order.get(kind, ("title",)):
        parent = current_by_kind.get(parent_kind)
        if parent is not None:
            return parent
    return current_by_kind["title"]


def _set_current_container(
    current_by_kind: dict[str, _StateContainer],
    container: _StateContainer,
) -> None:
    current_by_kind[container.kind] = container
    if container.kind == "title":
        for kind in ("chapter", "subchapter", "part", "article", "subpart", "subtitle", "division"):
            current_by_kind.pop(kind, None)
    elif container.kind == "chapter":
        for kind in ("subchapter", "part", "article", "subpart"):
            current_by_kind.pop(kind, None)
    elif container.kind in {"part", "subchapter"}:
        for kind in ("article", "subpart"):
            current_by_kind.pop(kind, None)
    elif container.kind == "article":
        current_by_kind.pop("subpart", None)


def _cic_section_from_heading(
    heading: Tag,
    *,
    jurisdiction: str,
    title: str,
    current_by_kind: dict[str, _StateContainer],
) -> _CicSection | None:
    heading_text = _clean_text(heading.get_text(" ", strip=True))
    parsed = _parse_cic_section_heading(heading_text, _tag_id(heading))
    if parsed is None:
        return None
    section, label = parsed
    parent = _deepest_cic_parent(current_by_kind)
    body = _section_body_from_heading(heading)
    references = _cic_references(heading)
    return _CicSection(
        title=title,
        section=section,
        heading=label,
        body=body,
        source_id=_tag_id(heading),
        parent_citation_path=parent.citation_path,
        level=parent.level + 1,
        ordinal=_section_ordinal(section),
        references_to=references,
    )


def _parse_cic_section_heading(text: str, element_id: str | None) -> tuple[str, str | None] | None:
    hyphen_section = (
        r"\d+[A-Za-z]*(?:\.\d+[A-Za-z]*)?"
        r"(?:-[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)?)+"
    )
    dotted_section = r"\d+[A-Za-z]*\.\d+[A-Za-z]*"
    patterns = (
        rf"^(?:§{{1,2}}\s*)?(?P<section>{hyphen_section})"
        rf"(?:\s+through\s+{hyphen_section})?\.?\s*(?P<label>.*)$",
        rf"^(?P<section>{dotted_section})\.?\s*(?P<label>.*)$",
    )
    for pattern in patterns:
        match = re.match(pattern, text)
        if match:
            return match.group("section"), _clean_text(match.group("label")) or None
    if element_id:
        id_match = re.search(
            r"s(?P<section>\d+[A-Za-z]*(?:[.-][0-9A-Za-z]+)+(?:[a-zA-Z])?)$",
            element_id,
        )
        if id_match:
            return id_match.group("section"), text or None
    return None


def _deepest_cic_parent(current_by_kind: dict[str, _StateContainer]) -> _StateContainer:
    for kind in (
        "subpart",
        "article",
        "part",
        "subchapter",
        "chapter",
        "division",
        "subtitle",
        "title",
    ):
        parent = current_by_kind.get(kind)
        if parent is not None:
            return parent
    return current_by_kind["title"]


def _section_body_from_heading(heading: Tag) -> str | None:
    section_div = heading.find_parent("div")
    if not isinstance(section_div, Tag):
        section_div = heading
    lines: list[str] = []
    for child in section_div.children:
        if not isinstance(child, Tag):
            continue
        if child is heading or child.name in {"h3", "nav", "script", "style"}:
            continue
        if child.name == "div" and child.find("h3"):
            continue
        text = _clean_text(child.get_text(" ", strip=True))
        if text:
            lines.append(text)
    body = "\n\n".join(lines).strip()
    return body or None


def _cic_references(heading: Tag) -> tuple[str, ...]:
    refs: set[str] = set()
    section_div = heading.find_parent("div")
    if not isinstance(section_div, Tag):
        return ()
    for link in section_div.find_all("a", href=True):
        href = str(link.get("href", ""))
        match = re.search(r"#.*s(?P<section>[0-9A-Za-z]+(?:[-.][0-9A-Za-z]+)+(?:[a-zA-Z])?)", href)
        if not match:
            continue
        section = match.group("section")
        title = _title_from_state_section(section)
        jurisdiction = _cic_jurisdiction_from_href(href)
        if jurisdiction:
            refs.add(f"{jurisdiction}/statute/{title}/{section}")
    return tuple(sorted(refs))


def _cic_jurisdiction_from_href(href: str) -> str | None:
    match = re.search(r"gov\.([a-z]{2})\.", href)
    if match:
        return f"us-{match.group(1)}"
    return None


def _cic_section_provision(
    section: _CicSection,
    *,
    jurisdiction: str,
    version: str,
    source_path: str,
    source_format: str = CIC_HTML_SOURCE_FORMAT,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    citation_path = f"{jurisdiction}/statute/{section.title}/{section.section}"
    return ProvisionRecord(
        id=deterministic_provision_id(citation_path),
        jurisdiction=jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        citation_path=citation_path,
        citation_label=f"{section.section}",
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=None,
        source_path=source_path,
        source_id=section.source_id,
        source_format=source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.parent_citation_path,
        parent_id=deterministic_provision_id(section.parent_citation_path),
        level=section.level,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=section.section,
        identifiers={"state:title": section.title, "state:section": section.section},
        metadata={
            "title": section.title,
            "section": section.section,
            "references_to": list(section.references_to),
        },
    )


def _replace_container_source(
    container: _StateContainer,
    *,
    source_path: str,
    source_format: str,
    sha256: str,
    metadata_extra: dict[str, Any],
) -> _StateContainer:
    metadata = dict(container.metadata)
    metadata.update(metadata_extra)
    return _StateContainer(
        jurisdiction=container.jurisdiction,
        title=container.title,
        kind=container.kind,
        num=container.num,
        heading=container.heading,
        citation_path=container.citation_path,
        parent_citation_path=container.parent_citation_path,
        level=container.level,
        ordinal=container.ordinal,
        source_path=source_path,
        source_url=container.source_url,
        source_id=container.source_id,
        source_format=source_format,
        sha256=sha256,
        metadata=metadata,
    )


def _container_inventory_item(container: _StateContainer) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=container.citation_path,
        source_url=container.source_url,
        source_path=container.source_path,
        source_format=container.source_format,
        sha256=container.sha256,
        metadata={
            **container.metadata,
            "kind": container.kind,
            "heading": container.heading,
            "parent_citation_path": container.parent_citation_path,
            "source_id": container.source_id,
        },
    )


def _container_provision(
    container: _StateContainer,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    legal_identifier = _container_legal_identifier(container)
    return ProvisionRecord(
        id=deterministic_provision_id(container.citation_path),
        jurisdiction=container.jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        citation_path=container.citation_path,
        citation_label=legal_identifier,
        heading=container.heading,
        body=None,
        version=version,
        source_url=container.source_url,
        source_path=container.source_path,
        source_id=container.source_id,
        source_format=container.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=container.parent_citation_path,
        parent_id=(
            deterministic_provision_id(container.parent_citation_path)
            if container.parent_citation_path
            else None
        ),
        level=container.level,
        ordinal=container.ordinal,
        kind=container.kind,
        legal_identifier=legal_identifier,
        identifiers={
            "state:title": container.title,
            f"state:{container.kind}": container.num,
        },
        metadata=container.metadata,
    )


def _container_legal_identifier(container: _StateContainer) -> str:
    label = "D.C. Code" if container.jurisdiction == "us-dc" else container.jurisdiction.upper()
    if container.kind == "title":
        return f"{label} title {container.num}"
    return f"{label} {container.kind} {container.num}"


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _release_date_from_name(name: str) -> str | None:
    match = re.search(r"release\d+\.(?P<date>\d{4}\.\d{2}(?:\.\d{2})?)", name)
    if not match:
        return None
    parts = match.group("date").split(".")
    if len(parts) == 2:
        parts.append("01")
    return "-".join(parts)


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _direct_local_child(elem: ET.Element, name: str) -> ET.Element | None:
    for child in elem:
        if _local_name(child.tag) == name:
            return child
    return None


def _direct_local_text(elem: ET.Element, name: str) -> str | None:
    child = _direct_local_child(elem, name)
    if child is None:
        return None
    text = _element_text(child)
    return text or None


def _element_text(elem: ET.Element) -> str:
    return _clean_text(" ".join(elem.itertext()))


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _clean_multiline_text(value: str | None) -> str:
    lines = [_clean_text(line) for line in (value or "").splitlines()]
    return "\n".join(line for line in lines if line).strip()


def _clean_kind(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "container"


def _clean_title_token(value: str) -> str:
    text = value.strip()
    text = re.sub(r"^0+(\d)", r"\1", text)
    if not re.fullmatch(r"[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)?", text):
        raise ValueError(f"invalid state title token: {value!r}")
    return text


def _title_from_state_section(section: str) -> str:
    match = re.match(r"(?P<title>[0-9A-Za-z]+)", section)
    if not match:
        raise ValueError(f"cannot infer title from state section: {section!r}")
    return _clean_title_token(match.group("title"))


def _clean_path_token(value: str) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"^0+(\d)", r"\1", text)
    text = re.sub(r"[^a-z0-9.-]+", "-", text).strip("-")
    return text or "0"


def _title_sort_key(title: str) -> tuple[int, str]:
    match = re.fullmatch(r"(?P<number>\d+)(?:\.(?P<decimal>\d+))?(?P<suffix>[A-Za-z]?)", title)
    if match:
        decimal = match.group("decimal")
        decimal_part = f".{int(decimal):04d}" if decimal is not None else ""
        return (int(match.group("number")), f"{decimal_part}{match.group('suffix')}")
    return (10_000, title)


def _ordinal(value: str | None) -> int | None:
    if not value:
        return None
    match = re.match(r"\d+", value)
    if not match:
        return None
    suffix = value[match.end() :]
    return int(match.group(0)) * 100 + (ord(suffix[0].upper()) if suffix else 0)


def _section_ordinal(section: str) -> int | None:
    numbers = [int(part) for part in re.findall(r"\d+", section)]
    if not numbers:
        return None
    ordinal = 0
    for number in numbers[:3]:
        ordinal = ordinal * 1_000 + min(number, 999)
    return ordinal


def _section_from_include_href(href: str) -> str | None:
    match = re.search(r"(?:^|/)sections/(?P<section>[^/]+)\.xml$", href)
    if not match:
        return None
    return match.group("section")


def _dc_title_url(title: str) -> str:
    return f"{DC_CODE_WEB_BASE}/titles/{title}"


def _dc_section_url(section: str) -> str:
    return f"{DC_CODE_WEB_BASE}/sections/{section}"


def _first_tag_text(soup: BeautifulSoup, name: str) -> str | None:
    tag = soup.find(name)
    if not isinstance(tag, Tag):
        return None
    return tag.get_text(" ", strip=True)


def _tag_id(tag: Tag) -> str | None:
    value = tag.get("id")
    return str(value) if value is not None else None
