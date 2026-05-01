"""State statute source adapters for source-first corpus ingestion."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import time
import zipfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from io import BytesIO, TextIOWrapper
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import parse_qs, quote, unquote_plus, urljoin, urlparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore, sha256_bytes
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

DC_CODE_WEB_BASE = "https://code.dccouncil.gov/us/dc/council/code"
DC_CODE_REPO_BASE = "https://github.com/dccouncil/law-xml-codified"
DC_XML_SOURCE_FORMAT = "dc-law-xml"
CIC_HTML_SOURCE_FORMAT = "cic-state-code-html"
CIC_ODT_SOURCE_FORMAT = "cic-state-code-odt"
COLORADO_DOCX_SOURCE_FORMAT = "colorado-crs-docx"
OHIO_REVISED_CODE_BASE_URL = "https://codes.ohio.gov"
OHIO_REVISED_CODE_SOURCE_FORMAT = "ohio-revised-code-html"
OHIO_USER_AGENT = "axiom-corpus/0.1"
MINNESOTA_STATUTES_BASE_URL = "https://www.revisor.mn.gov"
MINNESOTA_STATUTES_SOURCE_FORMAT = "minnesota-statutes-html"
MINNESOTA_USER_AGENT = "axiom-corpus/0.1"
CALIFORNIA_LEGINFO_BULK_URL = "https://downloads.leginfo.legislature.ca.gov/pubinfo_2025.zip"
CALIFORNIA_LEGINFO_BASE_URL = "https://leginfo.legislature.ca.gov"
CALIFORNIA_BULK_SOURCE_FORMAT = "california-leginfo-bulk"
TEXAS_STATUTES_BASE_URL = "https://statutes.capitol.texas.gov"
TEXAS_TCAS_API_BASE = "https://tcss.legis.texas.gov/api"
TEXAS_TCAS_RESOURCE_BASE = "https://tcss.legis.texas.gov/resources"
TEXAS_TCAS_TREE_SOURCE_FORMAT = "texas-tcas-json"
TEXAS_TCAS_HTML_SOURCE_FORMAT = "texas-tcas-html"
TEXAS_USER_AGENT = "axiom-corpus/0.1"
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
class _OhioTitle:
    token: str
    num: str
    heading: str
    href: str

    @property
    def source_url(self) -> str:
        return urljoin(OHIO_REVISED_CODE_BASE_URL, self.href)

    @property
    def citation_path(self) -> str:
        if self.token == "general-provisions":
            return "us-oh/statute/general-provisions"
        return f"us-oh/statute/title-{self.num}"


@dataclass(frozen=True)
class _OhioChapter:
    title: _OhioTitle
    num: str
    heading: str | None
    href: str

    @property
    def source_url(self) -> str:
        return urljoin(OHIO_REVISED_CODE_BASE_URL, self.href)

    @property
    def citation_path(self) -> str:
        return f"us-oh/statute/chapter-{self.num}"


@dataclass(frozen=True)
class _OhioSection:
    chapter: _OhioChapter
    section: str
    heading: str | None
    body: str | None
    source_url: str
    source_id: str | None
    effective_date: str | None
    latest_legislation: str | None
    pdf_url: str | None
    last_updated: str | None
    references_to: tuple[str, ...]

    @property
    def citation_path(self) -> str:
        return f"us-oh/statute/{self.section}"


@dataclass(frozen=True)
class _MinnesotaPart:
    token: str
    heading: str
    href: str
    ordinal: int

    @property
    def source_url(self) -> str:
        return urljoin(MINNESOTA_STATUTES_BASE_URL, self.href)

    @property
    def citation_path(self) -> str:
        return f"us-mn/statute/part-{_clean_path_token(self.token)}"


@dataclass(frozen=True)
class _MinnesotaChapter:
    part: _MinnesotaPart
    num: str
    heading: str | None
    href: str
    ordinal: int

    @property
    def source_url(self) -> str:
        return urljoin(MINNESOTA_STATUTES_BASE_URL, self.href)

    @property
    def full_source_url(self) -> str:
        return f"{self.source_url.rstrip('/')}/full"

    @property
    def citation_path(self) -> str:
        return f"us-mn/statute/{self.num}"


@dataclass(frozen=True)
class _MinnesotaSection:
    chapter: _MinnesotaChapter
    section: str
    heading: str | None
    body: str | None
    source_id: str | None
    status: str
    references_to: tuple[str, ...]

    @property
    def source_url(self) -> str:
        return f"{MINNESOTA_STATUTES_BASE_URL}/statutes/cite/{self.section}"

    @property
    def citation_path(self) -> str:
        return f"us-mn/statute/{self.section}"


@dataclass(frozen=True)
class _CaliforniaCode:
    code: str
    title: str

    @property
    def token(self) -> str:
        return self.code.lower()

    @property
    def citation_path(self) -> str:
        return f"us-ca/statute/{self.token}"


@dataclass(frozen=True)
class _CaliforniaTocTarget:
    law_code: str
    node_treepath: str
    section_num: str
    section_order: int | None
    title: str | None
    law_section_version_id: str | None
    seq_num: int | None
    parent_citation_path: str
    level: int


@dataclass(frozen=True)
class _CaliforniaSection:
    law_code: str
    section: str
    heading: str | None
    body: str | None
    source_id: str | None
    source_url: str
    parent_citation_path: str
    level: int
    ordinal: int | None
    references_to: tuple[str, ...]
    effective_date: str | None
    law_section_version_id: str | None
    active_flg: str | None
    history: str | None
    op_statues: str | None
    op_chapter: str | None
    op_section: str | None
    division: str | None
    title: str | None
    part: str | None
    chapter: str | None
    article: str | None
    content_file: str | None
    content_sha256: str

    @property
    def code_token(self) -> str:
        return self.law_code.lower()

    @property
    def citation_path(self) -> str:
        return f"us-ca/statute/{self.code_token}/{_california_section_token(self.section)}"


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


@dataclass(frozen=True)
class _TexasCode:
    code_id: str
    code: str
    name: str

    @property
    def token(self) -> str:
        return self.code.lower()


@dataclass(frozen=True)
class _TexasHtmlDocument:
    code: str
    resource_key: str
    htm_link: str
    parent_citation_path: str
    level: int

    @property
    def source_url(self) -> str:
        return _texas_resource_url(self.resource_key)

    @property
    def source_file_name(self) -> str:
        return self.resource_key.rsplit("/", 1)[-1]


@dataclass(frozen=True)
class _TexasSection:
    code: str
    section: str
    variant: str | None
    marker: str
    heading: str | None
    body: str | None
    source_id: str | None
    source_url: str
    source_document_id: str
    parent_citation_path: str
    level: int
    ordinal: int | None
    references_to: tuple[str, ...]
    anchors: tuple[str, ...]

    @property
    def citation_path(self) -> str:
        suffix = f"@{self.variant}" if self.variant else ""
        return f"us-tx/statute/{_texas_code_token(self.code)}/{self.section}{suffix}"


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


def extract_ohio_revised_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
) -> StateStatuteExtractReport:
    """Snapshot official Ohio Revised Code HTML and extract provisions."""
    jurisdiction = "us-oh"
    only_title_token = _ohio_title_filter(only_title)
    run_id = (
        state_run_id(version, jurisdiction=jurisdiction, only_title=only_title_token, limit=limit)
        if only_title_token or limit is not None
        else version
    )
    source_root = Path(source_dir) if source_dir is not None else None
    download_root = Path(download_dir) if download_dir is not None and source_root is None else None
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    session = _ohio_session()

    index_relative = "ohio-revised-code/index.html"
    index_bytes = _load_ohio_html(
        session,
        source_root,
        download_root,
        relative_name=index_relative,
        url=f"{OHIO_REVISED_CODE_BASE_URL}/ohio-revised-code",
    )
    index_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        index_relative,
    )
    index_sha = store.write_bytes(index_path, index_bytes)
    del index_sha
    source_paths: list[Path] = [index_path]
    titles = _parse_ohio_titles(index_bytes)
    if only_title_token is not None:
        titles = tuple(title for title in titles if title.token == only_title_token)
    if not titles:
        raise ValueError(f"no Ohio Revised Code titles selected for filter: {only_title!r}")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    remaining = limit
    title_count = 0
    container_count = 0
    section_count = 0
    seen_citation_paths: set[str] = set()

    for title in titles:
        if remaining is not None and remaining <= 0:
            break
        title_relative = f"ohio-revised-code/titles/{_ohio_title_file_token(title)}.html"
        title_bytes = _load_ohio_html(
            session,
            source_root,
            download_root,
            relative_name=title_relative,
            url=title.source_url,
        )
        title_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            title_relative,
        )
        title_sha = store.write_bytes(title_path, title_bytes)
        source_paths.append(title_path)
        title_source_key = _state_source_key(jurisdiction, run_id, title_relative)

        title_container = _ohio_title_container(
            title,
            source_path=title_source_key,
            sha256=title_sha,
        )
        if title_container.citation_path not in seen_citation_paths:
            seen_citation_paths.add(title_container.citation_path)
            items.append(_container_inventory_item(title_container))
            records.append(
                _container_provision(
                    title_container,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
            )
            title_count += 1
            container_count += 1
            if remaining is not None:
                remaining -= 1

        chapters = _parse_ohio_chapters(title_bytes, title=title)
        for chapter in chapters:
            if remaining is not None and remaining <= 0:
                break
            chapter_relative = f"ohio-revised-code/chapters/chapter-{chapter.num}.html"
            try:
                chapter_bytes = _load_ohio_html(
                    session,
                    source_root,
                    download_root,
                    relative_name=chapter_relative,
                    url=chapter.source_url,
                )
            except requests.RequestException as exc:
                errors.append(f"chapter {chapter.num}: {exc}")
                continue
            chapter_path = store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                chapter_relative,
            )
            chapter_sha = store.write_bytes(chapter_path, chapter_bytes)
            source_paths.append(chapter_path)
            chapter_source_key = _state_source_key(jurisdiction, run_id, chapter_relative)
            chapter_container = _ohio_chapter_container(
                chapter,
                source_path=chapter_source_key,
                sha256=chapter_sha,
            )
            if chapter_container.citation_path not in seen_citation_paths:
                seen_citation_paths.add(chapter_container.citation_path)
                items.append(_container_inventory_item(chapter_container))
                records.append(
                    _container_provision(
                        chapter_container,
                        version=run_id,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                    )
                )
                container_count += 1
                if remaining is not None:
                    remaining -= 1

            for section in _parse_ohio_sections(chapter_bytes, chapter=chapter):
                if remaining is not None and remaining <= 0:
                    break
                if section.citation_path in seen_citation_paths:
                    continue
                seen_citation_paths.add(section.citation_path)
                item = SourceInventoryItem(
                    citation_path=section.citation_path,
                    source_url=section.source_url,
                    source_path=chapter_source_key,
                    source_format=OHIO_REVISED_CODE_SOURCE_FORMAT,
                    sha256=chapter_sha,
                    metadata={
                        "kind": "section",
                        "title": chapter.title.num,
                        "chapter": chapter.num,
                        "section": section.section,
                        "heading": section.heading,
                        "effective_date": section.effective_date,
                        "latest_legislation": section.latest_legislation,
                        "pdf_url": section.pdf_url,
                        "last_updated": section.last_updated,
                        "references_to": list(section.references_to),
                        "source_id": section.source_id,
                        "parent_citation_path": chapter.citation_path,
                    },
                )
                records.append(
                    _ohio_section_provision(
                        section,
                        version=run_id,
                        source_path=chapter_source_key,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                    )
                )
                items.append(item)
                section_count += 1
                if remaining is not None:
                    remaining -= 1

    if not items:
        raise ValueError("no Ohio Revised Code provisions extracted")

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
        errors=tuple(errors),
    )


def extract_minnesota_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
    workers: int = 4,
    download_dir: str | Path | None = None,
) -> StateStatuteExtractReport:
    """Snapshot official Minnesota Statutes HTML and extract provisions."""
    jurisdiction = "us-mn"
    only_chapter = _minnesota_chapter_filter(only_title)
    run_id = (
        state_run_id(version, jurisdiction=jurisdiction, only_title=only_chapter, limit=limit)
        if only_chapter or limit is not None
        else version
    )
    source_root = Path(source_dir) if source_dir is not None else None
    download_root = Path(download_dir) if download_dir is not None and source_root is None else None
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    session = _minnesota_session()

    index_relative = "minnesota-statutes-html/index.html"
    index_bytes = _load_minnesota_html(
        session,
        source_root,
        download_root,
        relative_name=index_relative,
        url=f"{MINNESOTA_STATUTES_BASE_URL}/statutes/",
    )
    index_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, index_relative)
    store.write_bytes(index_path, index_bytes)
    source_paths: list[Path] = [index_path]

    parts = _parse_minnesota_parts(index_bytes)
    if not parts:
        raise ValueError("no Minnesota Statutes parts found")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    remaining = limit
    title_count = 0
    container_count = 0
    section_count = 0
    seen_citation_paths: set[str] = set()
    selected_parts: dict[str, _MinnesotaPart] = {}
    chapters: list[_MinnesotaChapter] = []
    part_sources: dict[str, tuple[str, str]] = {}

    for part in parts:
        part_relative = f"minnesota-statutes-html/parts/{_clean_path_token(part.token)}.html"
        try:
            part_bytes = _load_minnesota_html(
                session,
                source_root,
                download_root,
                relative_name=part_relative,
                url=part.source_url,
            )
        except requests.RequestException as exc:
            errors.append(f"part {part.heading}: {exc}")
            continue
        part_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            part_relative,
        )
        part_sha = store.write_bytes(part_path, part_bytes)
        source_paths.append(part_path)
        part_source_key = _state_source_key(jurisdiction, run_id, part_relative)
        part_chapters = _parse_minnesota_chapters(part_bytes, part=part)
        if only_chapter is not None:
            part_chapters = tuple(chapter for chapter in part_chapters if chapter.num == only_chapter)
        if not part_chapters:
            continue
        selected_parts[part.citation_path] = part
        part_sources[part.citation_path] = (part_source_key, part_sha)
        for chapter in part_chapters:
            if chapter.citation_path not in seen_citation_paths:
                chapters.append(chapter)
                seen_citation_paths.add(chapter.citation_path)

    seen_citation_paths.clear()
    if not chapters:
        raise ValueError(f"no Minnesota Statutes chapters selected for filter: {only_title!r}")

    for part in selected_parts.values():
        if remaining is not None and remaining <= 0:
            break
        source_key, source_sha = part_sources[part.citation_path]
        part_container = _minnesota_part_container(part, source_path=source_key, sha256=source_sha)
        seen_citation_paths.add(part_container.citation_path)
        items.append(_container_inventory_item(part_container))
        records.append(
            _container_provision(
                part_container,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
            )
        )
        title_count += 1
        container_count += 1
        if remaining is not None:
            remaining -= 1

    if remaining is None or remaining > 0:
        for chapter, chapter_bytes, error in _iter_minnesota_chapter_sources(
            source_root,
            download_root,
            tuple(chapters),
            workers=workers,
        ):
            if remaining is not None and remaining <= 0:
                break
            if error is not None:
                errors.append(f"chapter {chapter.num}: {error}")
                continue
            if chapter_bytes is None:
                continue
            chapter_relative = _minnesota_chapter_relative(chapter)
            chapter_path = store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                chapter_relative,
            )
            chapter_sha = store.write_bytes(chapter_path, chapter_bytes)
            source_paths.append(chapter_path)
            chapter_source_key = _state_source_key(jurisdiction, run_id, chapter_relative)
            parsed_heading, sections = _parse_minnesota_chapter_sections(
                chapter_bytes,
                chapter=chapter,
            )
            chapter_with_heading = _MinnesotaChapter(
                part=chapter.part,
                num=chapter.num,
                heading=parsed_heading or chapter.heading,
                href=chapter.href,
                ordinal=chapter.ordinal,
            )
            chapter_container = _minnesota_chapter_container(
                chapter_with_heading,
                source_path=chapter_source_key,
                sha256=chapter_sha,
            )
            if chapter_container.citation_path not in seen_citation_paths:
                seen_citation_paths.add(chapter_container.citation_path)
                items.append(_container_inventory_item(chapter_container))
                records.append(
                    _container_provision(
                        chapter_container,
                        version=run_id,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                    )
                )
                container_count += 1
                if remaining is not None:
                    remaining -= 1

            for section in sections:
                if remaining is not None and remaining <= 0:
                    break
                if section.citation_path in seen_citation_paths:
                    continue
                seen_citation_paths.add(section.citation_path)
                items.append(
                    SourceInventoryItem(
                        citation_path=section.citation_path,
                        source_url=section.source_url,
                        source_path=chapter_source_key,
                        source_format=MINNESOTA_STATUTES_SOURCE_FORMAT,
                        sha256=chapter_sha,
                        metadata={
                            "kind": "section",
                            "chapter": section.chapter.num,
                            "section": section.section,
                            "heading": section.heading,
                            "status": section.status,
                            "parent_citation_path": section.chapter.citation_path,
                            "source_id": section.source_id,
                            "references_to": list(section.references_to),
                        },
                    )
                )
                records.append(
                    _minnesota_section_provision(
                        section,
                        version=run_id,
                        source_path=chapter_source_key,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                    )
                )
                section_count += 1
                if remaining is not None:
                    remaining -= 1

    if not items:
        raise ValueError("no Minnesota Statutes provisions extracted")

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
        errors=tuple(errors),
    )


def extract_california_codes_bulk(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_zip: str | Path | None = None,
    source_url: str | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    include_inactive: bool = False,
) -> StateStatuteExtractReport:
    """Snapshot official California Legislative Counsel bulk data and extract codes."""
    jurisdiction = "us-ca"
    only_code = _california_code_filter(only_title)
    run_id = (
        state_run_id(version, jurisdiction=jurisdiction, only_title=only_code, limit=limit)
        if only_code or limit is not None
        else version
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    zip_path = _california_source_zip_path(
        source_zip=source_zip,
        download_dir=download_dir,
        source_url=source_url or CALIFORNIA_LEGINFO_BULK_URL,
    )
    zip_relative = f"california-leginfo-bulk/{zip_path.name}"
    zip_artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        zip_relative,
    )
    zip_sha = _write_file_artifact(zip_artifact_path, zip_path)
    zip_source_key = _state_source_key(jurisdiction, run_id, zip_relative)
    source_paths: list[Path] = [zip_artifact_path]

    with zipfile.ZipFile(zip_path) as archive:
        member_names = archive.namelist()
        codes_member = _california_zip_member(member_names, "CODES_TBL.dat")
        toc_member = _california_zip_member(member_names, "LAW_TOC_TBL.dat")
        toc_sections_member = _california_zip_member(member_names, "LAW_TOC_SECTIONS_TBL.dat")
        sections_member = _california_zip_member(member_names, "LAW_SECTION_TBL.dat")
        content_member_by_name = _california_content_member_index(member_names)

        code_source_key = zip_source_key
        toc_source_key = zip_source_key
        code_source_sha = zip_sha
        toc_source_sha = zip_sha

        codes = _california_codes_from_table(archive, codes_member)
        if only_code is not None:
            codes = tuple(code for code in codes if code.code == only_code)
        if not codes:
            raise ValueError(f"no California codes selected for filter: {only_title!r}")
        code_by_code = {code.code: code for code in codes}

        toc_containers, toc_path_by_key = _california_toc_containers(
            archive,
            toc_member,
            codes=code_by_code,
            source_path=toc_source_key,
            sha256=toc_source_sha,
            include_inactive=include_inactive,
        )
        toc_targets = _california_toc_section_targets(
            archive,
            toc_sections_member,
            code_by_code=code_by_code,
            toc_path_by_key=toc_path_by_key,
        )

        items: list[SourceInventoryItem] = []
        records: list[ProvisionRecord] = []
        errors: list[str] = []
        remaining = limit
        title_count = 0
        container_count = 0
        section_count = 0
        skipped_source_count = 0
        seen_citation_paths: set[str] = set()

        for code in codes:
            if remaining is not None and remaining <= 0:
                break
            code_container = _california_code_container(
                code,
                source_path=code_source_key,
                sha256=code_source_sha,
            )
            title_count += 1
            if code_container.citation_path not in seen_citation_paths:
                seen_citation_paths.add(code_container.citation_path)
                items.append(_container_inventory_item(code_container))
                records.append(
                    _container_provision(
                        code_container,
                        version=run_id,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                    )
                )
                container_count += 1
                if remaining is not None:
                    remaining -= 1

        for container in sorted(
            toc_containers,
            key=lambda container: (container.level, container.parent_citation_path or "", container.citation_path),
        ):
            if remaining is not None and remaining <= 0:
                break
            if container.citation_path in seen_citation_paths:
                continue
            seen_citation_paths.add(container.citation_path)
            items.append(_container_inventory_item(container))
            records.append(
                _container_provision(
                    container,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
            )
            container_count += 1
            if remaining is not None:
                remaining -= 1

        if remaining is None or remaining > 0:
            for row in _california_table_rows(archive, sections_member, _CALIFORNIA_SECTION_COLUMNS):
                if remaining is not None and remaining <= 0:
                    break
                law_code = (row.get("LAW_CODE") or "").strip().upper()
                if law_code not in code_by_code:
                    continue
                active_flg = row.get("ACTIVE_FLG") or None
                if not include_inactive and not _california_active_flag(active_flg):
                    skipped_source_count += 1
                    continue
                content_file = row.get("CONTENT_FILE") or None
                try:
                    content_bytes, content_member = _california_section_content(
                        archive,
                        content_member_by_name,
                        content_file,
                    )
                except (KeyError, ValueError) as exc:
                    errors.append(
                        f"{law_code} {row.get('SECTION_NUM') or row.get('ID') or ''}: {exc}"
                    )
                    continue
                target = _california_toc_target(row, toc_targets, code_by_code[law_code])
                section = _california_section_from_row(
                    row,
                    content_bytes=content_bytes,
                    content_sha256=sha256_bytes(content_bytes),
                    source_url_base=source_url or CALIFORNIA_LEGINFO_BULK_URL,
                    target=target,
                )
                if section.citation_path in seen_citation_paths:
                    continue
                seen_citation_paths.add(section.citation_path)
                items.append(
                    SourceInventoryItem(
                        citation_path=section.citation_path,
                        source_url=section.source_url,
                        source_path=zip_source_key,
                        source_format=CALIFORNIA_BULK_SOURCE_FORMAT,
                        sha256=zip_sha,
                        metadata=_california_section_metadata(section),
                    )
                )
                records.append(
                    _california_section_provision(
                        section,
                        version=run_id,
                        source_path=zip_source_key,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                    )
                )
                section_count += 1
                if remaining is not None:
                    remaining -= 1

    if not items:
        raise ValueError("no California code provisions extracted")

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


def extract_texas_tcas(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
    workers: int = 4,
    download_dir: str | Path | None = None,
) -> StateStatuteExtractReport:
    """Snapshot official Texas statutes from the TCSS statute API/resources."""
    jurisdiction = "us-tx"
    only_code = _texas_code_filter(only_title)
    run_id = (
        state_run_id(version, jurisdiction=jurisdiction, only_title=only_code, limit=limit)
        if only_code or limit is not None
        else version
    )
    source_root = Path(source_dir) if source_dir is not None else None
    download_root = Path(download_dir) if download_dir is not None and source_root is None else None
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    session = _texas_session()
    current_message = _load_texas_current_message(session, source_root, download_root)
    code_tree_bytes = _load_texas_asset(
        session,
        source_root,
        download_root,
        relative_name="assets/StatuteCodeTree.json",
        url=f"{TEXAS_STATUTES_BASE_URL}/assets/StatuteCodeTree.json",
    )
    code_tree_relative = "texas-tcas-json/StatuteCodeTree.json"
    code_tree_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        code_tree_relative,
    )
    code_tree_sha = store.write_bytes(code_tree_path, code_tree_bytes)
    source_paths: list[Path] = [code_tree_path]
    code_tree_source_key = _state_source_key(jurisdiction, run_id, code_tree_relative)
    codes = _texas_codes_from_asset(code_tree_bytes)
    if only_code is not None:
        codes = tuple(code for code in codes if code.code == only_code)
    if not codes:
        raise ValueError(f"no Texas statute codes selected for filter: {only_title!r}")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []
    remaining = limit
    title_count = 0
    container_count = 0
    section_count = 0
    seen_citation_paths: set[str] = set()
    seen_section_counts: dict[str, int] = {}
    html_documents_by_key: dict[str, _TexasHtmlDocument] = {}

    for code in codes:
        if remaining is not None and remaining <= 0:
            break
        tree_bytes = _load_texas_code_tree(session, source_root, download_root, code)
        tree_relative = f"texas-tcas-json/trees/{code.code}.json"
        tree_path = store.source_path(jurisdiction, DocumentClass.STATUTE, run_id, tree_relative)
        tree_sha = store.write_bytes(tree_path, tree_bytes)
        source_paths.append(tree_path)
        tree_source_key = _state_source_key(jurisdiction, run_id, tree_relative)

        code_container = _texas_code_container(
            code,
            source_path=code_tree_source_key,
            sha256=code_tree_sha,
            current_message=current_message,
        )
        containers, html_documents = _texas_tree_items(
            json.loads(tree_bytes.decode("utf-8")),
            code=code,
            root=code_container,
            source_path=tree_source_key,
            sha256=tree_sha,
            current_message=current_message,
        )
        title_count += 1
        for container in (code_container, *containers):
            if remaining is not None and remaining <= 0:
                break
            if container.citation_path in seen_citation_paths:
                continue
            seen_citation_paths.add(container.citation_path)
            items.append(_container_inventory_item(container))
            records.append(
                _container_provision(
                    container,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
            )
            container_count += 1
            if remaining is not None:
                remaining -= 1
        for document in html_documents:
            html_documents_by_key.setdefault(document.resource_key, document)

    if remaining is None or remaining > 0:
        for document, html_bytes, error in _iter_texas_html_sources(
            session,
            source_root,
            download_root,
            tuple(html_documents_by_key.values()),
            workers=workers,
        ):
            if remaining is not None and remaining <= 0:
                break
            if error is not None:
                errors.append(f"{document.resource_key}: {error}")
                continue
            if html_bytes is None:
                continue
            html_relative = f"texas-tcas-html/{document.resource_key}"
            html_path = store.source_path(
                jurisdiction,
                DocumentClass.STATUTE,
                run_id,
                html_relative,
            )
            html_sha = store.write_bytes(html_path, html_bytes)
            source_paths.append(html_path)
            html_source_key = _state_source_key(jurisdiction, run_id, html_relative)
            try:
                html_containers, sections = _parse_texas_html_document(
                    html_bytes,
                    document=document,
                    seen_container_paths=seen_citation_paths,
                    section_counts=seen_section_counts,
                )
            except ValueError as exc:
                errors.append(f"{document.resource_key}: {exc}")
                continue

            for container in html_containers:
                if remaining is not None and remaining <= 0:
                    break
                container = _replace_container_source(
                    container,
                    source_path=html_source_key,
                    source_format=TEXAS_TCAS_HTML_SOURCE_FORMAT,
                    sha256=html_sha,
                    metadata_extra={
                        "resource_key": document.resource_key,
                        "source_url": document.source_url,
                    },
                )
                items.append(_container_inventory_item(container))
                records.append(
                    _container_provision(
                        container,
                        version=run_id,
                        source_as_of=source_as_of_text,
                        expression_date=expression_date_text,
                    )
                )
                container_count += 1
                if remaining is not None:
                    remaining -= 1

            for section in sections:
                if remaining is not None and remaining <= 0:
                    break
                item = SourceInventoryItem(
                    citation_path=section.citation_path,
                    source_url=section.source_url,
                    source_path=html_source_key,
                    source_format=TEXAS_TCAS_HTML_SOURCE_FORMAT,
                    sha256=html_sha,
                    metadata={
                        "kind": "section",
                        "code": section.code,
                        "section": section.section,
                        "variant": section.variant,
                        "marker": section.marker,
                        "heading": section.heading,
                        "parent_citation_path": section.parent_citation_path,
                        "source_id": section.source_id,
                        "source_document_id": section.source_document_id,
                        "anchors": list(section.anchors),
                        "references_to": list(section.references_to),
                        "resource_key": document.resource_key,
                    },
                )
                record = _texas_section_provision(
                    section,
                    version=run_id,
                    source_path=html_source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
                items.append(item)
                records.append(record)
                section_count += 1
                if remaining is not None:
                    remaining -= 1

    if not items:
        raise ValueError("no Texas statutes extracted")

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


def _ohio_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": OHIO_USER_AGENT})
    return session


def _load_ohio_html(
    session: requests.Session,
    source_root: Path | None,
    download_root: Path | None,
    *,
    relative_name: str,
    url: str,
) -> bytes:
    if source_root is not None:
        return (source_root / relative_name).read_bytes()
    response: requests.Response | None = None
    for attempt in range(6):
        response = session.get(url, timeout=60)
        if response.status_code != 429:
            response.raise_for_status()
            content = response.content
            if download_root is not None:
                path = download_root / relative_name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            time.sleep(0.25)
            return content
        retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
        time.sleep(retry_after if retry_after is not None else min(2**attempt, 30))
    assert response is not None
    response.raise_for_status()
    content = response.content
    if download_root is not None:
        path = download_root / relative_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return content


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(float(value), 0)
    except ValueError:
        return None


def _parse_ohio_titles(html_bytes: bytes) -> tuple[_OhioTitle, ...]:
    soup = BeautifulSoup(html_bytes.decode("utf-8", errors="replace"), "html.parser")
    titles: dict[str, _OhioTitle] = {}
    for link in soup.select('a[href*="ohio-revised-code/"]'):
        if not isinstance(link, Tag):
            continue
        href = _ohio_path_from_href(
            str(link.get("href") or ""),
            f"{OHIO_REVISED_CODE_BASE_URL}/ohio-revised-code",
        )
        text = _clean_text(link.get_text(" ", strip=True))
        if href == "/ohio-revised-code/general-provisions":
            titles.setdefault(
                "general-provisions",
                _OhioTitle(
                    token="general-provisions",
                    num="general-provisions",
                    heading="General Provisions",
                    href=href,
                ),
            )
            continue
        match = re.fullmatch(r"/ohio-revised-code/title-(?P<num>\d+)", href)
        if not match:
            continue
        title_num = match.group("num")
        heading_match = re.match(rf"Title\s+{re.escape(title_num)}\s*\|\s*(?P<heading>.+)", text)
        heading = heading_match.group("heading") if heading_match else text
        titles.setdefault(
            title_num,
            _OhioTitle(token=title_num, num=title_num, heading=heading, href=href),
        )
    return tuple(sorted(titles.values(), key=lambda title: _ohio_title_sort_key(title.token)))


def _parse_ohio_chapters(
    html_bytes: bytes,
    *,
    title: _OhioTitle,
) -> tuple[_OhioChapter, ...]:
    soup = BeautifulSoup(html_bytes.decode("utf-8", errors="replace"), "html.parser")
    chapters: dict[str, _OhioChapter] = {}
    for link in soup.select('a[href*="chapter-"]'):
        if not isinstance(link, Tag):
            continue
        href = _ohio_path_from_href(str(link.get("href") or ""), title.source_url)
        match = re.fullmatch(r"/ohio-revised-code/chapter-(?P<num>[0-9A-Za-z.]+)", href)
        if not match:
            continue
        chapter_num = match.group("num")
        text = _clean_text(link.get_text(" ", strip=True))
        heading_match = re.match(
            rf"Chapter\s+{re.escape(chapter_num)}\s*\|\s*(?P<heading>.+)",
            text,
        )
        heading = heading_match.group("heading") if heading_match else text
        chapters.setdefault(
            chapter_num,
            _OhioChapter(title=title, num=chapter_num, heading=heading, href=href),
        )
    return tuple(sorted(chapters.values(), key=lambda chapter: _section_ordinal(chapter.num) or 0))


def _ohio_path_from_href(href: str, base_url: str) -> str:
    return urlparse(urljoin(base_url, href)).path


def _parse_ohio_sections(
    html_bytes: bytes,
    *,
    chapter: _OhioChapter,
) -> tuple[_OhioSection, ...]:
    soup = BeautifulSoup(html_bytes.decode("utf-8", errors="replace"), "html.parser")
    sections: list[_OhioSection] = []
    for content in soup.select("div.list-content"):
        if not isinstance(content, Tag):
            continue
        head = content.select_one(".content-head-text a")
        body_tag = content.select_one("section.laws-body")
        if not isinstance(head, Tag) or not isinstance(body_tag, Tag):
            continue
        header_text = _clean_text(head.get_text(" ", strip=True))
        parsed = _parse_ohio_section_header(header_text)
        if parsed is None:
            continue
        section_num, heading = parsed
        source_url = urljoin(chapter.source_url, str(head.get("href") or ""))
        body = _ohio_laws_body_text(body_tag)
        effective_date, latest_legislation, pdf_url = _ohio_section_info(content)
        sections.append(
            _OhioSection(
                chapter=chapter,
                section=section_num,
                heading=heading,
                body=body,
                source_url=source_url,
                source_id=f"section-{section_num}",
                effective_date=effective_date,
                latest_legislation=latest_legislation,
                pdf_url=pdf_url,
                last_updated=_ohio_last_updated(body_tag),
                references_to=_ohio_references(body_tag),
            )
        )
    return tuple(sections)


def _parse_ohio_section_header(text: str) -> tuple[str, str | None] | None:
    match = re.match(
        r"^Section\s+(?P<section>[0-9A-Za-z.]+)\s*\|\s*(?P<heading>.*)$",
        text,
    )
    if not match:
        return None
    heading = _clean_text(match.group("heading")).rstrip(".")
    return match.group("section"), heading or None


def _ohio_laws_body_text(body_tag: Tag) -> str | None:
    body_span = body_tag.find("span")
    root = body_span if isinstance(body_span, Tag) else body_tag
    paragraphs = [
        _clean_text(paragraph.get_text(" ", strip=True))
        for paragraph in root.find_all("p")
        if isinstance(paragraph, Tag)
    ]
    text = "\n".join(paragraph for paragraph in paragraphs if paragraph)
    return text or None


def _ohio_section_info(content: Tag) -> tuple[str | None, str | None, str | None]:
    effective_date = None
    latest_legislation = None
    pdf_url = None
    for module in content.select(".laws-section-info-module"):
        if not isinstance(module, Tag):
            continue
        label_tag = module.select_one(".label")
        value_tag = module.select_one(".value")
        if not isinstance(label_tag, Tag) or not isinstance(value_tag, Tag):
            continue
        label = _clean_text(label_tag.get_text(" ", strip=True)).rstrip(":").lower()
        if label == "effective":
            effective_date = _clean_text(value_tag.get_text(" ", strip=True)) or None
        elif label == "latest legislation":
            latest_legislation = _clean_text(value_tag.get_text(" ", strip=True)) or None
        elif label == "pdf":
            link = value_tag.find("a")
            if isinstance(link, Tag):
                href = str(link.get("href") or "")
                if href:
                    pdf_url = urljoin(OHIO_REVISED_CODE_BASE_URL, href)
    return effective_date, latest_legislation, pdf_url


def _ohio_last_updated(body_tag: Tag) -> str | None:
    notice = body_tag.select_one(".laws-notice p")
    if not isinstance(notice, Tag):
        return None
    return _clean_text(notice.get_text(" ", strip=True)) or None


def _ohio_references(body_tag: Tag) -> tuple[str, ...]:
    refs: set[str] = set()
    for link in body_tag.select("a.section-link"):
        if not isinstance(link, Tag):
            continue
        ref = _ohio_reference_from_href(str(link.get("href") or ""))
        if ref:
            refs.add(ref)
    return tuple(sorted(refs))


def _ohio_reference_from_href(href: str) -> str | None:
    match = re.search(r"/ohio-revised-code/section-(?P<section>[0-9A-Za-z.]+)$", href)
    if not match:
        return None
    return f"us-oh/statute/{match.group('section')}"


def _ohio_title_filter(only_title: str | None) -> str | None:
    if only_title is None:
        return None
    token = only_title.strip().lower()
    if token in {"general", "general-provisions"}:
        return "general-provisions"
    return _clean_title_token(only_title)


def _ohio_title_file_token(title: _OhioTitle) -> str:
    if title.token == "general-provisions":
        return "general-provisions"
    return f"title-{title.num}"


def _ohio_title_sort_key(token: str) -> tuple[int, str]:
    if token == "general-provisions":
        return (0, "")
    return (1, f"{_title_sort_key(token)[0]:04d}-{_title_sort_key(token)[1]}")


def _ohio_title_container(
    title: _OhioTitle,
    *,
    source_path: str,
    sha256: str,
) -> _StateContainer:
    return _StateContainer(
        jurisdiction="us-oh",
        title=title.num,
        kind="title",
        num=title.num,
        heading=title.heading,
        citation_path=title.citation_path,
        parent_citation_path=None,
        level=0,
        ordinal=_ordinal(title.num),
        source_path=source_path,
        source_url=title.source_url,
        source_id=f"title-{title.token}",
        source_format=OHIO_REVISED_CODE_SOURCE_FORMAT,
        sha256=sha256,
        metadata={
            "title": title.num,
            "title_token": title.token,
            "source_url": title.source_url,
        },
    )


def _ohio_chapter_container(
    chapter: _OhioChapter,
    *,
    source_path: str,
    sha256: str,
) -> _StateContainer:
    return _StateContainer(
        jurisdiction="us-oh",
        title=chapter.title.num,
        kind="chapter",
        num=chapter.num,
        heading=chapter.heading,
        citation_path=chapter.citation_path,
        parent_citation_path=chapter.title.citation_path,
        level=1,
        ordinal=_section_ordinal(chapter.num),
        source_path=source_path,
        source_url=chapter.source_url,
        source_id=f"chapter-{chapter.num}",
        source_format=OHIO_REVISED_CODE_SOURCE_FORMAT,
        sha256=sha256,
        metadata={
            "title": chapter.title.num,
            "chapter": chapter.num,
            "source_url": chapter.source_url,
        },
    )


def _ohio_section_provision(
    section: _OhioSection,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-oh",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        citation_label=f"Ohio Rev. Code § {section.section}",
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=section.source_url,
        source_path=source_path,
        source_id=section.source_id,
        source_format=OHIO_REVISED_CODE_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.chapter.citation_path,
        parent_id=deterministic_provision_id(section.chapter.citation_path),
        level=2,
        ordinal=_section_ordinal(section.section),
        kind="section",
        legal_identifier=f"Ohio Rev. Code § {section.section}",
        identifiers={
            "ohio:title": section.chapter.title.num,
            "ohio:chapter": section.chapter.num,
            "ohio:section": section.section,
        },
        metadata={
            "title": section.chapter.title.num,
            "chapter": section.chapter.num,
            "section": section.section,
            "effective_date": section.effective_date,
            "latest_legislation": section.latest_legislation,
            "pdf_url": section.pdf_url,
            "last_updated": section.last_updated,
            "references_to": list(section.references_to),
        },
    )


def _minnesota_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": MINNESOTA_USER_AGENT})
    return session


def _load_minnesota_html(
    session: requests.Session,
    source_root: Path | None,
    download_root: Path | None,
    *,
    relative_name: str,
    url: str,
) -> bytes:
    if source_root is not None:
        return (source_root / relative_name).read_bytes()
    response: requests.Response | None = None
    for attempt in range(5):
        response = session.get(url, timeout=90)
        if response.status_code != 429:
            response.raise_for_status()
            content = response.content
            if download_root is not None:
                path = download_root / relative_name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            return content
        retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
        time.sleep(retry_after if retry_after is not None else min(2**attempt, 30))
    assert response is not None
    response.raise_for_status()
    return response.content


def _parse_minnesota_parts(data: bytes) -> tuple[_MinnesotaPart, ...]:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    parts: list[_MinnesotaPart] = []
    seen: set[str] = set()
    for row in soup.select("tr"):
        if not isinstance(row, Tag):
            continue
        link = row.select_one('a[href*="/statutes/part/"]')
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "")
        token = _minnesota_part_token_from_href(href)
        if token is None or token in seen:
            continue
        cells = [cell for cell in row.find_all("td") if isinstance(cell, Tag)]
        heading = _clean_text(cells[1].get_text(" ", strip=True)) if len(cells) > 1 else None
        heading = heading or _clean_text(link.get_text(" ", strip=True)) or token
        parts.append(
            _MinnesotaPart(
                token=token,
                heading=heading,
                href=href,
                ordinal=len(parts),
            )
        )
        seen.add(token)
    return tuple(parts)


def _minnesota_part_token_from_href(href: str) -> str | None:
    parsed = urlparse(href)
    marker = "/statutes/part/"
    if marker not in parsed.path:
        return None
    token = parsed.path.split(marker, 1)[1].strip("/")
    for _ in range(2):
        token = unquote_plus(token)
    return _clean_text(token.replace("+", " ")) or None


def _parse_minnesota_chapters(
    data: bytes,
    *,
    part: _MinnesotaPart,
) -> tuple[_MinnesotaChapter, ...]:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    chapters: list[_MinnesotaChapter] = []
    seen: set[str] = set()
    for row in soup.select("tr"):
        if not isinstance(row, Tag):
            continue
        link = row.select_one('a[href*="/statutes/cite/"]')
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "")
        chapter = _minnesota_chapter_from_href(href, link.get_text(" ", strip=True))
        if chapter is None or chapter in seen:
            continue
        cells = [cell for cell in row.find_all("td") if isinstance(cell, Tag)]
        heading = _clean_text(cells[1].get_text(" ", strip=True)) if len(cells) > 1 else None
        chapters.append(
            _MinnesotaChapter(
                part=part,
                num=chapter,
                heading=heading or None,
                href=href,
                ordinal=len(chapters),
            )
        )
        seen.add(chapter)
    return tuple(chapters)


def _minnesota_chapter_from_href(href: str, label: str) -> str | None:
    parsed = urlparse(href)
    match = re.fullmatch(r"/statutes/cite/(?P<chapter>\d+[A-Z]?)", parsed.path)
    chapter = match.group("chapter") if match else _clean_text(label)
    if re.fullmatch(r"\d+[A-Z]?", chapter or ""):
        return str(chapter)
    return None


def _iter_minnesota_chapter_sources(
    source_root: Path | None,
    download_root: Path | None,
    chapters: tuple[_MinnesotaChapter, ...],
    *,
    workers: int,
) -> Iterator[tuple[_MinnesotaChapter, bytes | None, str | None]]:
    if source_root is not None:
        for chapter in chapters:
            try:
                yield chapter, (source_root / _minnesota_chapter_relative(chapter)).read_bytes(), None
            except OSError as exc:
                yield chapter, None, str(exc)
        return

    worker_count = max(1, workers)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        yield from executor.map(
            lambda chapter: _load_minnesota_chapter_source(download_root, chapter),
            chapters,
        )


def _load_minnesota_chapter_source(
    download_root: Path | None,
    chapter: _MinnesotaChapter,
) -> tuple[_MinnesotaChapter, bytes | None, str | None]:
    try:
        session = _minnesota_session()
        data = _load_minnesota_html(
            session,
            None,
            download_root,
            relative_name=_minnesota_chapter_relative(chapter),
            url=chapter.full_source_url,
        )
        return chapter, data, None
    except requests.RequestException as exc:
        return chapter, None, str(exc)


def _minnesota_chapter_relative(chapter: _MinnesotaChapter) -> str:
    return f"minnesota-statutes-html/chapters/chapter-{chapter.num}.html"


def _parse_minnesota_chapter_sections(
    data: bytes,
    *,
    chapter: _MinnesotaChapter,
) -> tuple[str | None, tuple[_MinnesotaSection, ...]]:
    soup = BeautifulSoup(data.decode("utf-8", errors="replace"), "html.parser")
    root = soup.select_one("div#xtend.statute") or soup.select_one("div.statute")
    if not isinstance(root, Tag):
        raise ValueError(f"Minnesota chapter {chapter.num} page has no statute body")
    parsed_heading = _minnesota_chapter_heading(root, chapter.num)
    chapter_for_sections = _MinnesotaChapter(
        part=chapter.part,
        num=chapter.num,
        heading=parsed_heading or chapter.heading,
        href=chapter.href,
        ordinal=chapter.ordinal,
    )
    sections: list[_MinnesotaSection] = []
    seen: set[str] = set()
    for section_tag in root.select("div.section, div.sr"):
        if not isinstance(section_tag, Tag):
            continue
        section = _minnesota_section_from_tag(section_tag)
        if section is None or section in seen:
            continue
        status = "active" if "section" in (section_tag.get("class") or []) else "inactive"
        heading = (
            _minnesota_active_section_heading(section_tag, section)
            if status == "active"
            else None
        )
        sections.append(
            _MinnesotaSection(
                chapter=chapter_for_sections,
                section=section,
                heading=heading,
                body=_minnesota_section_body(section_tag, status=status),
                source_id=str(section_tag.get("id") or f"stat.{section}"),
                status=status,
                references_to=_minnesota_references(section_tag, self_path=f"us-mn/statute/{section}"),
            )
        )
        seen.add(section)
    return parsed_heading, tuple(sections)


def _minnesota_chapter_heading(root: Tag, chapter_num: str) -> str | None:
    heading_tag = root.select_one("h2.chapter_title")
    if not isinstance(heading_tag, Tag):
        return None
    text = _clean_text(heading_tag.get_text(" ", strip=True))
    match = re.match(rf"^CHAPTER\s+{re.escape(chapter_num)}\.\s*(?P<heading>.+)$", text, re.I)
    if match:
        return _clean_text(match.group("heading")).rstrip(".") or None
    return text or None


def _minnesota_section_from_tag(tag: Tag) -> str | None:
    source_id = str(tag.get("id") or "")
    match = re.fullmatch(r"stat\.(?P<section>\d+[A-Z]?\.[0-9A-Za-z]+)", source_id)
    if match:
        return match.group("section")
    text = _clean_text(tag.get_text(" ", strip=True))
    match = re.match(r"^(?P<section>\d+[A-Z]?\.[0-9A-Za-z]+)\b", text)
    return match.group("section") if match else None


def _minnesota_active_section_heading(tag: Tag, section: str) -> str | None:
    heading_tag = tag.select_one("h1.shn")
    if not isinstance(heading_tag, Tag):
        return None
    text = _clean_text(heading_tag.get_text(" ", strip=True))
    heading = re.sub(rf"^{re.escape(section)}\s*", "", text).strip()
    return heading.rstrip(".") or None


def _minnesota_section_body(tag: Tag, *, status: str) -> str | None:
    copy = BeautifulSoup(str(tag), "html.parser")
    for link in copy.select("a.permalink"):
        link.decompose()
    if status == "active":
        heading = copy.select_one("h1.shn")
        if isinstance(heading, Tag):
            heading.decompose()
    text = _clean_text(copy.get_text("\n", strip=True))
    return text or None


def _minnesota_references(tag: Tag, *, self_path: str) -> tuple[str, ...]:
    refs: set[str] = set()
    for link in tag.find_all("a"):
        if not isinstance(link, Tag):
            continue
        ref = _minnesota_href_to_citation_path(str(link.get("href") or ""))
        if ref and ref != self_path:
            refs.add(ref)
    return tuple(sorted(refs))


def _minnesota_href_to_citation_path(href: str) -> str | None:
    parsed = urlparse(href)
    match = re.fullmatch(r"/statutes/cite/(?P<section>\d+[A-Z]?\.[0-9A-Za-z]+)", parsed.path)
    if match:
        return f"us-mn/statute/{match.group('section')}"
    fragment_match = re.match(
        r"stat\.(?P<section>\d+[A-Z]?\.[0-9A-Za-z]+)",
        parsed.fragment,
    )
    if fragment_match:
        return f"us-mn/statute/{fragment_match.group('section')}"
    return None


def _minnesota_chapter_filter(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip().upper().removeprefix("CHAPTER-").removeprefix("CHAPTER ")
    if not re.fullmatch(r"\d+[A-Z]?", text):
        raise ValueError(f"invalid Minnesota chapter filter: {value!r}")
    return text


def _minnesota_part_container(
    part: _MinnesotaPart,
    *,
    source_path: str,
    sha256: str,
) -> _StateContainer:
    return _StateContainer(
        jurisdiction="us-mn",
        title=part.token,
        kind="part",
        num=part.token,
        heading=part.heading,
        citation_path=part.citation_path,
        parent_citation_path=None,
        level=0,
        ordinal=part.ordinal,
        source_path=source_path,
        source_url=part.source_url,
        source_id=f"part-{_clean_path_token(part.token)}",
        source_format=MINNESOTA_STATUTES_SOURCE_FORMAT,
        sha256=sha256,
        metadata={
            "part": part.token,
            "source_url": part.source_url,
        },
    )


def _minnesota_chapter_container(
    chapter: _MinnesotaChapter,
    *,
    source_path: str,
    sha256: str,
) -> _StateContainer:
    return _StateContainer(
        jurisdiction="us-mn",
        title=chapter.num,
        kind="chapter",
        num=chapter.num,
        heading=chapter.heading,
        citation_path=chapter.citation_path,
        parent_citation_path=chapter.part.citation_path,
        level=1,
        ordinal=_section_ordinal(chapter.num) or chapter.ordinal,
        source_path=source_path,
        source_url=chapter.full_source_url,
        source_id=f"chapter-{chapter.num}",
        source_format=MINNESOTA_STATUTES_SOURCE_FORMAT,
        sha256=sha256,
        metadata={
            "part": chapter.part.token,
            "chapter": chapter.num,
            "source_url": chapter.full_source_url,
        },
    )


def _minnesota_section_provision(
    section: _MinnesotaSection,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-mn",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        citation_label=f"Minn. Stat. § {section.section}",
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=section.source_url,
        source_path=source_path,
        source_id=section.source_id,
        source_format=MINNESOTA_STATUTES_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.chapter.citation_path,
        parent_id=deterministic_provision_id(section.chapter.citation_path),
        level=2,
        ordinal=_section_ordinal(section.section),
        kind="section",
        legal_identifier=f"Minn. Stat. § {section.section}",
        identifiers={
            "minnesota:chapter": section.chapter.num,
            "minnesota:section": section.section,
        },
        metadata={
            "part": section.chapter.part.token,
            "chapter": section.chapter.num,
            "section": section.section,
            "status": section.status,
            "references_to": list(section.references_to),
        },
    )


_CALIFORNIA_CODE_COLUMNS = ("CODE", "TITLE")
_CALIFORNIA_TOC_COLUMNS = (
    "LAW_CODE",
    "DIVISION",
    "TITLE",
    "PART",
    "CHAPTER",
    "ARTICLE",
    "HEADING",
    "ACTIVE_FLG",
    "TRANS_UID",
    "TRANS_UPDATE",
    "NODE_SEQUENCE",
    "NODE_LEVEL",
    "NODE_POSITION",
    "NODE_TREEPATH",
    "CONTAINS_LAW_SECTIONS",
    "HISTORY_NOTE",
    "OP_STATUES",
    "OP_CHAPTER",
    "OP_SECTION",
)
_CALIFORNIA_TOC_SECTION_COLUMNS = (
    "ID",
    "LAW_CODE",
    "NODE_TREEPATH",
    "SECTION_NUM",
    "SECTION_ORDER",
    "TITLE",
    "OP_STATUES",
    "OP_CHAPTER",
    "OP_SECTION",
    "TRANS_UID",
    "TRANS_UPDATE",
    "LAW_SECTION_VERSION_ID",
    "SEQ_NUM",
)
_CALIFORNIA_SECTION_COLUMNS = (
    "ID",
    "LAW_CODE",
    "SECTION_NUM",
    "OP_STATUES",
    "OP_CHAPTER",
    "OP_SECTION",
    "EFFECTIVE_DATE",
    "LAW_SECTION_VERSION_ID",
    "DIVISION",
    "TITLE",
    "PART",
    "CHAPTER",
    "ARTICLE",
    "HISTORY",
    "CONTENT_FILE",
    "ACTIVE_FLG",
    "TRANS_UID",
    "TRANS_UPDATE",
)


def _california_source_zip_path(
    *,
    source_zip: str | Path | None,
    download_dir: str | Path | None,
    source_url: str,
) -> Path:
    if source_zip is not None:
        path = Path(source_zip)
        if not path.exists():
            raise ValueError(f"California bulk source ZIP does not exist: {path}")
        return path
    if download_dir is None:
        raise ValueError("California bulk adapter requires source_zip or download_dir")
    root = Path(download_dir)
    root.mkdir(parents=True, exist_ok=True)
    name = Path(urlparse(source_url).path).name or "pubinfo_2025.zip"
    path = root / name
    if path.exists():
        return path
    with requests.get(source_url, stream=True, timeout=90) as response:
        response.raise_for_status()
        with path.open("wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)
    return path


def _write_file_artifact(path: Path, source: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with source.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    if path.exists() or path.is_symlink():
        path.unlink()
    try:
        path.symlink_to(source.resolve())
    except OSError:
        with source.open("rb") as src, NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            for chunk in iter(lambda: src.read(1024 * 1024), b""):
                tmp.write(chunk)
        tmp_path.replace(path)
    return digest.hexdigest()


def _california_zip_member(member_names: list[str], basename: str) -> str:
    basename_upper = basename.upper()
    matches = [
        name
        for name in member_names
        if Path(name).name.upper() == basename_upper or name.upper() == basename_upper
    ]
    if not matches:
        raise ValueError(f"California bulk ZIP has no {basename}")
    return sorted(matches, key=lambda name: (len(name), name))[0]


def _california_table_rows(
    archive: zipfile.ZipFile,
    member: str,
    columns: tuple[str, ...],
) -> Iterator[dict[str, str]]:
    with archive.open(member) as raw:
        text = TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
        reader = csv.reader(text, delimiter="\t", quotechar="`")
        for raw_row in reader:
            if not raw_row or not any(cell.strip() for cell in raw_row):
                continue
            row = [cell.rstrip("\r") for cell in raw_row]
            if len(row) < len(columns):
                row.extend([""] * (len(columns) - len(row)))
            yield {
                column: _california_null_text(value)
                for column, value in zip(columns, row, strict=False)
            }


def _california_null_text(value: str) -> str:
    text = value.strip()
    return "" if text in {"", "\\N", "NULL"} else text


def _california_codes_from_table(
    archive: zipfile.ZipFile,
    member: str,
) -> tuple[_CaliforniaCode, ...]:
    codes: list[_CaliforniaCode] = []
    for row in _california_table_rows(archive, member, _CALIFORNIA_CODE_COLUMNS):
        code = (row.get("CODE") or "").strip().upper()
        title = _clean_text(row.get("TITLE"))
        if not code or not title:
            continue
        codes.append(_CaliforniaCode(code=code, title=title))
    return tuple(sorted(codes, key=lambda code: code.code))


def _california_code_filter(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip().upper().removeprefix("CODE-").removeprefix("CODE ")
    if not re.fullmatch(r"[A-Z0-9]{1,8}", text):
        raise ValueError(f"invalid California code filter: {value!r}")
    return text


def _california_code_container(
    code: _CaliforniaCode,
    *,
    source_path: str,
    sha256: str,
) -> _StateContainer:
    return _StateContainer(
        jurisdiction="us-ca",
        title=code.token,
        kind="code",
        num=code.code,
        heading=code.title,
        citation_path=code.citation_path,
        parent_citation_path=None,
        level=0,
        ordinal=None,
        source_path=source_path,
        source_url=CALIFORNIA_LEGINFO_BULK_URL,
        source_id=code.code,
        source_format=CALIFORNIA_BULK_SOURCE_FORMAT,
        sha256=sha256,
        metadata={
            "law_code": code.code,
            "code_title": code.title,
        },
    )


def _california_toc_containers(
    archive: zipfile.ZipFile,
    member: str,
    *,
    codes: dict[str, _CaliforniaCode],
    source_path: str,
    sha256: str,
    include_inactive: bool,
) -> tuple[tuple[_StateContainer, ...], dict[tuple[str, str], str]]:
    raw_rows = list(_california_table_rows(archive, member, _CALIFORNIA_TOC_COLUMNS))
    path_by_key: dict[tuple[str, str], str] = {}
    for row in raw_rows:
        law_code = (row.get("LAW_CODE") or "").strip().upper()
        treepath = row.get("NODE_TREEPATH") or ""
        if law_code not in codes or not treepath:
            continue
        path_by_key[(law_code, treepath)] = _california_toc_citation_path(law_code, treepath)

    containers: list[_StateContainer] = []
    for row in raw_rows:
        law_code = (row.get("LAW_CODE") or "").strip().upper()
        treepath = row.get("NODE_TREEPATH") or ""
        if law_code not in codes or not treepath:
            continue
        if not include_inactive and not _california_active_flag(row.get("ACTIVE_FLG")):
            continue
        code = codes[law_code]
        kind, num = _california_toc_kind_num(row)
        parent_treepath = _california_parent_treepath(treepath)
        parent_path = (
            path_by_key.get((law_code, parent_treepath))
            if parent_treepath is not None
            else None
        ) or code.citation_path
        node_level = _california_int(row.get("NODE_LEVEL"))
        containers.append(
            _StateContainer(
                jurisdiction="us-ca",
                title=code.token,
                kind=kind,
                num=num,
                heading=_clean_text(row.get("HEADING")) or None,
                citation_path=path_by_key[(law_code, treepath)],
                parent_citation_path=parent_path,
                level=(node_level + 1) if node_level is not None else 1,
                ordinal=_california_int(row.get("NODE_SEQUENCE"))
                or _california_int(row.get("NODE_POSITION")),
                source_path=source_path,
                source_url=None,
                source_id=treepath,
                source_format=CALIFORNIA_BULK_SOURCE_FORMAT,
                sha256=sha256,
                metadata={
                    "law_code": law_code,
                    "division": row.get("DIVISION") or None,
                    "title": row.get("TITLE") or None,
                    "part": row.get("PART") or None,
                    "chapter": row.get("CHAPTER") or None,
                    "article": row.get("ARTICLE") or None,
                    "active_flg": row.get("ACTIVE_FLG") or None,
                    "node_sequence": row.get("NODE_SEQUENCE") or None,
                    "node_level": row.get("NODE_LEVEL") or None,
                    "node_position": row.get("NODE_POSITION") or None,
                    "node_treepath": treepath,
                    "contains_law_sections": row.get("CONTAINS_LAW_SECTIONS") or None,
                    "history_note": row.get("HISTORY_NOTE") or None,
                    "op_statues": row.get("OP_STATUES") or None,
                    "op_chapter": row.get("OP_CHAPTER") or None,
                    "op_section": row.get("OP_SECTION") or None,
                },
            )
        )
    return tuple(containers), path_by_key


def _california_toc_citation_path(law_code: str, treepath: str) -> str:
    return f"us-ca/statute/{law_code.lower()}/node-{_clean_path_token(treepath)}"


def _california_toc_kind_num(row: dict[str, str]) -> tuple[str, str]:
    for key, kind in (
        ("ARTICLE", "article"),
        ("CHAPTER", "chapter"),
        ("PART", "part"),
        ("TITLE", "title"),
        ("DIVISION", "division"),
    ):
        value = _clean_text(row.get(key))
        if value:
            return kind, value
    return "node", row.get("NODE_TREEPATH") or row.get("NODE_POSITION") or "0"


def _california_parent_treepath(treepath: str) -> str | None:
    text = treepath.strip()
    if not text:
        return None
    for delimiter in (".", "/", "-", " "):
        if delimiter in text:
            parent = delimiter.join(part for part in text.split(delimiter)[:-1] if part)
            return parent or None
    return None


def _california_toc_section_targets(
    archive: zipfile.ZipFile,
    member: str,
    *,
    code_by_code: dict[str, _CaliforniaCode],
    toc_path_by_key: dict[tuple[str, str], str],
) -> dict[tuple[str, str], _CaliforniaTocTarget]:
    targets: dict[tuple[str, str], _CaliforniaTocTarget] = {}
    fallback_targets: dict[tuple[str, str], _CaliforniaTocTarget] = {}
    for row in _california_table_rows(archive, member, _CALIFORNIA_TOC_SECTION_COLUMNS):
        law_code = (row.get("LAW_CODE") or "").strip().upper()
        section = row.get("SECTION_NUM") or ""
        if law_code not in code_by_code or not section:
            continue
        treepath = row.get("NODE_TREEPATH") or ""
        parent_path = toc_path_by_key.get((law_code, treepath)) or code_by_code[law_code].citation_path
        target = _CaliforniaTocTarget(
            law_code=law_code,
            node_treepath=treepath,
            section_num=section,
            section_order=_california_int(row.get("SECTION_ORDER")),
            title=row.get("TITLE") or None,
            law_section_version_id=row.get("LAW_SECTION_VERSION_ID") or None,
            seq_num=_california_int(row.get("SEQ_NUM")),
            parent_citation_path=parent_path,
            level=_california_toc_target_level(treepath),
        )
        if target.law_section_version_id:
            targets.setdefault((law_code, target.law_section_version_id), target)
        fallback_targets.setdefault((law_code, section), target)
    targets.update({key: value for key, value in fallback_targets.items() if key not in targets})
    return targets


def _california_toc_target_level(treepath: str) -> int:
    if not treepath:
        return 1
    for delimiter in (".", "/", "-", " "):
        if delimiter in treepath:
            return len([part for part in treepath.split(delimiter) if part]) + 1
    return 2


def _california_active_flag(value: str | None) -> bool:
    return (value or "").strip().upper() in {"", "A", "Y", "1", "TRUE"}


def _california_int(value: str | None) -> int | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _california_section_content(
    archive: zipfile.ZipFile,
    member_by_name: dict[str, str],
    content_file: str | None,
) -> tuple[bytes, str]:
    if not content_file:
        raise ValueError("missing content XML filename")
    member = _california_content_member(member_by_name, content_file)
    return archive.read(member), member


def _california_content_member_index(member_names: list[str]) -> dict[str, str]:
    member_by_name: dict[str, str] = {}
    for member in member_names:
        member_by_name.setdefault(member.upper(), member)
        member_by_name.setdefault(Path(member).name.upper(), member)
    return member_by_name


def _california_content_member(member_by_name: dict[str, str], content_file: str) -> str:
    normalized = content_file.replace("\\", "/").strip("/")
    candidates = [
        normalized,
        Path(normalized).name,
        f"pubinfo/{normalized}",
        f"PUBINFO/{normalized}",
    ]
    for candidate in candidates:
        found = member_by_name.get(candidate.upper())
        if found is not None:
            return found
    raise KeyError(f"content XML not found: {content_file}")


def _california_toc_target(
    row: dict[str, str],
    targets: dict[tuple[str, str], _CaliforniaTocTarget],
    code: _CaliforniaCode,
) -> _CaliforniaTocTarget:
    law_code = (row.get("LAW_CODE") or "").strip().upper()
    version_id = row.get("LAW_SECTION_VERSION_ID") or ""
    section = row.get("SECTION_NUM") or ""
    target = targets.get((law_code, version_id)) if version_id else None
    target = target or targets.get((law_code, section))
    if target is not None:
        return target
    return _CaliforniaTocTarget(
        law_code=law_code,
        node_treepath="",
        section_num=section,
        section_order=None,
        title=None,
        law_section_version_id=version_id or None,
        seq_num=None,
        parent_citation_path=code.citation_path,
        level=1,
    )


def _california_section_from_row(
    row: dict[str, str],
    *,
    content_bytes: bytes,
    content_sha256: str,
    source_url_base: str,
    target: _CaliforniaTocTarget,
) -> _CaliforniaSection:
    del source_url_base
    law_code = (row.get("LAW_CODE") or "").strip().upper()
    section = row.get("SECTION_NUM") or ""
    heading, body = _california_section_heading_body(content_bytes, section=section)
    heading = heading or target.title
    return _CaliforniaSection(
        law_code=law_code,
        section=section,
        heading=heading,
        body=body,
        source_id=row.get("ID") or row.get("LAW_SECTION_VERSION_ID") or None,
        source_url=_california_section_url(law_code, section),
        parent_citation_path=target.parent_citation_path,
        level=target.level,
        ordinal=target.section_order or target.seq_num or _section_ordinal(section),
        references_to=_california_references(content_bytes, self_law_code=law_code, self_section=section),
        effective_date=row.get("EFFECTIVE_DATE") or None,
        law_section_version_id=row.get("LAW_SECTION_VERSION_ID") or None,
        active_flg=row.get("ACTIVE_FLG") or None,
        history=row.get("HISTORY") or None,
        op_statues=row.get("OP_STATUES") or None,
        op_chapter=row.get("OP_CHAPTER") or None,
        op_section=row.get("OP_SECTION") or None,
        division=row.get("DIVISION") or None,
        title=row.get("TITLE") or None,
        part=row.get("PART") or None,
        chapter=row.get("CHAPTER") or None,
        article=row.get("ARTICLE") or None,
        content_file=row.get("CONTENT_FILE") or None,
        content_sha256=content_sha256,
    )


def _california_section_url(law_code: str, section: str) -> str:
    return (
        f"{CALIFORNIA_LEGINFO_BASE_URL}/faces/codes_displaySection.xhtml"
        f"?lawCode={quote(law_code)}&sectionNum={quote(section)}"
    )


def _california_section_token(section: str) -> str:
    return _clean_path_token(section.rstrip("."))


def _california_section_heading_body(
    content_bytes: bytes,
    *,
    section: str,
) -> tuple[str | None, str | None]:
    text = content_bytes.decode("utf-8-sig", errors="replace")
    try:
        root = ET.fromstring(text)
        heading = _california_xml_heading(root, section=section)
        body = _california_xml_body(root)
    except ET.ParseError:
        soup = BeautifulSoup(text, "html.parser")
        heading = None
        title_tag = soup.find(["heading", "title", "h1", "h2", "h3"])
        if isinstance(title_tag, Tag):
            heading = _clean_text(title_tag.get_text(" ", strip=True)) or None
        body = _clean_multiline_text(soup.get_text("\n", strip=True)) or None
    return heading, body


def _california_xml_heading(root: ET.Element, *, section: str) -> str | None:
    for elem in root.iter():
        name = _local_name(elem.tag).lower()
        if name in {"heading", "title", "catchline", "caption"}:
            value = _element_text(elem)
            if value and value != section:
                return _california_strip_section_prefix(value, section)
    return None


def _california_strip_section_prefix(value: str, section: str) -> str:
    text = _clean_text(value)
    pattern = rf"^(?:section\s+|§\s*)?{re.escape(section)}[\.\s:-]+"
    return re.sub(pattern, "", text, flags=re.I).strip() or text


def _california_xml_body(root: ET.Element) -> str | None:
    preferred_block_names = {
        "p",
        "para",
        "paragraph",
        "subdivision",
        "subsection",
        "sectiontext",
    }
    lines: list[str] = []
    for elem in root.iter():
        name = _local_name(elem.tag).lower()
        if name in preferred_block_names:
            value = _element_text(elem)
            if value and value not in lines:
                lines.append(value)
    if not lines:
        for elem in root.iter():
            name = _local_name(elem.tag).lower()
            if name in {"content", "text"}:
                value = _element_text(elem)
                if value and value not in lines:
                    lines.append(value)
    if not lines:
        value = _element_text(root)
        return value or None
    return "\n".join(lines).strip() or None


def _california_references(
    content_bytes: bytes,
    *,
    self_law_code: str,
    self_section: str,
) -> tuple[str, ...]:
    text = content_bytes.decode("utf-8-sig", errors="replace")
    refs: set[str] = set()
    for match in re.finditer(
        r"codes_displaySection\.xhtml\?[^\"'<> ]*lawCode=(?P<code>[A-Za-z0-9]+)"
        r"[^\"'<> ]*sectionNum=(?P<section>[A-Za-z0-9_.-]+)",
        text,
    ):
        code = match.group("code").upper()
        section = unquote_plus(match.group("section"))
        ref = f"us-ca/statute/{code.lower()}/{_california_section_token(section)}"
        if code != self_law_code or _california_section_token(section) != _california_section_token(
            self_section
        ):
            refs.add(ref)
    for match in re.finditer(
        r"lawCode=(?P<code>[A-Za-z0-9]+).*?sectionNum=(?P<section>[A-Za-z0-9_.-]+)",
        text,
    ):
        code = match.group("code").upper()
        section = unquote_plus(match.group("section"))
        ref = f"us-ca/statute/{code.lower()}/{_california_section_token(section)}"
        if code != self_law_code or _california_section_token(section) != _california_section_token(
            self_section
        ):
            refs.add(ref)
    return tuple(sorted(refs))


def _california_section_metadata(section: _CaliforniaSection) -> dict[str, Any]:
    return {
        "kind": "section",
        "law_code": section.law_code,
        "section": section.section,
        "heading": section.heading,
        "parent_citation_path": section.parent_citation_path,
        "source_id": section.source_id,
        "effective_date": section.effective_date,
        "law_section_version_id": section.law_section_version_id,
        "active_flg": section.active_flg,
        "history": section.history,
        "op_statues": section.op_statues,
        "op_chapter": section.op_chapter,
        "op_section": section.op_section,
        "division": section.division,
        "title": section.title,
        "part": section.part,
        "chapter": section.chapter,
        "article": section.article,
        "content_file": section.content_file,
        "content_sha256": section.content_sha256,
        "references_to": list(section.references_to),
    }


def _california_section_provision(
    section: _CaliforniaSection,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    legal_identifier = f"Cal. {section.law_code} Code § {section.section}"
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-ca",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        citation_label=legal_identifier,
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=section.source_url,
        source_path=source_path,
        source_id=section.source_id,
        source_format=CALIFORNIA_BULK_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.parent_citation_path,
        parent_id=deterministic_provision_id(section.parent_citation_path),
        level=section.level,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=legal_identifier,
        identifiers={
            "california:law_code": section.law_code,
            "california:section": section.section,
        },
        metadata=_california_section_metadata(section),
    )


def _texas_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": TEXAS_USER_AGENT})
    return session


def _load_texas_current_message(
    session: requests.Session,
    source_root: Path | None,
    download_root: Path | None,
) -> str | None:
    relative_name = "metadata/StatutesCurrentMsg.txt"
    if source_root is not None:
        path = _texas_source_file(source_root, relative_name)
        if path.exists():
            return path.read_text(encoding="utf-8").strip() or None
        return None
    try:
        response = session.get(
            f"{TEXAS_TCAS_API_BASE}/GetProperty/StatutesCurrentMsg",
            timeout=60,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None
    text = response.text.strip()
    if download_root is not None:
        _write_texas_download(download_root, relative_name, text.encode("utf-8"))
    return text or None


def _load_texas_asset(
    session: requests.Session,
    source_root: Path | None,
    download_root: Path | None,
    *,
    relative_name: str,
    url: str,
) -> bytes:
    if source_root is not None:
        return _texas_source_file(source_root, relative_name).read_bytes()
    data = _request_bytes(session, url)
    if download_root is not None:
        _write_texas_download(download_root, relative_name, data)
    return data


def _load_texas_code_tree(
    session: requests.Session,
    source_root: Path | None,
    download_root: Path | None,
    code: _TexasCode,
) -> bytes:
    relative_name = f"trees/{code.code}.json"
    if source_root is not None:
        return _texas_source_file(source_root, relative_name).read_bytes()
    value_path = quote(f"S/{code.code_id}", safe="")
    url = (
        f"{TEXAS_TCAS_API_BASE}/StatuteCode/GetTopLevelHeadings/"
        f"{value_path}/{code.code}/1/false/false"
    )
    data = _request_bytes(session, url)
    if download_root is not None:
        _write_texas_download(download_root, relative_name, data)
    return data


def _request_bytes(session: requests.Session, url: str) -> bytes:
    response = session.get(url, timeout=90)
    response.raise_for_status()
    return response.content


def _texas_source_file(source_root: Path, relative_name: str) -> Path:
    path = source_root / relative_name
    if path.exists():
        return path
    if relative_name.startswith("html/"):
        legacy = source_root / relative_name.removeprefix("html/")
        if legacy.exists():
            return legacy
    return path


def _write_texas_download(root: Path, relative_name: str, data: bytes) -> None:
    path = root / relative_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _texas_codes_from_asset(data: bytes) -> tuple[_TexasCode, ...]:
    payload = json.loads(data.decode("utf-8-sig"))
    rows = payload.get("StatuteCode") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError("Texas StatuteCodeTree asset has no StatuteCode list")
    codes: list[_TexasCode] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code_id = row.get("codeID")
        code = row.get("code")
        name = row.get("CodeName")
        if code_id is None or code is None or name is None:
            continue
        codes.append(_TexasCode(code_id=str(code_id), code=str(code), name=str(name)))
    return tuple(codes)


def _texas_code_filter(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{1,4}", text):
        raise ValueError(f"invalid Texas code filter: {value!r}")
    return text


def _texas_code_token(code: str) -> str:
    return code.lower()


def _texas_code_container(
    code: _TexasCode,
    *,
    source_path: str,
    sha256: str,
    current_message: str | None,
) -> _StateContainer:
    return _StateContainer(
        jurisdiction="us-tx",
        title=code.token,
        kind="code",
        num=code.code,
        heading=code.name,
        citation_path=f"us-tx/statute/{code.token}",
        parent_citation_path=None,
        level=0,
        ordinal=_ordinal(code.code_id),
        source_path=source_path,
        source_url=TEXAS_STATUTES_BASE_URL,
        source_id=code.code_id,
        source_format=TEXAS_TCAS_TREE_SOURCE_FORMAT,
        sha256=sha256,
        metadata={
            "code": code.code,
            "code_id": code.code_id,
            "code_name": code.name,
            "current_message": current_message,
        },
    )


def _texas_tree_items(
    data: Any,
    *,
    code: _TexasCode,
    root: _StateContainer,
    source_path: str,
    sha256: str,
    current_message: str | None,
) -> tuple[tuple[_StateContainer, ...], tuple[_TexasHtmlDocument, ...]]:
    if not isinstance(data, list):
        raise ValueError(f"Texas code tree for {code.code} is not a list")
    containers: list[_StateContainer] = []
    documents: list[_TexasHtmlDocument] = []
    seen_containers = {root.citation_path}
    seen_resources: set[str] = set()

    def walk(nodes: list[Any], parent: _StateContainer) -> None:
        for raw in nodes:
            if not isinstance(raw, dict):
                continue
            name = _texas_plain_text(str(raw.get("name") or ""))
            parsed = _parse_texas_container_heading(name)
            node_parent = parent
            if parsed is not None:
                kind, num, heading = parsed
                citation_path = f"{parent.citation_path}/{kind}-{_clean_path_token(num)}"
                if citation_path not in seen_containers:
                    seen_containers.add(citation_path)
                    container = _StateContainer(
                        jurisdiction="us-tx",
                        title=code.token,
                        kind=kind,
                        num=num,
                        heading=heading,
                        citation_path=citation_path,
                        parent_citation_path=parent.citation_path,
                        level=parent.level + 1,
                        ordinal=_ordinal(num),
                        source_path=source_path,
                        source_url=None,
                        source_id=str(raw.get("valuePath") or raw.get("value") or ""),
                        source_format=TEXAS_TCAS_TREE_SOURCE_FORMAT,
                        sha256=sha256,
                        metadata={
                            "code": code.code,
                            "code_id": code.code_id,
                            "code_name": code.name,
                            "prefix": kind,
                            "num": num,
                            "tree_name": name,
                            "value": raw.get("value"),
                            "value_path": raw.get("valuePath"),
                            "current_message": current_message,
                        },
                    )
                    containers.append(container)
                    node_parent = container
                else:
                    node_parent = next(
                        (
                            container
                            for container in containers
                            if container.citation_path == citation_path
                        ),
                        parent,
                    )
            htm_link = raw.get("htmLink")
            if htm_link:
                try:
                    resource_key = _texas_resource_key(str(htm_link))
                except ValueError:
                    continue
                if resource_key not in seen_resources:
                    seen_resources.add(resource_key)
                    documents.append(
                        _TexasHtmlDocument(
                            code=code.code,
                            resource_key=resource_key,
                            htm_link=str(htm_link),
                            parent_citation_path=node_parent.citation_path,
                            level=node_parent.level + 1,
                        )
                    )
            children = raw.get("children")
            if isinstance(children, list):
                walk(children, node_parent)

    walk(data, root)
    return tuple(containers), tuple(documents)


def _iter_texas_html_sources(
    session: requests.Session,
    source_root: Path | None,
    download_root: Path | None,
    documents: tuple[_TexasHtmlDocument, ...],
    *,
    workers: int,
) -> Iterator[tuple[_TexasHtmlDocument, bytes | None, str | None]]:
    if source_root is not None:
        for document in documents:
            try:
                yield (
                    document,
                    _texas_source_file(source_root, f"html/{document.resource_key}").read_bytes(),
                    None,
                )
            except OSError as exc:
                yield document, None, str(exc)
        return

    worker_count = max(1, workers)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        yield from executor.map(
            lambda document: _load_texas_html_source(session, download_root, document),
            documents,
        )


def _load_texas_html_source(
    session: requests.Session,
    download_root: Path | None,
    document: _TexasHtmlDocument,
) -> tuple[_TexasHtmlDocument, bytes | None, str | None]:
    try:
        data = _request_bytes(session, document.source_url)
        if download_root is not None:
            _write_texas_download(download_root, f"html/{document.resource_key}", data)
        return document, data, None
    except requests.RequestException as exc:
        return document, None, str(exc)


def _parse_texas_html_document(
    html_bytes: bytes,
    *,
    document: _TexasHtmlDocument,
    seen_container_paths: set[str],
    section_counts: dict[str, int],
) -> tuple[tuple[_StateContainer, ...], tuple[_TexasSection, ...]]:
    soup = BeautifulSoup(html_bytes.decode("utf-8-sig", errors="replace"), "html.parser")
    paragraphs = [tag for tag in soup.find_all("p") if isinstance(tag, Tag)]
    if not paragraphs:
        raise ValueError("Texas HTML document has no paragraphs")

    containers: list[_StateContainer] = []
    sections: list[_TexasSection] = []
    pending_anchors: tuple[str, ...] = ()
    current_section: dict[str, Any] | None = None
    current_body: list[str] = []
    current_tags: list[Tag] = []
    current_html_containers: dict[str, _StateContainer] = {}

    def finish_section() -> None:
        nonlocal current_section, current_body, current_tags
        if current_section is None:
            return
        body = "\n".join(current_body).strip() or None
        section = _TexasSection(
            code=str(current_section["code"]),
            section=str(current_section["section"]),
            variant=current_section["variant"],
            marker=str(current_section["marker"]),
            heading=current_section["heading"],
            body=body,
            source_id=current_section["source_id"],
            source_url=str(current_section["source_url"]),
            source_document_id=document.source_file_name,
            parent_citation_path=str(current_section["parent_citation_path"]),
            level=int(current_section["level"]),
            ordinal=_section_ordinal(str(current_section["section"])),
            references_to=_texas_references(
                tuple(current_tags),
                current_code=document.code,
                self_path=str(current_section["citation_path"]),
            ),
            anchors=tuple(current_section["anchors"]),
        )
        sections.append(section)
        current_section = None
        current_body = []
        current_tags = []

    for paragraph in paragraphs:
        text = _clean_text(paragraph.get_text(" ", strip=True))
        anchor_names = _texas_anchor_names(paragraph)
        section_anchor = _texas_section_anchor(paragraph)
        if section_anchor is not None:
            finish_section()
            anchor_text = _clean_text(section_anchor.get_text(" ", strip=True))
            parsed = _parse_texas_section_heading(anchor_text)
            if parsed is None:
                pending_anchors = anchor_names
                continue
            marker, section, heading = parsed
            base_citation_path = f"us-tx/statute/{_texas_code_token(document.code)}/{section}"
            occurrence = section_counts.get(base_citation_path, 0) + 1
            section_counts[base_citation_path] = occurrence
            variant = _texas_section_variant(heading, occurrence)
            citation_path = f"{base_citation_path}@{variant}" if variant else base_citation_path
            source_url = str(section_anchor.get("href") or f"{document.source_url}#{section}")
            first_body = _texas_section_first_body(text, anchor_text)
            current_section = {
                "code": document.code,
                "section": section,
                "variant": variant,
                "citation_path": citation_path,
                "marker": marker,
                "heading": heading,
                "source_id": _texas_source_id(anchor_names or pending_anchors),
                "source_url": source_url,
                "parent_citation_path": _texas_current_html_parent_path(
                    document,
                    current_html_containers,
                ),
                "level": _texas_current_html_parent_level(document, current_html_containers) + 1,
                "anchors": anchor_names or pending_anchors,
            }
            if first_body:
                current_body.append(first_body)
            current_tags.append(paragraph)
            pending_anchors = ()
            continue

        parsed_container = _parse_texas_container_heading(text)
        if parsed_container is not None and _is_texas_structural_heading(paragraph):
            kind, num, heading = parsed_container
            if kind in {"subchapter", "part", "article"}:
                finish_section()
                parent_path, parent_level = _texas_html_container_parent(
                    kind,
                    document,
                    current_html_containers,
                )
                citation_path = f"{parent_path}/{kind}-{_clean_path_token(num)}"
                if citation_path not in seen_container_paths:
                    seen_container_paths.add(citation_path)
                    container = _StateContainer(
                        jurisdiction="us-tx",
                        title=_texas_code_token(document.code),
                        kind=kind,
                        num=num,
                        heading=heading,
                        citation_path=citation_path,
                        parent_citation_path=parent_path,
                        level=parent_level + 1,
                        ordinal=_ordinal(num),
                        source_path="",
                        source_url=document.source_url,
                        source_id=None,
                        source_format=TEXAS_TCAS_HTML_SOURCE_FORMAT,
                        sha256="",
                        metadata={
                            "code": document.code,
                            "prefix": kind,
                            "num": num,
                            "resource_key": document.resource_key,
                        },
                    )
                    containers.append(container)
                    current_html_containers[kind] = container
                else:
                    current_html_containers[kind] = _StateContainer(
                        jurisdiction="us-tx",
                        title=_texas_code_token(document.code),
                        kind=kind,
                        num=num,
                        heading=heading,
                        citation_path=citation_path,
                        parent_citation_path=parent_path,
                        level=parent_level + 1,
                        ordinal=_ordinal(num),
                        source_path="",
                        source_url=document.source_url,
                        source_id=None,
                        source_format=TEXAS_TCAS_HTML_SOURCE_FORMAT,
                        sha256="",
                        metadata={},
                    )
                _texas_clear_deeper_html_containers(kind, current_html_containers)
            pending_anchors = anchor_names
            continue

        if text and current_section is not None:
            current_body.append(text)
            current_tags.append(paragraph)
        pending_anchors = anchor_names or pending_anchors

    finish_section()
    return tuple(containers), tuple(sections)


def _texas_plain_text(value: str) -> str:
    if "<" in value and ">" in value:
        value = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return _clean_text(value.replace("\xa0", " "))


def _parse_texas_container_heading(text: str) -> tuple[str, str, str] | None:
    clean = _texas_plain_text(text)
    match = re.match(
        r"^(?P<prefix>TITLE|SUBTITLE|CHAPTER|SUBCHAPTER|PART|ARTICLE)\s+"
        r"(?P<num>[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*)\.?\s*(?P<label>.*)$",
        clean,
        re.IGNORECASE,
    )
    if not match:
        return None
    prefix = match.group("prefix").lower()
    num = match.group("num")
    label = _clean_text(match.group("label").strip(" ."))
    return prefix, num, label or clean


def _is_texas_structural_heading(tag: Tag) -> bool:
    class_attr = tag.get("class")
    classes = (
        {str(value).lower() for value in class_attr}
        if isinstance(class_attr, list)
        else ({str(class_attr).lower()} if class_attr else set())
    )
    style = str(tag.get("style") or "").lower()
    return "center" in classes or "font-weight:bold" in style or "font-weight: bold" in style


def _texas_anchor_names(tag: Tag) -> tuple[str, ...]:
    names: list[str] = []
    for anchor in tag.find_all("a"):
        if not isinstance(anchor, Tag):
            continue
        name = anchor.get("name")
        if name:
            names.append(str(name))
    return tuple(names)


def _texas_section_anchor(tag: Tag) -> Tag | None:
    for anchor in tag.find_all("a"):
        if not isinstance(anchor, Tag):
            continue
        if _parse_texas_section_heading(anchor.get_text(" ", strip=True)) is not None:
            return anchor
    return None


def _parse_texas_section_heading(text: str) -> tuple[str, str, str | None] | None:
    clean = _clean_text(text)
    match = re.match(
        r"^(?P<marker>Sec\.|Art\.)\s+"
        r"(?P<section>[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*[A-Za-z]?)\.\s*"
        r"(?P<heading>.*)$",
        clean,
        re.IGNORECASE,
    )
    if not match:
        return None
    marker = "Art." if match.group("marker").lower().startswith("art") else "Sec."
    heading = _clean_text(match.group("heading").strip(" .")) or None
    return marker, match.group("section"), heading


def _texas_section_first_body(full_text: str, heading_text: str) -> str | None:
    full = _clean_text(full_text)
    heading = _clean_text(heading_text)
    if full.startswith(heading):
        return _clean_text(full[len(heading) :]) or None
    return None


def _texas_section_variant(heading: str | None, occurrence: int) -> str | None:
    if occurrence <= 1:
        return None
    stem = _clean_path_token(heading or "duplicate")
    return f"{stem}-{occurrence}"


def _texas_source_id(anchors: tuple[str, ...]) -> str | None:
    for anchor in reversed(anchors):
        if re.fullmatch(r"\d+(?:\.\d+)?", anchor):
            return anchor
    return anchors[0] if anchors else None


def _texas_current_html_parent_path(
    document: _TexasHtmlDocument,
    current_html_containers: dict[str, _StateContainer],
) -> str:
    for kind in ("article", "part", "subchapter"):
        container = current_html_containers.get(kind)
        if container is not None:
            return container.citation_path
    return document.parent_citation_path


def _texas_current_html_parent_level(
    document: _TexasHtmlDocument,
    current_html_containers: dict[str, _StateContainer],
) -> int:
    for kind in ("article", "part", "subchapter"):
        container = current_html_containers.get(kind)
        if container is not None:
            return container.level
    return document.level - 1


def _texas_html_container_parent(
    kind: str,
    document: _TexasHtmlDocument,
    current_html_containers: dict[str, _StateContainer],
) -> tuple[str, int]:
    if kind == "article":
        for parent_kind in ("part", "subchapter"):
            parent = current_html_containers.get(parent_kind)
            if parent is not None:
                return parent.citation_path, parent.level
    if kind == "part":
        parent = current_html_containers.get("subchapter")
        if parent is not None:
            return parent.citation_path, parent.level
    return document.parent_citation_path, document.level - 1


def _texas_clear_deeper_html_containers(
    kind: str,
    current_html_containers: dict[str, _StateContainer],
) -> None:
    order = ("subchapter", "part", "article")
    if kind not in order:
        return
    index = order.index(kind)
    for deeper in order[index + 1 :]:
        current_html_containers.pop(deeper, None)


def _texas_references(
    tags: tuple[Tag, ...],
    *,
    current_code: str,
    self_path: str,
) -> tuple[str, ...]:
    refs: set[str] = set()
    for tag in tags:
        for anchor in tag.find_all("a"):
            if not isinstance(anchor, Tag):
                continue
            href = anchor.get("href")
            if not href:
                continue
            ref = _texas_href_to_citation_path(str(href), current_code=current_code)
            if ref and ref != self_path:
                refs.add(ref)
    return tuple(sorted(refs))


def _texas_href_to_citation_path(href: str, *, current_code: str) -> str | None:
    parsed = urlparse(href)
    query = parse_qs(parsed.query)
    value = query.get("Value") or query.get("value")
    code = query.get("Code") or query.get("code")
    if value:
        ref_code = (code[0] if code else current_code).upper()
        return _texas_value_to_citation_path(ref_code, value[0])
    path_parts = [part for part in parsed.path.split("/") if part]
    if "htm" in path_parts:
        htm_index = path_parts.index("htm")
        if htm_index > 0 and parsed.fragment:
            return _texas_value_to_citation_path(path_parts[htm_index - 1].upper(), parsed.fragment)
    return None


def _texas_value_to_citation_path(code: str, value: str) -> str | None:
    clean_value = _clean_text(value).strip("#")
    if not clean_value:
        return None
    return f"us-tx/statute/{_texas_code_token(code)}/{clean_value}"


def _texas_section_provision(
    section: _TexasSection,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-tx",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        citation_label=f"{section.marker} {section.section}",
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=section.source_url,
        source_path=source_path,
        source_id=section.source_id,
        source_format=TEXAS_TCAS_HTML_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.parent_citation_path,
        parent_id=deterministic_provision_id(section.parent_citation_path),
        level=section.level,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=f"{section.marker} {section.section}",
        identifiers={"texas:code": section.code, "texas:section": section.section},
        metadata={
            "code": section.code,
            "section": section.section,
            "variant": section.variant,
            "marker": section.marker,
            "anchors": list(section.anchors),
            "references_to": list(section.references_to),
            "source_document_id": section.source_document_id,
        },
    )


def _texas_resource_key(htm_link: str) -> str:
    link = htm_link.split("#", 1)[0].strip()
    key = link.removeprefix("/")
    file_name = key.rsplit("/", 1)[-1]
    if not key.endswith(".htm") or "/htm/" not in key or file_name == ".htm":
        raise ValueError(f"unsupported Texas HTML resource link: {htm_link!r}")
    return key


def _texas_resource_url(resource_key: str) -> str:
    return f"{TEXAS_TCAS_RESOURCE_BASE}/{resource_key.lstrip('/')}"


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
    import fitz

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
