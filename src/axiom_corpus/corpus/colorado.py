"""Colorado source adapters for regulations and rule manuals."""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import quote, urljoin

import requests  # type: ignore[import-untyped]
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

COLORADO_CCR_BASE_URL = "https://www.sos.state.co.us"
COLORADO_CCR_WELCOME_URL = f"{COLORADO_CCR_BASE_URL}/CCR/Welcome.do"
COLORADO_CCR_BROWSE_URL = f"{COLORADO_CCR_BASE_URL}/CCR/NumericalDeptList.do"
COLORADO_CCR_SOURCE_FORMAT = "colorado-ccr-pdf"
COLORADO_CCR_RULE_INFO_SOURCE_FORMAT = "colorado-ccr-rule-info-html"
COLORADO_CCR_INDEX_SOURCE_FORMAT = "colorado-ccr-index-html"
COLORADO_CCR_USER_AGENT = "axiom-corpus/0.1"


@dataclass(frozen=True)
class ColoradoCcrExtractReport:
    """Result from a Colorado CCR extraction run."""

    jurisdiction: str
    document_class: str
    document_count: int
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
class _ColoradoCcrDocument:
    series: str
    title: str
    rule_info_url: str
    pdf_url: str
    rule_version_id: str
    effective_date: str
    department: str | None
    agency: str | None
    rule_info_html: bytes | None = None
    pdf_bytes: bytes | None = None
    source_file_name: str | None = None

    @property
    def token(self) -> str:
        return _ccr_path_token(self.series)


@dataclass(frozen=True)
class _ColoradoCcrDiscovery:
    current_through: str | None
    welcome_html: bytes | None
    browse_html: bytes | None
    documents: tuple[_ColoradoCcrDocument, ...]


@dataclass(frozen=True)
class _ColoradoCcrSection:
    document: _ColoradoCcrDocument
    section: str
    variant: str | None
    heading: str | None
    body: str | None
    source_id: str
    ordinal: int | None

    @property
    def citation_path(self) -> str:
        suffix = f"@{self.variant}" if self.variant else ""
        return f"us-co/regulation/{self.document.token}/{self.section}{suffix}"


@dataclass(frozen=True)
class _ColoradoCcrDocumentExtract:
    document: _ColoradoCcrDocument
    pdf_bytes: bytes
    full_text: str
    sections: tuple[_ColoradoCcrSection, ...]


def colorado_ccr_run_id(
    version: str,
    *,
    only_series: str | None = None,
    limit: int | None = None,
) -> str:
    """Return a scoped Colorado CCR ingest run id."""
    parts = [version]
    if only_series:
        parts.append(_ccr_path_token(only_series))
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def extract_colorado_ccr(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_series: str | None = None,
    limit: int | None = None,
    workers: int = 4,
    release_dir: str | Path | None = None,
    download_dir: str | Path | None = None,
    progress_stream: TextIO | None = None,
) -> ColoradoCcrExtractReport:
    """Snapshot current Colorado CCR PDFs and extract normalized provisions."""
    document_class = DocumentClass.REGULATION.value
    run_id = colorado_ccr_run_id(version, only_series=only_series, limit=limit)
    if release_dir is not None:
        discovery = _load_colorado_ccr_release_dir(
            Path(release_dir), only_series=only_series, limit=limit
        )
    else:
        discovery = _discover_colorado_ccr_current_documents(
            only_series=only_series,
            limit=limit,
            progress_stream=progress_stream,
        )
    if not discovery.documents:
        raise ValueError("no Colorado CCR documents discovered")
    source_as_of_text = source_as_of or discovery.current_through or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    download_root = Path(download_dir) if download_dir is not None and release_dir is None else None
    if download_root is not None:
        _write_colorado_ccr_release_dir(download_root, discovery)

    source_paths: list[Path] = []
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    errors: list[str] = []

    if discovery.welcome_html:
        welcome_path = store.source_path(
            "us-co",
            DocumentClass.REGULATION,
            run_id,
            "colorado-ccr-html/Welcome.html",
        )
        welcome_sha = store.write_bytes(welcome_path, discovery.welcome_html)
        source_paths.append(welcome_path)
    else:
        welcome_sha = None
    if discovery.browse_html:
        browse_path = store.source_path(
            "us-co",
            DocumentClass.REGULATION,
            run_id,
            "colorado-ccr-html/NumericalDeptList.html",
        )
        store.write_bytes(browse_path, discovery.browse_html)
        source_paths.append(browse_path)

    root_path = "us-co/regulation"
    root_source_path = (
        f"sources/us-co/{document_class}/{run_id}/colorado-ccr-html/Welcome.html"
        if discovery.welcome_html
        else None
    )
    items.append(
        SourceInventoryItem(
            citation_path=root_path,
            source_url=COLORADO_CCR_WELCOME_URL,
            source_path=root_source_path,
            source_format=COLORADO_CCR_INDEX_SOURCE_FORMAT,
            sha256=welcome_sha,
            metadata={
                "kind": "collection",
                "current_through": discovery.current_through,
                "document_count": len(discovery.documents),
            },
        )
    )
    records.append(
        _ccr_root_provision(
            version=run_id,
            source_path=root_source_path,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            current_through=discovery.current_through,
            document_count=len(discovery.documents),
        )
    )

    extracts = _extract_colorado_ccr_documents(
        discovery.documents,
        workers=workers,
        progress_stream=progress_stream,
        errors=errors,
    )
    for extract in extracts:
        document = extract.document
        document_relative = f"colorado-ccr-pdf/{_ccr_file_name(document.series)}"
        pdf_artifact_path = store.source_path(
            "us-co",
            DocumentClass.REGULATION,
            run_id,
            document_relative,
        )
        pdf_sha = store.write_bytes(pdf_artifact_path, extract.pdf_bytes)
        source_paths.append(pdf_artifact_path)
        pdf_source_key = f"sources/us-co/{document_class}/{run_id}/{document_relative}"
        if download_root is not None:
            (download_root / _ccr_file_name(document.series)).write_bytes(extract.pdf_bytes)

        rule_info_source_key = None
        rule_info_sha = None
        if document.rule_info_html:
            rule_info_relative = (
                f"colorado-ccr-html/rule-info/{_ccr_file_name(document.series, '.html')}"
            )
            rule_info_path = store.source_path(
                "us-co",
                DocumentClass.REGULATION,
                run_id,
                rule_info_relative,
            )
            rule_info_sha = store.write_bytes(rule_info_path, document.rule_info_html)
            source_paths.append(rule_info_path)
            rule_info_source_key = f"sources/us-co/{document_class}/{run_id}/{rule_info_relative}"

        document_path = f"us-co/regulation/{document.token}"
        document_metadata = _ccr_document_metadata(
            document,
            current_through=discovery.current_through,
            rule_info_source_path=rule_info_source_key,
            rule_info_sha256=rule_info_sha,
            section_count=len(extract.sections),
        )
        items.append(
            SourceInventoryItem(
                citation_path=document_path,
                source_url=document.pdf_url,
                source_path=pdf_source_key,
                source_format=COLORADO_CCR_SOURCE_FORMAT,
                sha256=pdf_sha,
                metadata={**document_metadata, "kind": "document"},
            )
        )
        records.append(
            _ccr_document_provision(
                document,
                version=run_id,
                source_path=pdf_source_key,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                full_text=extract.full_text if not extract.sections else None,
                metadata=document_metadata,
            )
        )

        for section in extract.sections:
            section_metadata = {
                **document_metadata,
                "kind": "section",
                "section": section.section,
                "parent_citation_path": document_path,
                "source_id": section.source_id,
            }
            if section.variant:
                section_metadata["variant"] = section.variant
            items.append(
                SourceInventoryItem(
                    citation_path=section.citation_path,
                    source_url=document.pdf_url,
                    source_path=pdf_source_key,
                    source_format=COLORADO_CCR_SOURCE_FORMAT,
                    sha256=pdf_sha,
                    metadata=section_metadata,
                )
            )
            records.append(
                _ccr_section_provision(
                    section,
                    version=run_id,
                    source_path=pdf_source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    metadata=section_metadata,
                )
            )

    if len(records) == 1:
        raise ValueError("no Colorado CCR provisions extracted")

    inventory_path = store.inventory_path("us-co", DocumentClass.REGULATION, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path("us-co", DocumentClass.REGULATION, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction="us-co",
        document_class=document_class,
        version=run_id,
    )
    coverage_path = store.coverage_path("us-co", DocumentClass.REGULATION, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return ColoradoCcrExtractReport(
        jurisdiction="us-co",
        document_class=document_class,
        document_count=len(extracts),
        section_count=sum(len(extract.sections) for extract in extracts),
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        skipped_source_count=len(discovery.documents) - len(extracts),
        errors=tuple(errors),
    )


def _discover_colorado_ccr_current_documents(
    *,
    only_series: str | None,
    limit: int | None,
    progress_stream: TextIO | None,
) -> _ColoradoCcrDiscovery:
    session = requests.Session()
    session.headers.update({"User-Agent": COLORADO_CCR_USER_AGENT})
    welcome_html = _get_bytes(session, COLORADO_CCR_WELCOME_URL)
    current_through = _ccr_current_through(welcome_html.decode("utf-8", errors="replace"))
    browse_html = _get_bytes(session, COLORADO_CCR_BROWSE_URL)
    agency_links = _ccr_agency_links(browse_html.decode("utf-8", errors="replace"))
    target_series = _normalize_series(only_series) if only_series else None
    rule_links: dict[str, tuple[str, str | None, str | None]] = {}
    for index, (agency_url, department, agency) in enumerate(agency_links, start=1):
        if progress_stream and index % 50 == 0:
            print(
                f"discovered Colorado CCR agency page {index}/{len(agency_links)}",
                file=progress_stream,
            )
        agency_html = _get_text(session, agency_url)
        for rule_url, link_series in _ccr_rule_info_links(agency_html):
            if target_series and _normalize_series(link_series) != target_series:
                continue
            rule_links.setdefault(rule_url, (rule_url, department, agency))

    documents: list[_ColoradoCcrDocument] = []
    for index, (rule_url, department, agency) in enumerate(rule_links.values(), start=1):
        if limit is not None and len(documents) >= limit:
            break
        if progress_stream and index % 100 == 0:
            print(f"read Colorado CCR rule info {index}/{len(rule_links)}", file=progress_stream)
        rule_info_html = _get_bytes(session, rule_url)
        document = _ccr_document_from_rule_info(
            rule_url,
            rule_info_html,
            fallback_department=department,
            fallback_agency=agency,
        )
        if document is None:
            continue
        if target_series and _normalize_series(document.series) != target_series:
            continue
        documents.append(document)
    return _ColoradoCcrDiscovery(
        current_through=current_through,
        welcome_html=welcome_html,
        browse_html=browse_html,
        documents=tuple(
            sorted(documents, key=lambda document: _ccr_series_sort_key(document.series))
        ),
    )


def _load_colorado_ccr_release_dir(
    release_dir: Path,
    *,
    only_series: str | None,
    limit: int | None,
) -> _ColoradoCcrDiscovery:
    manifest_path = release_dir / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"Colorado CCR release manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    target_series = _normalize_series(only_series) if only_series else None
    documents: list[_ColoradoCcrDocument] = []
    for row in manifest.get("documents", []):
        series = str(row["series"])
        if target_series and _normalize_series(series) != target_series:
            continue
        pdf_file = release_dir / str(row["file_name"])
        rule_info_file_name = row.get("rule_info_file")
        rule_info_html = (
            (release_dir / str(rule_info_file_name)).read_bytes() if rule_info_file_name else None
        )
        documents.append(
            _ColoradoCcrDocument(
                series=series,
                title=str(row.get("title") or series),
                rule_info_url=str(row.get("rule_info_url") or ""),
                pdf_url=str(row.get("pdf_url") or ""),
                rule_version_id=str(row.get("rule_version_id") or ""),
                effective_date=str(
                    row.get("effective_date") or manifest.get("current_through") or ""
                ),
                department=row.get("department"),
                agency=row.get("agency"),
                rule_info_html=rule_info_html,
                pdf_bytes=pdf_file.read_bytes(),
                source_file_name=pdf_file.name,
            )
        )
        if limit is not None and len(documents) >= limit:
            break
    welcome_path = release_dir / "Welcome.html"
    browse_path = release_dir / "NumericalDeptList.html"
    return _ColoradoCcrDiscovery(
        current_through=manifest.get("current_through"),
        welcome_html=welcome_path.read_bytes() if welcome_path.exists() else None,
        browse_html=browse_path.read_bytes() if browse_path.exists() else None,
        documents=tuple(
            sorted(documents, key=lambda document: _ccr_series_sort_key(document.series))
        ),
    )


def _write_colorado_ccr_release_dir(release_dir: Path, discovery: _ColoradoCcrDiscovery) -> None:
    release_dir.mkdir(parents=True, exist_ok=True)
    if discovery.welcome_html:
        (release_dir / "Welcome.html").write_bytes(discovery.welcome_html)
    if discovery.browse_html:
        (release_dir / "NumericalDeptList.html").write_bytes(discovery.browse_html)
    manifest_documents: list[dict[str, Any]] = []
    for document in discovery.documents:
        file_name = _ccr_file_name(document.series)
        rule_info_file = f"rule-info-{_ccr_file_name(document.series, '.html')}"
        if document.rule_info_html:
            (release_dir / rule_info_file).write_bytes(document.rule_info_html)
        manifest_documents.append(
            {
                "series": document.series,
                "title": document.title,
                "rule_info_url": document.rule_info_url,
                "pdf_url": document.pdf_url,
                "rule_version_id": document.rule_version_id,
                "effective_date": document.effective_date,
                "department": document.department,
                "agency": document.agency,
                "file_name": file_name,
                "rule_info_file": rule_info_file if document.rule_info_html else None,
            }
        )
    (release_dir / "manifest.json").write_text(
        json.dumps(
            {
                "current_through": discovery.current_through,
                "documents": manifest_documents,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def _extract_colorado_ccr_documents(
    documents: Iterable[_ColoradoCcrDocument],
    *,
    workers: int,
    progress_stream: TextIO | None,
    errors: list[str],
) -> tuple[_ColoradoCcrDocumentExtract, ...]:
    document_tuple = tuple(documents)
    if workers <= 1:
        extracts: list[_ColoradoCcrDocumentExtract] = []
        for index, document in enumerate(document_tuple, start=1):
            if progress_stream and index % 25 == 0:
                print(
                    f"processed Colorado CCR PDF {index}/{len(document_tuple)}",
                    file=progress_stream,
                )
            try:
                extracts.append(_extract_colorado_ccr_document(document))
            except (requests.RequestException, RuntimeError, ValueError) as exc:
                errors.append(f"{document.series}: {exc}")
        return tuple(
            sorted(extracts, key=lambda extract: _ccr_series_sort_key(extract.document.series))
        )

    extracts = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(_extract_colorado_ccr_document, document): document
            for document in document_tuple
        }
        for index, future in enumerate(as_completed(future_map), start=1):
            document = future_map[future]
            if progress_stream and (index % 25 == 0 or index == len(future_map)):
                print(f"processed Colorado CCR PDF {index}/{len(future_map)}", file=progress_stream)
            try:
                extracts.append(future.result())
            except (requests.RequestException, RuntimeError, ValueError) as exc:
                errors.append(f"{document.series}: {exc}")
    return tuple(
        sorted(extracts, key=lambda extract: _ccr_series_sort_key(extract.document.series))
    )


def _extract_colorado_ccr_document(document: _ColoradoCcrDocument) -> _ColoradoCcrDocumentExtract:
    pdf_bytes = document.pdf_bytes
    if pdf_bytes is None:
        response = requests.get(
            document.pdf_url,
            timeout=120,
            headers={"User-Agent": COLORADO_CCR_USER_AGENT},
        )
        response.raise_for_status()
        pdf_bytes = response.content
    lines = _ccr_pdf_lines(pdf_bytes, document)
    full_text = "\n".join(lines).strip()
    sections = _ccr_sections_from_lines(lines, document)
    return _ColoradoCcrDocumentExtract(
        document=document,
        pdf_bytes=pdf_bytes,
        full_text=full_text,
        sections=sections,
    )


def _ccr_pdf_lines(data: bytes, document: _ColoradoCcrDocument) -> tuple[str, ...]:
    import fitz  # type: ignore[import-untyped]

    try:
        pdf = fitz.open(stream=data, filetype="pdf")
    except Exception as exc:  # pragma: no cover - depends on source corruption
        raise ValueError(f"cannot open PDF: {exc}") from exc
    lines: list[str] = []
    with pdf:
        for page in pdf:
            text = page.get_text("text", sort=True)
            for raw_line in text.splitlines():
                line = _clean_text(raw_line)
                if not line or _is_ccr_noise_line(line, document):
                    continue
                lines.append(line)
    if not lines:
        raise ValueError("PDF had no extractable text")
    return tuple(lines)


def _ccr_sections_from_lines(
    lines: tuple[str, ...],
    document: _ColoradoCcrDocument,
) -> tuple[_ColoradoCcrSection, ...]:
    sections: list[_ColoradoCcrSection] = []
    current_section: str | None = None
    current_heading: str | None = None
    current_body: list[str] = []
    current_source_id: str | None = None
    section_occurrences: dict[str, int] = {}
    seen_paths: set[str] = set()

    def finish() -> None:
        nonlocal current_section, current_heading, current_body, current_source_id
        if current_section is None:
            return
        body = "\n".join(current_body).strip() or None
        occurrence = section_occurrences.get(current_section, 0) + 1
        section_occurrences[current_section] = occurrence
        variant = (
            _ccr_section_variant(current_heading, body, occurrence) if occurrence > 1 else None
        )
        candidate = _ColoradoCcrSection(
            document=document,
            section=current_section,
            variant=variant,
            heading=current_heading,
            body=body,
            source_id=current_source_id or f"pdf-section-{len(sections) + 1}",
            ordinal=_ccr_section_ordinal(current_section),
        )
        if candidate.citation_path in seen_paths:
            candidate = _replace_ccr_section_variant(
                candidate, f"{variant or 'version'}-{occurrence}"
            )
        seen_paths.add(candidate.citation_path)
        sections.append(candidate)
        current_section = None
        current_heading = None
        current_body = []
        current_source_id = None

    for line in lines:
        parsed = _parse_ccr_section_heading(line)
        if parsed is not None:
            finish()
            current_section, current_heading = parsed
            current_source_id = f"pdf-section-{len(sections) + 1}"
            continue
        if current_section is not None:
            current_body.append(line)

    finish()
    return tuple(sections)


def _parse_ccr_section_heading(line: str) -> tuple[str, str | None] | None:
    match = re.match(
        r"^(?P<section>\d+(?:\.\d+)+[A-Za-z]?)"
        r"(?:\s+(?P<heading>[A-Z][A-Za-z0-9][^\n]{0,220}))?$",
        line,
    )
    if not match:
        return None
    heading = _clean_text(match.group("heading")) or None
    return match.group("section"), heading


def _ccr_section_variant(heading: str | None, body: str | None, occurrence: int) -> str:
    for value in (heading, body):
        token = _ccr_variant_token(value)
        if token:
            return token
    return f"version-{occurrence}"


def _ccr_variant_token(value: str | None) -> str | None:
    if not value:
        return None
    token = re.sub(r"[^a-z0-9.-]+", "-", value.lower()).strip("-.")
    return token[:100].strip("-.") or None


def _replace_ccr_section_variant(
    section: _ColoradoCcrSection,
    variant: str,
) -> _ColoradoCcrSection:
    return _ColoradoCcrSection(
        document=section.document,
        section=section.section,
        variant=variant,
        heading=section.heading,
        body=section.body,
        source_id=section.source_id,
        ordinal=section.ordinal,
    )


def _is_ccr_noise_line(line: str, document: _ColoradoCcrDocument) -> bool:
    normalized = line.replace(" ", "")
    if normalized in {"CodeofColoradoRegulations", "SecretaryofState", "StateofColorado"}:
        return True
    if re.fullmatch(r"\d+", line):
        return True
    if re.fullmatch(r"_+", line):
        return True
    if line == document.series or line == document.title:
        return True
    if document.agency and line == document.agency:
        return True
    if document.department and line == document.department:
        return True
    if line.startswith("[Editor") and "Notes follow the text" in line:
        return True
    return line.startswith("CODE OF COLORADO REGULATIONS")


def _ccr_agency_links(html: str) -> tuple[tuple[str, str | None, str | None], ...]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[tuple[str, str | None, str | None]] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = str(link.get("href"))
        if "NumericalCCRDocList.do" not in href:
            continue
        url = urljoin(COLORADO_CCR_BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)
        row_text = _clean_text(link.get_text(" ", strip=True))
        agency = re.sub(r"^\d+\s+", "", row_text) or None
        department = _query_value(url, "deptName")
        links.append((url, department, agency))
    return tuple(links)


def _ccr_rule_info_links(html: str) -> tuple[tuple[str, str], ...]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[tuple[str, str]] = []
    seen: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = str(link.get("href"))
        if "DisplayRule.do" not in href or "ruleinfo" not in href:
            continue
        url = urljoin(COLORADO_CCR_BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)
        links.append((url, _clean_text(link.get_text(" ", strip=True))))
    return tuple(links)


def _ccr_document_from_rule_info(
    rule_info_url: str,
    html_bytes: bytes,
    *,
    fallback_department: str | None,
    fallback_agency: str | None,
) -> _ColoradoCcrDocument | None:
    html = html_bytes.decode("utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")
    title_text = _clean_text(
        (soup.find("p", class_="pagehead5") or soup.find("h1") or soup).get_text(" ")
    )
    series = _query_value(rule_info_url, "seriesNum")
    if not series:
        match = re.match(r"(?P<series>\d+\s+CCR\s+\d+-\d+)", title_text)
        series = match.group("series") if match else None
    if not series:
        return None
    title = title_text.removeprefix(series).strip() or series
    current_match = re.search(
        r"Current version.*?OpenRuleWindow\('(?P<rule_version_id>\d+)',\s*'(?P<file_name>[^']+)'\s*\)"
        r".*?effective\s+(?P<effective_date>\d{1,2}/\d{1,2}/\d{4})",
        html,
        flags=re.S,
    )
    if not current_match:
        current_match = re.search(
            r"OpenRuleWindow\('(?P<rule_version_id>\d+)',\s*'(?P<file_name>[^']+)'\s*\)"
            r".*?(?P<effective_date>\d{1,2}/\d{1,2}/\d{4})",
            html,
            flags=re.S,
        )
    if not current_match:
        return None
    rule_version_id = current_match.group("rule_version_id")
    file_name = current_match.group("file_name")
    effective_date = _us_date_to_iso(current_match.group("effective_date"))
    pdf_url = (
        f"{COLORADO_CCR_BASE_URL}/CCR/GenerateRulePdf.do?"
        f"ruleVersionId={quote(rule_version_id)}&fileName={quote(file_name)}"
    )
    return _ColoradoCcrDocument(
        series=series,
        title=title,
        rule_info_url=rule_info_url,
        pdf_url=pdf_url,
        rule_version_id=rule_version_id,
        effective_date=effective_date,
        department=_query_value(rule_info_url, "deptName") or fallback_department,
        agency=_query_value(rule_info_url, "agencyName") or fallback_agency,
        rule_info_html=html_bytes,
    )


def _ccr_current_through(html: str) -> str | None:
    match = re.search(r"effective on or before\s*<b>(?P<date>\d{2}/\d{2}/\d{4})", html, flags=re.I)
    if not match:
        return None
    return _us_date_to_iso(match.group("date"))


def _ccr_root_provision(
    *,
    version: str,
    source_path: str | None,
    source_as_of: str,
    expression_date: str,
    current_through: str | None,
    document_count: int,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id("us-co/regulation"),
        jurisdiction="us-co",
        document_class=DocumentClass.REGULATION.value,
        citation_path="us-co/regulation",
        citation_label="Code of Colorado Regulations",
        heading="Code of Colorado Regulations",
        body=None,
        version=version,
        source_url=COLORADO_CCR_WELCOME_URL,
        source_path=source_path,
        source_format=COLORADO_CCR_INDEX_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=None,
        parent_id=None,
        level=0,
        ordinal=0,
        kind="collection",
        legal_identifier="Code of Colorado Regulations",
        identifiers={"state:code": "CCR"},
        metadata={"current_through": current_through, "document_count": document_count},
    )


def _ccr_document_provision(
    document: _ColoradoCcrDocument,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    full_text: str | None,
    metadata: dict[str, Any],
) -> ProvisionRecord:
    citation_path = f"us-co/regulation/{document.token}"
    return ProvisionRecord(
        id=deterministic_provision_id(citation_path),
        jurisdiction="us-co",
        document_class=DocumentClass.REGULATION.value,
        citation_path=citation_path,
        citation_label=document.series,
        heading=f"{document.series} {document.title}",
        body=full_text,
        version=version,
        source_url=document.pdf_url,
        source_path=source_path,
        source_id=document.rule_version_id,
        source_format=COLORADO_CCR_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path="us-co/regulation",
        parent_id=deterministic_provision_id("us-co/regulation"),
        level=1,
        ordinal=_ccr_series_ordinal(document.series),
        kind="document",
        legal_identifier=document.series,
        identifiers={"co:ccr": document.series, "co:rule_version_id": document.rule_version_id},
        metadata=metadata,
    )


def _ccr_section_provision(
    section: _ColoradoCcrSection,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    metadata: dict[str, Any],
) -> ProvisionRecord:
    parent_path = f"us-co/regulation/{section.document.token}"
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-co",
        document_class=DocumentClass.REGULATION.value,
        citation_path=section.citation_path,
        citation_label=f"{section.document.series} {section.section}",
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=section.document.pdf_url,
        source_path=source_path,
        source_id=section.source_id,
        source_format=COLORADO_CCR_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=parent_path,
        parent_id=deterministic_provision_id(parent_path),
        level=2,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=f"{section.document.series} {section.section}",
        identifiers={
            "co:ccr": section.document.series,
            "co:section": section.section,
            "co:rule_version_id": section.document.rule_version_id,
            **({"co:variant": section.variant} if section.variant else {}),
        },
        metadata=metadata,
    )


def _ccr_document_metadata(
    document: _ColoradoCcrDocument,
    *,
    current_through: str | None,
    rule_info_source_path: str | None,
    rule_info_sha256: str | None,
    section_count: int,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "series": document.series,
        "title": document.title,
        "rule_version_id": document.rule_version_id,
        "effective_date": document.effective_date,
        "department": document.department,
        "agency": document.agency,
        "rule_info_url": document.rule_info_url,
        "current_through": current_through,
        "section_count": section_count,
        "document_subtype": _ccr_document_subtype(document),
    }
    if rule_info_source_path:
        metadata["rule_info_source_path"] = rule_info_source_path
    if rule_info_sha256:
        metadata["rule_info_sha256"] = rule_info_sha256
    return metadata


def _ccr_document_subtype(document: _ColoradoCcrDocument) -> str:
    if "manual" in document.title.lower():
        return "rule_manual"
    return "rule"


def _get_bytes(session: requests.Session, url: str) -> bytes:
    response = session.get(url, timeout=120)
    response.raise_for_status()
    return bytes(response.content)


def _get_text(session: requests.Session, url: str) -> str:
    return _get_bytes(session, url).decode("utf-8", errors="replace")


def _query_value(url: str, key: str) -> str | None:
    match = re.search(rf"(?:[?&]){re.escape(key)}=(?P<value>[^&]+)", url)
    if not match:
        return None
    from urllib.parse import unquote_plus

    return unquote_plus(match.group("value"))


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _us_date_to_iso(value: str) -> str:
    month, day, year = value.rstrip(".").split("/")
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def _ccr_path_token(series: str) -> str:
    return re.sub(r"[^a-z0-9.-]+", "-", series.lower()).strip("-")


def _ccr_file_name(series: str, suffix: str = ".pdf") -> str:
    return f"{_ccr_path_token(series)}{suffix}"


def _normalize_series(series: str) -> str:
    return re.sub(r"\s+", " ", series.strip()).lower()


def _ccr_series_sort_key(series: str) -> tuple[int, int, int, str]:
    match = re.match(r"(?P<title>\d+)\s+CCR\s+(?P<agency>\d+)-(?P<rule>\d+)", series, flags=re.I)
    if not match:
        return (999_999, 999_999, 999_999, series)
    return (
        int(match.group("title")),
        int(match.group("agency")),
        int(match.group("rule")),
        series,
    )


def _ccr_series_ordinal(series: str) -> int | None:
    sort_key = _ccr_series_sort_key(series)
    if sort_key[0] == 999_999:
        return None
    return sort_key[0] * 1_000_000 + sort_key[1] * 1_000 + sort_key[2]


def _ccr_section_ordinal(section: str) -> int | None:
    numbers = [int(part) for part in re.findall(r"\d+", section)]
    if not numbers:
        return None
    ordinal = min(numbers[0], 99_999)
    for number in numbers[1:3]:
        ordinal = ordinal * 100 + min(number, 99)
    return ordinal


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


if __name__ == "__main__":  # pragma: no cover - ad hoc discovery helper
    discovery = _discover_colorado_ccr_current_documents(
        only_series=None,
        limit=None,
        progress_stream=sys.stderr,
    )
    print(
        json.dumps(
            {"current_through": discovery.current_through, "documents": len(discovery.documents)}
        )
    )
