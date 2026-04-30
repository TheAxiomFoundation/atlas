"""Federal US Code source adapter for source-first corpus ingestion."""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

USC_READER_BASE = "https://uscode.house.gov/view.xhtml"
USLM_SOURCE_FORMAT = "uslm-xml"

US_CODE_TITLE_NAMES: dict[str, str] = {
    "1": "General Provisions",
    "2": "The Congress",
    "3": "The President",
    "4": "Flag and Seal, Seat of Government, and the States",
    "5": "Government Organization and Employees",
    "6": "Domestic Security",
    "7": "Agriculture",
    "8": "Aliens and Nationality",
    "9": "Arbitration",
    "10": "Armed Forces",
    "11": "Bankruptcy",
    "12": "Banks and Banking",
    "13": "Census",
    "14": "Coast Guard",
    "15": "Commerce and Trade",
    "16": "Conservation",
    "17": "Copyrights",
    "18": "Crimes and Criminal Procedure",
    "19": "Customs Duties",
    "20": "Education",
    "21": "Food and Drugs",
    "22": "Foreign Relations and Intercourse",
    "23": "Highways",
    "24": "Hospitals and Asylums",
    "25": "Indians",
    "26": "Internal Revenue Code",
    "27": "Intoxicating Liquors",
    "28": "Judiciary and Judicial Procedure",
    "29": "Labor",
    "30": "Mineral Lands and Mining",
    "31": "Money and Finance",
    "32": "National Guard",
    "33": "Navigation and Navigable Waters",
    "34": "Crime Control and Law Enforcement",
    "35": "Patents",
    "36": "Patriotic and National Observances",
    "37": "Pay and Allowances of the Uniformed Services",
    "38": "Veterans' Benefits",
    "39": "Postal Service",
    "40": "Public Buildings, Property, and Works",
    "41": "Public Contracts",
    "42": "The Public Health and Welfare",
    "43": "Public Lands",
    "44": "Public Printing and Documents",
    "45": "Railroads",
    "46": "Shipping",
    "47": "Telecommunications",
    "48": "Territories and Insular Possessions",
    "49": "Transportation",
    "50": "War and National Defense",
    "51": "National and Commercial Space Programs",
    "52": "Voting and Elections",
    "54": "National Park Service and Related Programs",
}

_TITLE_IDENTIFIER_RE = re.compile(r"/us/usc/t(?P<title>[^/]+)")
_SECTION_IDENTIFIER_RE = re.compile(r"/us/usc/t(?P<title>[^/]+)/s(?P<section>[^/]+)")
_SECTION_NUM_RE = re.compile(r"(?:§+\s*|section\s+)?(?P<section>[0-9A-Za-z][0-9A-Za-z.-]*)", re.I)
_BODY_SKIP_TAGS = {"num", "heading", "sourceCredit", "notes", "annotations"}
_BODY_BLOCK_TAGS = {
    "p",
    "subsection",
    "paragraph",
    "subparagraph",
    "clause",
    "subclause",
    "item",
    "subitem",
    "continuation",
    "chapeau",
    "table",
}


@dataclass(frozen=True)
class UscSection:
    title: str
    section: str
    identifier: str | None
    heading: str | None
    body: str
    references_to: tuple[str, ...]

    @property
    def citation_path(self) -> str:
        return f"us/statute/{self.title}/{self.section}"


@dataclass(frozen=True)
class UscTitleDocument:
    title: str
    heading: str | None
    sections: tuple[UscSection, ...]
    created_date: str | None = None
    publication_name: str | None = None

    @property
    def citation_path(self) -> str:
        return f"us/statute/{self.title}"


@dataclass(frozen=True)
class UscInventory:
    items: tuple[SourceInventoryItem, ...]
    title_count: int
    section_count: int

    @property
    def unique_citation_count(self) -> int:
        return len({item.citation_path for item in self.items})


@dataclass(frozen=True)
class UscExtractReport:
    title: str | None
    title_count: int
    section_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]


def usc_run_id(version: str, title: str | int | None = None, limit: int | None = None) -> str:
    parts = [version]
    if title is not None:
        parts.append(f"title-{_clean_title_token(title)}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def decode_uslm_bytes(data: bytes) -> str:
    """Decode a USLM XML payload while tolerating a UTF-8 BOM."""
    return data.decode("utf-8-sig")


def infer_uslm_title(xml_content: str) -> str:
    """Return the US Code title token declared by a USLM document."""
    return parse_uslm_title(xml_content).title


def parse_uslm_title(xml_content: str, title: str | int | None = None) -> UscTitleDocument:
    root = ET.fromstring(xml_content)
    title_token = _clean_title_token(title) if title is not None else _title_from_xml(root)
    title_heading = _title_heading(root, title_token)
    sections = tuple(_iter_sections(root, title_token))
    return UscTitleDocument(
        title=title_token,
        heading=title_heading,
        sections=sections,
        created_date=_created_date(root),
        publication_name=_first_local_text(root, "docPublicationName"),
    )


def build_usc_inventory_from_xml(
    xml_content: str,
    *,
    title: str | int | None = None,
    run_id: str | None = None,
    source_sha256: str | None = None,
    source_download_url: str | None = None,
    limit: int | None = None,
) -> UscInventory:
    document = parse_uslm_title(xml_content, title=title)
    source_path = (
        _usc_source_key(run_id, document.title)
        if run_id is not None
        else _usc_source_relative_name(document.title)
    )
    items: list[SourceInventoryItem] = [
        SourceInventoryItem(
            citation_path=document.citation_path,
            source_url=_usc_title_url(document.title),
            source_path=source_path,
            source_format=USLM_SOURCE_FORMAT,
            sha256=source_sha256,
            metadata=_title_metadata(document, source_download_url),
        )
    ]
    for section in document.sections:
        items.append(
            SourceInventoryItem(
                citation_path=section.citation_path,
                source_url=_usc_section_url(section.title, section.section),
                source_path=source_path,
                source_format=USLM_SOURCE_FORMAT,
                sha256=source_sha256,
                metadata=_section_metadata(section, document, source_download_url),
            )
        )
        if limit is not None and len(items) >= limit:
            break
    return UscInventory(
        items=tuple(items[:limit] if limit is not None else items),
        title_count=1,
        section_count=len(document.sections),
    )


def iter_usc_title_provisions(
    xml_content: str,
    *,
    version: str,
    source_path: str,
    title: str | int | None = None,
    source_as_of: str | None = None,
    expression_date: str | None = None,
    source_download_url: str | None = None,
    allowed_citation_paths: set[str] | None = None,
) -> Iterator[ProvisionRecord]:
    document = parse_uslm_title(xml_content, title=title)
    source_as_of_text = source_as_of or document.created_date or version
    expression_date_text = expression_date or source_as_of_text
    title_record = _title_provision(
        document,
        version=version,
        source_path=source_path,
        source_as_of=source_as_of_text,
        expression_date=expression_date_text,
        source_download_url=source_download_url,
    )
    if allowed_citation_paths is None or title_record.citation_path in allowed_citation_paths:
        yield title_record

    for section in document.sections:
        if (
            allowed_citation_paths is not None
            and section.citation_path not in allowed_citation_paths
        ):
            continue
        yield _section_provision(
            section,
            document,
            version=version,
            source_path=source_path,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            source_download_url=source_download_url,
        )


def extract_usc(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_xml: str | Path,
    title: str | int | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    source_download_url: str | None = None,
    limit: int | None = None,
) -> UscExtractReport:
    source_xml_path = Path(source_xml)
    source_bytes = source_xml_path.read_bytes()
    xml_content = decode_uslm_bytes(source_bytes)
    document = parse_uslm_title(xml_content, title=title)
    run_id = usc_run_id(version, document.title, limit)
    source_relative_name = _usc_source_relative_name(document.title)
    source_artifact_path = store.source_path(
        "us",
        DocumentClass.STATUTE,
        run_id,
        source_relative_name,
    )
    source_sha256 = store.write_bytes(source_artifact_path, source_bytes)
    source_key = _usc_source_key(run_id, document.title)
    inventory = build_usc_inventory_from_xml(
        xml_content,
        title=document.title,
        run_id=run_id,
        source_sha256=source_sha256,
        source_download_url=source_download_url,
        limit=limit,
    )
    allowed_citation_paths = {item.citation_path for item in inventory.items}
    records = tuple(
        iter_usc_title_provisions(
            xml_content,
            version=run_id,
            source_path=source_key,
            title=document.title,
            source_as_of=source_as_of,
            expression_date=(
                _date_text(expression_date, source_as_of or version)
                if expression_date is not None
                else None
            ),
            source_download_url=source_download_url,
            allowed_citation_paths=allowed_citation_paths,
        )
    )
    inventory_path = store.inventory_path("us", DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, inventory.items)
    provisions_path = store.provisions_path("us", DocumentClass.STATUTE, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        inventory.items,
        records,
        jurisdiction="us",
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
    coverage_path = store.coverage_path("us", DocumentClass.STATUTE, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return UscExtractReport(
        title=document.title,
        title_count=1,
        section_count=inventory.section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=(source_artifact_path,),
    )


def extract_usc_directory(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path,
    only_title: str | int | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    source_download_url: str | None = None,
    limit: int | None = None,
) -> UscExtractReport:
    only_title_token = _clean_title_token(only_title) if only_title is not None else None
    run_id = usc_run_id(version, only_title_token, limit) if only_title_token or limit else version
    source_files = tuple(_iter_uslm_source_files(Path(source_dir), only_title_token))
    if not source_files:
        raise ValueError(f"no USLM XML files found in {source_dir}")

    all_items: list[SourceInventoryItem] = []
    all_records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    title_count = 0
    section_count = 0
    remaining = limit

    for source_xml_path in source_files:
        source_bytes = source_xml_path.read_bytes()
        xml_content = decode_uslm_bytes(source_bytes)
        document = parse_uslm_title(xml_content)
        if only_title_token and document.title != only_title_token:
            continue
        source_artifact_path = store.source_path(
            "us",
            DocumentClass.STATUTE,
            run_id,
            _usc_source_relative_name(document.title),
        )
        source_sha256 = store.write_bytes(source_artifact_path, source_bytes)
        source_key = _usc_source_key(run_id, document.title)
        source_as_of_text = source_as_of or document.created_date or version
        expression_date_text = _date_text(expression_date, source_as_of_text)
        inventory = build_usc_inventory_from_xml(
            xml_content,
            title=document.title,
            run_id=run_id,
            source_sha256=source_sha256,
            source_download_url=source_download_url,
            limit=remaining,
        )
        allowed_citation_paths = {item.citation_path for item in inventory.items}
        records = tuple(
            iter_usc_title_provisions(
                xml_content,
                version=run_id,
                source_path=source_key,
                title=document.title,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                source_download_url=source_download_url,
                allowed_citation_paths=allowed_citation_paths,
            )
        )
        all_items.extend(inventory.items)
        all_records.extend(records)
        source_paths.append(source_artifact_path)
        title_count += 1
        section_count += inventory.section_count
        if remaining is not None:
            remaining -= len(inventory.items)
            if remaining <= 0:
                break

    if not all_items:
        raise ValueError(f"no matching USLM XML files found in {source_dir}")

    inventory_path = store.inventory_path("us", DocumentClass.STATUTE, run_id)
    store.write_inventory(inventory_path, all_items)
    provisions_path = store.provisions_path("us", DocumentClass.STATUTE, run_id)
    store.write_provisions(provisions_path, all_records)
    coverage = compare_provision_coverage(
        tuple(all_items),
        tuple(all_records),
        jurisdiction="us",
        document_class=DocumentClass.STATUTE.value,
        version=run_id,
    )
    coverage_path = store.coverage_path("us", DocumentClass.STATUTE, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return UscExtractReport(
        title=only_title_token,
        title_count=title_count,
        section_count=section_count,
        provisions_written=len(all_records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _iter_uslm_source_files(source_dir: Path, only_title: str | None) -> Iterator[Path]:
    candidates: list[tuple[tuple[int, str], Path]] = []
    for path in source_dir.glob("usc*.xml"):
        match = re.fullmatch(r"usc(?P<title>[0-9]+[a-z]?)\.xml", path.name.lower())
        if not match:
            continue
        title = _clean_title_token(match.group("title"))
        if only_title is not None and title != only_title:
            continue
        candidates.append((_title_sort_key(title), path))
    for _key, path in sorted(candidates):
        yield path


def _title_sort_key(title: str) -> tuple[int, str]:
    match = re.fullmatch(r"(?P<number>\d+)(?P<suffix>[a-z]?)", title)
    if not match:
        return (10_000, title)
    return (int(match.group("number")), match.group("suffix"))


def _clean_title_token(value: str | int) -> str:
    text = str(value).strip().lower()
    if not re.fullmatch(r"[0-9]+[a-z]?", text):
        raise ValueError(f"invalid US Code title token: {value!r}")
    return text


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _iter_by_local(elem: ET.Element, name: str) -> Iterator[ET.Element]:
    if _local_name(elem.tag) == name:
        yield elem
    for child in elem:
        yield from _iter_by_local(child, name)


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _element_text(elem: ET.Element) -> str:
    if _local_name(elem.tag) == "table":
        return _table_to_markdown(elem)
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        child_text = _element_text(child)
        if child_text:
            parts.append(child_text)
        if child.tail:
            parts.append(child.tail)
    return _clean_text(" ".join(parts))


def _table_to_markdown(table: ET.Element) -> str:
    rows: list[list[str]] = []
    for row_elem in table.iter():
        if _local_name(row_elem.tag) != "tr":
            continue
        cells = [
            _clean_text(" ".join(cell.itertext()))
            for cell in row_elem
            if _local_name(cell.tag) in {"td", "th"}
        ]
        if cells:
            rows.append(cells)
    if not rows:
        return ""
    column_count = max(len(row) for row in rows)
    padded_rows = [row + [""] * (column_count - len(row)) for row in rows]
    widths = [max(3, max(len(row[index]) for row in padded_rows)) for index in range(column_count)]

    def format_row(row: list[str]) -> str:
        return (
            "| "
            + " | ".join(row[index].ljust(widths[index]) for index in range(column_count))
            + " |"
        )

    separator = "| " + " | ".join("-" * width for width in widths) + " |"
    return "\n".join(
        [format_row(padded_rows[0]), separator, *(format_row(row) for row in padded_rows[1:])]
    )


def _direct_child_text(elem: ET.Element, name: str) -> str | None:
    for child in elem:
        if _local_name(child.tag) == name:
            text = _element_text(child)
            return text or None
    return None


def _title_from_xml(root: ET.Element) -> str:
    for doc_number in _iter_by_local(root, "docNumber"):
        text = _clean_text(doc_number.text)
        if text:
            return _clean_title_token(text)
    for elem in _iter_by_local(root, "title"):
        title = _title_from_identifier(elem.get("identifier"))
        if title:
            return title
    title = _title_from_identifier(root.get("identifier"))
    if title:
        return title
    raise ValueError("cannot determine US Code title from USLM XML")


def _title_from_identifier(identifier: str | None) -> str | None:
    match = _TITLE_IDENTIFIER_RE.search(identifier or "")
    if not match:
        return None
    return _clean_title_token(match.group("title"))


def _first_local_text(root: ET.Element, name: str) -> str | None:
    for elem in _iter_by_local(root, name):
        text = _element_text(elem)
        if text:
            return text
    return None


def _created_date(root: ET.Element) -> str | None:
    text = _first_local_text(root, "created")
    if not text:
        return None
    return text.split("T", 1)[0]


def _section_from_identifier(identifier: str | None, title: str) -> str | None:
    match = _SECTION_IDENTIFIER_RE.search(identifier or "")
    if not match or _clean_title_token(match.group("title")) != title:
        return None
    return match.group("section")


def _section_from_num(elem: ET.Element) -> str | None:
    num_text = _direct_child_text(elem, "num")
    match = _SECTION_NUM_RE.search(num_text or "")
    return match.group("section").rstrip(".") if match else None


def _title_heading(root: ET.Element, title: str) -> str | None:
    fallback: str | None = None
    for elem in _iter_by_local(root, "title"):
        heading = _direct_child_text(elem, "heading")
        if not heading:
            continue
        if fallback is None:
            fallback = heading
        identifier_title = _title_from_identifier(elem.get("identifier"))
        if identifier_title == title:
            return heading
    return fallback or US_CODE_TITLE_NAMES.get(title) or f"Title {title}"


def _iter_sections(root: ET.Element, title: str) -> Iterator[UscSection]:
    seen: set[str] = set()
    for elem in _iter_by_local(root, "section"):
        identifier = elem.get("identifier")
        section = _section_from_identifier(identifier, title) or _section_from_num(elem)
        if not section:
            continue
        section = section.strip()
        citation_path = f"us/statute/{title}/{section}"
        if citation_path in seen:
            continue
        seen.add(citation_path)
        yield UscSection(
            title=title,
            section=section,
            identifier=identifier,
            heading=_direct_child_text(elem, "heading"),
            body=_section_body(elem),
            references_to=_extract_usc_references(elem),
        )


def _section_body(elem: ET.Element) -> str:
    parts: list[str] = []
    for child in elem:
        tag = _local_name(child.tag)
        if tag in _BODY_SKIP_TAGS:
            continue
        if tag == "content":
            parts.extend(_content_blocks(child))
            continue
        text = _element_text(child)
        if text:
            parts.append(text)
    return "\n\n".join(part for part in parts if part)


def _content_blocks(elem: ET.Element) -> list[str]:
    blocks: list[str] = []
    for child in elem:
        tag = _local_name(child.tag)
        if tag in _BODY_BLOCK_TAGS:
            text = _element_text(child)
            if text:
                blocks.append(text)
        elif tag == "content":
            blocks.extend(_content_blocks(child))
    if not blocks:
        text = _element_text(elem)
        if text:
            blocks.append(text)
    return blocks


def _extract_usc_references(elem: ET.Element) -> tuple[str, ...]:
    references: set[str] = set()
    for ref in _iter_by_local(elem, "ref"):
        href = ref.get("href")
        match = _SECTION_IDENTIFIER_RE.match(href or "")
        if match:
            references.add(
                f"us/statute/{_clean_title_token(match.group('title'))}/{match.group('section')}"
            )
    return tuple(sorted(references))


def _section_ordinal(section: str) -> int | None:
    match = re.match(r"(?P<number>\d+)(?P<suffix>.*)", section)
    if not match:
        return None
    suffix = match.group("suffix")
    suffix_offset = 0 if not suffix else 1
    return int(match.group("number")) * 10 + suffix_offset


def _title_ordinal(title: str) -> int | None:
    return int(title) if title.isdigit() else None


def _usc_title_url(title: str) -> str:
    return f"{USC_READER_BASE}?req=granuleid:USC-prelim-title{title}&num=0&edition=prelim"


def _usc_section_url(title: str, section: str) -> str:
    return (
        f"{USC_READER_BASE}?req=granuleid:USC-prelim-title{title}-section{section}"
        "&num=0&edition=prelim"
    )


def _usc_source_relative_name(title: str) -> str:
    return f"uslm/usc{title}.xml"


def _usc_source_key(run_id: str, title: str) -> str:
    return f"sources/us/{DocumentClass.STATUTE.value}/{run_id}/{_usc_source_relative_name(title)}"


def _usc_identifiers(
    title: str,
    section: str | None = None,
    source_id: str | None = None,
) -> dict[str, str]:
    identifiers = {"usc:title": title}
    if section is not None:
        identifiers["usc:section"] = section
    if source_id is not None:
        identifiers["uslm:identifier"] = source_id
    return identifiers


def _title_metadata(
    document: UscTitleDocument,
    source_download_url: str | None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "title",
        "title": document.title,
        "heading": document.heading,
        "section_count": len(document.sections),
    }
    if document.created_date:
        metadata["created_date"] = document.created_date
    if document.publication_name:
        metadata["publication_name"] = document.publication_name
    if source_download_url:
        metadata["source_download_url"] = source_download_url
    return metadata


def _section_metadata(
    section: UscSection,
    document: UscTitleDocument,
    source_download_url: str | None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": "section",
        "title": section.title,
        "section": section.section,
        "title_heading": document.heading,
        "heading": section.heading,
        "parent_citation_path": document.citation_path,
        "references_to": list(section.references_to),
    }
    if document.created_date:
        metadata["created_date"] = document.created_date
    if document.publication_name:
        metadata["publication_name"] = document.publication_name
    if section.identifier:
        metadata["identifier"] = section.identifier
    if source_download_url:
        metadata["source_download_url"] = source_download_url
    return metadata


def _title_provision(
    document: UscTitleDocument,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    source_download_url: str | None,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(document.citation_path),
        jurisdiction="us",
        document_class=DocumentClass.STATUTE.value,
        citation_path=document.citation_path,
        citation_label=f"Title {document.title}, U.S. Code",
        heading=document.heading,
        body=None,
        version=version,
        source_url=_usc_title_url(document.title),
        source_path=source_path,
        source_id=f"/us/usc/t{document.title}",
        source_format=USLM_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        level=0,
        ordinal=_title_ordinal(document.title),
        kind="title",
        legal_identifier=f"Title {document.title}, U.S. Code",
        identifiers=_usc_identifiers(document.title),
        metadata=_title_metadata(document, source_download_url),
    )


def _section_provision(
    section: UscSection,
    document: UscTitleDocument,
    *,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    source_download_url: str | None,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        citation_label=f"{section.title} U.S.C. § {section.section}",
        heading=section.heading,
        body=section.body,
        version=version,
        source_url=_usc_section_url(section.title, section.section),
        source_path=source_path,
        source_id=section.identifier,
        source_format=USLM_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=document.citation_path,
        parent_id=deterministic_provision_id(document.citation_path),
        level=1,
        ordinal=_section_ordinal(section.section),
        kind="section",
        legal_identifier=f"{section.title} U.S.C. § {section.section}",
        identifiers=_usc_identifiers(section.title, section.section, section.identifier),
        metadata=_section_metadata(section, document, source_download_url),
    )
