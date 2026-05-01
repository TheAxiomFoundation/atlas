"""eCFR source discovery and normalized provision extraction."""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, TextIO, cast
from xml.etree import ElementTree as ET

from axiom_corpus.corpus.artifacts import CorpusArtifactStore, sha256_bytes
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.io import load_provisions
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

ECFR_API_BASE = "https://www.ecfr.gov/api/versioner/v1"
ECFR_READER_BASE = "https://www.ecfr.gov/current"
USER_AGENT = "axiom-corpus/0.1"
DEFAULT_CFR_TITLES = tuple(range(1, 51))


@dataclass(frozen=True)
class EcfrPartTarget:
    title: int
    part: str
    chapter: str | None = None
    subchapter: str | None = None
    label: str | None = None


@dataclass(frozen=True)
class EcfrInventory:
    items: tuple[SourceInventoryItem, ...]
    title_count: int
    part_count: int

    @property
    def unique_citation_count(self) -> int:
        return len({item.citation_path for item in self.items})


@dataclass(frozen=True)
class EcfrExtractReport:
    title_count: int
    part_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]
    title_error_count: int = 0
    title_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class _EcfrTitleResult:
    title: int
    provisions: tuple[ProvisionRecord, ...] = ()
    source_paths: tuple[Path, ...] = ()
    source_sha256: str | None = None
    error: str | None = None


def ecfr_run_id(
    version: str,
    only_title: int | None,
    only_part: str | None,
    limit: int | None,
) -> str:
    parts = [version]
    if only_title is not None:
        parts.append(f"title-{only_title}")
    if only_part is not None:
        parts.append(f"part-{only_part}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


_scoped_run_id = ecfr_run_id


def fetch_ecfr_structure(title: int, as_of: str) -> dict[str, Any]:
    url = f"{ECFR_API_BASE}/structure/{as_of}/title-{title}.json"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read())
    return cast(dict[str, Any], data)


def fetch_ecfr_title_xml(title: int, as_of: str) -> str:
    url = f"{ECFR_API_BASE}/full/{as_of}/title-{title}.xml"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=600) as resp:
        data = resp.read()
    return bytes(data).decode("utf-8")


def fetch_ecfr_part_xml(title: int, part: str, as_of: str) -> str:
    part_query = urllib.parse.quote(part, safe="")
    url = f"{ECFR_API_BASE}/full/{as_of}/title-{title}.xml?part={part_query}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=180) as resp:
        data = resp.read()
    return bytes(data).decode("utf-8")


def _retry_after_seconds(exc: urllib.error.HTTPError, attempt: int) -> float:
    retry_after = exc.headers.get("Retry-After")
    if retry_after:
        try:
            return min(300.0, max(1.0, float(retry_after)))
        except ValueError:
            pass
    if exc.code == 429:
        return min(300.0, 30.0 * attempt)
    return min(30.0, 2.0**attempt)


def _fetch_with_retries(label: str, fetch: Callable[[], str], retries: int = 8) -> str:
    last_exc: BaseException | None = None
    for attempt in range(1, retries + 1):
        try:
            return fetch()
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code in (404, 410):
                raise
            if attempt == retries:
                raise
            time.sleep(_retry_after_seconds(exc, attempt))
            continue
        except (TimeoutError, urllib.error.URLError) as exc:
            last_exc = exc
            if attempt == retries:
                raise
        time.sleep(min(30.0, 2.0**attempt))
    raise RuntimeError(f"failed to fetch {label}: {last_exc}")


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _element_text(elem: ET.Element) -> str:
    return _clean_text("".join(elem.itertext()))


def _ecfr_part_url(
    title: int,
    part: str,
    chapter: str | None,
    subchapter: str | None,
) -> str:
    base = f"{ECFR_READER_BASE}/title-{title}"
    if chapter and subchapter:
        return f"{base}/chapter-{chapter}/subchapter-{subchapter}/part-{part}"
    if chapter:
        return f"{base}/chapter-{chapter}/part-{part}"
    return f"{base}/part-{part}"


def _ecfr_subpart_url(
    title: int,
    part: str,
    subpart: str,
    chapter: str | None,
    subchapter: str | None,
) -> str:
    return f"{_ecfr_part_url(title, part, chapter, subchapter)}/subpart-{subpart}"


def _ecfr_section_url(
    title: int,
    part: str,
    section: str,
    chapter: str | None,
    subchapter: str | None,
) -> str:
    base = f"{ECFR_READER_BASE}/title-{title}"
    anchor = f"#p-{part}.{section}"
    if chapter and subchapter:
        return f"{base}/chapter-{chapter}/subchapter-{subchapter}/part-{part}{anchor}"
    if chapter:
        return f"{base}/chapter-{chapter}/part-{part}{anchor}"
    return f"{base}/part-{part}{anchor}"


def _ecfr_source_relative_name(title: int, only_part: str | None) -> str:
    if only_part is not None:
        return f"ecfr/title-{title}-part-{only_part}.xml"
    return f"ecfr/title-{title}.xml"


def _ecfr_source_key(run_id: str, title: int, only_part: str | None) -> str:
    return (
        f"sources/us/{DocumentClass.REGULATION.value}/{run_id}/"
        f"{_ecfr_source_relative_name(title, only_part)}"
    )


def _section_citation_from_identifier(title: int, identifier: str) -> tuple[str, str] | None:
    match = re.fullmatch(r"([0-9A-Za-z]+)\.([0-9A-Za-z][0-9A-Za-z.-]*)", identifier)
    if not match:
        return None
    part, section = match.groups()
    return f"us/regulation/{title}/{part}/{section}", section


def _title_from_citation_path(citation_path: str) -> int:
    parts = citation_path.split("/")
    if len(parts) < 4 or parts[0] != "us" or parts[1] != "regulation":
        raise ValueError(f"not an eCFR citation path: {citation_path}")
    return int(parts[2])


def _section_citation_from_element(title: int, elem: ET.Element) -> tuple[str, str, str] | None:
    n_attr = elem.get("N", "")
    match = re.search(r"([0-9A-Za-z]+)\.([0-9A-Za-z][0-9A-Za-z.-]*)", n_attr)
    if not match:
        return None
    part, section = match.groups()
    return f"us/regulation/{title}/{part}/{section}", part, section


def _walk_part_targets(
    node: dict[str, Any],
    title: int,
    chapter: str | None = None,
    subchapter: str | None = None,
) -> Iterator[EcfrPartTarget]:
    node_type = node.get("type")
    identifier = str(node.get("identifier") or "")
    if node_type == "chapter":
        chapter = identifier
    elif node_type == "subchapter":
        subchapter = identifier
    elif node_type == "part":
        if not node.get("reserved") and identifier:
            yield EcfrPartTarget(
                title=title,
                part=identifier,
                chapter=chapter,
                subchapter=subchapter,
                label=node.get("label"),
            )
        return

    for child in node.get("children", []) or ():
        yield from _walk_part_targets(child, title, chapter, subchapter)


def part_targets_from_structure(structure: dict[str, Any]) -> tuple[EcfrPartTarget, ...]:
    title = int(structure["identifier"])
    return tuple(_walk_part_targets(structure, title))


def _clean_part_heading(label: str | None, part: str) -> str | None:
    heading = _clean_text(label)
    heading = re.sub(rf"^Part\s+{re.escape(part)}\s*[—–-]\s*", "", heading, flags=re.I)
    return heading or None


def _clean_subpart_heading(label: str | None, subpart: str) -> str | None:
    heading = _clean_text(label)
    heading = re.sub(
        rf"^Subpart\s+{re.escape(subpart)}\s*[—–-]\s*",
        "",
        heading,
        flags=re.I,
    )
    return heading or None


def _section_ordinal(section: str) -> int | None:
    match = re.match(r"(\d+)", section)
    if not match:
        return None
    return int(match.group(1)) * 10 + (0 if section.isdigit() else 1)


def _part_ordinal(part: str) -> int | None:
    return int(part) if part.isdigit() else None


def _subpart_ordinal(subpart: str) -> int | None:
    return ord(subpart.upper()) if len(subpart) == 1 and subpart.isalpha() else None


def _walk_inventory_items(
    node: dict[str, Any],
    title: int,
    run_id: str | None,
    only_part: str | None,
    source_sha256_by_title: Mapping[int, str] | None,
    chapter: str | None = None,
    subchapter: str | None = None,
    part: str | None = None,
    subpart: str | None = None,
) -> Iterator[SourceInventoryItem]:
    node_type = node.get("type")
    identifier = str(node.get("identifier") or "")
    source_path = (
        _ecfr_source_key(run_id, title, only_part)
        if run_id is not None
        else _ecfr_source_relative_name(title, only_part)
    )
    source_sha256 = source_sha256_by_title.get(title) if source_sha256_by_title else None
    if node_type == "chapter":
        chapter = identifier
    elif node_type == "subchapter":
        subchapter = identifier
    elif node_type == "part":
        if node.get("reserved") or not identifier:
            return
        part = identifier
        if only_part is not None and part != only_part:
            return
        part_path = f"us/regulation/{title}/{part}"
        yield SourceInventoryItem(
            citation_path=part_path,
            source_url=_ecfr_part_url(title, part, chapter, subchapter),
            source_path=source_path,
            source_format="ecfr-xml",
            sha256=source_sha256,
            metadata={
                "kind": "part",
                "title": title,
                "part": part,
                "chapter": chapter,
                "subchapter": subchapter,
                "label": node.get("label"),
                "heading": _clean_part_heading(node.get("label"), part),
            },
        )
    elif node_type == "subpart":
        if node.get("reserved") or not identifier or not part:
            return
        subpart = identifier
        subpart_path = f"us/regulation/{title}/{part}/subpart-{subpart}"
        yield SourceInventoryItem(
            citation_path=subpart_path,
            source_url=_ecfr_subpart_url(title, part, subpart, chapter, subchapter),
            source_path=source_path,
            source_format="ecfr-xml",
            sha256=source_sha256,
            metadata={
                "kind": "subpart",
                "title": title,
                "part": part,
                "subpart": subpart,
                "chapter": chapter,
                "subchapter": subchapter,
                "parent_citation_path": f"us/regulation/{title}/{part}",
                "label": node.get("label"),
                "heading": _clean_subpart_heading(node.get("label"), subpart),
            },
        )
    elif node_type == "section":
        if not node.get("reserved"):
            parsed = _section_citation_from_identifier(title, identifier)
            if parsed is not None:
                citation_path, section = parsed
                actual_part = citation_path.split("/")[-2]
                if only_part is not None and actual_part != only_part:
                    return
                parent_citation_path = (
                    f"us/regulation/{title}/{actual_part}/subpart-{subpart}"
                    if subpart
                    else f"us/regulation/{title}/{actual_part}"
                )
                yield SourceInventoryItem(
                    citation_path=citation_path,
                    source_url=_ecfr_section_url(
                        title,
                        actual_part,
                        section,
                        chapter,
                        subchapter,
                    ),
                    source_path=source_path,
                    source_format="ecfr-xml",
                    sha256=source_sha256,
                    metadata={
                        "kind": "section",
                        "title": title,
                        "part": part or actual_part,
                        "section": section,
                        "subpart": subpart,
                        "chapter": chapter,
                        "subchapter": subchapter,
                        "parent_citation_path": parent_citation_path,
                        "label": node.get("label"),
                        "label_description": node.get("label_description"),
                        "received_on": node.get("received_on"),
                    },
                )
        return

    for child in node.get("children", []) or ():
        yield from _walk_inventory_items(
            child,
            title,
            run_id,
            only_part,
            source_sha256_by_title,
            chapter,
            subchapter,
            part,
            subpart,
        )


def build_ecfr_inventory_from_structures(
    structures: tuple[dict[str, Any], ...],
    only_part: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
    source_sha256_by_title: Mapping[int, str] | None = None,
) -> EcfrInventory:
    items: list[SourceInventoryItem] = []
    part_count = 0
    for structure in structures:
        part_targets = tuple(
            target
            for target in part_targets_from_structure(structure)
            if only_part is None or target.part == only_part
        )
        part_count += len(part_targets)
        for item in _walk_inventory_items(
            structure,
            int(structure["identifier"]),
            run_id,
            only_part,
            source_sha256_by_title,
        ):
            items.append(item)
            if limit is not None and len(items) >= limit:
                return EcfrInventory(
                    items=tuple(items),
                    title_count=len(structures),
                    part_count=part_count,
                )
    return EcfrInventory(items=tuple(items), title_count=len(structures), part_count=part_count)


def build_ecfr_inventory(
    as_of: str,
    only_title: int | None = None,
    only_part: str | None = None,
    limit: int | None = None,
    run_id: str | None = None,
) -> EcfrInventory:
    titles = (only_title,) if only_title is not None else DEFAULT_CFR_TITLES
    structures = tuple(_fetch_available_structures(titles, as_of, strict=only_title is not None))
    return build_ecfr_inventory_from_structures(
        structures,
        only_part=only_part,
        limit=limit,
        run_id=run_id,
    )


def _fetch_available_structures(
    titles: tuple[int, ...],
    as_of: str,
    strict: bool,
) -> Iterator[dict[str, Any]]:
    for title in titles:
        try:
            yield fetch_ecfr_structure(title, as_of)
        except urllib.error.HTTPError as exc:
            if strict or exc.code not in (404, 410):
                raise


def _section_heading(elem: ET.Element, part: str, section: str) -> str | None:
    head = elem.find("HEAD")
    if head is None:
        return None
    heading = _element_text(head)
    heading = re.sub(rf"^§\s*{re.escape(part)}\.{re.escape(section)}\s*", "", heading)
    return heading.strip(" .") or None


def _section_body(elem: ET.Element) -> str:
    paragraphs = [_element_text(p) for p in elem.findall(".//P")]
    return "\n\n".join(paragraph for paragraph in paragraphs if paragraph)


def _ecfr_identifiers(
    title: int,
    part: str,
    section: str | None = None,
    subpart: str | None = None,
) -> dict[str, str]:
    identifiers = {"ecfr:title": str(title), "ecfr:part": part}
    if subpart:
        identifiers["ecfr:subpart"] = subpart
    if section:
        identifiers["ecfr:section"] = section
    return identifiers


def _metadata_text(metadata: Mapping[str, Any], key: str) -> str | None:
    value = metadata.get(key)
    if value is None:
        return None
    text = _clean_text(str(value))
    return text or None


def _metadata_int(metadata: Mapping[str, Any], key: str) -> int | None:
    value = metadata.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _inventory_heading(item: SourceInventoryItem) -> str | None:
    metadata = item.metadata or {}
    heading = _metadata_text(metadata, "heading")
    if heading:
        return heading
    kind = _metadata_text(metadata, "kind")
    label_description = _metadata_text(metadata, "label_description")
    if label_description:
        return label_description.strip(" .") or None
    label = _metadata_text(metadata, "label")
    if not label:
        return None
    if kind == "section":
        part = _metadata_text(metadata, "part")
        section = _metadata_text(metadata, "section")
        if part and section:
            label = re.sub(rf"^§\s*{re.escape(part)}\.{re.escape(section)}\s*", "", label)
        return label.strip(" .") or None
    if kind == "subpart":
        subpart = _metadata_text(metadata, "subpart")
        return _clean_subpart_heading(label, subpart) if subpart else label
    if kind == "part":
        part = _metadata_text(metadata, "part")
        return _clean_part_heading(label, part) if part else label
    return label


def _inventory_legal_identifier(metadata: Mapping[str, Any]) -> str | None:
    title = _metadata_int(metadata, "title")
    part = _metadata_text(metadata, "part")
    if title is None or not part:
        return None
    kind = _metadata_text(metadata, "kind")
    if kind == "section":
        section = _metadata_text(metadata, "section")
        if section:
            return f"{title} CFR {part}.{section}"
    if kind == "subpart":
        subpart = _metadata_text(metadata, "subpart")
        if subpart:
            return f"{title} CFR part {part}, subpart {subpart}"
    if kind == "part":
        return f"{title} CFR part {part}"
    return None


def _inventory_identifiers(metadata: Mapping[str, Any]) -> dict[str, str] | None:
    title = _metadata_int(metadata, "title")
    part = _metadata_text(metadata, "part")
    if title is None or not part:
        return None
    return _ecfr_identifiers(
        title,
        part,
        section=_metadata_text(metadata, "section"),
        subpart=_metadata_text(metadata, "subpart"),
    )


def _inventory_level(metadata: Mapping[str, Any]) -> int | None:
    kind = _metadata_text(metadata, "kind")
    if kind == "part":
        return 0
    if kind == "subpart":
        return 1
    if kind == "section":
        return 2 if _metadata_text(metadata, "subpart") else 1
    return None


def _inventory_ordinal(metadata: Mapping[str, Any]) -> int | None:
    kind = _metadata_text(metadata, "kind")
    if kind == "part":
        part = _metadata_text(metadata, "part")
        return _part_ordinal(part) if part else None
    if kind == "subpart":
        subpart = _metadata_text(metadata, "subpart")
        return _subpart_ordinal(subpart) if subpart else None
    if kind == "section":
        section = _metadata_text(metadata, "section")
        return _section_ordinal(section) if section else None
    return None


def _structure_only_provision(
    item: SourceInventoryItem,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    metadata = dict(item.metadata or {})
    parent_citation_path = _metadata_text(metadata, "parent_citation_path")
    legal_identifier = _inventory_legal_identifier(metadata)
    metadata.update(
        {
            "body_status": "not_in_ecfr_full_xml",
            "structure_only": True,
        }
    )
    return ProvisionRecord(
        id=deterministic_provision_id(item.citation_path),
        jurisdiction="us",
        document_class=DocumentClass.REGULATION.value,
        citation_path=item.citation_path,
        citation_label=legal_identifier,
        heading=_inventory_heading(item),
        body=None,
        version=version,
        source_url=item.source_url,
        source_path=item.source_path,
        source_format=item.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=parent_citation_path,
        parent_id=deterministic_provision_id(parent_citation_path)
        if parent_citation_path
        else None,
        level=_inventory_level(metadata),
        ordinal=_inventory_ordinal(metadata),
        kind=_metadata_text(metadata, "kind"),
        legal_identifier=legal_identifier,
        identifiers=_inventory_identifiers(metadata),
        metadata=metadata,
    )


def _records_with_structure_only_placeholders(
    inventory: tuple[SourceInventoryItem, ...],
    existing_records: Mapping[str, ProvisionRecord],
    version: str,
    source_as_of: str,
    expression_date: str,
    failed_titles: set[int],
) -> tuple[ProvisionRecord, ...]:
    records_by_citation = dict(existing_records)
    for item in inventory:
        if item.citation_path in records_by_citation:
            continue
        title = _title_from_citation_path(item.citation_path)
        if title in failed_titles or not item.sha256:
            continue
        records_by_citation[item.citation_path] = _structure_only_provision(
            item,
            version=version,
            source_as_of=source_as_of,
            expression_date=expression_date,
        )
    return tuple(
        records_by_citation[item.citation_path]
        for item in inventory
        if item.citation_path in records_by_citation
    )


def _part_provision(
    elem: ET.Element,
    target: EcfrPartTarget,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    part = target.part
    citation_path = f"us/regulation/{target.title}/{part}"
    head = elem.find("HEAD")
    heading = _clean_part_heading(_element_text(head) if head is not None else target.label, part)
    return ProvisionRecord(
        id=deterministic_provision_id(citation_path),
        jurisdiction="us",
        document_class=DocumentClass.REGULATION.value,
        citation_path=citation_path,
        citation_label=f"{target.title} CFR part {part}",
        heading=heading,
        body=None,
        version=version,
        source_url=_ecfr_part_url(target.title, part, target.chapter, target.subchapter),
        source_path=source_path,
        source_id=elem.get("NODE"),
        source_format="ecfr-xml",
        source_as_of=source_as_of,
        expression_date=expression_date,
        level=0,
        ordinal=_part_ordinal(part),
        kind="part",
        legal_identifier=f"{target.title} CFR part {part}",
        identifiers=_ecfr_identifiers(target.title, part),
        metadata={
            "title": target.title,
            "part": part,
            "chapter": target.chapter,
            "subchapter": target.subchapter,
        },
    )


def _subpart_provision(
    elem: ET.Element,
    target: EcfrPartTarget,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord | None:
    subpart = elem.get("N")
    if not subpart:
        return None
    part = target.part
    part_path = f"us/regulation/{target.title}/{part}"
    citation_path = f"{part_path}/subpart-{subpart}"
    head = elem.find("HEAD")
    heading_text = _element_text(head) if head is not None else None
    heading = _clean_subpart_heading(heading_text, subpart)
    return ProvisionRecord(
        id=deterministic_provision_id(citation_path),
        jurisdiction="us",
        document_class=DocumentClass.REGULATION.value,
        citation_path=citation_path,
        citation_label=f"{target.title} CFR part {part}, subpart {subpart}",
        heading=f"Subpart {subpart} - {heading}" if heading else f"Subpart {subpart}",
        body=None,
        version=version,
        source_url=_ecfr_subpart_url(
            target.title, part, subpart, target.chapter, target.subchapter
        ),
        source_path=source_path,
        source_id=elem.get("NODE"),
        source_format="ecfr-xml",
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=part_path,
        parent_id=deterministic_provision_id(part_path),
        level=1,
        ordinal=_subpart_ordinal(subpart),
        kind="subpart",
        legal_identifier=f"{target.title} CFR part {part}, subpart {subpart}",
        identifiers=_ecfr_identifiers(target.title, part, subpart=subpart),
        metadata={
            "title": target.title,
            "part": part,
            "subpart": subpart,
            "chapter": target.chapter,
            "subchapter": target.subchapter,
        },
    )


def _section_provision(
    elem: ET.Element,
    title: int,
    target: EcfrPartTarget,
    version: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    parent_citation_path: str,
    level: int,
    subpart: str | None = None,
) -> ProvisionRecord | None:
    parsed = _section_citation_from_element(title, elem)
    if parsed is None:
        return None
    citation_path, part, section = parsed
    heading = _section_heading(elem, part, section)
    return ProvisionRecord(
        id=deterministic_provision_id(citation_path),
        jurisdiction="us",
        document_class=DocumentClass.REGULATION.value,
        citation_path=citation_path,
        citation_label=f"{title} CFR {part}.{section}",
        heading=heading,
        body=_section_body(elem),
        version=version,
        source_url=_ecfr_section_url(title, part, section, target.chapter, target.subchapter),
        source_path=source_path,
        source_id=elem.get("NODE"),
        source_format="ecfr-xml",
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=parent_citation_path,
        parent_id=deterministic_provision_id(parent_citation_path),
        level=level,
        ordinal=_section_ordinal(section),
        kind="section",
        legal_identifier=f"{title} CFR {part}.{section}",
        identifiers=_ecfr_identifiers(title, part, section=section, subpart=subpart),
        metadata={
            "title": title,
            "part": part,
            "section": section,
            "subpart": subpart,
            "chapter": target.chapter,
            "subchapter": target.subchapter,
        },
    )


def iter_ecfr_title_provisions(
    xml_content: str,
    targets: tuple[EcfrPartTarget, ...],
    version: str,
    source_path: str,
    source_as_of: str | None = None,
    expression_date: str | None = None,
    allowed_citation_paths: set[str] | None = None,
) -> Iterator[ProvisionRecord]:
    root = ET.fromstring(xml_content)
    target_by_part = {target.part: target for target in targets}
    for div5 in root.iter("DIV5"):
        if div5.get("TYPE") != "PART":
            continue
        part = div5.get("N")
        if not part:
            continue
        target = target_by_part.get(part)
        if target is None:
            continue
        part_record = _part_provision(
            div5,
            target,
            version=version,
            source_path=source_path,
            source_as_of=source_as_of or version,
            expression_date=expression_date or source_as_of or version,
        )
        if allowed_citation_paths is None or part_record.citation_path in allowed_citation_paths:
            yield part_record

        subpart_divs = tuple(
            div6 for div6 in div5.findall("./DIV6") if div6.get("TYPE") == "SUBPART"
        )
        if subpart_divs:
            for div6 in subpart_divs:
                subpart_record = _subpart_provision(
                    div6,
                    target,
                    version=version,
                    source_path=source_path,
                    source_as_of=source_as_of or version,
                    expression_date=expression_date or source_as_of or version,
                )
                if subpart_record is None:
                    continue
                if (
                    allowed_citation_paths is None
                    or subpart_record.citation_path in allowed_citation_paths
                ):
                    yield subpart_record
                for div8 in div6.iter("DIV8"):
                    if div8.get("TYPE") != "SECTION":
                        continue
                    record = _section_provision(
                        div8,
                        target.title,
                        target,
                        version=version,
                        source_path=source_path,
                        source_as_of=source_as_of or version,
                        expression_date=expression_date or source_as_of or version,
                        parent_citation_path=subpart_record.citation_path,
                        level=2,
                        subpart=div6.get("N"),
                    )
                    if record is None:
                        continue
                    if (
                        allowed_citation_paths is not None
                        and record.citation_path not in allowed_citation_paths
                    ):
                        continue
                    yield record
            continue

        parent_citation_path = f"us/regulation/{target.title}/{part}"
        for div8 in div5.iter("DIV8"):
            if div8.get("TYPE") != "SECTION":
                continue
            record = _section_provision(
                div8,
                target.title,
                target,
                version=version,
                source_path=source_path,
                source_as_of=source_as_of or version,
                expression_date=expression_date or source_as_of or version,
                parent_citation_path=parent_citation_path,
                level=1,
            )
            if record is None:
                continue
            if (
                allowed_citation_paths is not None
                and record.citation_path not in allowed_citation_paths
            ):
                continue
            yield record


def extract_ecfr(
    store: CorpusArtifactStore,
    version: str,
    as_of: str,
    expression_date: date | None = None,
    only_title: int | None = None,
    only_part: str | None = None,
    limit: int | None = None,
    workers: int = 2,
    progress_stream: TextIO | None = None,
) -> EcfrExtractReport:
    expression_date_text = (expression_date or date.fromisoformat(as_of)).isoformat()
    titles = (only_title,) if only_title is not None else DEFAULT_CFR_TITLES
    structures = tuple(_fetch_available_structures(titles, as_of, strict=only_title is not None))
    run_id = ecfr_run_id(version, only_title, only_part, limit)
    source_paths: list[Path] = []
    source_sha256_by_title: dict[int, str] = {}

    for structure in structures:
        title = int(structure["identifier"])
        structure_path = store.source_path(
            "us",
            DocumentClass.REGULATION,
            run_id,
            f"ecfr/title-{title}.structure.json",
        )
        store.write_json(structure_path, structure)
        source_paths.append(structure_path)

    inventory = build_ecfr_inventory_from_structures(
        structures,
        only_part=only_part,
        limit=limit,
        run_id=run_id,
    )
    allowed_citation_paths = {item.citation_path for item in inventory.items}

    existing_records = {
        record.citation_path: record
        for record in load_provisions(store.provisions_path("us", DocumentClass.REGULATION, run_id))
        if record.citation_path in allowed_citation_paths
    }
    title_paths: dict[int, set[str]] = {}
    for citation_path in allowed_citation_paths:
        title_paths.setdefault(_title_from_citation_path(citation_path), set()).add(citation_path)

    pending_titles: list[tuple[int, tuple[EcfrPartTarget, ...], set[str]]] = []
    for structure in structures:
        title = int(structure["identifier"])
        paths = title_paths.get(title, set())
        if not paths:
            continue
        if paths <= set(existing_records):
            continue
        targets = tuple(
            target
            for target in part_targets_from_structure(structure)
            if only_part is None or target.part == only_part
        )
        pending_titles.append((title, targets, paths))

    if pending_titles and progress_stream is not None:
        print(
            f"extracting {len(pending_titles)} eCFR title XML file(s) with "
            f"{min(max(1, workers), len(pending_titles))} worker(s)",
            file=progress_stream,
            flush=True,
        )

    title_errors: list[str] = []
    failed_titles: set[int] = set()
    for result in _extract_title_results(
        store,
        pending_titles,
        run_id,
        version,
        as_of,
        expression_date_text,
        allowed_citation_paths,
        only_part=only_part,
        workers=workers,
    ):
        source_paths.extend(result.source_paths)
        if result.source_sha256 is not None:
            source_sha256_by_title[result.title] = result.source_sha256
        if result.error is not None:
            failed_titles.add(result.title)
            title_errors.append(result.error)
            if progress_stream is not None:
                print(f"error title {result.title}: {result.error}", file=progress_stream)
            continue
        for record in result.provisions:
            existing_records[record.citation_path] = record
        if progress_stream is not None:
            print(
                f"extracted title {result.title} ({len(result.provisions)} provisions)",
                file=progress_stream,
                flush=True,
            )

    for structure in structures:
        title = int(structure["identifier"])
        source_path = store.source_path(
            "us",
            DocumentClass.REGULATION,
            run_id,
            _ecfr_source_relative_name(title, only_part),
        )
        if title not in source_sha256_by_title and source_path.exists():
            source_sha256_by_title[title] = sha256_bytes(source_path.read_bytes())

    inventory = build_ecfr_inventory_from_structures(
        structures,
        only_part=only_part,
        limit=limit,
        run_id=run_id,
        source_sha256_by_title=source_sha256_by_title,
    )
    inventory_path = store.inventory_path("us", DocumentClass.REGULATION, run_id)
    store.write_inventory(inventory_path, inventory.items)

    records = _records_with_structure_only_placeholders(
        inventory.items,
        existing_records,
        version=run_id,
        source_as_of=as_of,
        expression_date=expression_date_text,
        failed_titles=failed_titles,
    )
    provisions_path = store.provisions_path("us", DocumentClass.REGULATION, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        inventory.items,
        records,
        jurisdiction="us",
        document_class=DocumentClass.REGULATION.value,
        version=run_id,
    )
    coverage_path = store.coverage_path("us", DocumentClass.REGULATION, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return EcfrExtractReport(
        title_count=len(structures),
        part_count=inventory.part_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
        title_error_count=len(title_errors),
        title_errors=tuple(title_errors),
    )


def _extract_title_results(
    store: CorpusArtifactStore,
    pending_titles: list[tuple[int, tuple[EcfrPartTarget, ...], set[str]]],
    run_id: str,
    version: str,
    as_of: str,
    expression_date: str,
    allowed_citation_paths: set[str],
    *,
    only_part: str | None,
    workers: int,
) -> Iterator[_EcfrTitleResult]:
    max_workers = max(1, workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _extract_one_title,
                store,
                title,
                targets,
                run_id,
                version,
                as_of,
                expression_date,
                allowed_citation_paths,
                only_part,
            ): title
            for title, targets, _paths in pending_titles
        }
        for future in as_completed(futures):
            yield future.result()


def _extract_one_title(
    store: CorpusArtifactStore,
    title: int,
    targets: tuple[EcfrPartTarget, ...],
    run_id: str,
    version: str,
    as_of: str,
    expression_date: str,
    allowed_citation_paths: set[str],
    only_part: str | None,
) -> _EcfrTitleResult:
    source_relative_name = _ecfr_source_relative_name(title, only_part)
    source_path = store.source_path(
        "us",
        DocumentClass.REGULATION,
        run_id,
        source_relative_name,
    )
    try:
        if source_path.exists():
            source_bytes = source_path.read_bytes()
            xml_content = source_bytes.decode("utf-8")
            source_sha256 = sha256_bytes(source_bytes)
        else:
            fetch_label = f"{title} CFR part {only_part}" if only_part else f"{title} CFR"
            xml_content = _fetch_with_retries(
                fetch_label,
                lambda: (
                    fetch_ecfr_part_xml(title, only_part, as_of)
                    if only_part is not None
                    else fetch_ecfr_title_xml(title, as_of)
                ),
            )
            source_sha256 = store.write_text(source_path, xml_content)
        provisions = tuple(
            iter_ecfr_title_provisions(
                xml_content,
                targets,
                version=run_id,
                source_path=_ecfr_source_key(run_id, title, only_part),
                source_as_of=as_of,
                expression_date=expression_date,
                allowed_citation_paths=allowed_citation_paths,
            )
        )
    except (TimeoutError, urllib.error.HTTPError, urllib.error.URLError, ET.ParseError) as exc:
        return _EcfrTitleResult(title=title, error=f"{title} CFR: {exc}")
    return _EcfrTitleResult(
        title=title,
        provisions=provisions,
        source_paths=(source_path,),
        source_sha256=source_sha256,
    )
