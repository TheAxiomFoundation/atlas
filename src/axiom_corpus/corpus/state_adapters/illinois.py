"""Illinois ILCS source-first corpus adapter."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urljoin

import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

ILLINOIS_ILCS_BASE_URL = "https://www.ilga.gov/ftp/ILCS/"
ILLINOIS_ILCS_SOURCE_FORMAT = "illinois-ilcs-html"
ILLINOIS_USER_AGENT = "axiom-corpus/0.1"

_DOC_NAME_RE = re.compile(
    r"^(?P<chapter>\d{4})(?P<act>\d{5})(?P<doc_type>[AFHK])(?P<identifier>.*?)(?:\.html?)?$",
    re.IGNORECASE,
)
_DOC_TOKEN_RE = re.compile(r"\b\d{9}[AFHK][A-Za-z0-9.\-]*\b")
_HREF_RE = re.compile(r"""href\s*=\s*["'](?P<href>[^"']+)["']""", re.IGNORECASE)
_ILCS_CITATION_RE = re.compile(
    r"\((?P<chapter>\d+)\s+ILCS\s+(?P<act>\d+)\s*/\s*(?P<section>[^)]+)\)"
)
_SECTION_HEADING_RE = re.compile(r"^(?:Sec\.|Section)\s+", re.IGNORECASE)


@dataclass(frozen=True)
class IllinoisIlcsDocumentName:
    """Parsed ILCS FTP document file name."""

    stem: str
    chapter: str
    act: str
    doc_type: str
    identifier: str

    @property
    def chapter_int(self) -> int:
        return int(self.chapter)

    @property
    def act_int(self) -> int:
        value = int(self.act)
        if self.act.endswith("0"):
            return value // 10
        return value

    @property
    def section(self) -> str | None:
        if self.doc_type != "K":
            return None
        return self.identifier or None

    @property
    def citation(self) -> str | None:
        if self.section is None:
            return None
        return f"{self.chapter_int} ILCS {self.act_int}/{self.section}"

    @property
    def citation_path(self) -> str:
        base = f"us-il/statute/{self.chapter_int}/{self.act_int}"
        if self.section is None:
            return base
        return f"{base}/{self.section}"

    @property
    def act_citation_path(self) -> str:
        return f"us-il/statute/{self.chapter_int}/{self.act_int}"

    @property
    def chapter_citation_path(self) -> str:
        return f"us-il/statute/{self.chapter_int}"


@dataclass(frozen=True)
class IllinoisIlcsSection:
    document: IllinoisIlcsDocumentName
    citation: str
    section: str
    heading: str | None
    body: str
    references_to: tuple[str, ...]

    @property
    def citation_path(self) -> str:
        return f"us-il/statute/{self.document.chapter_int}/{self.document.act_int}/{self.section}"


def parse_illinois_ilcs_doc_name(value: str) -> IllinoisIlcsDocumentName:
    """Parse an official ILCS FTP document token or HTML file name."""
    name = Path(unquote(value)).name
    stem = name.rsplit(".", 1)[0]
    match = _DOC_NAME_RE.match(name)
    if not match:
        raise ValueError(f"not an ILCS document name: {value!r}")
    return IllinoisIlcsDocumentName(
        stem=stem,
        chapter=match.group("chapter"),
        act=match.group("act"),
        doc_type=match.group("doc_type").upper(),
        identifier=match.group("identifier"),
    )


def parse_illinois_ilcs_links(text: str) -> tuple[str, ...]:
    """Extract ILCS document links or document tokens from FTP index/readme text."""
    links: list[str] = []
    seen: set[str] = set()
    seen_stems: set[str] = set()
    for href in _HREF_RE.findall(text):
        decoded = unquote(href)
        if (
            decoded.lower().endswith((".html", ".htm"))
            and _looks_like_ilcs_path(decoded)
            and decoded not in seen
        ):
            seen.add(decoded)
            seen_stems.add(parse_illinois_ilcs_doc_name(decoded).stem)
            links.append(decoded)
    for token in _DOC_TOKEN_RE.findall(text):
        stem = token.rsplit(".", 1)[0] if token.lower().endswith(".html") else token
        decoded = f"{stem}.html"
        if decoded not in seen and stem not in seen_stems:
            seen.add(decoded)
            seen_stems.add(stem)
            links.append(decoded)
    return tuple(links)


def parse_illinois_section_sequence(text: str) -> dict[str, int]:
    """Return official Section Sequence ordering by document stem."""
    sequence: dict[str, int] = {}
    for token in _DOC_TOKEN_RE.findall(text):
        stem = token.rsplit(".", 1)[0] if token.lower().endswith(".html") else token
        if stem not in sequence:
            sequence[stem] = len(sequence)
    return sequence


def parse_illinois_ilcs_section(
    html: str | bytes,
    *,
    document: IllinoisIlcsDocumentName,
) -> IllinoisIlcsSection:
    """Extract the canonical citation, heading, body, and references from an ILCS section page."""
    text = _html_text(html)
    citation_match = _ILCS_CITATION_RE.search(text)
    if citation_match:
        chapter = citation_match.group("chapter")
        act = citation_match.group("act")
        section = citation_match.group("section").strip()
        citation = f"{chapter} ILCS {act}/{section}"
    elif document.citation:
        section = document.section or ""
        citation = document.citation
    else:
        raise ValueError(f"ILCS section page has no citation: {document.stem}")

    heading = _section_heading(text, section)
    references = tuple(
        f"{match.group('chapter')} ILCS {match.group('act')}/{match.group('section').strip()}"
        for match in _ILCS_CITATION_RE.finditer(text)
    )
    return IllinoisIlcsSection(
        document=document,
        citation=citation,
        section=section,
        heading=heading,
        body=text,
        references_to=tuple(dict.fromkeys(ref for ref in references if ref != citation)),
    )


def extract_illinois_ilcs(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_chapter: str | int | None = None,
    only_act: str | int | None = None,
    limit: int | None = None,
    base_url: str = ILLINOIS_ILCS_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official Illinois ILCS HTML and extract normalized provisions."""
    jurisdiction = "us-il"
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    chapter_filter = int(only_chapter) if only_chapter is not None else None
    act_filter = int(only_act) if only_act is not None else None
    run_id = _illinois_run_id(
        version,
        jurisdiction=jurisdiction,
        chapter_filter=chapter_filter,
        act_filter=act_filter,
        limit=limit,
    )

    source_root = Path(source_dir) if source_dir is not None else None
    source_entries = (
        _discover_local_sources(source_root)
        if source_root is not None
        else _discover_remote_sources(
            base_url,
            limit=limit,
            chapter_filter=chapter_filter,
            act_filter=act_filter,
        )
    )
    sequence = (
        _load_local_section_sequence(source_root)
        if source_root is not None
        else _load_remote_section_sequence(base_url)
    )
    source_entries = tuple(
        sorted(
            source_entries,
            key=lambda entry: (
                sequence.get(entry[0].stem, 10_000_000),
                entry[0].chapter_int,
                entry[0].act_int,
                entry[0].doc_type,
                entry[0].identifier,
                entry[1],
            ),
        )
    )

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    errors: list[str] = []
    remaining = limit

    for document, relative_name, source_url, data in source_entries:
        if remaining is not None and remaining <= 0:
            break
        if chapter_filter is not None and document.chapter_int != chapter_filter:
            continue
        if act_filter is not None and document.act_int != act_filter:
            continue

        artifact_relative = f"illinois-ilcs/{relative_name}"
        artifact_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            artifact_relative,
        )
        sha256 = store.write_bytes(artifact_path, data)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, artifact_relative)

        chapter_path = document.chapter_citation_path
        if chapter_path not in seen:
            seen.add(chapter_path)
            title_count += 1
            container_count += 1
            items.append(
                _inventory_item(
                    citation_path=chapter_path,
                    source_url=source_url,
                    source_path=source_key,
                    sha256=sha256,
                    metadata={"kind": "chapter", "chapter": str(document.chapter_int)},
                )
            )
            records.append(
                _provision_record(
                    jurisdiction=jurisdiction,
                    citation_path=chapter_path,
                    version=run_id,
                    source_url=source_url,
                    source_path=source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    kind="chapter",
                    heading=f"{document.chapter_int} ILCS",
                    legal_identifier=f"{document.chapter_int} ILCS",
                    level=1,
                    ordinal=document.chapter_int,
                    identifiers={"ilcs:chapter": str(document.chapter_int)},
                )
            )

        act_path = document.act_citation_path
        if act_path not in seen:
            seen.add(act_path)
            container_count += 1
            act_title = _act_title(data) or f"{document.chapter_int} ILCS {document.act_int}"
            items.append(
                _inventory_item(
                    citation_path=act_path,
                    source_url=source_url,
                    source_path=source_key,
                    sha256=sha256,
                    metadata={
                        "kind": "act",
                        "chapter": str(document.chapter_int),
                        "act": str(document.act_int),
                        "parent_citation_path": chapter_path,
                    },
                )
            )
            records.append(
                _provision_record(
                    jurisdiction=jurisdiction,
                    citation_path=act_path,
                    version=run_id,
                    source_url=source_url,
                    source_path=source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    kind="act",
                    heading=act_title,
                    legal_identifier=f"{document.chapter_int} ILCS {document.act_int}",
                    parent_citation_path=chapter_path,
                    level=2,
                    ordinal=document.act_int,
                    identifiers={
                        "ilcs:chapter": str(document.chapter_int),
                        "ilcs:act": str(document.act_int),
                    },
                )
            )

        if document.doc_type != "K":
            skipped_source_count += 1
            continue
        try:
            section = parse_illinois_ilcs_section(data, document=document)
        except ValueError as exc:
            skipped_source_count += 1
            errors.append(f"{relative_name}: {exc}")
            continue
        if section.citation_path in seen:
            continue
        seen.add(section.citation_path)
        section_count += 1
        ordinal = sequence.get(document.stem)
        items.append(
            _inventory_item(
                citation_path=section.citation_path,
                source_url=source_url,
                source_path=source_key,
                sha256=sha256,
                metadata={
                    "kind": "section",
                    "chapter": str(document.chapter_int),
                    "act": str(document.act_int),
                    "section": section.section,
                    "heading": section.heading,
                    "parent_citation_path": act_path,
                    "references_to": list(section.references_to),
                    "source_id": document.stem,
                },
            )
        )
        records.append(
            _provision_record(
                jurisdiction=jurisdiction,
                citation_path=section.citation_path,
                version=run_id,
                source_url=source_url,
                source_path=source_key,
                source_id=document.stem,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind="section",
                heading=section.heading,
                body=section.body,
                legal_identifier=section.citation,
                parent_citation_path=act_path,
                level=3,
                ordinal=ordinal,
                identifiers={
                    "ilcs:chapter": str(document.chapter_int),
                    "ilcs:act": str(document.act_int),
                    "ilcs:section": section.section,
                },
                metadata={"references_to": list(section.references_to)},
            )
        )
        if remaining is not None:
            remaining -= 1

    if not items:
        raise ValueError("no Illinois ILCS provisions extracted")

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


def _discover_local_sources(
    source_root: Path | None,
) -> tuple[tuple[IllinoisIlcsDocumentName, str, str | None, bytes], ...]:
    if source_root is None:
        return ()
    entries: list[tuple[IllinoisIlcsDocumentName, str, str | None, bytes]] = []
    for html_path in sorted(path for path in source_root.rglob("*.html") if path.is_file()):
        try:
            document = parse_illinois_ilcs_doc_name(html_path.name)
        except ValueError:
            continue
        relative_name = html_path.relative_to(source_root).as_posix()
        entries.append((document, relative_name, f"file://{html_path}", html_path.read_bytes()))
    return tuple(entries)


def _discover_remote_sources(
    base_url: str,
    *,
    limit: int | None,
    chapter_filter: int | None,
    act_filter: int | None,
) -> tuple[tuple[IllinoisIlcsDocumentName, str, str | None, bytes], ...]:
    session = requests.Session()
    session.headers.update({"User-Agent": ILLINOIS_USER_AGENT})
    paths = _remote_document_paths(
        session,
        base_url,
        limit=limit,
        chapter_filter=chapter_filter,
        act_filter=act_filter,
    )
    entries: list[tuple[IllinoisIlcsDocumentName, str, str | None, bytes]] = []
    for relative_name in paths:
        try:
            document = parse_illinois_ilcs_doc_name(relative_name)
        except ValueError:
            continue
        url = urljoin(base_url, quote(relative_name, safe="/%"))
        response = session.get(url, timeout=20)
        response.raise_for_status()
        entries.append((document, relative_name, url, response.content))
    return tuple(entries)


def _remote_document_paths(
    session: requests.Session,
    base_url: str,
    *,
    limit: int | None,
    chapter_filter: int | None = None,
    act_filter: int | None = None,
) -> tuple[str, ...]:
    index = _remote_text(session, base_url)
    direct_links = [
        link
        for link in parse_illinois_ilcs_links(index)
        if "/" in link and _document_path_matches(link, chapter_filter, act_filter)
    ]
    if direct_links:
        return _limit_section_paths(direct_links, limit)

    paths: list[str] = []
    for chapter_href in _directory_hrefs(index, prefix="Ch"):
        if not _directory_matches(chapter_href, "Ch", chapter_filter):
            continue
        chapter_url = urljoin(base_url, quote(chapter_href, safe="/%"))
        chapter_text = _remote_text(session, chapter_url)
        for act_href in _directory_hrefs(chapter_text, prefix="Act"):
            if not _directory_matches(act_href, "Act", act_filter):
                continue
            act_relative = f"{chapter_href.rstrip('/')}/{act_href.rstrip('/')}"
            act_url = urljoin(base_url, quote(f"{act_relative}/", safe="/%"))
            act_text = _remote_text(session, act_url)
            for doc_href in parse_illinois_ilcs_links(act_text):
                relative_name = f"{act_relative}/{Path(doc_href).name}"
                paths.append(relative_name)
                if limit is not None and _section_path_count(paths) >= limit:
                    return tuple(paths)
    return tuple(paths)


def _document_path_matches(
    path: str,
    chapter_filter: int | None,
    act_filter: int | None,
) -> bool:
    try:
        document = parse_illinois_ilcs_doc_name(path)
    except ValueError:
        return False
    if chapter_filter is not None and document.chapter_int != chapter_filter:
        return False
    return act_filter is None or document.act_int == act_filter


def _limit_section_paths(paths: list[str], limit: int | None) -> tuple[str, ...]:
    if limit is None:
        return tuple(paths)
    out: list[str] = []
    section_count = 0
    for path in paths:
        out.append(path)
        try:
            document = parse_illinois_ilcs_doc_name(path)
        except ValueError:
            continue
        if document.doc_type == "K":
            section_count += 1
        if section_count >= limit:
            break
    return tuple(out)


def _section_path_count(paths: list[str]) -> int:
    count = 0
    for path in paths:
        try:
            document = parse_illinois_ilcs_doc_name(path)
        except ValueError:
            continue
        if document.doc_type == "K":
            count += 1
    return count


def _directory_matches(
    href: str,
    prefix: str,
    expected: int | None,
) -> bool:
    if expected is None:
        return True
    match = re.search(rf"\b{re.escape(prefix)}\s+(\d+)\b", href)
    return bool(match and int(match.group(1)) == expected)


def _remote_text(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=20)
    response.raise_for_status()
    return response.text


def _directory_hrefs(text: str, *, prefix: str) -> tuple[str, ...]:
    hrefs: list[str] = []
    for href in _HREF_RE.findall(text):
        decoded = _ilcs_relative_href(href)
        segment = decoded.strip("/").split("/")[-1] if decoded else ""
        if segment.startswith(f"{prefix} "):
            hrefs.append(f"{segment}/")
    return tuple(dict.fromkeys(hrefs))


def _ilcs_relative_href(href: str) -> str:
    decoded = unquote(href).strip()
    if "/ftp/ILCS/" in decoded:
        return decoded.split("/ftp/ILCS/", 1)[1]
    return decoded.lstrip("/")


def _load_local_section_sequence(source_root: Path | None) -> dict[str, int]:
    if source_root is None:
        return {}
    candidates = (
        source_root / "aReadMe" / "Section Sequence.txt",
        source_root / "Section Sequence.txt",
    )
    for candidate in candidates:
        if candidate.exists():
            return parse_illinois_section_sequence(candidate.read_text(encoding="utf-8"))
    return {}


def _load_remote_section_sequence(base_url: str) -> dict[str, int]:
    try:
        response = requests.get(
            urljoin(base_url, "aReadMe/Section%20Sequence.txt"),
            headers={"User-Agent": ILLINOIS_USER_AGENT},
            timeout=20,
        )
        response.raise_for_status()
    except requests.RequestException:
        return {}
    return parse_illinois_section_sequence(response.text)


def _html_text(html: str | bytes) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    lines = [
        re.sub(r"\s+", " ", unescape(line)).strip()
        for line in soup.get_text("\n").splitlines()
    ]
    return "\n".join(line for line in lines if line)


def _section_heading(text: str, section: str) -> str | None:
    for line in text.splitlines():
        if not _SECTION_HEADING_RE.match(line):
            continue
        match = re.match(
            rf"^(?:Sec\.|Section)\s+{re.escape(section)}\.?\s*(?P<heading>.*)$",
            line,
            re.IGNORECASE,
        )
        if match:
            heading = match.group("heading").strip()
            return heading.rstrip(".") or None
    return None


def _act_title(data: bytes) -> str | None:
    text = _html_text(data)
    for line in text.splitlines():
        if line.endswith("Act.") or line.endswith("Act"):
            return line
    return None


def _inventory_item(
    *,
    citation_path: str,
    source_url: str | None,
    source_path: str,
    sha256: str,
    metadata: dict[str, Any],
) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=citation_path,
        source_url=_non_file_url(source_url),
        source_path=source_path,
        source_format=ILLINOIS_ILCS_SOURCE_FORMAT,
        sha256=sha256,
        metadata=metadata,
    )


def _provision_record(
    *,
    jurisdiction: str,
    citation_path: str,
    version: str,
    source_url: str | None,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    kind: str,
    heading: str | None,
    legal_identifier: str,
    level: int,
    ordinal: int | None,
    body: str | None = None,
    source_id: str | None = None,
    parent_citation_path: str | None = None,
    identifiers: dict[str, str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(citation_path),
        jurisdiction=jurisdiction,
        document_class=DocumentClass.STATUTE.value,
        citation_path=citation_path,
        body=body,
        heading=heading,
        citation_label=legal_identifier,
        version=version,
        source_url=_non_file_url(source_url),
        source_path=source_path,
        source_id=source_id,
        source_format=ILLINOIS_ILCS_SOURCE_FORMAT,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=parent_citation_path,
        parent_id=(
            deterministic_provision_id(parent_citation_path) if parent_citation_path else None
        ),
        level=level,
        ordinal=ordinal,
        kind=kind,
        legal_identifier=legal_identifier,
        identifiers=identifiers,
        metadata=metadata,
    )


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _illinois_run_id(
    version: str,
    *,
    jurisdiction: str,
    chapter_filter: int | None,
    act_filter: int | None,
    limit: int | None,
) -> str:
    if chapter_filter is None and act_filter is None and limit is None:
        return version
    parts = [version, jurisdiction]
    if chapter_filter is not None:
        parts.append(f"chapter-{chapter_filter}")
    if act_filter is not None:
        parts.append(f"act-{act_filter}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _non_file_url(value: str | None) -> str | None:
    if value and not value.startswith("file://"):
        return value
    return None


def _looks_like_ilcs_path(value: str) -> bool:
    try:
        parse_illinois_ilcs_doc_name(value)
    except ValueError:
        return False
    return True
