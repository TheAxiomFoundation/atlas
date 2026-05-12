"""Generic official-document ingestion for policy sources."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Self, TextIO

import fitz
import requests
import yaml
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore, safe_segment
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

OFFICIAL_DOCUMENT_USER_AGENT = (
    "Axiom/1.0 (Legal Archive; contact@axiom-foundation.org) "
    "https://github.com/TheAxiomFoundation/axiom-corpus"
)
_GOOGLE_DRIVE_FILE_RE = re.compile(r"https?://drive\.google\.com/file/d/([^/]+)/")
_HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
_TEXT_TAGS = _HEADING_TAGS | {"p", "li", "table", "blockquote"}


@dataclass(frozen=True)
class OfficialDocumentSource:
    """One primary official document to snapshot and normalize."""

    source_id: str
    jurisdiction: str
    document_class: str
    title: str
    source_url: str
    citation_path: str | None = None
    download_url: str | None = None
    source_format: str | None = None
    source_as_of: str | None = None
    expression_date: str | None = None
    local_path: str | None = None
    metadata: dict[str, Any] | None = None

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        return cls(
            source_id=str(data["source_id"]),
            jurisdiction=str(data["jurisdiction"]),
            document_class=str(data.get("document_class", DocumentClass.POLICY.value)),
            title=str(data["title"]),
            source_url=str(data["source_url"]),
            citation_path=data.get("citation_path"),
            download_url=data.get("download_url"),
            source_format=data.get("source_format"),
            source_as_of=data.get("source_as_of"),
            expression_date=data.get("expression_date"),
            local_path=data.get("local_path"),
            metadata=data.get("metadata"),
        )


@dataclass(frozen=True)
class OfficialDocumentManifest:
    """Manifest of primary official documents for one corpus scope."""

    documents: tuple[OfficialDocumentSource, ...]

    @classmethod
    def load(cls, path: str | Path) -> Self:
        data = yaml.safe_load(Path(path).read_text())
        if not isinstance(data, dict):
            raise ValueError("official document manifest must be a YAML mapping")
        documents = data.get("documents")
        if not isinstance(documents, list):
            raise ValueError("official document manifest must contain a documents list")
        return cls(
            documents=tuple(
                OfficialDocumentSource.from_mapping(row)
                for row in documents
                if isinstance(row, dict)
            )
        )

    def require_unique_sources(self) -> None:
        seen: set[str] = set()
        for source in self.documents:
            if source.source_id in seen:
                raise ValueError(f"duplicate source_id: {source.source_id}")
            seen.add(source.source_id)


@dataclass(frozen=True)
class OfficialDocumentExtractReport:
    """Result from a generic official-document extraction run."""

    jurisdiction: str
    document_class: str
    document_count: int
    block_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]


@dataclass(frozen=True)
class _DownloadedDocument:
    source: OfficialDocumentSource
    content: bytes
    content_type: str | None
    final_url: str


@dataclass(frozen=True)
class _DocumentBlock:
    kind: str
    ordinal: int
    heading: str | None
    body: str
    metadata: dict[str, Any]


def official_documents_run_id(
    version: str,
    *,
    only_source_id: str | None = None,
    limit: int | None = None,
) -> str:
    """Return a scoped run id for a manifest-driven official-document run."""
    parts = [version]
    if only_source_id:
        parts.append(safe_segment(only_source_id))
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def extract_official_documents(
    store: CorpusArtifactStore,
    *,
    manifest_path: str | Path,
    version: str,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_source_id: str | None = None,
    limit: int | None = None,
    progress_stream: TextIO | None = None,
) -> OfficialDocumentExtractReport:
    """Snapshot official HTML/PDF documents and extract normalized records."""
    manifest = OfficialDocumentManifest.load(manifest_path)
    manifest.require_unique_sources()
    documents = _select_documents(manifest.documents, only_source_id=only_source_id, limit=limit)
    if not documents:
        raise ValueError("no official documents selected")
    jurisdiction, document_class = _single_scope(documents)
    run_id = official_documents_run_id(version, only_source_id=only_source_id, limit=limit)
    default_source_as_of = source_as_of or version
    default_expression_date = _date_text(expression_date, default_source_as_of)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": OFFICIAL_DOCUMENT_USER_AGENT,
            "Accept": "text/html,application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    source_paths: list[Path] = []
    inventory: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    block_count = 0

    for source in documents:
        _progress(progress_stream, f"extracting {source.source_id}")
        downloaded = _download_document(source, session=session)
        source_format = _infer_source_format(source, downloaded)
        relative_source = (
            f"official-documents/{safe_segment(source.source_id)}{_extension(source_format)}"
        )
        artifact_path = store.source_path(jurisdiction, document_class, run_id, relative_source)
        source_sha = store.write_bytes(artifact_path, downloaded.content)
        source_paths.append(artifact_path)
        source_key = f"sources/{jurisdiction}/{document_class}/{run_id}/{relative_source}"
        source_as_of_text = source.source_as_of or default_source_as_of
        expression_date_text = source.expression_date or default_expression_date

        blocks = tuple(
            _extract_blocks(downloaded.content, source_format, source_url=source.source_url)
        )
        block_count += len(blocks)
        inventory.extend(
            _inventory_items(
                source,
                blocks=blocks,
                source_key=source_key,
                source_format=source_format,
                source_sha=source_sha,
                content_type=downloaded.content_type,
                final_url=downloaded.final_url,
            )
        )
        records.extend(
            _provision_records(
                source,
                blocks=blocks,
                version=run_id,
                source_key=source_key,
                source_format=source_format,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                content_type=downloaded.content_type,
                final_url=downloaded.final_url,
            )
        )

    inventory_path = store.inventory_path(jurisdiction, document_class, run_id)
    provisions_path = store.provisions_path(jurisdiction, document_class, run_id)
    coverage_path = store.coverage_path(jurisdiction, document_class, run_id)
    store.write_inventory(inventory_path, inventory)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(inventory),
        tuple(records),
        jurisdiction=jurisdiction,
        document_class=document_class,
        version=run_id,
    )
    store.write_json(coverage_path, coverage.to_mapping())

    return OfficialDocumentExtractReport(
        jurisdiction=jurisdiction,
        document_class=document_class,
        document_count=len(documents),
        block_count=block_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def google_drive_download_url(url: str) -> str | None:
    """Return a direct download URL for a public Google Drive file URL."""
    match = _GOOGLE_DRIVE_FILE_RE.match(url)
    if not match:
        return None
    return f"https://drive.google.com/uc?export=download&id={match.group(1)}"


def _select_documents(
    documents: tuple[OfficialDocumentSource, ...],
    *,
    only_source_id: str | None,
    limit: int | None,
) -> tuple[OfficialDocumentSource, ...]:
    selected = [source for source in documents if only_source_id in {None, source.source_id}]
    if limit is not None:
        selected = selected[:limit]
    return tuple(selected)


def _single_scope(documents: tuple[OfficialDocumentSource, ...]) -> tuple[str, str]:
    jurisdictions = {source.jurisdiction for source in documents}
    document_classes = {source.document_class for source in documents}
    if len(jurisdictions) != 1 or len(document_classes) != 1:
        raise ValueError("official document extraction requires one jurisdiction/document_class")
    return next(iter(jurisdictions)), next(iter(document_classes))


def _download_document(
    source: OfficialDocumentSource,
    *,
    session: requests.Session,
) -> _DownloadedDocument:
    if source.local_path:
        path = Path(source.local_path)
        return _DownloadedDocument(
            source=source,
            content=path.read_bytes(),
            content_type=None,
            final_url=path.as_uri(),
        )
    download_url = (
        source.download_url or google_drive_download_url(source.source_url) or source.source_url
    )
    response = session.get(download_url, timeout=90, allow_redirects=True)
    response.raise_for_status()
    return _DownloadedDocument(
        source=source,
        content=response.content,
        content_type=response.headers.get("content-type"),
        final_url=response.url,
    )


def _infer_source_format(source: OfficialDocumentSource, downloaded: _DownloadedDocument) -> str:
    if source.source_format:
        return source.source_format.lower()
    content_type = (downloaded.content_type or "").lower()
    if downloaded.content.startswith(b"%PDF") or "pdf" in content_type:
        return "pdf"
    if "html" in content_type or downloaded.content.lstrip().startswith((b"<!doctype", b"<html")):
        return "html"
    raise ValueError(f"cannot infer source format for {source.source_id}")


def _extension(source_format: str) -> str:
    if source_format == "pdf":
        return ".pdf"
    if source_format == "html":
        return ".html"
    return f".{safe_segment(source_format)}"


def _extract_blocks(
    content: bytes, source_format: str, *, source_url: str
) -> tuple[_DocumentBlock, ...]:
    if source_format == "pdf":
        return _extract_pdf_blocks(content)
    if source_format == "html":
        return _extract_html_blocks(content, source_url=source_url)
    raise ValueError(f"unsupported official document source_format: {source_format}")


def _extract_pdf_blocks(content: bytes) -> tuple[_DocumentBlock, ...]:
    blocks: list[_DocumentBlock] = []
    with fitz.open(stream=content, filetype="pdf") as document:
        for index, page in enumerate(document, start=1):
            text = _normalize_text(page.get_text("text"))
            if not text:
                continue
            blocks.append(
                _DocumentBlock(
                    kind="page",
                    ordinal=len(blocks) + 1,
                    heading=f"Page {index}",
                    body=text,
                    metadata={"page_number": index},
                )
            )
    return tuple(blocks)


def _extract_html_blocks(content: bytes, *, source_url: str) -> tuple[_DocumentBlock, ...]:
    soup = BeautifulSoup(content, "html.parser", from_encoding="utf-8")
    for selector in (
        "script",
        "style",
        "noscript",
        "svg",
        "form",
        "nav",
        "header",
        "footer",
        "aside",
        ".breadcrumb",
        ".breadcrumbs",
        "[aria-label='breadcrumb']",
    ):
        for node in soup.select(selector):
            node.decompose()
    root = _main_content(soup)
    title = _document_title(soup)
    blocks: list[_DocumentBlock] = []
    heading = title
    parts: list[str] = []

    def flush() -> None:
        nonlocal parts
        body = _normalize_text("\n\n".join(parts))
        if body:
            blocks.append(
                _DocumentBlock(
                    kind="block",
                    ordinal=len(blocks) + 1,
                    heading=heading,
                    body=body,
                    metadata={"source_url": source_url},
                )
            )
        parts = []

    for node in root.find_all(_TEXT_TAGS):
        if not isinstance(node, Tag) or _inside_text_tag(node):
            continue
        text = _normalize_text(node.get_text(" ", strip=True))
        if not text:
            continue
        if node.name in _HEADING_TAGS:
            flush()
            heading = text
            continue
        parts.append(text)
        if sum(len(part) for part in parts) >= 2_500:
            flush()
    flush()
    if blocks:
        return tuple(blocks)
    fallback = _normalize_text(root.get_text(" ", strip=True))
    if not fallback:
        return ()
    return (
        _DocumentBlock(
            kind="block",
            ordinal=1,
            heading=title,
            body=fallback,
            metadata={"source_url": source_url},
        ),
    )


def _main_content(soup: BeautifulSoup) -> Tag:
    for selector in ("main", "article", "[role='main']", "#main-content", ".main-content"):
        node = soup.select_one(selector)
        if isinstance(node, Tag):
            return node
    if isinstance(soup.body, Tag):
        return soup.body
    return soup


def _document_title(soup: BeautifulSoup) -> str | None:
    h1 = soup.find("h1")
    if isinstance(h1, Tag):
        text = _normalize_text(h1.get_text(" ", strip=True))
        if text:
            return text
    if soup.title:
        text = _normalize_text(soup.title.get_text(" ", strip=True))
        if text:
            return text
    return None


def _inside_text_tag(node: Tag) -> bool:
    for parent in node.parents:
        if not isinstance(parent, Tag):
            continue
        if parent.name in _TEXT_TAGS:
            return True
    return False


def _inventory_items(
    source: OfficialDocumentSource,
    *,
    blocks: tuple[_DocumentBlock, ...],
    source_key: str,
    source_format: str,
    source_sha: str,
    content_type: str | None,
    final_url: str,
) -> tuple[SourceInventoryItem, ...]:
    root_path = _root_citation_path(source)
    metadata = _source_metadata(
        source,
        content_type=content_type,
        final_url=final_url,
        block_count=len(blocks),
    )
    items = [
        SourceInventoryItem(
            citation_path=root_path,
            source_url=source.source_url,
            source_path=source_key,
            source_format=source_format,
            sha256=source_sha,
            metadata={"kind": "document", **metadata},
        )
    ]
    for block in blocks:
        items.append(
            SourceInventoryItem(
                citation_path=_block_citation_path(source, block),
                source_url=source.source_url,
                source_path=source_key,
                source_format=source_format,
                sha256=source_sha,
                metadata={"kind": block.kind, **metadata, **block.metadata},
            )
        )
    return tuple(items)


def _provision_records(
    source: OfficialDocumentSource,
    *,
    blocks: tuple[_DocumentBlock, ...],
    version: str,
    source_key: str,
    source_format: str,
    source_as_of: str,
    expression_date: str,
    content_type: str | None,
    final_url: str,
) -> tuple[ProvisionRecord, ...]:
    root_path = _root_citation_path(source)
    root_id = deterministic_provision_id(root_path)
    metadata = _source_metadata(
        source,
        content_type=content_type,
        final_url=final_url,
        block_count=len(blocks),
    )
    records = [
        ProvisionRecord(
            id=root_id,
            jurisdiction=source.jurisdiction,
            document_class=source.document_class,
            citation_path=root_path,
            heading=source.title,
            citation_label=source.title,
            version=version,
            source_url=source.source_url,
            source_path=source_key,
            source_id=source.source_id,
            source_format=source_format,
            source_as_of=source_as_of,
            expression_date=expression_date,
            level=1,
            ordinal=1,
            kind="document",
            metadata={"kind": "document", **metadata},
        )
    ]
    for block in blocks:
        citation_path = _block_citation_path(source, block)
        records.append(
            ProvisionRecord(
                id=deterministic_provision_id(citation_path),
                jurisdiction=source.jurisdiction,
                document_class=source.document_class,
                citation_path=citation_path,
                body=block.body,
                heading=block.heading,
                citation_label=f"{source.title} {block.ordinal}",
                version=version,
                source_url=source.source_url,
                source_path=source_key,
                source_id=source.source_id,
                source_format=source_format,
                source_as_of=source_as_of,
                expression_date=expression_date,
                parent_citation_path=root_path,
                parent_id=root_id,
                level=2,
                ordinal=block.ordinal,
                kind=block.kind,
                metadata={"kind": block.kind, **metadata, **block.metadata},
            )
        )
    return tuple(records)


def _source_metadata(
    source: OfficialDocumentSource,
    *,
    content_type: str | None,
    final_url: str,
    block_count: int,
) -> dict[str, Any]:
    metadata = dict(source.metadata or {})
    metadata.update(
        {
            "title": source.title,
            "content_type": content_type,
            "download_url": final_url,
            "block_count": block_count,
        }
    )
    return metadata


def _root_citation_path(source: OfficialDocumentSource) -> str:
    if source.citation_path:
        return _validate_citation_path(
            source.citation_path,
            jurisdiction=source.jurisdiction,
            document_class=source.document_class,
        )
    return f"{source.jurisdiction}/{source.document_class}/{safe_segment(source.source_id)}"


def _block_citation_path(source: OfficialDocumentSource, block: _DocumentBlock) -> str:
    return f"{_root_citation_path(source)}/{block.kind}-{block.ordinal}"


def _validate_citation_path(
    citation_path: str,
    *,
    jurisdiction: str,
    document_class: str,
) -> str:
    """Return a manifest-supplied citation path after basic scope validation."""
    normalized = citation_path.strip().strip("/")
    expected_prefix = f"{jurisdiction}/{document_class}/"
    if not normalized.startswith(expected_prefix):
        raise ValueError(f"citation_path must start with {expected_prefix!r}: {citation_path!r}")
    for part in normalized.split("/"):
        safe_segment(part)
    return normalized


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _normalize_text(text: str) -> str:
    lines = [" ".join(line.split()) for line in text.splitlines()]
    paragraphs: list[str] = []
    current: list[str] = []
    for line in lines:
        if not line:
            if current:
                paragraphs.append(" ".join(current))
                current = []
            continue
        current.append(line)
    if current:
        paragraphs.append(" ".join(current))
    return "\n\n".join(paragraphs)


def _progress(stream: TextIO | None, message: str) -> None:
    if stream is None:
        return
    print(message, file=stream)
    stream.flush()


if __name__ == "__main__":
    print(
        "Use `axiom-corpus-ingest extract-official-documents` to run this adapter.",
        file=sys.stderr,
    )
