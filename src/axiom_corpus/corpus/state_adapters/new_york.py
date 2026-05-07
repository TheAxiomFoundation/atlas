"""New York Senate OpenLegislation source-first corpus adapter."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any, TextIO
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

NEW_YORK_SENATE_BASE_URL = "https://www.nysenate.gov"
NEW_YORK_SENATE_SOURCE_FORMAT = "new-york-senate-html"
NEW_YORK_SENATE_USER_AGENT = (
    "curl/8.7.1 axiom-corpus/0.1 (New York statute ingestion; contact@axiom-foundation.org)"
)
NEW_YORK_OPENLEG_API_BASE_URL = "https://legislation.nysenate.gov"
NEW_YORK_OPENLEG_SOURCE_FORMAT = "new-york-openleg-json"

_LAW_HREF_RE = re.compile(r"^/legislation/laws/(?P<law>[A-Z0-9]+)$")
_LAW_PATH_RE = re.compile(r"^/legislation/laws/(?P<law>[A-Z0-9]+)(?:/(?P<location>[^/?#]+))?$")
_SECTION_HEADLINE_RE = re.compile(r"^SECTION\s+(?P<section>.+)$", re.I)
_REVISION_RE = re.compile(r"Viewing\s+most\s+recent\s+revision\s+\(from\s+(?P<date>[^)]+)\)", re.I)


@dataclass(frozen=True)
class NewYorkLawLink:
    """One consolidated law link from the official Senate index."""

    law_id: str
    name: str
    source_url: str
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-ny/statute/{self.law_id}"


@dataclass(frozen=True)
class NewYorkChildLink:
    """One child node link from a Senate law page."""

    law_id: str
    location_id: str
    label: str
    source_url: str
    ordinal: int


@dataclass(frozen=True)
class NewYorkOpenLegLawInfo:
    """One law volume from the official OpenLegislation API."""

    law_id: str
    name: str
    law_type: str
    chapter: str | None
    ordinal: int

    @property
    def citation_path(self) -> str:
        return f"us-ny/statute/{self.law_id}"


@dataclass(frozen=True)
class NewYorkParsedPage:
    """Parsed official Senate law page."""

    law_id: str
    location_id: str | None
    kind: str
    display_number: str
    heading: str | None
    body: str | None
    child_links: tuple[NewYorkChildLink, ...]
    revision_date: str | None
    breadcrumb_labels: tuple[str, ...]

    @property
    def source_id(self) -> str:
        return self.location_id or self.law_id

    @property
    def citation_path(self) -> str:
        if self.location_id is None:
            return f"us-ny/statute/{self.law_id}"
        return f"us-ny/statute/{self.law_id}/{self.location_id}"

    @property
    def legal_identifier(self) -> str:
        if self.kind == "law":
            return f"N.Y. {self.heading or self.law_id}"
        if self.kind == "section":
            return f"N.Y. {self.law_id} Law § {self.display_number}"
        return f"N.Y. {self.law_id} Law {self.kind.title()} {self.display_number}"


@dataclass(frozen=True)
class _NewYorkFetchResult:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _NewYorkOpenLegFetchResult:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _NewYorkQueueItem:
    law_id: str
    location_id: str | None
    source_url: str
    parent_citation_path: str | None
    level: int
    ordinal: int


class _NewYorkFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        base_url: str,
        request_delay_seconds: float = 0.35,
        timeout_seconds: float = 15.0,
        request_attempts: int = 2,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.base_url = base_url.rstrip("/")
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._lock = Lock()
        self._last_request_at = 0.0

    def fetch(self, source_url: str) -> _NewYorkFetchResult:
        relative_path = _source_relative_path(source_url, self.base_url)
        if self.source_dir is not None:
            path = _find_local_source(self.source_dir, relative_path)
            return _NewYorkFetchResult(
                relative_path=relative_path,
                source_url=source_url,
                data=path.read_bytes(),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return _NewYorkFetchResult(
                    relative_path=relative_path,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )
        self.wait_for_request_slot()
        data = _download_new_york_page(
            source_url,
            attempts=self.request_attempts,
            timeout_seconds=self.timeout_seconds,
        )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            _write_cache_bytes(cached_path, data)
        return _NewYorkFetchResult(relative_path=relative_path, source_url=source_url, data=data)

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        with self._lock:
            elapsed = time.monotonic() - self._last_request_at
            wait_seconds = self.request_delay_seconds - elapsed
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self._last_request_at = time.monotonic()


class _NewYorkOpenLegFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        api_base_url: str,
        api_key: str | None,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.api_base_url = api_base_url.rstrip("/")
        self.api_key = api_key

    def fetch_law_index(self) -> _NewYorkOpenLegFetchResult:
        return self.fetch_json(
            relative_path=f"{NEW_YORK_OPENLEG_SOURCE_FORMAT}/laws.json",
            api_path="api/3/laws",
            params={"limit": "1000"},
        )

    def fetch_law(self, law_id: str) -> _NewYorkOpenLegFetchResult:
        return self.fetch_json(
            relative_path=f"{NEW_YORK_OPENLEG_SOURCE_FORMAT}/{law_id}.json",
            api_path=f"api/3/laws/{law_id}",
            params={"full": "true"},
        )

    def fetch_json(
        self,
        *,
        relative_path: str,
        api_path: str,
        params: dict[str, str],
    ) -> _NewYorkOpenLegFetchResult:
        source_url = _openleg_api_url(self.api_base_url, api_path, params)
        if self.source_dir is not None:
            path = _find_local_source(self.source_dir, relative_path)
            return _NewYorkOpenLegFetchResult(
                relative_path=relative_path,
                source_url=source_url,
                data=path.read_bytes(),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return _NewYorkOpenLegFetchResult(
                    relative_path=relative_path,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )
        if not self.api_key:
            raise ValueError("New York OpenLegislation API extraction requires an API key")
        data = _download_openleg_json(
            self.api_base_url,
            api_path,
            params={**params, "key": self.api_key},
        )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            _write_cache_bytes(cached_path, data)
        return _NewYorkOpenLegFetchResult(
            relative_path=relative_path,
            source_url=source_url,
            data=data,
        )


def extract_new_york_consolidated_laws(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
    workers: int = 1,
    download_dir: str | Path | None = None,
    base_url: str = NEW_YORK_SENATE_BASE_URL,
    request_delay_seconds: float = 0.35,
    timeout_seconds: float = 15.0,
    request_attempts: int = 2,
    progress_stream: TextIO | None = None,
) -> StateStatuteExtractReport:
    """Snapshot official NY Senate law HTML and extract normalized provisions."""
    jurisdiction = "us-ny"
    title_filter = only_title.upper() if only_title else None
    run_id = _new_york_run_id(version, title_filter=title_filter, limit=limit)
    worker_count = max(1, workers)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _NewYorkFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        base_url=base_url,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    index_url = urljoin(base_url.rstrip("/") + "/", "legislation/laws/CONSOLIDATED")
    index_page = fetcher.fetch(index_url)
    index_artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        index_page.relative_path,
    )
    store.write_bytes(index_artifact_path, index_page.data)
    source_paths: list[Path] = [index_artifact_path]

    laws = parse_new_york_law_index(index_page.data, base_url=base_url)
    if title_filter is not None:
        laws = tuple(law for law in laws if law.law_id == title_filter)
    if not laws:
        raise ValueError(f"no New York laws selected for filter: {only_title!r}")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    errors: list[str] = []
    remaining_sections = limit

    def log_progress(message: str) -> None:
        if progress_stream is not None:
            print(message, file=progress_stream, flush=True)

    def handle_fetched_page(
        current: _NewYorkQueueItem,
        fetched: _NewYorkFetchResult,
    ) -> NewYorkParsedPage:
        nonlocal title_count, container_count, section_count, remaining_sections
        artifact_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            fetched.relative_path,
        )
        sha256 = store.write_bytes(artifact_path, fetched.data)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, fetched.relative_path)
        parsed = parse_new_york_law_page(
            fetched.data,
            source_url=current.source_url,
            base_url=base_url,
        )
        if parsed.citation_path not in seen:
            seen.add(parsed.citation_path)
            if parsed.kind == "law":
                title_count += 1
            else:
                container_count += 1 if parsed.kind != "section" else 0
            if parsed.kind == "section":
                section_count += 1
                if remaining_sections is not None:
                    remaining_sections -= 1
            _append_inventory_and_record(
                items,
                records,
                citation_path=parsed.citation_path,
                version=run_id,
                source_url=fetched.source_url,
                source_path=source_key,
                source_id=parsed.source_id,
                sha256=sha256,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                kind=parsed.kind,
                heading=parsed.heading,
                body=parsed.body,
                legal_identifier=parsed.legal_identifier,
                parent_citation_path=current.parent_citation_path,
                level=current.level,
                ordinal=current.ordinal,
                identifiers=_page_identifiers(parsed),
                metadata={
                    "kind": parsed.kind,
                    "law_id": parsed.law_id,
                    "location_id": parsed.location_id,
                    "display_number": parsed.display_number,
                    "revision_date": parsed.revision_date,
                    "breadcrumb_labels": list(parsed.breadcrumb_labels),
                },
            )
            if len(records) == 1 or len(records) % 250 == 0:
                log_progress(
                    "new-york extracted "
                    f"records={len(records)} sections={section_count} "
                    f"source_files={len(source_paths)}"
                )

        return parsed

    def child_queue_items(
        page: NewYorkParsedPage,
        current: _NewYorkQueueItem,
    ) -> list[_NewYorkQueueItem]:
        next_items: list[_NewYorkQueueItem] = []
        for child in page.child_links:
            is_section = _looks_like_section_label(child.label)
            if is_section and remaining_sections is not None and remaining_sections <= 0:
                break
            next_items.append(
                _NewYorkQueueItem(
                    law_id=child.law_id,
                    location_id=child.location_id,
                    source_url=child.source_url,
                    parent_citation_path=page.citation_path,
                    level=current.level + 1,
                    ordinal=child.ordinal,
                )
            )
        return next_items

    def safe_fetch_page(
        current: _NewYorkQueueItem,
    ) -> tuple[_NewYorkQueueItem, _NewYorkFetchResult | None, Exception | None]:
        try:
            return current, fetcher.fetch(current.source_url), None
        except (OSError, requests.RequestException) as exc:
            return current, None, exc

    stack = list(
        reversed(
            [
                _NewYorkQueueItem(
                    law_id=law.law_id,
                    location_id=None,
                    source_url=law.source_url,
                    parent_citation_path=None,
                    level=0,
                    ordinal=law.ordinal,
                )
                for law in laws
            ]
        )
    )
    executor = ThreadPoolExecutor(max_workers=worker_count) if worker_count > 1 else None
    try:
        while stack:
            current = stack.pop()
            if remaining_sections is not None and remaining_sections <= 0:
                break
            current, fetched, fetch_error = safe_fetch_page(current)
            if fetch_error is not None or fetched is None:
                skipped_source_count += 1
                errors.append(f"{current.source_url}: {fetch_error}")
                log_progress(f"new-york skipped {current.source_url}: {fetch_error}")
                continue

            parsed = handle_fetched_page(current, fetched)
            if remaining_sections is not None and remaining_sections <= 0:
                continue

            next_items = child_queue_items(parsed, current)
            if (
                executor is not None
                and remaining_sections is None
                and len(next_items) > 1
                and _all_section_children(parsed.child_links)
            ):
                unexpected_children: list[_NewYorkQueueItem] = []
                for child_item, child_fetched, child_error in executor.map(
                    safe_fetch_page,
                    next_items,
                ):
                    if child_error is not None or child_fetched is None:
                        skipped_source_count += 1
                        errors.append(f"{child_item.source_url}: {child_error}")
                        log_progress(f"new-york skipped {child_item.source_url}: {child_error}")
                        continue
                    child_page = handle_fetched_page(child_item, child_fetched)
                    unexpected_children.extend(child_queue_items(child_page, child_item))
                stack.extend(reversed(unexpected_children))
                continue

            stack.extend(reversed(next_items))
    finally:
        if executor is not None:
            executor.shutdown()

    if not items:
        raise ValueError("no New York provisions extracted")

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
        source_paths=tuple(dict.fromkeys(source_paths)),
        skipped_source_count=skipped_source_count,
        errors=tuple(errors),
    )


def extract_new_york_openleg_api(
    store: CorpusArtifactStore,
    *,
    version: str,
    api_key: str | None = None,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    api_base_url: str = NEW_YORK_OPENLEG_API_BASE_URL,
) -> StateStatuteExtractReport:
    """Snapshot official OpenLegislation JSON and extract normalized provisions."""
    jurisdiction = "us-ny"
    title_filter = only_title.upper() if only_title else None
    run_id = _new_york_run_id(version, title_filter=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _NewYorkOpenLegFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        api_base_url=api_base_url,
        api_key=api_key,
    )

    law_index = fetcher.fetch_law_index()
    law_index_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        law_index.relative_path,
    )
    store.write_bytes(law_index_path, law_index.data)
    source_paths: list[Path] = [law_index_path]

    laws = parse_new_york_openleg_laws(law_index.data)
    if title_filter is not None:
        laws = tuple(law for law in laws if law.law_id == title_filter)
    if not laws:
        raise ValueError(f"no New York laws selected for filter: {only_title!r}")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    skipped_source_count = 0
    errors: list[str] = []
    remaining_sections = limit

    def append_openleg_record(
        *,
        citation_path: str,
        fetched: _NewYorkOpenLegFetchResult,
        sha256: str,
        source_id: str,
        kind: str,
        heading: str | None,
        body: str | None,
        legal_identifier: str,
        parent_citation_path: str | None,
        level: int,
        ordinal: int,
        identifiers: dict[str, str],
        metadata: dict[str, Any],
    ) -> None:
        _append_inventory_and_record(
            items,
            records,
            citation_path=citation_path,
            version=run_id,
            source_url=fetched.source_url,
            source_path=_state_source_key(jurisdiction, run_id, fetched.relative_path),
            source_id=source_id,
            sha256=sha256,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            kind=kind,
            heading=heading,
            body=body,
            legal_identifier=legal_identifier,
            parent_citation_path=parent_citation_path,
            level=level,
            ordinal=ordinal,
            identifiers=identifiers,
            metadata=metadata,
            source_format=NEW_YORK_OPENLEG_SOURCE_FORMAT,
        )

    def append_api_document(
        law: NewYorkOpenLegLawInfo,
        doc: dict[str, Any],
        *,
        fetched: _NewYorkOpenLegFetchResult,
        sha256: str,
        parent_citation_path: str,
        level: int,
        ordinal: int,
    ) -> None:
        nonlocal container_count, section_count, remaining_sections
        location_id = str(doc.get("locationId") or "").strip()
        if not location_id:
            return
        doc_type = str(doc.get("docType") or "container").strip().lower()
        is_section = doc_type == "section"
        if is_section and remaining_sections is not None and remaining_sections <= 0:
            return
        citation_path = f"us-ny/statute/{law.law_id}/{location_id}"
        if citation_path not in seen:
            seen.add(citation_path)
            display = str(doc.get("docLevelId") or location_id).strip()
            heading = _clean_text(str(doc.get("title") or ""))
            body = _api_document_text(doc) if is_section else None
            legal_identifier = _api_legal_identifier(
                law.law_id,
                kind=doc_type,
                display_number=display,
            )
            if is_section:
                section_count += 1
                if remaining_sections is not None:
                    remaining_sections -= 1
            else:
                container_count += 1
            append_openleg_record(
                citation_path=citation_path,
                fetched=fetched,
                sha256=sha256,
                source_id=location_id,
                kind=doc_type,
                heading=heading or None,
                body=body,
                legal_identifier=legal_identifier,
                parent_citation_path=parent_citation_path,
                level=level,
                ordinal=ordinal,
                identifiers=_api_document_identifiers(
                    law.law_id,
                    location_id=location_id,
                    display_number=display,
                    kind=doc_type,
                ),
                metadata={
                    "kind": doc_type,
                    "law_id": law.law_id,
                    "location_id": location_id,
                    "display_number": display,
                    "active_date": doc.get("activeDate"),
                    "sequence_no": doc.get("sequenceNo"),
                    "repealed": doc.get("repealed"),
                },
            )
        if remaining_sections is not None and remaining_sections <= 0:
            return
        for child_ordinal, child in enumerate(_api_child_documents(doc)):
            append_api_document(
                law,
                child,
                fetched=fetched,
                sha256=sha256,
                parent_citation_path=citation_path,
                level=level + 1,
                ordinal=child_ordinal,
            )
            if remaining_sections is not None and remaining_sections <= 0:
                break

    for law in laws:
        if remaining_sections is not None and remaining_sections <= 0:
            break
        try:
            fetched = fetcher.fetch_law(law.law_id)
        except (OSError, requests.RequestException, ValueError) as exc:
            skipped_source_count += 1
            errors.append(f"{law.law_id}: {exc}")
            continue
        artifact_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            fetched.relative_path,
        )
        sha256 = store.write_bytes(artifact_path, fetched.data)
        source_paths.append(artifact_path)
        try:
            payload = _openleg_json_payload(fetched.data)
            law_result = payload.get("result")
            if not isinstance(law_result, dict):
                raise ValueError("missing OpenLegislation law result")
            root_doc = law_result.get("documents")
            if not isinstance(root_doc, dict):
                raise ValueError("missing OpenLegislation documents")
        except ValueError as exc:
            skipped_source_count += 1
            errors.append(f"{law.law_id}: {exc}")
            continue
        if law.citation_path not in seen:
            seen.add(law.citation_path)
            title_count += 1
            append_openleg_record(
                citation_path=law.citation_path,
                fetched=fetched,
                sha256=sha256,
                source_id=law.law_id,
                kind="law",
                heading=law.name,
                body=None,
                legal_identifier=f"N.Y. {law.name}",
                parent_citation_path=None,
                level=0,
                ordinal=law.ordinal,
                identifiers={"new_york:law": law.law_id},
                metadata={
                    "kind": "law",
                    "law_id": law.law_id,
                    "law_type": law.law_type,
                    "chapter": law.chapter,
                },
            )
        root_children = _api_child_documents(root_doc)
        for child_ordinal, child in enumerate(root_children):
            append_api_document(
                law,
                child,
                fetched=fetched,
                sha256=sha256,
                parent_citation_path=law.citation_path,
                level=1,
                ordinal=child_ordinal,
            )
            if remaining_sections is not None and remaining_sections <= 0:
                break

    if not items:
        if errors:
            raise ValueError(
                "no New York OpenLegislation provisions extracted; "
                + "; ".join(errors[:3])
            )
        raise ValueError("no New York OpenLegislation provisions extracted")

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
        source_paths=tuple(dict.fromkeys(source_paths)),
        skipped_source_count=skipped_source_count,
        errors=tuple(errors),
    )


def parse_new_york_law_index(
    html: str | bytes,
    *,
    base_url: str = NEW_YORK_SENATE_BASE_URL,
) -> tuple[NewYorkLawLink, ...]:
    """Parse the official consolidated-laws index into law links."""
    soup = BeautifulSoup(html, "lxml")
    links: list[NewYorkLawLink] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = _normalize_href(str(anchor.get("href") or ""), base_url)
        parsed = urlparse(href)
        match = _LAW_HREF_RE.match(parsed.path)
        if not match:
            continue
        law_id = match.group("law")
        if law_id in {"ALL", "CONSOLIDATED"} or law_id in seen:
            continue
        seen.add(law_id)
        label = _clean_text(anchor.get_text(" ", strip=True))
        name = label[len(law_id) :].strip(" -") if label.startswith(law_id) else label
        links.append(
            NewYorkLawLink(
                law_id=law_id,
                name=name or law_id,
                source_url=href,
                ordinal=len(links),
            )
        )
    return tuple(links)


def parse_new_york_law_page(
    html: str | bytes,
    *,
    source_url: str,
    base_url: str = NEW_YORK_SENATE_BASE_URL,
) -> NewYorkParsedPage:
    """Parse one official NY Senate law node page."""
    law_id, location_id = _law_location_from_url(source_url)
    soup = BeautifulSoup(html, "lxml")
    headline = _clean_text(_node_text(soup.select_one(".nys-openleg-result-title-headline")))
    short_title = _clean_text(_node_text(soup.select_one(".nys-openleg-result-title-short")))
    location_text = _clean_text(_node_text(soup.select_one(".nys-openleg-result-title-location")))
    kind, display_number = _kind_and_display(headline, location_id=location_id)
    body = _page_body(soup) if kind == "section" else None
    revision_date = _revision_date(soup)
    child_links = _page_child_links(soup, law_id=law_id, base_url=base_url)
    breadcrumbs = tuple(
        _clean_text(node.get_text(" ", strip=True))
        for node in soup.select(".nys-openleg-result-breadcrumb-name")
        if _clean_text(node.get_text(" ", strip=True))
    )
    return NewYorkParsedPage(
        law_id=law_id,
        location_id=location_id,
        kind=kind,
        display_number=display_number,
        heading=short_title or location_text or headline or None,
        body=body,
        child_links=child_links,
        revision_date=revision_date,
        breadcrumb_labels=breadcrumbs,
    )


def parse_new_york_openleg_laws(
    payload: str | bytes,
) -> tuple[NewYorkOpenLegLawInfo, ...]:
    """Parse official OpenLegislation law-list JSON into consolidated law infos."""
    data = _openleg_json_payload(payload)
    result = data.get("result")
    if not isinstance(result, dict):
        return ()
    items = result.get("items")
    if not isinstance(items, list):
        return ()
    laws: list[NewYorkOpenLegLawInfo] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        law_id = str(item.get("lawId") or "").strip().upper()
        if not law_id or law_id in seen:
            continue
        law_type = str(item.get("lawType") or "").strip()
        if law_type and law_type.lower() != "consolidated":
            continue
        seen.add(law_id)
        laws.append(
            NewYorkOpenLegLawInfo(
                law_id=law_id,
                name=_clean_text(str(item.get("name") or law_id)),
                law_type=law_type or "CONSOLIDATED",
                chapter=(
                    _clean_text(str(item.get("chapter")))
                    if item.get("chapter") is not None
                    else None
                ),
                ordinal=len(laws),
            )
        )
    return tuple(laws)


def _openleg_json_payload(payload: str | bytes) -> dict[str, Any]:
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("OpenLegislation response is not a JSON object")
    if data.get("success") is False:
        message = _clean_text(str(data.get("message") or "unknown OpenLegislation error"))
        raise ValueError(f"OpenLegislation API response was unsuccessful: {message}")
    return data


def _api_child_documents(doc: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    documents = doc.get("documents")
    if not isinstance(documents, dict):
        return ()
    items = documents.get("items")
    if not isinstance(items, list):
        return ()
    return tuple(item for item in items if isinstance(item, dict))


def _api_document_text(doc: dict[str, Any]) -> str | None:
    text = doc.get("text")
    if text is None:
        return None
    return re.sub(r"\n{3,}", "\n\n", str(text)).strip() or None


def _api_legal_identifier(law_id: str, *, kind: str, display_number: str) -> str:
    if kind == "section":
        return f"N.Y. {law_id} Law § {display_number}"
    if kind == "law":
        return f"N.Y. {law_id}"
    return f"N.Y. {law_id} Law {kind.title()} {display_number}"


def _api_document_identifiers(
    law_id: str,
    *,
    location_id: str,
    display_number: str,
    kind: str,
) -> dict[str, str]:
    identifiers = {
        "new_york:law": law_id,
        "new_york:location": location_id,
    }
    if kind == "section":
        identifiers["new_york:section"] = display_number
    return identifiers


def _page_child_links(
    soup: BeautifulSoup,
    *,
    law_id: str,
    base_url: str,
) -> tuple[NewYorkChildLink, ...]:
    items = soup.select_one(".nys-openleg-items-container")
    if not isinstance(items, Tag):
        return ()
    links: list[NewYorkChildLink] = []
    seen: set[tuple[str, str]] = set()
    for anchor in items.select("a.nys-openleg-result-item-link[href]"):
        href = _normalize_href(str(anchor.get("href") or ""), base_url)
        child_law_id, location_id = _law_location_from_url(href)
        if child_law_id != law_id or location_id is None:
            continue
        key = (child_law_id, location_id)
        if key in seen:
            continue
        seen.add(key)
        links.append(
            NewYorkChildLink(
                law_id=child_law_id,
                location_id=location_id,
                label=_clean_text(anchor.get_text(" ", strip=True)),
                source_url=href,
                ordinal=len(links),
            )
        )
    return tuple(links)


def _page_body(soup: BeautifulSoup) -> str | None:
    node = soup.select_one(".nys-openleg-result-text")
    if not isinstance(node, Tag):
        return None
    text = node.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() or None


def _revision_date(soup: BeautifulSoup) -> str | None:
    node = soup.select_one(".nys-openleg-history-container")
    if not isinstance(node, Tag):
        return None
    match = _REVISION_RE.search(node.get_text(" ", strip=True))
    if not match:
        return None
    return match.group("date").strip()


def _kind_and_display(headline: str, *, location_id: str | None) -> tuple[str, str]:
    if location_id is None:
        return "law", headline or ""
    section_match = _SECTION_HEADLINE_RE.match(headline)
    if section_match:
        return "section", section_match.group("section").strip()
    if headline:
        parts = headline.split(None, 1)
        if parts:
            kind = parts[0].lower()
            display = parts[1].strip() if len(parts) > 1 else location_id
            return kind, display
    return "container", location_id


def _page_identifiers(page: NewYorkParsedPage) -> dict[str, str]:
    identifiers = {"new_york:law": page.law_id}
    if page.location_id is not None:
        identifiers["new_york:location"] = page.location_id
    if page.kind == "section":
        identifiers["new_york:section"] = page.display_number
    return identifiers


def _append_inventory_and_record(
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    *,
    citation_path: str,
    version: str,
    source_url: str,
    source_path: str,
    source_id: str,
    sha256: str,
    source_as_of: str,
    expression_date: str,
    kind: str,
    heading: str | None,
    legal_identifier: str,
    level: int,
    ordinal: int,
    identifiers: dict[str, str],
    metadata: dict[str, Any],
    source_format: str = NEW_YORK_SENATE_SOURCE_FORMAT,
    body: str | None = None,
    parent_citation_path: str | None = None,
) -> None:
    clean_metadata = {key: value for key, value in metadata.items() if value is not None}
    if parent_citation_path is not None:
        clean_metadata["parent_citation_path"] = parent_citation_path
    items.append(
        SourceInventoryItem(
            citation_path=citation_path,
            source_url=source_url,
            source_path=source_path,
            source_format=source_format,
            sha256=sha256,
            metadata=clean_metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(citation_path),
            jurisdiction="us-ny",
            document_class=DocumentClass.STATUTE.value,
            citation_path=citation_path,
            body=body,
            heading=heading,
            citation_label=legal_identifier,
            version=version,
            source_url=source_url,
            source_path=source_path,
            source_id=source_id,
            source_format=source_format,
            source_as_of=source_as_of,
            expression_date=expression_date,
            parent_citation_path=parent_citation_path,
            parent_id=(
                deterministic_provision_id(parent_citation_path)
                if parent_citation_path is not None
                else None
            ),
            level=level,
            ordinal=ordinal,
            kind=kind,
            legal_identifier=legal_identifier,
            identifiers=identifiers,
            metadata=clean_metadata,
        )
    )


def _find_local_source(source_dir: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    candidates = [source_dir / relative]
    if relative.parts and relative.parts[0] in {
        NEW_YORK_SENATE_SOURCE_FORMAT,
        NEW_YORK_OPENLEG_SOURCE_FORMAT,
    }:
        candidates.append(source_dir.joinpath(*relative.parts[1:]))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(relative_path)


def _download_new_york_page(
    url: str,
    *,
    attempts: int = 2,
    timeout_seconds: float = 15.0,
) -> bytes:  # pragma: no cover
    if shutil.which("curl"):
        return _download_new_york_page_with_curl(
            url,
            attempts=attempts,
            timeout_seconds=timeout_seconds,
        )
    return _download_new_york_page_with_requests(
        url,
        attempts=attempts,
        timeout_seconds=timeout_seconds,
    )


def _download_new_york_page_with_curl(
    url: str,
    *,
    attempts: int,
    timeout_seconds: float,
) -> bytes:  # pragma: no cover
    last_error: requests.RequestException | None = None
    max_time = str(max(1.0, timeout_seconds))
    connect_timeout = str(max(1.0, min(timeout_seconds, 5.0)))
    for attempt in range(attempts):
        try:
            result = subprocess.run(
                [
                    "curl",
                    "--fail",
                    "--location",
                    "--silent",
                    "--show-error",
                    "--max-time",
                    max_time,
                    "--connect-timeout",
                    connect_timeout,
                    "-A",
                    NEW_YORK_SENATE_USER_AGENT,
                    url,
                ],
                check=False,
                capture_output=True,
                timeout=timeout_seconds + 2,
            )
        except subprocess.TimeoutExpired as exc:
            last_error = requests.RequestException(f"curl timed out for {url}: {exc}")
        else:
            if result.returncode == 0:
                return result.stdout
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            last_error = requests.RequestException(
                f"curl failed for {url} with exit {result.returncode}: {stderr}"
            )
        if attempt + 1 < attempts:
            time.sleep(1.5 * (attempt + 1))
    assert last_error is not None
    raise last_error


def _download_new_york_page_with_requests(
    url: str,
    *,
    attempts: int,
    timeout_seconds: float,
) -> bytes:  # pragma: no cover
    last_error: requests.RequestException | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": NEW_YORK_SENATE_USER_AGENT},
                timeout=timeout_seconds,
            )
            if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
                continue
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
    assert last_error is not None
    raise last_error


def _download_openleg_json(
    api_base_url: str,
    api_path: str,
    *,
    params: dict[str, str],
    attempts: int = 3,
) -> bytes:  # pragma: no cover
    url = _openleg_api_url(api_base_url, api_path, {})
    last_error: requests.RequestException | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(url, params=params, timeout=60)
            if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
                continue
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
    assert last_error is not None
    raise last_error


def _openleg_api_url(api_base_url: str, api_path: str, params: dict[str, str]) -> str:
    base = api_base_url.rstrip("/") + "/"
    url = urljoin(base, api_path.lstrip("/"))
    if not params:
        return url
    from urllib.parse import urlencode

    return f"{url}?{urlencode(params)}"


def _write_cache_bytes(path: Path, data: bytes) -> None:  # pragma: no cover
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _source_relative_path(source_url: str, base_url: str) -> str:
    law_id, location_id = _law_location_from_url(source_url)
    if law_id == "CONSOLIDATED":
        return f"{NEW_YORK_SENATE_SOURCE_FORMAT}/CONSOLIDATED.html"
    if location_id is None:
        return f"{NEW_YORK_SENATE_SOURCE_FORMAT}/{law_id}/index.html"
    return f"{NEW_YORK_SENATE_SOURCE_FORMAT}/{law_id}/{location_id}.html"


def _law_location_from_url(source_url: str) -> tuple[str, str | None]:
    parsed = urlparse(source_url)
    match = _LAW_PATH_RE.match(parsed.path)
    if not match:
        raise ValueError(f"not a New York law URL: {source_url}")
    location = match.group("location")
    return match.group("law"), unquote(location) if location else None


def _normalize_href(href: str, base_url: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", href)


def _looks_like_section_label(label: str) -> bool:
    return label.upper().startswith("SECTION ")


def _all_section_children(children: tuple[NewYorkChildLink, ...]) -> bool:
    return bool(children) and all(_looks_like_section_label(child.label) for child in children)


def _new_york_run_id(version: str, *, title_filter: str | None, limit: int | None) -> str:
    if title_filter is None and limit is None:
        return version
    parts = [version, "us-ny"]
    if title_filter is not None:
        parts.append(title_filter.lower())
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _node_text(node: Tag | None) -> str:
    if not isinstance(node, Tag):
        return ""
    return node.get_text(" ", strip=True)


def _clean_text(value: str | None) -> str:
    text = (value or "").replace("\xa0", " ").replace("\u2002", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)
