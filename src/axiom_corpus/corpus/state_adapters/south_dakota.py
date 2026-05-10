"""South Dakota Codified Laws source-first corpus adapter."""

from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

SOUTH_DAKOTA_STATUTES_BASE_URL = "https://sdlegislature.gov"
SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT = "south-dakota-statutes-json"
SOUTH_DAKOTA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_SECTION_LABEL_RE = re.compile(
    r"\b(?P<label>\d+[A-Z]?-\d+[A-Z]?-[0-9A-Z]+(?:\.[0-9A-Z]+)*)\b",
    re.I,
)
_CHAPTER_LABEL_RE = re.compile(r"^(?P<title>\d+[A-Z]?)-(?P<chapter>\d+[A-Z]?)$", re.I)
_BODY_HEADER_RE = re.compile(
    r"^(?P<label>\d+[A-Z]?-\d+[A-Z]?-[0-9A-Z]+(?:\.[0-9A-Z]+)*)\s*\.\s*(?P<heading>.*)$",
    re.I,
)
_REPEATED_LABEL_RE = re.compile(
    r"^(?P<label>\d+[A-Z]?-\d+[A-Z]?-[0-9A-Z]+(?:\.[0-9A-Z]+)*)\s*\.\s*",
    re.I,
)


@dataclass(frozen=True)
class SouthDakotaSource:
    """One recorded South Dakota API source document."""

    source_url: str
    source_path: str
    source_format: str
    sha256: str


@dataclass(frozen=True)
class SouthDakotaTitle:
    """One title listed by the official South Dakota statutes API."""

    label: str
    heading: str | None
    statute_id: int | None
    ordinal: int
    raw: dict[str, Any]

    @property
    def source_id(self) -> str:
        return f"title-{_slug(self.label)}"

    @property
    def citation_path(self) -> str:
        return f"us-sd/statute/{self.source_id}"

    @property
    def legal_identifier(self) -> str:
        return f"S.D. Codified Laws title {self.label}"


@dataclass(frozen=True)
class SouthDakotaChapter:
    """One chapter in the South Dakota Codified Laws."""

    label: str
    title: str
    heading: str | None
    statute_id: int | None
    parent_citation_path: str
    ordinal: int
    raw: dict[str, Any]
    source: SouthDakotaSource

    @property
    def source_id(self) -> str:
        return f"chapter-{_slug(self.label)}"

    @property
    def citation_path(self) -> str:
        return f"us-sd/statute/title-{_slug(self.title)}/chapter-{_slug(self.label)}"

    @property
    def legal_identifier(self) -> str:
        return f"S.D. Codified Laws ch. {self.label}"


@dataclass(frozen=True)
class SouthDakotaSection:
    """One section or official section stub from a South Dakota chapter."""

    label: str
    heading: str | None
    body: str | None
    parent_citation_path: str
    ordinal: int
    source_history: tuple[str, ...]
    references_to: tuple[str, ...]
    status: str | None
    source: SouthDakotaSource

    @property
    def source_id(self) -> str:
        return _slug(self.label)

    @property
    def citation_path(self) -> str:
        return f"us-sd/statute/{_slug(self.label)}"

    @property
    def legal_identifier(self) -> str:
        return f"S.D. Codified Laws \u00a7 {self.label}"


@dataclass(frozen=True)
class _SectionDraft:
    label: str
    heading: str | None
    ordinal: int
    body: str | None = None
    source_history: tuple[str, ...] = ()
    references_to: tuple[str, ...] = ()
    status: str | None = None


@dataclass(frozen=True)
class _SourceDocument:
    label: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _FetchResult:
    label: str
    source: _SourceDocument | None = None
    error: BaseException | None = None


class _SouthDakotaFetcher:
    def __init__(
        self,
        *,
        base_url: str,
        source_dir: Path | None,
        download_dir: Path | None,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_effective_date(self) -> bytes:
        return self._fetch_json(
            f"{SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT}/effective-date.json",
            urljoin(self.base_url, "/api/Statutes/LastStatuesEffectiveDate"),
        )

    def fetch_titles(self) -> bytes:
        return self._fetch_json(
            f"{SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT}/titles.json",
            urljoin(self.base_url, "/api/Statutes/Title"),
        )

    def fetch_statute(self, label: str) -> _SourceDocument:
        return _SourceDocument(
            label=label,
            source_url=urljoin(self.base_url, f"/api/Statutes/Statute/{label}"),
            data=self._fetch_json(
                f"{SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT}/statute-{_slug(label)}.json",
                urljoin(self.base_url, f"/api/Statutes/Statute/{label}"),
            ),
        )

    def wait_for_request_slot(self) -> None:  # pragma: no cover
        if self.request_delay_seconds <= 0:
            return
        with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)
            self._last_request_at = time.monotonic()

    def _fetch_json(self, relative_path: str, source_url: str) -> bytes:
        if self.source_dir is not None:
            path = self.source_dir / relative_path
            if path.exists():
                return path.read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_south_dakota_json(
            source_url,
            fetcher=self,
            request_delay_seconds=self.request_delay_seconds,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
        )
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return data


def extract_south_dakota_codified_laws(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    only_chapter: str | None = None,
    limit: int | None = None,
    workers: int = 8,
    download_dir: str | Path | None = None,
    base_url: str = SOUTH_DAKOTA_STATUTES_BASE_URL,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
) -> StateStatuteExtractReport:
    """Snapshot official South Dakota Codified Laws API JSON and extract provisions."""
    jurisdiction = "us-sd"
    title_filter = _title_filter(only_title)
    chapter_filter = _chapter_filter(only_chapter)
    run_id = _south_dakota_run_id(version, title_filter=title_filter, chapter_filter=chapter_filter, limit=limit)
    source_as_of_text = source_as_of or version

    fetcher = _SouthDakotaFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    source_paths: list[Path] = []
    effective_date_data = fetcher.fetch_effective_date()
    effective_date_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        f"{SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT}/effective-date.json",
    )
    store.write_bytes(effective_date_path, effective_date_data)
    source_paths.append(effective_date_path)
    effective_date_text = _effective_date_text(effective_date_data)
    expression_date_text = _date_text(expression_date, effective_date_text or source_as_of_text)

    titles_data = fetcher.fetch_titles()
    titles_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        f"{SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT}/titles.json",
    )
    store.write_bytes(titles_path, titles_data)
    source_paths.append(titles_path)

    title_rows = _title_rows(titles_data, only_title=title_filter)
    if not title_rows:
        raise ValueError(f"no South Dakota titles selected for filter: {only_title!r}")

    records: list[ProvisionRecord] = []
    items: list[SourceInventoryItem] = []
    seen: set[str] = set()
    title_count = 0
    chapter_count = 0
    section_count = 0
    limit_remaining = limit

    chapter_targets: list[tuple[str, str, int, str]] = []
    title_details = _fetch_south_dakota_statutes(fetcher, [title["Statute"] for title in title_rows], workers=workers)
    for title_source_doc, title_row in zip(title_details, title_rows, strict=True):
        title_label = str(title_row["Statute"])
        title_source_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            f"{SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT}/statute-{_slug(title_label)}.json",
        )
        title_sha256 = store.write_bytes(title_source_path, title_source_doc.data)
        source_paths.append(title_source_path)
        title_source = SouthDakotaSource(
            source_url=title_source_doc.source_url,
            source_path=_store_relative_path(store, title_source_path),
            source_format=SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT,
            sha256=title_sha256,
        )
        title = SouthDakotaTitle(
            label=title_label,
            heading=_clean_whitespace(title_row.get("CatchLine")),
            statute_id=_optional_int(title_row.get("StatuteId")),
            ordinal=len(records) + 1,
            raw=title_row,
        )
        _append_unique(
            title.citation_path,
            seen=seen,
            items=items,
            records=records,
            item=_inventory_item(
                title.citation_path,
                source=title_source,
                metadata=_title_metadata(title, effective_date_text=effective_date_text),
            ),
            record=_title_record(
                title,
                source=title_source,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                effective_date=effective_date_text,
            ),
        )
        title_count += 1

        title_payload = _json_object(title_source_doc.data)
        chapter_labels = _chapter_labels_from_title_html(title_payload.get("Html") or "", title_label)
        for ordinal, chapter_label in enumerate(chapter_labels, start=1):
            if chapter_filter is not None and _chapter_filter(chapter_label) != chapter_filter:
                continue
            chapter_targets.append((title_label, chapter_label, ordinal, title.citation_path))

    fetched_chapters = _fetch_south_dakota_statutes(
        fetcher,
        [chapter_label for _, chapter_label, _, _ in chapter_targets],
        workers=workers,
    )
    chapter_by_label = {source.label: source for source in fetched_chapters}
    for title_label, chapter_label, chapter_ordinal, title_citation_path in chapter_targets:
        source_doc = chapter_by_label[chapter_label]
        chapter_source_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            f"{SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT}/statute-{_slug(chapter_label)}.json",
        )
        chapter_sha256 = store.write_bytes(chapter_source_path, source_doc.data)
        source_paths.append(chapter_source_path)
        source = SouthDakotaSource(
            source_url=source_doc.source_url,
            source_path=_store_relative_path(store, chapter_source_path),
            source_format=SOUTH_DAKOTA_STATUTES_SOURCE_FORMAT,
            sha256=chapter_sha256,
        )
        chapter_payload = _json_object(source_doc.data)
        chapter = SouthDakotaChapter(
            label=chapter_label,
            title=title_label,
            heading=_clean_whitespace(chapter_payload.get("CatchLine")),
            statute_id=_optional_int(chapter_payload.get("StatuteId")),
            parent_citation_path=title_citation_path,
            ordinal=chapter_ordinal,
            raw=chapter_payload,
            source=source,
        )
        _append_unique(
            chapter.citation_path,
            seen=seen,
            items=items,
            records=records,
            item=_inventory_item(
                chapter.citation_path,
                source=source,
                metadata=_chapter_metadata(chapter, effective_date_text=effective_date_text),
            ),
            record=_chapter_record(
                chapter,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                effective_date=effective_date_text,
            ),
        )
        chapter_count += 1

        sections = parse_south_dakota_chapter_html(
            chapter_payload.get("Html") or "",
            chapter_label=chapter_label,
            source=source,
            parent_citation_path=chapter.citation_path,
            limit=limit_remaining,
        )
        for section in sections:
            _append_unique(
                section.citation_path,
                seen=seen,
                items=items,
                records=records,
                item=_inventory_item(
                    section.citation_path,
                    source=source,
                    metadata=_section_metadata(section, effective_date_text=effective_date_text),
                ),
                record=_section_record(
                    section,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    effective_date=effective_date_text,
                ),
            )
            section_count += 1
            if limit_remaining is not None:
                limit_remaining -= 1
                if limit_remaining <= 0:
                    break
        if limit_remaining is not None and limit_remaining <= 0:
            break

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
        container_count=chapter_count,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def parse_south_dakota_chapter_html(
    html: str,
    *,
    chapter_label: str,
    source: SouthDakotaSource,
    parent_citation_path: str,
    limit: int | None = None,
) -> tuple[SouthDakotaSection, ...]:
    """Parse one official South Dakota chapter HTML bundle into sections."""
    texts = _paragraph_texts(html)
    body_start = _body_start_index(texts, chapter_label)
    toc_texts = texts[:body_start] if body_start is not None else texts
    body_texts = texts[body_start:] if body_start is not None else []

    drafts: dict[str, _SectionDraft] = {}
    order: list[str] = []
    for text in toc_texts:
        parsed = _parse_toc_entry(text, chapter_label)
        if parsed is None:
            continue
        label, heading = parsed
        if label not in drafts:
            order.append(label)
            drafts[label] = _SectionDraft(
                label=label,
                heading=heading,
                ordinal=len(order),
                status=_status(heading, None),
                references_to=tuple(_extract_references(heading, self_label=label)),
            )

    for parsed in _body_drafts(body_texts, chapter_label):
        existing = drafts.get(parsed.label)
        if existing is None:
            order.append(parsed.label)
            drafts[parsed.label] = _SectionDraft(
                label=parsed.label,
                heading=parsed.heading,
                body=parsed.body,
                ordinal=len(order),
                source_history=parsed.source_history,
                references_to=parsed.references_to,
                status=parsed.status,
            )
        else:
            drafts[parsed.label] = _SectionDraft(
                label=parsed.label,
                heading=parsed.heading or existing.heading,
                body=parsed.body,
                ordinal=existing.ordinal,
                source_history=parsed.source_history,
                references_to=parsed.references_to or existing.references_to,
                status=parsed.status or existing.status,
            )

    sections: list[SouthDakotaSection] = []
    for label in order:
        if limit is not None and len(sections) >= limit:
            break
        draft = drafts[label]
        sections.append(
            SouthDakotaSection(
                label=draft.label,
                heading=draft.heading,
                body=draft.body,
                parent_citation_path=parent_citation_path,
                ordinal=draft.ordinal,
                source_history=draft.source_history,
                references_to=draft.references_to,
                status=draft.status,
                source=source,
            )
        )
    return tuple(sections)


def _body_drafts(texts: list[str], chapter_label: str) -> tuple[_SectionDraft, ...]:
    drafts: list[_SectionDraft] = []
    current_label: str | None = None
    current_heading: str | None = None
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_label, current_heading, current_lines
        if current_label is None:
            return
        body, source_history = _split_body_and_source_history(current_lines)
        text_for_refs = "\n".join([current_heading or "", body or ""])
        drafts.append(
            _SectionDraft(
                label=current_label,
                heading=current_heading,
                body=body,
                ordinal=len(drafts) + 1,
                source_history=source_history,
                references_to=tuple(_extract_references(text_for_refs, self_label=current_label)),
                status=_status(current_heading, body),
            )
        )
        current_label = None
        current_heading = None
        current_lines = []

    for text in texts:
        parsed = _parse_body_header(text, chapter_label)
        if parsed is not None:
            flush()
            current_label, current_heading = parsed
            continue
        if current_label is not None:
            current_lines.append(text)
    flush()
    return tuple(drafts)


def _fetch_south_dakota_statutes(
    fetcher: _SouthDakotaFetcher,
    labels: list[str],
    *,
    workers: int,
) -> tuple[_SourceDocument, ...]:
    results: list[_FetchResult] = []
    if workers <= 1 or len(labels) <= 1:
        for label in labels:
            try:
                results.append(_FetchResult(label, source=fetcher.fetch_statute(label)))
            except BaseException as exc:
                results.append(_FetchResult(label, error=exc))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetcher.fetch_statute, label): label for label in labels}
            for future in as_completed(futures):
                label = futures[future]
                try:
                    results.append(_FetchResult(label, source=future.result()))
                except BaseException as exc:
                    results.append(_FetchResult(label, error=exc))
    errors = [f"{result.label}: {result.error}" for result in results if result.error]
    if errors:
        raise RuntimeError("; ".join(errors[:5]))
    by_label = {result.label: result.source for result in results if result.source is not None}
    return tuple(by_label[label] for label in labels if by_label.get(label) is not None)


def _download_south_dakota_json(
    source_url: str,
    *,
    fetcher: _SouthDakotaFetcher,
    request_delay_seconds: float,
    timeout_seconds: float,
    request_attempts: int,
) -> bytes:
    last_error: BaseException | None = None
    for attempt in range(1, max(1, request_attempts) + 1):
        try:
            fetcher.wait_for_request_slot()
            response = requests.get(
                source_url,
                timeout=timeout_seconds,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": SOUTH_DAKOTA_USER_AGENT,
                },
            )
            response.raise_for_status()
            data = response.content
            json.loads(data)
            return data
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < request_attempts:
                time.sleep(_retry_delay(exc, request_delay_seconds=request_delay_seconds, attempt=attempt))
    if last_error is not None:
        raise last_error
    raise ValueError(f"South Dakota source request failed: {source_url}")


def _retry_delay(
    exc: BaseException,
    *,
    request_delay_seconds: float,
    attempt: int,
) -> float:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), request_delay_seconds)
            except ValueError:
                pass
        if exc.response.status_code == 429:
            return max(request_delay_seconds, min(60.0, 5.0 * attempt))
    return max(0.0, request_delay_seconds) + 0.5 * attempt


def _title_rows(titles_data: bytes, *, only_title: str | None) -> list[dict[str, Any]]:
    rows = _json_array(titles_data)
    selected = [
        row
        for row in rows
        if row.get("Type") == "Title"
        and (only_title is None or _title_filter(row.get("Statute")) == only_title)
    ]
    return sorted(selected, key=lambda row: _label_sort_key(str(row.get("Statute") or "")))


def _chapter_labels_from_title_html(html: str, title_label: str) -> tuple[str, ...]:
    first_document = re.split(r"</html>\s*<hr>\s*<br>", html, maxsplit=1, flags=re.I)[0]
    soup = BeautifulSoup(first_document, "html.parser")
    labels: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a"):
        label = _label_from_href(anchor.get("href"))
        if label is None or not _is_chapter_label_for_title(label, title_label):
            continue
        normalized = _normalize_label(label)
        if normalized in seen:
            continue
        seen.add(normalized)
        labels.append(normalized)
    return tuple(labels)


def _paragraph_texts(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    texts: list[str] = []
    for paragraph in soup.find_all("p"):
        text = _clean_whitespace(paragraph.get_text(" "))
        if text:
            texts.append(text)
    return texts


def _body_start_index(texts: list[str], chapter_label: str) -> int | None:
    toc_labels: set[str] = set()
    for index, text in enumerate(texts):
        parsed = _parse_body_header(text, chapter_label)
        if parsed is not None and parsed[0] in toc_labels:
            return index
        toc_entry = _parse_toc_entry(text, chapter_label)
        if toc_entry is not None:
            toc_labels.add(toc_entry[0])
    return None


def _parse_toc_entry(text: str, chapter_label: str) -> tuple[str, str | None] | None:
    if not text.startswith(chapter_label + "-"):
        return None
    match = _SECTION_LABEL_RE.match(text)
    if match is None:
        return None
    label = _normalize_label(match.group("label"))
    if not label.startswith(chapter_label + "-"):
        return None
    heading = _clean_heading(text[match.end() :])
    return label, heading


def _parse_body_header(text: str, chapter_label: str) -> tuple[str, str | None] | None:
    match = _BODY_HEADER_RE.match(text)
    if match is None:
        return None
    label = _normalize_label(match.group("label"))
    if not label.startswith(chapter_label + "-"):
        return None
    heading = _clean_heading(match.group("heading"))
    return label, heading


def _clean_heading(value: str | None) -> str | None:
    text = _clean_whitespace(value)
    if not text:
        return None
    text = _REPEATED_LABEL_RE.sub("", text).strip()
    return _strip_terminal_period(text)


def _split_body_and_source_history(lines: list[str]) -> tuple[str | None, tuple[str, ...]]:
    body_lines: list[str] = []
    source_history: list[str] = []
    for line in lines:
        if not line:
            continue
        if line.lower().startswith("source:"):
            history = _clean_whitespace(line.split(":", 1)[1])
            if history:
                source_history.append(history)
            continue
        body_lines.append(line)
    return _normalize_body(body_lines), tuple(source_history)


def _normalize_body(lines: list[str]) -> str | None:
    cleaned = [_clean_whitespace(line) for line in lines]
    cleaned = [line for line in cleaned if line]
    if not cleaned:
        return None
    return "\n".join(cleaned)


def _extract_references(text: str, *, self_label: str) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()
    self_path = f"us-sd/statute/{_slug(self_label)}"
    for match in _SECTION_LABEL_RE.finditer(text):
        label = _normalize_label(match.group("label"))
        path = f"us-sd/statute/{_slug(label)}"
        if path == self_path or path in seen:
            continue
        seen.add(path)
        refs.append(path)
    return refs


def _title_record(
    title: SouthDakotaTitle,
    *,
    source: SouthDakotaSource,
    version: str,
    source_as_of: str,
    expression_date: str,
    effective_date: str | None,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(title.citation_path),
        jurisdiction="us-sd",
        document_class=DocumentClass.STATUTE.value,
        citation_path=title.citation_path,
        body=None,
        heading=title.heading,
        citation_label=title.legal_identifier,
        version=version,
        source_url=source.source_url,
        source_path=source.source_path,
        source_id=title.source_id,
        source_format=source.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=None,
        parent_id=None,
        level=0,
        ordinal=title.ordinal,
        kind="title",
        legal_identifier=title.legal_identifier,
        identifiers={"south_dakota:title": title.label},
        metadata=_title_metadata(title, effective_date_text=effective_date),
    )


def _chapter_record(
    chapter: SouthDakotaChapter,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
    effective_date: str | None,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(chapter.citation_path),
        jurisdiction="us-sd",
        document_class=DocumentClass.STATUTE.value,
        citation_path=chapter.citation_path,
        body=None,
        heading=chapter.heading,
        citation_label=chapter.legal_identifier,
        version=version,
        source_url=chapter.source.source_url,
        source_path=chapter.source.source_path,
        source_id=chapter.source_id,
        source_format=chapter.source.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=chapter.parent_citation_path,
        parent_id=deterministic_provision_id(chapter.parent_citation_path),
        level=1,
        ordinal=chapter.ordinal,
        kind="chapter",
        legal_identifier=chapter.legal_identifier,
        identifiers={
            "south_dakota:title": chapter.title,
            "south_dakota:chapter": chapter.label,
        },
        metadata=_chapter_metadata(chapter, effective_date_text=effective_date),
    )


def _section_record(
    section: SouthDakotaSection,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
    effective_date: str | None,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(section.citation_path),
        jurisdiction="us-sd",
        document_class=DocumentClass.STATUTE.value,
        citation_path=section.citation_path,
        body=section.body,
        heading=section.heading,
        citation_label=section.legal_identifier,
        version=version,
        source_url=section.source.source_url,
        source_path=section.source.source_path,
        source_id=section.source_id,
        source_format=section.source.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=section.parent_citation_path,
        parent_id=deterministic_provision_id(section.parent_citation_path),
        level=2,
        ordinal=section.ordinal,
        kind="section",
        legal_identifier=section.legal_identifier,
        identifiers={"south_dakota:section": section.label},
        metadata=_section_metadata(section, effective_date_text=effective_date),
    )


def _inventory_item(
    citation_path: str,
    *,
    source: SouthDakotaSource,
    metadata: dict[str, Any],
) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=citation_path,
        source_url=source.source_url,
        source_path=source.source_path,
        source_format=source.source_format,
        sha256=source.sha256,
        metadata=metadata,
    )


def _append_unique(
    citation_path: str,
    *,
    seen: set[str],
    items: list[SourceInventoryItem],
    records: list[ProvisionRecord],
    item: SourceInventoryItem,
    record: ProvisionRecord,
) -> None:
    if citation_path in seen:
        return
    seen.add(citation_path)
    items.append(item)
    records.append(record)


def _title_metadata(title: SouthDakotaTitle, *, effective_date_text: str | None) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source_authority": "South Dakota Legislative Research Council",
        "source_api_type": "Title",
        "upstream": _trim_raw(title.raw),
    }
    if effective_date_text:
        metadata["last_statutes_effective_date"] = effective_date_text
    return metadata


def _chapter_metadata(
    chapter: SouthDakotaChapter, *, effective_date_text: str | None
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source_authority": "South Dakota Legislative Research Council",
        "source_api_type": "Chapter",
        "south_dakota_statute_id": chapter.statute_id,
        "upstream": _trim_raw(chapter.raw),
    }
    if effective_date_text:
        metadata["last_statutes_effective_date"] = effective_date_text
    return {key: value for key, value in metadata.items() if value is not None}


def _section_metadata(
    section: SouthDakotaSection, *, effective_date_text: str | None
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source_authority": "South Dakota Legislative Research Council",
        "source_api_type": "Chapter",
    }
    if section.source_history:
        metadata["source_history"] = list(section.source_history)
    if section.references_to:
        metadata["references_to"] = list(section.references_to)
    if section.status:
        metadata["status"] = section.status
    if effective_date_text:
        metadata["last_statutes_effective_date"] = effective_date_text
    return metadata


def _trim_raw(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in raw.items() if key not in {"Html", "Word"}}


def _status(heading: str | None, body: str | None) -> str | None:
    text = f"{heading or ''}\n{body or ''}".lower()
    if "unconstitutional" in text:
        return "unconstitutional"
    if "transferred" in text:
        return "transferred"
    if "superseded" in text:
        return "superseded"
    if "repealed" in text:
        return "repealed"
    return None


def _effective_date_text(data: bytes) -> str | None:
    value = json.loads(data)
    if not isinstance(value, str) or not value:
        return None
    return value.split("T", 1)[0]


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _json_array(data: bytes) -> list[dict[str, Any]]:
    value = json.loads(data)
    if not isinstance(value, list):
        raise ValueError("expected South Dakota API array")
    return [item for item in value if isinstance(item, dict)]


def _json_object(data: bytes) -> dict[str, Any]:
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("expected South Dakota API object")
    return value


def _label_from_href(href: str | None) -> str | None:
    if not href:
        return None
    parsed = urlparse(href)
    query = parsed.query
    if "Statute=" in query:
        for part in query.split("&"):
            if part.startswith("Statute="):
                return part.split("=", 1)[1]
    path = parsed.path.rstrip("/")
    if "/Statutes/" in path:
        return path.rsplit("/", 1)[-1]
    return None


def _is_chapter_label_for_title(label: str, title_label: str) -> bool:
    match = _CHAPTER_LABEL_RE.match(label)
    return match is not None and _normalize_label(match.group("title")) == _normalize_label(title_label)


def _normalize_label(value: str) -> str:
    return value.strip().strip(".").upper()


def _clean_whitespace(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def _strip_terminal_period(value: str) -> str:
    return value.rstrip().removesuffix(".").rstrip()


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    return _normalize_label(str(value))


def _chapter_filter(value: str | None) -> str | None:
    if value is None:
        return None
    return _normalize_label(value)


def _label_sort_key(label: str) -> tuple[int, str, int, str, int, str]:
    match = re.match(r"^(?P<title>\d+)(?P<title_letter>[A-Z]?)(?:-(?P<chapter>\d+)(?P<chapter_letter>[A-Z]?))?(?:-(?P<section>\d+)(?P<section_rest>.*))?$", label, re.I)
    if match is None:
        return (10**9, label, 10**9, "", 10**9, "")
    return (
        int(match.group("title")),
        (match.group("title_letter") or "").upper(),
        int(match.group("chapter") or 0),
        (match.group("chapter_letter") or "").upper(),
        int(match.group("section") or 0),
        (match.group("section_rest") or "").upper(),
    )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _south_dakota_run_id(
    version: str,
    *,
    title_filter: str | None,
    chapter_filter: str | None,
    limit: int | None,
) -> str:
    parts = [version]
    if title_filter:
        parts.append(f"title-{_slug(title_filter)}")
    if chapter_filter:
        parts.append(f"chapter-{_slug(chapter_filter)}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _store_relative_path(store: CorpusArtifactStore, path: Path) -> str:
    try:
        return str(path.relative_to(store.root))
    except ValueError:
        return str(path)


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)
