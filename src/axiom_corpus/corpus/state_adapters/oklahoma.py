"""Oklahoma Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from striprtf.striprtf import rtf_to_text

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

OKLAHOMA_STATUTES_BASE_URL = "https://www.oklegislature.gov/OK_Statutes/CompleteTitles/"
OKLAHOMA_STATUTES_INDEX_SOURCE_FORMAT = "oklahoma-statutes-rtf-index"
OKLAHOMA_STATUTES_RTF_SOURCE_FORMAT = "oklahoma-statutes-rtf"
OKLAHOMA_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_RTF_HREF_RE = re.compile(r"^os(?P<title>\d+[A-Za-z]?)\.rtf$", re.I)
_SECTION_HEADER_RE = re.compile(
    r"^§(?P<label>[0-9A-Za-z]+-[0-9A-Za-z](?:[0-9A-Za-z.\-]*[0-9A-Za-z])?)"
    r"\.\s+(?P<heading>\S.*)$"
)
_SPECIAL_SECTION_HEADER_RE = re.compile(
    r"^§(?P<label>[0-9A-Za-z]+-Rule\s+\d+(?:\.\d+)*)"
    r"\.\s+(?P<heading>\S.*)$"
)
_RULE_HEADER_RE = re.compile(
    r"^Rule\s+(?P<label>\d+(?::\d+)?(?:[-:]\d+)*(?:\.\d+)*)"
    r"\.\s+(?P<heading>\S.*)$"
)
_TOC_PAGE_SUFFIX_RE = re.compile(r"(?:\t|\s{2,})\d+\s*$")
_SOURCE_HISTORY_RE = re.compile(
    r"^(?:Added by|Amended by|R\.L\.|Laws|Renumbered|Repealed by|"
    r"Promulgated by|Amendment promulgated by|Emergency adopted by)",
    re.I,
)
_SOURCE_HISTORY_SEARCH_RE = re.compile(
    r"(?P<history>(?:Added by|Amended by|R\.L\.|Laws|Renumbered|Repealed by|"
    r"Promulgated by|Amendment promulgated by|Emergency adopted by).*?)$",
    re.I,
)
_DIRECT_REFERENCE_RE = re.compile(
    r"§\s*(?P<label>[0-9A-Za-z]+-[0-9A-Za-z](?:[0-9A-Za-z.\-]*[0-9A-Za-z])?)\b"
)
_TITLE_REFERENCE_RE = re.compile(
    r"\bSections?\s+(?P<section>[0-9A-Za-z](?:[0-9A-Za-z.\-]*[0-9A-Za-z])?)"
    r"(?:\s+through\s+[0-9A-Za-z](?:[0-9A-Za-z.\-]*[0-9A-Za-z])?)?"
    r"\s+of\s+Title\s+(?P<title>[0-9A-Za-z]+)\b",
    re.I,
)


@dataclass(frozen=True)
class OklahomaTitleListing:
    """One Oklahoma title RTF file listed by the official directory."""

    title: str
    file_name: str
    source_url: str
    ordinal: int


@dataclass(frozen=True)
class OklahomaSource:
    """Recorded Oklahoma title source file."""

    title: str
    file_name: str
    source_url: str
    source_path: str
    source_format: str
    sha256: str


@dataclass(frozen=True)
class OklahomaProvision:
    """One parsed Oklahoma title or provision."""

    kind: str
    title: str
    citation_label: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int
    source: OklahomaSource
    source_history: tuple[str, ...] = ()
    references_to: tuple[str, ...] = ()
    status: str | None = None

    @property
    def source_id(self) -> str:
        if self.kind == "title":
            return f"title-{_slug(self.title)}"
        if self.citation_label.startswith("Rule "):
            return f"{_slug(self.title)}-{_slug(self.citation_label)}"
        return _slug(self.citation_label)

    @property
    def citation_path(self) -> str:
        if self.kind == "title":
            return f"us-ok/statute/title-{_slug(self.title)}"
        if self.citation_label.startswith("Rule "):
            return f"us-ok/statute/{_slug(self.title)}-{_slug(self.citation_label)}"
        return f"us-ok/statute/{_slug(self.citation_label)}"

    @property
    def legal_identifier(self) -> str:
        if self.kind == "title":
            return f"Okla. Stat. tit. {self.title}"
        if self.citation_label.startswith("Rule "):
            return f"Okla. Stat. tit. {self.title}, {self.citation_label}"
        return f"Okla. Stat. tit. {self.title}, § {self.citation_label}"


@dataclass(frozen=True)
class _OklahomaSourceFile:
    listing: OklahomaTitleListing
    data: bytes


@dataclass(frozen=True)
class _OklahomaFetchResult:
    listing: OklahomaTitleListing
    source: _OklahomaSourceFile | None = None
    error: BaseException | None = None


class _OklahomaFetcher:
    def __init__(
        self,
        *,
        source_dir: Path | None,
        download_dir: Path | None,
        base_url: str,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.base_url = base_url.rstrip("/") + "/"
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._last_request_at = 0.0
        self._request_lock = Lock()

    def fetch_index(self) -> bytes | None:
        if self.source_dir is not None:
            for name in ("index.html", "CompleteTitles.html"):
                path = self.source_dir / name
                if path.exists():
                    return path.read_bytes()
            return None
        return self._fetch(
            OKLAHOMA_STATUTES_INDEX_SOURCE_FORMAT + "/index.html",
            self.base_url,
        )

    def discover_listings(
        self,
        *,
        index_data: bytes | None,
        only_title: str | None,
    ) -> tuple[OklahomaTitleListing, ...]:
        listings: list[OklahomaTitleListing] = []
        if index_data is not None:
            soup = BeautifulSoup(index_data.decode("cp1252", errors="replace"), "html.parser")
            seen: set[str] = set()
            for anchor in soup.find_all("a"):
                text = _clean_whitespace(anchor.get_text(" "))
                match = _RTF_HREF_RE.fullmatch(text)
                if match is None or text.lower() in seen:
                    continue
                title = _normalize_title(match.group("title"))
                if only_title is not None and _title_filter(title) != only_title:
                    continue
                seen.add(text.lower())
                href = anchor.get("href") or text
                listings.append(
                    OklahomaTitleListing(
                        title=title,
                        file_name=text,
                        source_url=urljoin(self.base_url, href),
                        ordinal=len(listings) + 1,
                    )
                )
        elif self.source_dir is not None:
            for path in sorted(self.source_dir.glob("os*.rtf"), key=_title_sort_key_for_path):
                match = _RTF_HREF_RE.fullmatch(path.name)
                if match is None:
                    continue
                title = _normalize_title(match.group("title"))
                if only_title is not None and _title_filter(title) != only_title:
                    continue
                listings.append(
                    OklahomaTitleListing(
                        title=title,
                        file_name=path.name,
                        source_url=urljoin(self.base_url, path.name),
                        ordinal=len(listings) + 1,
                    )
                )
        return tuple(sorted(listings, key=lambda listing: _title_sort_key(listing.title)))

    def fetch_title(self, listing: OklahomaTitleListing) -> _OklahomaSourceFile:
        return _OklahomaSourceFile(
            listing=listing,
            data=self._fetch(
                f"{OKLAHOMA_STATUTES_RTF_SOURCE_FORMAT}/{listing.file_name}",
                listing.source_url,
                file_name=listing.file_name,
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

    def _fetch(self, relative_path: str, source_url: str, *, file_name: str | None = None) -> bytes:
        if self.source_dir is not None and file_name is not None:
            return (self.source_dir / file_name).read_bytes()
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return cached_path.read_bytes()
        data = _download_oklahoma_source(
            source_url,
            fetcher=self,
            request_delay_seconds=self.request_delay_seconds,
            timeout_seconds=self.timeout_seconds,
            request_attempts=self.request_attempts,
        )
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return data


def extract_oklahoma_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    workers: int = 4,
    download_dir: str | Path | None = None,
    base_url: str = OKLAHOMA_STATUTES_BASE_URL,
    request_delay_seconds: float = 0.05,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
) -> StateStatuteExtractReport:
    """Snapshot official Oklahoma title RTF files and extract provisions."""
    jurisdiction = "us-ok"
    title_filter = _title_filter(only_title)
    run_id = _oklahoma_run_id(version, title_filter=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)

    fetcher = _OklahomaFetcher(
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        base_url=base_url,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )
    source_paths: list[Path] = []
    index_data = fetcher.fetch_index()
    if index_data is not None:
        index_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            f"{OKLAHOMA_STATUTES_INDEX_SOURCE_FORMAT}/index.html",
        )
        store.write_bytes(index_path, index_data)
        source_paths.append(index_path)

    listings = fetcher.discover_listings(index_data=index_data, only_title=title_filter)
    if not listings:
        raise ValueError(f"no Oklahoma statute title RTF files selected for filter: {only_title!r}")

    fetched = _fetch_oklahoma_titles(fetcher, listings, workers=workers)
    records: list[ProvisionRecord] = []
    items: list[SourceInventoryItem] = []
    seen: set[str] = set()
    title_count = 0
    section_count = 0
    limit_remaining = limit

    for source_file in fetched:
        source_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            f"{OKLAHOMA_STATUTES_RTF_SOURCE_FORMAT}/{source_file.listing.file_name}",
        )
        sha256 = store.write_bytes(source_path, source_file.data)
        source_paths.append(source_path)
        source = OklahomaSource(
            title=source_file.listing.title,
            file_name=source_file.listing.file_name,
            source_url=source_file.listing.source_url,
            source_path=_store_relative_path(store, source_path),
            source_format=OKLAHOMA_STATUTES_RTF_SOURCE_FORMAT,
            sha256=sha256,
        )
        title_heading = _title_heading_from_rtf(source_file.data, source_file.listing.title)
        title_provision = OklahomaProvision(
            kind="title",
            title=source_file.listing.title,
            citation_label=source_file.listing.title,
            heading=title_heading,
            body=None,
            parent_citation_path=None,
            level=0,
            ordinal=source_file.listing.ordinal,
            source=source,
        )
        for provision in (title_provision,):
            if provision.citation_path in seen:
                continue
            seen.add(provision.citation_path)
            items.append(_inventory_item(provision))
            records.append(
                _record(
                    provision,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
            )
            title_count += 1

        sections = parse_oklahoma_title_rtf(
            source_file.data,
            source=source,
            title_heading=title_heading,
            limit=limit_remaining,
        )
        for provision in sections:
            if provision.citation_path in seen:
                continue
            seen.add(provision.citation_path)
            items.append(_inventory_item(provision))
            records.append(
                _record(
                    provision,
                    version=run_id,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
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
        container_count=0,
        section_count=section_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def parse_oklahoma_title_rtf(
    rtf_data: str | bytes,
    *,
    source: OklahomaSource,
    title_heading: str | None = None,
    limit: int | None = None,
) -> tuple[OklahomaProvision, ...]:
    """Parse one official Oklahoma title RTF file into normalized sections."""
    _ = title_heading
    text = _rtf_to_text(rtf_data)
    lines = _body_lines(text.splitlines())
    headers: list[tuple[int, str, str, str]] = []
    for index, line in enumerate(lines):
        parsed = _parse_header(_clean_whitespace(line))
        if parsed is None:
            continue
        label, heading = parsed
        headers.append((index, label, heading, _kind_for_label(label)))

    if not headers:
        raise ValueError(f"Oklahoma title {source.title} has no section or rule headings")

    provisions: list[OklahomaProvision] = []
    for ordinal, (line_index, label, heading, kind) in enumerate(headers, start=1):
        if limit is not None and len(provisions) >= limit:
            break
        next_index = headers[ordinal][0] if ordinal < len(headers) else len(lines)
        body_lines = lines[line_index + 1 : next_index]
        body = _normalize_body(body_lines)
        text_for_refs = "\n".join([heading, body or ""])
        provisions.append(
            OklahomaProvision(
                kind=kind,
                title=source.title,
                citation_label=label,
                heading=_strip_terminal_period(heading),
                body=body,
                parent_citation_path=f"us-ok/statute/title-{_slug(source.title)}",
                level=1,
                ordinal=ordinal,
                source=source,
                source_history=tuple(_source_history(body_lines)),
                references_to=tuple(_extract_references(text_for_refs, self_label=label)),
                status=_status(heading, body),
            )
        )
    return tuple(provisions)


def _fetch_oklahoma_titles(
    fetcher: _OklahomaFetcher,
    listings: tuple[OklahomaTitleListing, ...],
    *,
    workers: int,
) -> tuple[_OklahomaSourceFile, ...]:
    results: list[_OklahomaFetchResult] = []
    if workers <= 1 or len(listings) <= 1:
        for listing in listings:
            try:
                results.append(_OklahomaFetchResult(listing, source=fetcher.fetch_title(listing)))
            except BaseException as exc:
                results.append(_OklahomaFetchResult(listing, error=exc))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetcher.fetch_title, listing): listing for listing in listings}
            for future in as_completed(futures):
                listing = futures[future]
                try:
                    results.append(_OklahomaFetchResult(listing, source=future.result()))
                except BaseException as exc:
                    results.append(_OklahomaFetchResult(listing, error=exc))
    errors = [f"{result.listing.file_name}: {result.error}" for result in results if result.error]
    if errors:
        raise RuntimeError("; ".join(errors[:5]))
    return tuple(
        result.source
        for result in sorted(results, key=lambda result: _title_sort_key(result.listing.title))
        if result.source is not None
    )


def _download_oklahoma_source(
    source_url: str,
    *,
    fetcher: _OklahomaFetcher,
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
                headers={"User-Agent": OKLAHOMA_USER_AGENT},
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last_error = exc
            if attempt < request_attempts:
                time.sleep(max(0.0, request_delay_seconds) + 0.5 * attempt)
    if last_error is not None:
        raise last_error
    raise ValueError(f"Oklahoma source request failed: {source_url}")


def _rtf_to_text(rtf_data: str | bytes) -> str:
    if isinstance(rtf_data, bytes):
        raw_text = rtf_data.decode("cp1252", errors="replace")
    else:
        raw_text = rtf_data
    return rtf_to_text(raw_text)


def _body_lines(lines: list[str]) -> list[str]:
    heading_positions: dict[str, int] = {}
    for index, line in enumerate(lines):
        parsed = _parse_header(_clean_whitespace(line), strip_toc_page=True)
        if parsed is None:
            continue
        key = _header_key(parsed[0])
        if key in heading_positions:
            return lines[index:]
        heading_positions[key] = index
    return lines


def _parse_header(line: str, *, strip_toc_page: bool = False) -> tuple[str, str] | None:
    if not line:
        return None
    stripped = _strip_toc_page(line) if strip_toc_page else line.strip()
    for pattern in (_SPECIAL_SECTION_HEADER_RE, _SECTION_HEADER_RE, _RULE_HEADER_RE):
        match = pattern.match(stripped)
        if match is not None:
            raw_label = match.group("label")
            if pattern is _RULE_HEADER_RE:
                raw_label = f"Rule {raw_label}"
            label = _normalize_label(raw_label)
            heading = _strip_terminal_period(match.group("heading"))
            return label, heading
    return None


def _strip_toc_page(line: str) -> str:
    return _TOC_PAGE_SUFFIX_RE.sub("", line).strip()


def _header_key(label: str) -> str:
    return _slug(label)


def _kind_for_label(label: str) -> str:
    return "rule" if label.startswith("Rule ") else "section"


def _inventory_item(provision: OklahomaProvision) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=provision.citation_path,
        source_url=provision.source.source_url,
        source_path=provision.source.source_path,
        source_format=provision.source.source_format,
        sha256=provision.source.sha256,
        metadata=_metadata(provision),
    )


def _record(
    provision: OklahomaProvision,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(provision.citation_path),
        jurisdiction="us-ok",
        document_class=DocumentClass.STATUTE.value,
        citation_path=provision.citation_path,
        body=provision.body,
        heading=provision.heading,
        citation_label=provision.legal_identifier,
        version=version,
        source_url=provision.source.source_url,
        source_path=provision.source.source_path,
        source_id=provision.source_id,
        source_format=provision.source.source_format,
        source_as_of=source_as_of,
        expression_date=expression_date,
        parent_citation_path=provision.parent_citation_path,
        parent_id=(
            deterministic_provision_id(provision.parent_citation_path)
            if provision.parent_citation_path
            else None
        ),
        level=provision.level,
        ordinal=provision.ordinal,
        kind=provision.kind,
        legal_identifier=provision.legal_identifier,
        identifiers=_identifiers(provision),
        metadata=_metadata(provision),
    )


def _identifiers(provision: OklahomaProvision) -> dict[str, str]:
    identifiers = {"oklahoma:title": provision.title}
    if provision.kind != "title":
        identifiers["oklahoma:provision"] = provision.citation_label
    return identifiers


def _metadata(provision: OklahomaProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "title": provision.title,
        "file_name": provision.source.file_name,
    }
    if provision.kind != "title":
        metadata["provision"] = provision.citation_label
    if provision.source_history:
        metadata["source_history"] = list(provision.source_history)
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.status:
        metadata["status"] = provision.status
    return metadata


def _source_history(lines: list[str]) -> list[str]:
    history: list[str] = []
    for paragraph in _paragraphs(lines):
        if _SOURCE_HISTORY_RE.match(paragraph):
            history.append(paragraph)
            continue
        match = _SOURCE_HISTORY_SEARCH_RE.search(paragraph)
        if match is not None:
            history.append(match.group("history"))
    return history[-6:]


def _extract_references(text: str, *, self_label: str) -> list[str]:
    refs: list[str] = []
    for match in _DIRECT_REFERENCE_RE.finditer(text):
        label = _normalize_label(match.group("label"))
        if label != self_label:
            refs.append(f"us-ok/statute/{_slug(label)}")
    for match in _TITLE_REFERENCE_RE.finditer(text):
        label = _normalize_label(f"{match.group('title')}-{match.group('section')}")
        if label != self_label:
            refs.append(f"us-ok/statute/{_slug(label)}")
    return _dedupe_preserve_order(refs)


def _status(heading: str | None, body: str | None) -> str | None:
    text = "\n".join([heading or "", body or ""])
    if re.search(r"\bRepealed\b", text, re.I):
        return "repealed"
    if re.search(r"\bRenumbered\b", text, re.I):
        return "renumbered"
    if re.search(r"\bExpired\b", text, re.I):
        return "expired"
    if re.search(r"\bExecuted\b", text, re.I):
        return "executed"
    return None


def _normalize_body(lines: list[str]) -> str | None:
    paragraphs = _paragraphs(lines)
    return "\n\n".join(paragraphs) or None


def _paragraphs(lines: list[str]) -> list[str]:
    paragraphs: list[str] = []
    current: list[str] = []
    for line in lines:
        clean = _clean_whitespace(line)
        if not clean:
            if current:
                paragraphs.append(" ".join(current))
                current = []
            continue
        current.append(clean)
    if current:
        paragraphs.append(" ".join(current))
    return paragraphs


def _title_heading_from_rtf(data: bytes, title: str) -> str:
    raw = data.decode("cp1252", errors="replace")
    pattern = re.compile(
        rf"\bTITLE\s+{re.escape(title)}\.\s*(?P<heading>[A-Z][^\\{{}}\r\n]+)",
        re.I,
    )
    match = pattern.search(raw)
    if match is not None:
        return f"Title {title}. {_title_case_heading(match.group('heading'))}"
    appendix_match = re.search(
        r"\bTITLE\s+74,\s*(?P<heading>APPENDIX\s+I[^\\{}\r\n]+)",
        raw,
        re.I,
    )
    if appendix_match is not None and title == "74E":
        return f"Title 74, {_title_case_heading(appendix_match.group('heading'))}"
    return f"Title {title}"


def _title_case_heading(value: str) -> str:
    text = _clean_whitespace(value)
    return " ".join(part if part.isupper() and len(part) <= 3 else part.title() for part in text.split())


def _normalize_title(value: str) -> str:
    return value.strip().upper()


def _title_filter(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = re.sub(r"^(?:title|tit\.)[-\s]*", "", text, flags=re.I)
    return _normalize_title(text)


def _normalize_label(value: str) -> str:
    text = _clean_whitespace(value).removeprefix("§")
    text = text.replace(" - ", "-")
    text = re.sub(r"\s+", " ", text)
    if text.startswith("Rule "):
        return "Rule " + text.removeprefix("Rule ").strip()
    return text.upper() if re.fullmatch(r"[0-9A-Za-z]+-[0-9A-Za-z.\-]+", text) else text


def _oklahoma_run_id(
    version: str,
    *,
    title_filter: str | None,
    limit: int | None,
) -> str:
    if title_filter is None and limit is None:
        return version
    parts = [version, "us-ok"]
    if title_filter is not None:
        parts.append(f"title-{_slug(title_filter)}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _title_sort_key_for_path(path: Path) -> tuple[int, str]:
    match = _RTF_HREF_RE.fullmatch(path.name)
    if match is None:
        return (10**9, path.name.lower())
    return _title_sort_key(_normalize_title(match.group("title")))


def _title_sort_key(title: str) -> tuple[int, str]:
    match = re.match(r"(?P<number>\d+)(?P<suffix>[A-Z]*)$", title)
    if match is None:
        return (10**9, title)
    return int(match.group("number")), match.group("suffix")


def _clean_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def _strip_terminal_period(value: str) -> str:
    return value.strip().removesuffix(".").strip()


def _slug(value: str) -> str:
    text = value.strip().lower()
    text = text.replace("§", "")
    text = text.replace(".", "-")
    text = re.sub(r"[^0-9a-z]+", "-", text)
    return text.strip("-")


def _store_relative_path(store: CorpusArtifactStore, path: Path) -> str:
    try:
        return path.relative_to(store.root).as_posix()
    except ValueError:
        return path.as_posix()


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
