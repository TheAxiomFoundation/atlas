"""New York Codes, Rules and Regulations source-first adapter."""

from __future__ import annotations

import re
import sys
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Protocol, TextIO
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore, sha256_bytes
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

NYCRR_BASE_URL = "https://govt.westlaw.com"
NYCRR_ROOT_URL = f"{NYCRR_BASE_URL}/nycrr/Browse/Index"
NYCRR_SOURCE_FORMAT = "nycrr-westlaw-html"
NYCRR_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_NYCRR_BROWSE_PATH = "/nycrr/Browse/Home/NewYork/UnofficialNewYorkCodesRulesandRegulations"
_BH_PARAMS = {
    "bhcp": "1",
    "bhab": "0",
    "bhav": "-1",
    "bhov": "-3",
    "bhqs": "1",
}


class _Response(Protocol):
    content: bytes
    text: str
    url: str

    def raise_for_status(self) -> None: ...


class _Session(Protocol):
    def get(self, url: str, *, timeout: int = 30) -> _Response: ...


@dataclass(frozen=True)
class NycrrExtractReport:
    """Result from a NYCRR extraction run."""

    jurisdiction: str
    document_class: str
    page_count: int
    browse_page_count: int
    document_page_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]


@dataclass(frozen=True)
class _QueuedPage:
    url: str
    parent_citation_path: str | None
    link_text: str | None
    ordinal: int | None


@dataclass(frozen=True)
class _FetchedPage:
    url: str
    html: str
    sha256: str
    source_path: Path
    source_key: str


def nycrr_run_id(
    version: str,
    *,
    only_title: int | None = None,
    limit: int | None = None,
) -> str:
    """Return a scoped NYCRR run id."""
    parts = [version]
    if only_title is not None:
        parts.append(f"title-{only_title}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def extract_nycrr(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: int | None = None,
    limit: int | None = None,
    delay_seconds: float = 0.25,
    refresh: bool = False,
    session: _Session | None = None,
    progress_stream: TextIO | None = None,
) -> NycrrExtractReport:
    """Snapshot the public NYCRR browse tree and extract normalized provisions."""
    run_id = nycrr_run_id(version, only_title=only_title, limit=limit)
    client = _nycrr_session(session)
    expression_date_text = _date_text(expression_date, version)
    source_as_of_text = source_as_of or version

    root_url = _normalize_url(
        NYCRR_ROOT_URL,
        include_browserhawk=True,
        extra_query={"transitionType": "Default", "contextData": "(sc.Default)"},
    )
    queue: deque[_QueuedPage] = deque(
        [_QueuedPage(root_url, None, "Unofficial New York Codes, Rules and Regulations", 0)]
    )
    queued: set[str] = {root_url}
    seen: set[str] = set()
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    source_paths: list[Path] = []
    browse_page_count = 0
    document_page_count = 0

    while queue:
        if limit is not None and len(records) >= limit:
            break
        page = queue.popleft()
        if page.url in seen:
            continue
        seen.add(page.url)
        fetched = _read_or_fetch_page(
            store,
            client,
            run_id,
            page.url,
            refresh=refresh,
            delay_seconds=delay_seconds,
        )
        source_paths.append(fetched.source_path)
        soup = BeautifulSoup(fetched.html, "lxml")
        _raise_if_browserhawk_blocked(soup, page.url)
        page_type = "document" if soup.select_one("#co_document") else "browse"
        if page_type == "document":
            document_page_count += 1
        else:
            browse_page_count += 1
        citation_path = _citation_path_for_page(soup, page, page_type)
        if citation_path in {record.citation_path for record in records}:
            citation_path = f"{citation_path}@{_page_guid(page.url) or len(records) + 1}"
        metadata = _page_metadata(soup, page, page_type, fetched.url)
        items.append(
            SourceInventoryItem(
                citation_path=citation_path,
                source_url=_display_url(fetched.url),
                source_path=fetched.source_key,
                source_format=NYCRR_SOURCE_FORMAT,
                sha256=fetched.sha256,
                metadata=metadata,
            )
        )
        records.append(
            _provision_record(
                soup,
                citation_path=citation_path,
                parent_citation_path=page.parent_citation_path,
                version=run_id,
                source_url=_display_url(fetched.url),
                source_path=fetched.source_key,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                page_type=page_type,
                metadata=metadata,
                ordinal=page.ordinal,
            )
        )

        if progress_stream is not None and len(records) % 100 == 0:
            print(
                f"nycrr extracted {len(records)} pages "
                f"({browse_page_count} browse, {document_page_count} documents)",
                file=progress_stream,
                flush=True,
            )

        if limit is not None and len(records) >= limit:
            break
        child_links = _child_links(soup, only_title=only_title if citation_path == _root_path() else None)
        for ordinal, (href, text) in enumerate(child_links, start=1):
            child_url = _normalize_url(href, include_browserhawk=True)
            if child_url is None or child_url in seen or child_url in queued:
                continue
            queued.add(child_url)
            queue.append(_QueuedPage(child_url, citation_path, text, ordinal))

    inventory_path = store.inventory_path("us-ny", DocumentClass.REGULATION, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path("us-ny", DocumentClass.REGULATION, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction="us-ny",
        document_class=DocumentClass.REGULATION.value,
        version=run_id,
    )
    coverage_path = store.coverage_path("us-ny", DocumentClass.REGULATION, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return NycrrExtractReport(
        jurisdiction="us-ny",
        document_class=DocumentClass.REGULATION.value,
        page_count=len(records),
        browse_page_count=browse_page_count,
        document_page_count=document_page_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def _nycrr_session(session: _Session | None = None) -> _Session:
    if session is not None:
        return session
    real_session = requests.Session()
    real_session.headers.update({"User-Agent": NYCRR_USER_AGENT})
    real_session.cookies.set("bhCookieSess", "1", domain=".govt.westlaw.com", path="/")
    real_session.cookies.set("bhCookiePerm", "1", domain=".govt.westlaw.com", path="/")
    return real_session


def _read_or_fetch_page(
    store: CorpusArtifactStore,
    session: _Session,
    run_id: str,
    url: str,
    *,
    refresh: bool,
    delay_seconds: float,
) -> _FetchedPage:
    relative_name = _source_relative_name(url)
    source_path = store.source_path("us-ny", DocumentClass.REGULATION, run_id, relative_name)
    source_key = f"sources/us-ny/{DocumentClass.REGULATION.value}/{run_id}/{relative_name}"
    if source_path.exists() and not refresh:
        html_bytes = source_path.read_bytes()
        return _FetchedPage(url, html_bytes.decode("utf-8", errors="replace"), sha256_bytes(html_bytes), source_path, source_key)
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    response = session.get(url, timeout=30)
    response.raise_for_status()
    html_bytes = response.content
    sha256 = store.write_bytes(source_path, html_bytes)
    return _FetchedPage(response.url or url, response.text, sha256, source_path, source_key)


def _normalize_url(
    href: str,
    *,
    include_browserhawk: bool,
    extra_query: Mapping[str, str] | None = None,
) -> str | None:
    url = urljoin(NYCRR_BASE_URL, href.replace("&amp;", "&"))
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if extra_query:
        query.update(extra_query)
    path = parsed.path
    if path == _NYCRR_BROWSE_PATH and "guid" not in query:
        return None
    if not (
        path == "/nycrr/Browse/Index"
        or path == _NYCRR_BROWSE_PATH
        or path.startswith("/nycrr/Document/")
    ):
        return None
    keep_keys = ["guid", "viewType", "originationContext", "transitionType", "contextData"]
    kept = {key: query[key] for key in keep_keys if key in query}
    if include_browserhawk:
        kept.update(_BH_PARAMS)
    return urlunsplit((parsed.scheme, parsed.netloc, path, urlencode(kept), ""))


def _display_url(url: str) -> str:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key in _BH_PARAMS:
        query.pop(key, None)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ""))


def _source_relative_name(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.path.startswith("/nycrr/Document/"):
        return f"nycrr/document/{parsed.path.rsplit('/', 1)[-1]}.html"
    if parsed.path == "/nycrr/Browse/Index":
        return "nycrr/browse/index.html"
    guid = _page_guid(url)
    if guid:
        return f"nycrr/browse/{guid}.html"
    return "nycrr/browse/unknown.html"


def _page_guid(url: str) -> str | None:
    parsed = urlsplit(url)
    if parsed.path.startswith("/nycrr/Document/"):
        return parsed.path.rsplit("/", 1)[-1]
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    return query.get("guid")


def _child_links(
    soup: BeautifulSoup,
    *,
    only_title: int | None = None,
) -> tuple[tuple[str, str], ...]:
    links: list[tuple[str, str]] = []
    for anchor in soup.select("section.co_innertube a[href]"):
        href = anchor.get("href") or ""
        text = _clean_text(anchor.get_text(" ", strip=True))
        if not text:
            continue
        if not (
            href.startswith("/nycrr/Browse/Home/NewYork/UnofficialNewYorkCodesRulesandRegulations")
            or href.startswith("/nycrr/Document/")
        ):
            continue
        if only_title is not None and not text.startswith(f"Title {only_title} "):
            continue
        links.append((href, text))
    return tuple(links)


def _citation_path_for_page(
    soup: BeautifulSoup,
    page: _QueuedPage,
    page_type: str,
) -> str:
    if page.parent_citation_path is None:
        return _root_path()
    if page_type == "document":
        return _document_citation_path(soup, page.parent_citation_path, page.link_text)
    heading = _page_heading(soup, page.link_text)
    return f"{page.parent_citation_path}/{_heading_token(heading)}"


def _root_path() -> str:
    return "us-ny/regulation"


def _document_citation_path(
    soup: BeautifulSoup,
    parent_citation_path: str,
    link_text: str | None,
) -> str:
    citation = _citation_label(soup)
    token = None
    if citation:
        match = re.match(r"^\s*(?P<title>\d+)\s+CRR-NY\s+(?P<section>.+?)\s*$", citation)
        if match:
            token = _slug(match.group("section").split(",", 1)[0])
    if not token and link_text:
        section_match = re.match(r"^s\s+([A-Za-z0-9.:-]+)", link_text.strip())
        token = _slug(section_match.group(1) if section_match else link_text)
    return f"{parent_citation_path}/{token or 'document'}"


def _heading_token(heading: str | None) -> str:
    text = heading or "node"
    match = re.match(
        r"^(Title|Chapter|Subchapter|Article|Part)\s+([A-Za-z0-9.IVXLCivxlc-]+)",
        text.strip(),
    )
    if match:
        return f"{match.group(1).lower()}-{_slug(match.group(2).rstrip('.'))}"
    return _slug(text)


def _page_heading(soup: BeautifulSoup, fallback: str | None = None) -> str | None:
    if soup.select_one("#co_document"):
        title = soup.select_one("#co_document .co_title .co_headtext")
        if title:
            return _clean_text(title.get_text(" ", strip=True))
    h1 = soup.find("h1")
    if h1:
        return _clean_text(h1.get_text(" ", strip=True))
    return fallback


def _citation_label(soup: BeautifulSoup) -> str | None:
    citation = soup.select_one("#citation")
    if citation:
        return _clean_text(citation.get_text(" ", strip=True))
    return None


def _provision_record(
    soup: BeautifulSoup,
    *,
    citation_path: str,
    parent_citation_path: str | None,
    version: str,
    source_url: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
    page_type: str,
    metadata: dict[str, Any],
    ordinal: int | None,
) -> ProvisionRecord:
    current_through = metadata.get("current_through")
    heading = _page_heading(soup, metadata.get("link_text"))
    citation_label = _citation_label(soup)
    return ProvisionRecord(
        jurisdiction="us-ny",
        document_class=DocumentClass.REGULATION.value,
        citation_path=citation_path,
        id=deterministic_provision_id(citation_path),
        body=_document_body(soup) if page_type == "document" else None,
        heading=heading,
        citation_label=citation_label,
        version=version,
        source_url=source_url,
        source_path=source_path,
        source_id="nycrr-westlaw",
        source_format=NYCRR_SOURCE_FORMAT,
        source_document_id=metadata.get("guid"),
        source_as_of=str(current_through or source_as_of),
        expression_date=expression_date,
        parent_citation_path=parent_citation_path,
        parent_id=deterministic_provision_id(parent_citation_path) if parent_citation_path else None,
        level=_level(citation_path),
        ordinal=ordinal,
        kind=_record_kind(page_type, heading, citation_label, citation_path),
        legal_identifier=citation_label or heading,
        identifiers=_identifiers(metadata, citation_label, heading),
        metadata=metadata,
    )


def _page_metadata(
    soup: BeautifulSoup,
    page: _QueuedPage,
    page_type: str,
    fetched_url: str,
) -> dict[str, Any]:
    heading = _page_heading(soup, page.link_text)
    metadata: dict[str, Any] = {
        "source": "New York Department of State NYCRR, published via Thomson Reuters Westlaw",
        "source_caveat": "Online NYCRR is unofficial and not for evidentiary use.",
        "page_type": page_type,
        "guid": _page_guid(fetched_url),
        "link_text": page.link_text,
        "heading": heading,
        "fetched_url": fetched_url,
    }
    current_through = _current_through(soup)
    if current_through:
        metadata["current_through"] = current_through
    citation = _citation_label(soup)
    if citation:
        metadata["citation"] = citation
    return {key: value for key, value in metadata.items() if value is not None}


def _document_body(soup: BeautifulSoup) -> str | None:
    body = soup.select_one("#co_document .co_contentBlock.co_body")
    if body is None:
        return None
    text = _clean_multiline_text(body.get_text("\n", strip=True))
    return text or None


def _current_through(soup: BeautifulSoup) -> str | None:
    text = _clean_text(soup.get_text(" ", strip=True))
    match = re.search(r"Current through\s+(.+?)(?:\s+End of Document|\s+IMPORTANT NOTE|$)", text)
    if not match:
        return None
    return match.group(1).strip().rstrip(".")


def _record_kind(
    page_type: str,
    heading: str | None,
    citation_label: str | None,
    citation_path: str,
) -> str:
    if citation_path == _root_path():
        return "collection"
    if page_type == "document":
        if citation_label and re.search(r"\d+\s+CRR-NY\s+\d", citation_label):
            return "section"
        return "document"
    token = _heading_token(heading)
    return token.split("-", 1)[0] if "-" in token else "collection"


def _identifiers(
    metadata: Mapping[str, Any],
    citation_label: str | None,
    heading: str | None,
) -> dict[str, str]:
    identifiers: dict[str, str] = {}
    guid = metadata.get("guid")
    if guid:
        identifiers["nycrr:guid"] = str(guid)
    if citation_label:
        identifiers["nycrr:citation"] = citation_label
        match = re.match(r"^\s*(?P<title>\d+)\s+CRR-NY\s+(?P<section>.+?)\s*$", citation_label)
        if match:
            identifiers["nycrr:title"] = match.group("title")
            identifiers["nycrr:section"] = match.group("section")
    if heading:
        heading_match = re.match(r"^(Title|Chapter|Subchapter|Article|Part)\s+(.+)$", heading)
        if heading_match:
            identifiers[f"nycrr:{heading_match.group(1).lower()}"] = heading_match.group(2)
    return identifiers


def _level(citation_path: str) -> int:
    return max(0, len(citation_path.split("/")) - 2)


def _raise_if_browserhawk_blocked(soup: BeautifulSoup, url: str) -> None:
    h1 = soup.find("h1")
    heading = h1.get_text(" ", strip=True) if h1 else ""
    if "not optimized for Weblinks" in heading:
        raise RuntimeError(f"NYCRR BrowserHawk validation did not complete for {url}")


def _date_text(value: date | str | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, date):
        return value.isoformat()
    return value


def _slug(value: str) -> str:
    lowered = value.strip().lower().replace("§", "s")
    lowered = lowered.replace("—", "-").replace("–", "-")
    slug = re.sub(r"[^a-z0-9.]+", "-", lowered).strip("-")
    return slug or "node"


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _clean_multiline_text(value: str) -> str:
    lines = [_clean_text(line) for line in value.splitlines()]
    return "\n".join(line for line in lines if line)


if __name__ == "__main__":  # pragma: no cover
    report = extract_nycrr(
        CorpusArtifactStore(Path("data/corpus")),
        version=date.today().isoformat(),
        progress_stream=sys.stderr,
    )
    print(report)
