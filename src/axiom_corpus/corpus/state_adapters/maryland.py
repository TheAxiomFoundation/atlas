"""Maryland Code source-first corpus adapter."""

from __future__ import annotations

import html
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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

MARYLAND_CODE_BASE_URL = "https://mgaleg.maryland.gov/mgawebsite"
MARYLAND_CODE_SOURCE_FORMAT = "maryland-code-html-json"
MARYLAND_USER_AGENT = "axiom-corpus/0.1 (contact@axiom-foundation.org)"

_ARTICLES_RELATIVE_PATH = "maryland-code-json/articles.json"
_SECTIONS_RELATIVE_DIR = "maryland-code-json/sections"
_SECTION_HTML_RELATIVE_DIR = "maryland-code-html"
_ARTICLE_TEXT_RE = re.compile(r"^(?P<heading>.+?)\s*-\s*\((?P<code>[^)]+)\)\s*$")
_SECTION_REF_RE = re.compile(
    r"\u00a7+\s*(?P<section>[0-9A-Z]+(?:[-\u2013][0-9A-Z.]+)+)",
    re.I,
)


@dataclass(frozen=True)
class MarylandArticle:
    """One Maryland Code article from the official article API."""

    code: str
    heading: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return self.code

    @property
    def citation_path(self) -> str:
        return f"us-md/statute/{self.code}"

    @property
    def legal_identifier(self) -> str:
        return f"Md. Code, {self.heading}"


@dataclass(frozen=True)
class MarylandTitle:
    """A title container inferred from Maryland section numbering."""

    article_code: str
    article_heading: str
    title: str
    parent_citation_path: str
    ordinal: int

    @property
    def source_id(self) -> str:
        return f"{self.article_code}-title-{_slug(self.title)}"

    @property
    def citation_path(self) -> str:
        return f"us-md/statute/{self.article_code}/title-{_slug(self.title)}"

    @property
    def legal_identifier(self) -> str:
        return f"Md. Code, {self.article_heading} Title {self.title}"


@dataclass(frozen=True)
class MarylandSectionTarget:
    """One Maryland section target from the official sections API."""

    article_code: str
    article_heading: str
    section: str
    ordinal: int
    api_value: str | None = None

    @property
    def source_id(self) -> str:
        return f"{self.article_code}-{self.section}"

    @property
    def title(self) -> str:
        return _title_from_section(self.section)

    @property
    def parent_citation_path(self) -> str:
        return f"us-md/statute/{self.article_code}/title-{_slug(self.title)}"

    @property
    def citation_path(self) -> str:
        return f"us-md/statute/{self.article_code}/{self.section}"

    @property
    def legal_identifier(self) -> str:
        return f"Md. Code, {self.article_heading} \u00a7 {self.section}"

    @property
    def relative_path(self) -> str:
        return f"{_SECTION_HTML_RELATIVE_DIR}/{self.article_code}/{_safe_file_stem(self.section)}.html"


@dataclass(frozen=True)
class MarylandParsedSection:
    """Parsed Maryland section body."""

    body: str | None
    references_to: tuple[str, ...]
    status: str | None = None


@dataclass(frozen=True)
class MarylandProvision:
    """Normalized Maryland article, title, or section node."""

    kind: str
    article_code: str
    article_heading: str
    source_id: str
    display_number: str
    citation_path: str
    legal_identifier: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int | None
    title: str | None = None
    section: str | None = None
    api_value: str | None = None
    references_to: tuple[str, ...] = ()
    status: str | None = None


@dataclass(frozen=True)
class _MarylandSource:
    relative_path: str
    source_url: str
    data: bytes


@dataclass(frozen=True)
class _RecordedSource:
    source_url: str
    source_path: str
    sha256: str


class _MarylandFetcher:
    def __init__(
        self,
        *,
        base_url: str,
        source_dir: Path | None,
        download_dir: Path | None,
        enactments: bool,
        request_delay_seconds: float,
        timeout_seconds: float,
        request_attempts: int,
    ) -> None:
        self.base_url = _base_url(base_url)
        self.source_dir = source_dir
        self.download_dir = download_dir
        self.enactments = enactments
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.timeout_seconds = timeout_seconds
        self.request_attempts = max(1, request_attempts)
        self._request_lock = Lock()
        self._last_request_at = 0.0

    def fetch_articles(self) -> _MarylandSource:
        return self._fetch(
            relative_path=_ARTICLES_RELATIVE_PATH,
            path="api/Laws/GetArticles",
            params={"enactments": _bool_param(self.enactments)},
        )

    def fetch_sections(self, article: MarylandArticle) -> _MarylandSource:
        return self._fetch(
            relative_path=f"{_SECTIONS_RELATIVE_DIR}/{article.code}.json",
            path="api/Laws/GetSections",
            params={
                "articleCode": article.code,
                "enactments": _bool_param(self.enactments),
            },
        )

    def fetch_section(self, target: MarylandSectionTarget) -> _MarylandSource:
        return self._fetch(
            relative_path=target.relative_path,
            path="Laws/StatuteText",
            params={
                "article": target.article_code,
                "section": target.section,
                "enactments": _bool_param(self.enactments),
            },
        )

    def _fetch(
        self,
        *,
        relative_path: str,
        path: str,
        params: dict[str, str],
    ) -> _MarylandSource:
        source_url = _build_url(self.base_url, path, params)
        if self.source_dir is not None:
            return _MarylandSource(
                relative_path=relative_path,
                source_url=source_url,
                data=_read_source_file(self.source_dir, relative_path),
            )
        if self.download_dir is not None:
            cached_path = self.download_dir / relative_path
            if cached_path.exists():
                return _MarylandSource(
                    relative_path=relative_path,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )

        data = self._download(source_url)
        if self.download_dir is not None:
            _write_cache_bytes(self.download_dir / relative_path, data)
        return _MarylandSource(relative_path=relative_path, source_url=source_url, data=data)

    def _download(self, source_url: str) -> bytes:
        last_error: BaseException | None = None
        for attempt in range(self.request_attempts):
            with self._request_lock:
                elapsed = time.monotonic() - self._last_request_at
                if elapsed < self.request_delay_seconds:
                    time.sleep(self.request_delay_seconds - elapsed)
                self._last_request_at = time.monotonic()
            try:
                response = requests.get(
                    source_url,
                    headers={"User-Agent": MARYLAND_USER_AGENT},
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.content
            except requests.RequestException as exc:
                last_error = exc
                if attempt + 1 < self.request_attempts:
                    time.sleep(min(2.0 * (attempt + 1), 10.0))
        raise RuntimeError(f"failed to fetch Maryland source {source_url}: {last_error}") from last_error


def extract_maryland_code(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_article: str | None = None,
    limit: int | None = None,
    workers: int = 8,
    download_dir: str | Path | None = None,
    base_url: str = MARYLAND_CODE_BASE_URL,
    include_constitution: bool = False,
    enactments: bool = False,
    request_delay_seconds: float = 0.02,
    timeout_seconds: float = 60.0,
    request_attempts: int = 3,
) -> StateStatuteExtractReport:
    """Snapshot official Maryland Code sources and extract provisions."""
    jurisdiction = "us-md"
    article_filter = _optional_filter(only_article)
    run_id = _maryland_run_id(
        version,
        only_article=article_filter,
        limit=limit,
        include_constitution=include_constitution,
        enactments=enactments,
    )
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    fetcher = _MarylandFetcher(
        base_url=base_url,
        source_dir=Path(source_dir) if source_dir is not None else None,
        download_dir=Path(download_dir) if download_dir is not None else None,
        enactments=enactments,
        request_delay_seconds=request_delay_seconds,
        timeout_seconds=timeout_seconds,
        request_attempts=request_attempts,
    )

    source_paths: list[Path] = []
    source_by_relative: dict[str, _RecordedSource] = {}
    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    errors: list[str] = []

    articles_source = fetcher.fetch_articles()
    articles_recorded = _record_source(
        store,
        jurisdiction=jurisdiction,
        run_id=run_id,
        source=articles_source,
        source_by_relative=source_by_relative,
        source_paths=source_paths,
    )
    articles = tuple(
        article
        for article in parse_maryland_articles(articles_source.data)
        if (include_constitution or article.code.startswith("g"))
        and (
            article_filter is None
            or _same_filter(article.code, article_filter)
            or _same_filter(article.heading, article_filter)
        )
    )
    if not articles:
        raise ValueError(f"no Maryland article sources selected for filter: {only_article!r}")

    section_targets: list[MarylandSectionTarget] = []
    for article in articles:
        if limit is not None and len(section_targets) >= limit:
            break
        sections_source = fetcher.fetch_sections(article)
        sections_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=sections_source,
            source_by_relative=source_by_relative,
            source_paths=source_paths,
        )
        targets = list(parse_maryland_sections(sections_source.data, article=article))
        if limit is not None:
            targets = targets[: max(0, limit - len(section_targets))]

        added = _append_provision(
            _article_provision(article),
            articles_recorded,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            records=records,
            items=items,
            seen=seen,
        )
        if added:
            title_count += 1

        for title in _titles_from_targets(article, targets):
            added = _append_provision(
                _title_provision(title),
                sections_recorded,
                version=run_id,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                records=records,
                items=items,
                seen=seen,
            )
            if added:
                container_count += 1

        section_targets.extend(targets)

    fetched_sections = _fetch_section_pages(
        fetcher,
        section_targets,
        workers=max(1, workers),
    )
    for target, source, parsed, error in fetched_sections:
        if error is not None:
            errors.append(f"{target.article_code} {target.section}: {error}")
            continue
        if source is None or parsed is None:
            continue
        section_recorded = _record_source(
            store,
            jurisdiction=jurisdiction,
            run_id=run_id,
            source=source,
            source_by_relative=source_by_relative,
            source_paths=source_paths,
        )
        added = _append_provision(
            _section_provision(target, parsed),
            section_recorded,
            version=run_id,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            records=records,
            items=items,
            seen=seen,
        )
        if added:
            section_count += 1

    if not records:
        raise ValueError("no Maryland provisions extracted")
    if errors and section_count == 0:
        raise ValueError(f"no Maryland sections extracted: {errors[:5]}")

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


def parse_maryland_articles(data: str | bytes | list[dict[str, Any]]) -> tuple[MarylandArticle, ...]:
    """Parse the official Maryland article API response."""
    rows = _load_json_array(data)
    articles: list[MarylandArticle] = []
    for ordinal, row in enumerate(rows, start=1):
        raw_code = str(row.get("Value") or "").strip().lower()
        text = str(row.get("DisplayText") or "").strip()
        match = _ARTICLE_TEXT_RE.match(text)
        code = _clean_code(raw_code or (match.group("code") if match else ""))
        heading = _clean_heading(match.group("heading") if match else text)
        if not code or not heading:
            continue
        articles.append(MarylandArticle(code=code, heading=heading, ordinal=ordinal))
    return tuple(articles)


def parse_maryland_sections(
    data: str | bytes | list[dict[str, Any]],
    *,
    article: MarylandArticle,
) -> tuple[MarylandSectionTarget, ...]:
    """Parse the official Maryland sections API response for one article."""
    rows = _load_json_array(data)
    targets: list[MarylandSectionTarget] = []
    for ordinal, row in enumerate(rows, start=1):
        section = _clean_section(str(row.get("DisplayText") or ""))
        if not section:
            continue
        api_value = row.get("Value")
        targets.append(
            MarylandSectionTarget(
                article_code=article.code,
                article_heading=article.heading,
                section=section,
                ordinal=ordinal,
                api_value=str(api_value) if api_value is not None else None,
            )
        )
    return tuple(targets)


def parse_maryland_section(
    data: str | bytes,
    *,
    target: MarylandSectionTarget,
) -> MarylandParsedSection:
    """Parse one official Maryland StatuteText HTML page."""
    soup = BeautifulSoup(data, "html.parser")
    statute_node = soup.find(id="StatuteText")
    if not isinstance(statute_node, Tag):
        raise ValueError(f"missing StatuteText node for {target.article_code} {target.section}")

    for node in statute_node.select("button, .row"):
        node.decompose()
    for br in statute_node.find_all("br"):
        br.replace_with("\n")

    text = html.unescape(statute_node.get_text("\n", strip=False))
    lines = [_clean_body_line(line) for line in text.splitlines()]
    lines = [line for line in lines if line]
    if lines and lines[0].lower().startswith("article - "):
        lines = lines[1:]
    if lines and _is_section_marker(lines[0], target.section):
        lines = lines[1:]
    body = "\n".join(lines).strip() or None
    return MarylandParsedSection(
        body=body,
        references_to=_references_from_body(body, target=target),
        status=_status_from_body(body) or ("source-empty" if body is None else None),
    )


def _fetch_section_pages(
    fetcher: _MarylandFetcher,
    targets: list[MarylandSectionTarget],
    *,
    workers: int,
) -> list[tuple[MarylandSectionTarget, _MarylandSource | None, MarylandParsedSection | None, Exception | None]]:
    def fetch_one(target: MarylandSectionTarget) -> tuple[
        MarylandSectionTarget,
        _MarylandSource | None,
        MarylandParsedSection | None,
        Exception | None,
    ]:
        try:
            source = fetcher.fetch_section(target)
            return target, source, parse_maryland_section(source.data, target=target), None
        except Exception as exc:  # noqa: BLE001
            return target, None, None, exc

    if workers <= 1:
        return [fetch_one(target) for target in targets]
    results: list[
        tuple[MarylandSectionTarget, _MarylandSource | None, MarylandParsedSection | None, Exception | None]
    ] = []
    ordered: dict[
        int,
        tuple[MarylandSectionTarget, _MarylandSource | None, MarylandParsedSection | None, Exception | None],
    ] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, target): index for index, target in enumerate(targets)}
        for future in as_completed(futures):
            ordered[futures[future]] = future.result()
    for index in range(len(targets)):
        results.append(ordered[index])
    return results


def _record_source(
    store: CorpusArtifactStore,
    *,
    jurisdiction: str,
    run_id: str,
    source: _MarylandSource,
    source_by_relative: dict[str, _RecordedSource],
    source_paths: list[Path],
) -> _RecordedSource:
    existing = source_by_relative.get(source.relative_path)
    if existing is not None:
        return existing
    relative_name = f"{MARYLAND_CODE_SOURCE_FORMAT}/{source.relative_path}"
    artifact_path = store.source_path(
        jurisdiction,
        DocumentClass.STATUTE,
        run_id,
        relative_name,
    )
    sha256 = store.write_bytes(artifact_path, source.data)
    source_paths.append(artifact_path)
    recorded = _RecordedSource(
        source_url=source.source_url,
        source_path=_state_source_key(jurisdiction, run_id, relative_name),
        sha256=sha256,
    )
    source_by_relative[source.relative_path] = recorded
    return recorded


def _append_provision(
    provision: MarylandProvision,
    source: _RecordedSource,
    *,
    version: str,
    source_as_of: str,
    expression_date: str,
    records: list[ProvisionRecord],
    items: list[SourceInventoryItem],
    seen: set[str],
) -> bool:
    if provision.citation_path in seen:
        return False
    seen.add(provision.citation_path)
    metadata = _metadata(provision)
    items.append(
        SourceInventoryItem(
            citation_path=provision.citation_path,
            source_url=source.source_url,
            source_path=source.source_path,
            source_format=MARYLAND_CODE_SOURCE_FORMAT,
            sha256=source.sha256,
            metadata=metadata,
        )
    )
    records.append(
        ProvisionRecord(
            id=deterministic_provision_id(provision.citation_path),
            jurisdiction="us-md",
            document_class=DocumentClass.STATUTE.value,
            citation_path=provision.citation_path,
            body=provision.body,
            heading=provision.heading,
            citation_label=provision.legal_identifier,
            version=version,
            source_url=source.source_url,
            source_path=source.source_path,
            source_id=provision.source_id,
            source_format=MARYLAND_CODE_SOURCE_FORMAT,
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
            metadata=metadata,
        )
    )
    return True


def _article_provision(article: MarylandArticle) -> MarylandProvision:
    return MarylandProvision(
        kind="article",
        article_code=article.code,
        article_heading=article.heading,
        source_id=article.source_id,
        display_number=article.code,
        citation_path=article.citation_path,
        legal_identifier=article.legal_identifier,
        heading=article.heading,
        body=None,
        parent_citation_path=None,
        level=0,
        ordinal=article.ordinal,
    )


def _title_provision(title: MarylandTitle) -> MarylandProvision:
    return MarylandProvision(
        kind="title",
        article_code=title.article_code,
        article_heading=title.article_heading,
        source_id=title.source_id,
        display_number=title.title,
        citation_path=title.citation_path,
        legal_identifier=title.legal_identifier,
        heading=f"Title {title.title}",
        body=None,
        parent_citation_path=title.parent_citation_path,
        level=1,
        ordinal=title.ordinal,
        title=title.title,
    )


def _section_provision(
    target: MarylandSectionTarget,
    parsed: MarylandParsedSection,
) -> MarylandProvision:
    return MarylandProvision(
        kind="section",
        article_code=target.article_code,
        article_heading=target.article_heading,
        source_id=target.source_id,
        display_number=target.section,
        citation_path=target.citation_path,
        legal_identifier=target.legal_identifier,
        heading=f"Section {target.section}" if parsed.body is None else None,
        body=parsed.body,
        parent_citation_path=target.parent_citation_path,
        level=2,
        ordinal=target.ordinal,
        title=target.title,
        section=target.section,
        api_value=target.api_value,
        references_to=parsed.references_to,
        status=parsed.status,
    )


def _titles_from_targets(
    article: MarylandArticle,
    targets: list[MarylandSectionTarget],
) -> tuple[MarylandTitle, ...]:
    seen: dict[str, int] = {}
    for target in targets:
        seen.setdefault(target.title, len(seen) + 1)
    return tuple(
        MarylandTitle(
            article_code=article.code,
            article_heading=article.heading,
            title=title,
            parent_citation_path=article.citation_path,
            ordinal=ordinal,
        )
        for title, ordinal in seen.items()
    )


def _metadata(provision: MarylandProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "article_code": provision.article_code,
        "article_heading": provision.article_heading,
        "display_number": provision.display_number,
    }
    if provision.title:
        metadata["title"] = provision.title
    if provision.section:
        metadata["section"] = provision.section
    if provision.api_value:
        metadata["maryland_api_value"] = provision.api_value
    if provision.parent_citation_path:
        metadata["parent_citation_path"] = provision.parent_citation_path
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.status:
        metadata["status"] = provision.status
    return metadata


def _identifiers(provision: MarylandProvision) -> dict[str, str]:
    identifiers = {
        "maryland:article": provision.article_code,
        "maryland:source_id": provision.source_id,
    }
    if provision.title:
        identifiers["maryland:title"] = provision.title
    if provision.section:
        identifiers["maryland:section"] = provision.section
    return identifiers


def _load_json_array(data: str | bytes | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    parsed = json.loads(data)
    if not isinstance(parsed, list):
        raise ValueError("Maryland API response must be a JSON array")
    return [row for row in parsed if isinstance(row, dict)]


def _references_from_body(
    body: str | None,
    *,
    target: MarylandSectionTarget,
) -> tuple[str, ...]:
    if not body:
        return ()
    refs: list[str] = []
    for match in _SECTION_REF_RE.finditer(body):
        section = _clean_section(match.group("section").rstrip("."))
        if section and section != target.section:
            refs.append(f"us-md/statute/{target.article_code}/{section}")
    return tuple(dict.fromkeys(refs))


def _status_from_body(body: str | None) -> str | None:
    if not body:
        return None
    first_line = body.splitlines()[0].strip().lower()
    if first_line.startswith("repealed") or first_line.startswith("[repealed"):
        return "repealed"
    return None


def _is_section_marker(value: str, section: str) -> bool:
    normalized = _clean_body_line(value).replace(" ", "").rstrip(".")
    expected = f"\u00a7{section}".replace(" ", "").rstrip(".")
    return normalized == expected


def _title_from_section(section: str) -> str:
    return section.split("-", 1)[0].strip().upper() or section


def _clean_code(value: str) -> str:
    return re.sub(r"[^0-9a-z]+", "", value.strip().lower())


def _clean_section(value: str) -> str:
    return _clean_body_line(value).strip(".")


def _clean_heading(value: str) -> str:
    return _clean_body_line(value).strip(" :.")


def _clean_body_line(value: str) -> str:
    text = html.unescape(value)
    text = text.replace("\xa0", " ").replace("\u2011", "-")
    text = text.replace("\u2013", "-").replace("\u2014", "--")
    return re.sub(r"[ \t\r\f\v]+", " ", text).strip()


def _safe_file_stem(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z.-]+", "-", value.strip()).strip("-") or "section"


def _slug(value: str) -> str:
    return re.sub(r"[^0-9a-z]+", "-", value.lower()).strip("-")


def _optional_filter(value: str | None) -> str | None:
    return _clean_heading(value) if value is not None and str(value).strip() else None


def _same_filter(value: str, expected: str) -> bool:
    return _slug(value) == _slug(expected)


def _bool_param(value: bool) -> str:
    return "true" if value else "false"


def _build_url(base_url: str, path: str, params: dict[str, str]) -> str:
    request = requests.Request("GET", urljoin(_base_url(base_url), path), params=params)
    prepared = request.prepare()
    if prepared.url is None:
        raise ValueError(f"failed to build Maryland source URL for {path}")
    return prepared.url


def _read_source_file(source_dir: Path, relative_path: str) -> bytes:
    candidates = [
        source_dir / relative_path,
        source_dir / MARYLAND_CODE_SOURCE_FORMAT / relative_path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.read_bytes()
    raise FileNotFoundError(f"missing Maryland source file: {relative_path}")


def _write_cache_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _base_url(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _maryland_run_id(
    version: str,
    *,
    only_article: str | None,
    limit: int | None,
    include_constitution: bool,
    enactments: bool,
) -> str:
    if (
        only_article is None
        and limit is None
        and not include_constitution
        and not enactments
    ):
        return version
    parts = [version, "us-md"]
    if only_article is not None:
        parts.append(f"article-{_slug(only_article)}")
    if include_constitution:
        parts.append("with-constitution")
    if enactments:
        parts.append("enactments")
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
