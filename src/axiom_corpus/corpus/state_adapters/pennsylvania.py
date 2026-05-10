"""Pennsylvania Consolidated Statutes source-first corpus adapter."""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.states import StateStatuteExtractReport
from axiom_corpus.corpus.supabase import deterministic_provision_id

PENNSYLVANIA_BASE_URL = "https://www.palegis.us/statutes/consolidated/"
PENNSYLVANIA_SOURCE_FORMAT = "pennsylvania-consolidated-statutes-html"
PENNSYLVANIA_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 Chrome/120 Safari/537.36"
)
PENNSYLVANIA_REQUEST_DELAY_SECONDS = 0.2
PENNSYLVANIA_REQUEST_ATTEMPTS = 3
PENNSYLVANIA_TIMEOUT_SECONDS = 120.0

PENNSYLVANIA_TITLES: dict[int, str] = {
    1: "General Provisions",
    2: "Administrative Law and Procedure",
    3: "Agriculture",
    4: "Amusements",
    5: "Athletics and Sports",
    7: "Banks and Banking",
    8: "Boroughs and Incorporated Towns",
    9: "Burial Grounds",
    10: "Charitable Organizations",
    11: "Cities",
    12: "Commerce and Trade",
    13: "Commercial Code",
    15: "Corporations and Unincorporated Associations",
    16: "Counties",
    17: "Credit Unions",
    18: "Crimes and Offenses",
    19: "Decedents, Estates and Fiduciaries",
    20: "Decedents, Estates and Fiduciaries",
    22: "Detectives and Private Police",
    23: "Domestic Relations",
    24: "Education",
    25: "Elections",
    27: "Environmental Resources",
    30: "Fish",
    32: "Forests, Waters and State Parks",
    34: "Game",
    35: "Health and Safety",
    37: "Historical and Museums",
    38: "Holidays and Observances",
    40: "Insurance",
    42: "Judiciary and Judicial Procedure",
    44: "Law and Justice",
    45: "Legal Notices",
    46: "Legislature",
    48: "Lodges",
    51: "Military Affairs",
    53: "Municipalities Generally",
    54: "Names",
    58: "Oil and Gas",
    61: "Prisons and Parole",
    62: "Procurement",
    63: "Professions and Occupations (State Licensed)",
    64: "Public Authorities and Quasi-Public Corporations",
    65: "Public Officers",
    66: "Public Utilities",
    67: "Public Welfare",
    68: "Real and Personal Property",
    69: "Savings Associations",
    71: "State Government",
    72: "Taxation and Fiscal Affairs",
    73: "Trade and Commerce",
    74: "Transportation",
    75: "Vehicles",
    76: "Veterans and War Veterans' Organizations",
    77: "Workmen's Compensation",
    79: "Zoning and Planning",
}

_TITLE_MARKER_RE = re.compile(r"^TITLE\s+(?P<title>\d+)$", re.I)
_CHAPTER_RE = re.compile(
    r"^(?:CHAPTER|Chapter)\s+(?P<chapter>\d+[A-Z]?)"
    r"(?:\.\s*|\s+)?(?P<heading>.+)?$",
    re.I,
)
_SECTION_HEADING_RE = re.compile(
    r"^\u00a7\s*(?P<section>[0-9A-Z.]+)\.?\s*(?P<heading>.*)$",
    re.I,
)
_PA_TEXT_REFERENCE_RE = re.compile(
    r"\b(?:(?P<title>\d+)\s+Pa\.?\s*C\.?S\.?\s*\u00a7+\s*)?"
    r"(?:section\s+|\u00a7+\s*)"
    r"(?P<section>\d+[A-Z]?(?:\.\d+)?)\b",
    re.I,
)


@dataclass(frozen=True)
class PennsylvaniaProvision:
    """Parsed Pennsylvania title, chapter, or section node."""

    kind: str
    title: int
    source_id: str
    display_number: str
    heading: str | None
    body: str | None
    parent_citation_path: str | None
    level: int
    ordinal: int | None
    references_to: tuple[str, ...] = ()
    source_history: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    status: str | None = None

    @property
    def citation_path(self) -> str:
        if self.kind == "title":
            return f"us-pa/statute/title-{self.title}"
        if self.kind == "chapter":
            return f"us-pa/statute/title-{self.title}/chapter-{self.display_number}"
        return f"us-pa/statute/{self.title}/{self.display_number}"

    @property
    def legal_identifier(self) -> str:
        if self.kind == "title":
            return f"{self.title} Pa.C.S."
        if self.kind == "chapter":
            return f"{self.title} Pa.C.S. Chapter {self.display_number}"
        return f"{self.title} Pa.C.S. \u00a7 {self.display_number}"


@dataclass(frozen=True)
class _PennsylvaniaTitleSource:
    title: int
    relative_path: str
    source_url: str
    data: bytes


def extract_pennsylvania_statutes(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_dir: str | Path | None = None,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    only_title: str | int | None = None,
    limit: int | None = None,
    download_dir: str | Path | None = None,
    base_url: str = PENNSYLVANIA_BASE_URL,
    request_delay_seconds: float = PENNSYLVANIA_REQUEST_DELAY_SECONDS,
    request_attempts: int = PENNSYLVANIA_REQUEST_ATTEMPTS,
    timeout_seconds: float = PENNSYLVANIA_TIMEOUT_SECONDS,
) -> StateStatuteExtractReport:
    """Snapshot official Pennsylvania title HTML and extract normalized provisions."""
    jurisdiction = "us-pa"
    title_filter = _pennsylvania_title_filter(only_title)
    run_id = _pennsylvania_run_id(version, only_title=title_filter, limit=limit)
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, source_as_of_text)
    source_paths: list[Path] = []

    title_sources = tuple(
        _iter_pennsylvania_title_sources(
            source_dir=Path(source_dir) if source_dir is not None else None,
            download_dir=Path(download_dir) if download_dir is not None else None,
            base_url=base_url,
            only_title=title_filter,
            request_delay_seconds=request_delay_seconds,
            request_attempts=request_attempts,
            timeout_seconds=timeout_seconds,
        )
    )
    if not title_sources:
        raise ValueError(f"no Pennsylvania title sources selected for filter: {only_title!r}")

    items: list[SourceInventoryItem] = []
    records: list[ProvisionRecord] = []
    seen: set[str] = set()
    title_count = 0
    container_count = 0
    section_count = 0
    errors: list[str] = []
    remaining = limit

    for source in title_sources:
        if remaining is not None and remaining <= 0:
            break
        artifact_path = store.source_path(
            jurisdiction,
            DocumentClass.STATUTE,
            run_id,
            source.relative_path,
        )
        sha256 = store.write_bytes(artifact_path, source.data)
        source_paths.append(artifact_path)
        source_key = _state_source_key(jurisdiction, run_id, source.relative_path)
        try:
            provisions = parse_pennsylvania_title_html(source.data, title=source.title)
        except ValueError as exc:
            errors.append(f"title {source.title}: {exc}")
            continue
        if not provisions:
            errors.append(f"title {source.title}: no provisions parsed")
            continue

        for provision in provisions:
            if remaining is not None and remaining <= 0:
                break
            if provision.citation_path in seen:
                continue
            seen.add(provision.citation_path)
            items.append(
                _inventory_item(
                    provision,
                    source_url=source.source_url,
                    source_path=source_key,
                    sha256=sha256,
                )
            )
            records.append(
                _provision_record(
                    provision,
                    version=run_id,
                    source_url=source.source_url,
                    source_path=source_key,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                )
            )
            if provision.kind == "title":
                title_count += 1
            elif provision.kind == "section":
                section_count += 1
            else:
                container_count += 1
            if remaining is not None:
                remaining -= 1

    if not items:
        raise ValueError("no Pennsylvania provisions extracted")

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


def parse_pennsylvania_title_html(
    html: str | bytes,
    *,
    title: int | str | None = None,
) -> tuple[PennsylvaniaProvision, ...]:
    """Parse one official Pennsylvania Consolidated Statutes title page."""
    soup = BeautifulSoup(html, "lxml")
    body = soup.select_one(".BodyContainer") or soup.body or soup
    text = _clean_text(body.get_text(" ", strip=True))
    if "403 - Forbidden" in text or "Request blocked" in text:
        raise ValueError("source page is blocked")
    parsed_title = _title_filter(title) if title is not None else _infer_title(body)
    if parsed_title is None:
        raise ValueError("could not infer Pennsylvania title number")

    heading = _title_heading(body, parsed_title)
    provisions: list[PennsylvaniaProvision] = [
        PennsylvaniaProvision(
            kind="title",
            title=parsed_title,
            source_id=f"title-{parsed_title}",
            display_number=str(parsed_title),
            heading=heading,
            body=None,
            parent_citation_path=None,
            level=0,
            ordinal=0,
        )
    ]

    seen_chapters: set[str] = set()
    section_markers = tuple(_iter_section_markers(body, parsed_title))
    for section_ordinal, marker in enumerate(section_markers, start=1):
        section = _section_from_marker(marker, parsed_title)
        if not section:
            continue
        chapter = _chapter_from_section(section)
        if chapter not in seen_chapters:
            seen_chapters.add(chapter)
            provisions.append(
                PennsylvaniaProvision(
                    kind="chapter",
                    title=parsed_title,
                    source_id=f"{parsed_title}-chapter-{chapter}",
                    display_number=chapter,
                    heading=_chapter_heading(marker, chapter),
                    body=None,
                    parent_citation_path=f"us-pa/statute/title-{parsed_title}",
                    level=1,
                    ordinal=len(seen_chapters),
                    status=_chapter_status(marker, chapter),
                )
            )
        heading_tag = _next_text_tag(marker)
        section_heading = _section_heading(heading_tag, section)
        body_lines, notes, history, references = _section_content(
            marker,
            title=parsed_title,
            section=section,
        )
        body_text = "\n".join(body_lines).strip() or None
        references_to = tuple(
            ref for ref in dict.fromkeys(references) if ref != f"us-pa/statute/{parsed_title}/{section}"
        )
        provisions.append(
            PennsylvaniaProvision(
                kind="section",
                title=parsed_title,
                source_id=f"{parsed_title}-{section}",
                display_number=section,
                heading=section_heading,
                body=body_text,
                parent_citation_path=f"us-pa/statute/title-{parsed_title}/chapter-{chapter}",
                level=2,
                ordinal=section_ordinal,
                references_to=references_to,
                source_history=tuple(dict.fromkeys(history)),
                notes=tuple(dict.fromkeys(notes)),
                status=_section_status(section_heading, body_text, notes, history),
            )
        )
    return tuple(provisions)


def _iter_pennsylvania_title_sources(
    *,
    source_dir: Path | None,
    download_dir: Path | None,
    base_url: str,
    only_title: int | None,
    request_delay_seconds: float,
    request_attempts: int,
    timeout_seconds: float,
) -> Iterator[_PennsylvaniaTitleSource]:
    if source_dir is not None:
        yield from _iter_pennsylvania_title_sources_from_dir(source_dir, only_title=only_title)
        return

    titles = (only_title,) if only_title is not None else tuple(sorted(PENNSYLVANIA_TITLES))
    last_request = 0.0
    for title in titles:
        source_url = _title_url(title, base_url)
        relative_path = _title_relative_path(title)
        if download_dir is not None:
            cached_path = download_dir / relative_path
            if cached_path.exists():
                yield _PennsylvaniaTitleSource(
                    title=title,
                    relative_path=relative_path,
                    source_url=source_url,
                    data=cached_path.read_bytes(),
                )
                continue
        elapsed = time.monotonic() - last_request
        if elapsed < request_delay_seconds:
            time.sleep(request_delay_seconds - elapsed)
        data = _download_title(
            source_url,
            request_attempts=request_attempts,
            timeout_seconds=timeout_seconds,
        )
        last_request = time.monotonic()
        if download_dir is not None:
            cached_path = download_dir / relative_path
            cached_path.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(dir=cached_path.parent, delete=False) as tmp:
                tmp.write(data)
                tmp_path = Path(tmp.name)
            tmp_path.replace(cached_path)
        yield _PennsylvaniaTitleSource(
            title=title,
            relative_path=relative_path,
            source_url=source_url,
            data=data,
        )


def _iter_pennsylvania_title_sources_from_dir(
    source_dir: Path,
    *,
    only_title: int | None,
) -> Iterator[_PennsylvaniaTitleSource]:
    if not source_dir.exists():
        raise ValueError(f"Pennsylvania source directory does not exist: {source_dir}")
    candidates: list[tuple[int, Path]] = []
    for path in source_dir.rglob("*.html"):
        if not path.is_file():
            continue
        title = _title_from_path(path)
        if title is None:
            continue
        if only_title is not None and title != only_title:
            continue
        candidates.append((title, path))
    for title, path in sorted(candidates, key=lambda item: item[0]):
        yield _PennsylvaniaTitleSource(
            title=title,
            relative_path=_title_relative_path(title),
            source_url=_title_url(title, PENNSYLVANIA_BASE_URL),
            data=path.read_bytes(),
        )


def _download_title(
    source_url: str,
    *,
    request_attempts: int,
    timeout_seconds: float,
) -> bytes:
    last_error: Exception | None = None
    for attempt in range(1, max(request_attempts, 1) + 1):
        try:
            response = requests.get(
                source_url,
                headers={"User-Agent": PENNSYLVANIA_USER_AGENT},
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            data = response.content
            if b"403 - Forbidden" in data or b"Request blocked" in data:
                raise ValueError(f"Pennsylvania title source was blocked: {source_url}")
            return data
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < request_attempts:
                time.sleep(min(2**attempt, 8))
    raise ValueError(f"failed to download Pennsylvania source {source_url}: {last_error}")


def _iter_section_markers(root: Tag, title: int) -> Iterator[Tag]:
    marker_re = _section_marker_re(title)
    for tag in root.find_all(["div", "a"]):
        if not isinstance(tag, Tag):
            continue
        marker = _marker_text(tag)
        if marker and marker_re.fullmatch(marker):
            yield tag


def _section_marker_re(title: int) -> re.Pattern[str]:
    prefix = f"0?{title}" if title < 10 else str(title)
    return re.compile(rf"^{prefix}c(?P<section>[0-9A-Z.]+)s$", re.I)


def _marker_text(tag: Tag) -> str | None:
    if tag.name == "div" and "Comment" in set(tag.get("class") or ()):
        return _clean_text(tag.get_text(" ", strip=True))
    if tag.name == "a":
        value = tag.get("name") or tag.get("id")
        if value:
            return _clean_text(str(value))
    return None


def _section_from_marker(marker: Tag, title: int) -> str | None:
    marker_text = _marker_text(marker)
    if marker_text is None:
        return None
    match = _section_marker_re(title).fullmatch(marker_text)
    if match is None:
        return None
    return match.group("section").upper()


def _section_content(
    marker: Tag,
    *,
    title: int,
    section: str,
) -> tuple[list[str], list[str], list[str], list[str]]:
    body_lines: list[str] = []
    notes: list[str] = []
    history: list[str] = []
    references: list[str] = []
    heading_seen = False
    self_path = f"us-pa/statute/{title}/{section}"
    for sibling in marker.next_siblings:
        if isinstance(sibling, NavigableString):
            text = _clean_text(str(sibling))
            if text:
                body_lines.append(text)
            continue
        if not isinstance(sibling, Tag):
            continue
        if _marker_text(sibling):
            break
        if _is_layout_tag(sibling):
            continue
        text = _clean_text(sibling.get_text(" ", strip=True))
        if not text:
            continue
        if not heading_seen and _is_section_heading_text(text, section):
            heading_seen = True
            continue
        body_lines.append(text)
        if _is_note(text):
            notes.append(text)
        if _is_source_history(text):
            history.append(text)
        references.extend(_link_references(sibling, default_title=title, self_path=self_path))
        references.extend(_text_references(text, default_title=title, self_path=self_path))
    return body_lines, notes, history, references


def _link_references(root: Tag, *, default_title: int, self_path: str) -> tuple[str, ...]:
    refs: list[str] = []
    for link in root.find_all("a"):
        if not isinstance(link, Tag):
            continue
        href = str(link.get("href") or "")
        parsed = urlparse(href)
        query = parse_qs(parsed.query)
        section_values = query.get("sctn") or query.get("section")
        title_values = query.get("ttl")
        if not section_values:
            marker = href.rsplit("#", 1)[-1] if "#" in href else ""
            section = _section_from_anchor(marker)
            ref_title = default_title
        else:
            section = section_values[0]
            ref_title = int(title_values[0]) if title_values and title_values[0].isdigit() else default_title
        if not section:
            continue
        ref = f"us-pa/statute/{ref_title}/{_clean_section(section)}"
        if ref != self_path:
            refs.append(ref)
    return tuple(dict.fromkeys(refs))


def _text_references(text: str, *, default_title: int, self_path: str) -> tuple[str, ...]:
    refs: list[str] = []
    for match in _PA_TEXT_REFERENCE_RE.finditer(text):
        ref_title = int(match.group("title")) if match.group("title") else default_title
        ref = f"us-pa/statute/{ref_title}/{_clean_section(match.group('section'))}"
        if ref != self_path:
            refs.append(ref)
    return tuple(dict.fromkeys(refs))


def _section_from_anchor(value: str) -> str | None:
    match = re.search(r"c(?P<section>[0-9A-Z.]+)s$", value, re.I)
    if not match:
        return None
    return _clean_section(match.group("section"))


def _next_text_tag(marker: Tag) -> Tag | None:
    for sibling in marker.next_siblings:
        if not isinstance(sibling, Tag):
            continue
        if _marker_text(sibling):
            return None
        if _is_layout_tag(sibling):
            continue
        text = _clean_text(sibling.get_text(" ", strip=True))
        if text:
            return sibling
    return None


def _section_heading(tag: Tag | None, section: str) -> str | None:
    if tag is None:
        return None
    text = _clean_text(tag.get_text(" ", strip=True))
    match = _SECTION_HEADING_RE.match(text)
    if match and _clean_section(match.group("section")) == section:
        heading = _clean_text(match.group("heading"))
        return heading.removesuffix(".") or None
    return text or None


def _is_section_heading_text(text: str, section: str) -> bool:
    match = _SECTION_HEADING_RE.match(text)
    return bool(match and _clean_section(match.group("section")) == section)


def _chapter_from_section(section: str) -> str:
    base = section.split(".", 1)[0].upper()
    match = re.fullmatch(r"(?P<chapter>\d+[A-Z]?)(?P<section>\d{2})", base)
    if match:
        return match.group("chapter")
    if len(base) <= 3 and base[0].isdigit():
        return str(int(base[0]))
    if len(base) > 2:
        return base[:-2]
    return base


def _chapter_heading(marker: Tag, chapter: str) -> str | None:
    chapter_text = _nearest_previous_chapter_text(marker, chapter)
    if chapter_text:
        return chapter_text
    return f"Chapter {chapter}"


def _nearest_previous_chapter_text(marker: Tag, chapter: str) -> str | None:
    chapter_line: Tag | None = None
    for tag in marker.find_all_previous("p"):
        text = _clean_text(tag.get_text(" ", strip=True))
        match = _CHAPTER_RE.match(text)
        if not match:
            continue
        if _clean_section(match.group("chapter")) != chapter:
            continue
        chapter_line = tag
        inline_heading = _clean_text(match.group("heading"))
        if inline_heading and inline_heading.upper() not in {"(RESERVED)", "(REPEALED)"}:
            return inline_heading.removesuffix(".")
        break
    if chapter_line is None:
        return None
    heading_parts: list[str] = []
    for sibling in chapter_line.next_siblings:
        if isinstance(sibling, NavigableString):
            continue
        if not isinstance(sibling, Tag):
            continue
        if _marker_text(sibling):
            break
        text = _clean_text(sibling.get_text(" ", strip=True))
        if not text:
            continue
        normalized = text.upper()
        if normalized in {"SUBCHAPTER", "SEC.", "SECS.", "SECTION"}:
            break
        if normalized.startswith(("SUBCHAPTER ", "SEC.", "SECTION ", "\u00a7")):
            break
        if _is_chapter_noise(text):
            continue
        heading_parts.append(text.removesuffix("."))
        if len(heading_parts) >= 3:
            break
    heading = " ".join(heading_parts)
    return _title_case(heading) if heading else None


def _chapter_status(marker: Tag, chapter: str) -> str | None:
    chapter_line: Tag | None = None
    for tag in marker.find_all_previous("p"):
        text = _clean_text(tag.get_text(" ", strip=True))
        match = _CHAPTER_RE.match(text)
        if match and _clean_section(match.group("chapter")) == chapter:
            chapter_line = tag
            break
    if chapter_line is None:
        return None
    texts: list[str] = []
    for sibling in chapter_line.next_siblings:
        if isinstance(sibling, Tag):
            if _marker_text(sibling):
                break
            text = _clean_text(sibling.get_text(" ", strip=True))
            if text:
                texts.append(text)
            if len(texts) >= 6:
                break
    joined = " ".join(texts).lower()
    return "repealed" if "repealed" in joined else None


def _title_from_marker(marker: Tag) -> int:
    marker_text = _marker_text(marker) or ""
    match = re.match(r"^0?(?P<title>\d+)c", marker_text)
    if match:
        return int(match.group("title"))
    return 0


def _title_heading(root: Tag, title: int) -> str:
    marker = _find_title_marker(root, title)
    if marker is not None:
        next_tag = _next_nonempty_p(marker)
        if next_tag is not None:
            text = _clean_text(next_tag.get_text(" ", strip=True))
            if text and not _TITLE_MARKER_RE.match(text):
                return _title_case(text)
    return PENNSYLVANIA_TITLES.get(title, f"Title {title}")


def _find_title_marker(root: Tag, title: int) -> Tag | None:
    for tag in root.find_all("p"):
        text = _clean_text(tag.get_text(" ", strip=True))
        match = _TITLE_MARKER_RE.match(text)
        if match and int(match.group("title")) == title:
            return tag
    return None


def _next_nonempty_p(tag: Tag) -> Tag | None:
    for sibling in tag.next_siblings:
        if isinstance(sibling, Tag) and sibling.name == "p":
            text = _clean_text(sibling.get_text(" ", strip=True))
            if text:
                return sibling
    return None


def _infer_title(root: Tag) -> int | None:
    marker = root.find("p", string=lambda value: bool(value and _TITLE_MARKER_RE.match(value.strip())))
    if isinstance(marker, Tag):
        match = _TITLE_MARKER_RE.match(_clean_text(marker.get_text(" ", strip=True)))
        if match:
            return int(match.group("title"))
    for tag in root.find_all(["div", "a"]):
        marker_text = _marker_text(tag)
        if not marker_text:
            continue
        match = re.match(r"^0?(?P<title>\d+)c", marker_text)
        if match:
            return int(match.group("title"))
    return None


def _pennsylvania_title_filter(value: str | int | None) -> int | None:
    if value is None:
        return None
    return _title_filter(value)


def _title_filter(value: str | int) -> int:
    match = re.search(r"\d+", str(value))
    if not match:
        raise ValueError(f"invalid Pennsylvania title filter: {value!r}")
    return int(match.group(0))


def _title_from_path(path: Path) -> int | None:
    match = re.search(r"(?:title[-_])?(?P<title>\d+)$", path.stem, re.I)
    if not match:
        return None
    title = int(match.group("title"))
    return title if title in PENNSYLVANIA_TITLES else title


def _pennsylvania_run_id(version: str, *, only_title: int | None, limit: int | None) -> str:
    if only_title is None and limit is None:
        return version
    parts = [version, "us-pa"]
    if only_title is not None:
        parts.append(f"title-{only_title}")
    if limit is not None:
        parts.append(f"limit-{limit}")
    return "-".join(parts)


def _title_relative_path(title: int) -> str:
    return f"{PENNSYLVANIA_SOURCE_FORMAT}/title-{title}.html"


def _title_url(title: int, base_url: str) -> str:
    return urljoin(
        _base_url(base_url),
        "view-statute?" + urlencode({"txtType": "HTM", "ttl": title, "iFrame": "true"}),
    )


def _base_url(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _inventory_item(
    provision: PennsylvaniaProvision,
    *,
    source_url: str,
    source_path: str,
    sha256: str,
) -> SourceInventoryItem:
    return SourceInventoryItem(
        citation_path=provision.citation_path,
        source_url=source_url,
        source_path=source_path,
        source_format=PENNSYLVANIA_SOURCE_FORMAT,
        sha256=sha256,
        metadata=_metadata(provision),
    )


def _provision_record(
    provision: PennsylvaniaProvision,
    *,
    version: str,
    source_url: str,
    source_path: str,
    source_as_of: str,
    expression_date: str,
) -> ProvisionRecord:
    return ProvisionRecord(
        id=deterministic_provision_id(provision.citation_path),
        jurisdiction="us-pa",
        document_class=DocumentClass.STATUTE.value,
        citation_path=provision.citation_path,
        body=provision.body,
        heading=provision.heading,
        citation_label=provision.legal_identifier,
        version=version,
        source_url=source_url,
        source_path=source_path,
        source_id=provision.source_id,
        source_format=PENNSYLVANIA_SOURCE_FORMAT,
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
        identifiers={
            "pennsylvania:title": str(provision.title),
            f"pennsylvania:{provision.kind}": provision.display_number,
            "pennsylvania:source_id": provision.source_id,
        },
        metadata=_metadata(provision),
    )


def _metadata(provision: PennsylvaniaProvision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kind": provision.kind,
        "title": str(provision.title),
        "display_number": provision.display_number,
    }
    if provision.parent_citation_path:
        metadata["parent_citation_path"] = provision.parent_citation_path
    if provision.references_to:
        metadata["references_to"] = list(provision.references_to)
    if provision.notes:
        metadata["notes"] = list(provision.notes)
    if provision.source_history:
        metadata["source_history"] = list(provision.source_history)
    if provision.status:
        metadata["status"] = provision.status
    return metadata


def _state_source_key(jurisdiction: str, run_id: str, relative_name: str) -> str:
    return f"sources/{jurisdiction}/{DocumentClass.STATUTE.value}/{run_id}/{relative_name}"


def _date_text(value: date | str | None, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value.isoformat()
    return value


def _section_status(
    heading: str | None,
    body: str | None,
    notes: tuple[str, ...] | list[str],
    history: tuple[str, ...] | list[str],
) -> str | None:
    joined = " ".join([heading or "", body or "", *notes, *history]).lower()
    if "repealed" in joined[:200]:
        return "repealed"
    if "expired" in joined[:200]:
        return "expired"
    return None


def _is_note(text: str) -> bool:
    return bool(re.match(r"^(?:Compiler's Note|Cross References|References in Text)\.", text, re.I))


def _is_source_history(text: str) -> bool:
    return bool(re.match(r"^(?:History\.--|Enactment\.|\d{4}\s+Amendment\.)", text, re.I))


def _is_chapter_noise(text: str) -> bool:
    normalized = text.upper()
    return normalized in {"(RESERVED)", "(REPEALED)", "SUBCHAPTER", "SEC.", "SECS."}


def _is_layout_tag(tag: Tag) -> bool:
    return tag.name in {"script", "style"}


def _clean_section(value: str) -> str:
    return _clean_text(value).strip().rstrip(".").upper()


def _clean_text(value: str | None) -> str:
    text = (value or "").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s+([,.;:])", r"\1", text)


def _title_case(value: str) -> str:
    if not value.isupper():
        return value
    words = value.title().split()
    small_words = {"A", "An", "And", "As", "At", "But", "By", "For", "In", "Of", "On", "Or", "The", "To"}
    normalized: list[str] = []
    for index, word in enumerate(words):
        if 0 < index < len(words) - 1 and word in small_words:
            normalized.append(word.lower())
        else:
            normalized.append(word)
    return " ".join(normalized)
