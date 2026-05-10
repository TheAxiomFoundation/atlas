"""New York State Register rulemaking source adapter."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Protocol, TextIO
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from axiom_corpus.corpus.artifacts import CorpusArtifactStore
from axiom_corpus.corpus.coverage import ProvisionCoverageReport, compare_provision_coverage
from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem
from axiom_corpus.corpus.supabase import deterministic_provision_id

NY_STATE_REGISTER_URL = "https://dos.ny.gov/state-register"
NY_DOS_BASE_URL = "https://dos.ny.gov"
NY_STATE_REGISTER_SOURCE_FORMAT = "ny-dos-state-register-pdf"
NY_STATE_REGISTER_INDEX_SOURCE_FORMAT = "ny-dos-state-register-html"


class _Response(Protocol):
    content: bytes
    text: str
    url: str

    def raise_for_status(self) -> None: ...


class _Session(Protocol):
    def get(self, url: str, *, timeout: int = 30) -> _Response: ...


@dataclass(frozen=True)
class NyRulemakingExtractReport:
    """Result from a NY State Register extraction run."""

    jurisdiction: str
    document_class: str
    issue_count: int
    notice_count: int
    provisions_written: int
    inventory_path: Path
    provisions_path: Path
    coverage_path: Path
    coverage: ProvisionCoverageReport
    source_paths: tuple[Path, ...]


@dataclass(frozen=True)
class _StateRegisterIssue:
    title: str
    page_url: str
    pdf_url: str
    token: str


@dataclass(frozen=True)
class _StateRegisterNotice:
    notice_id: str
    agency: str | None
    action_type: str
    subject: str | None
    body: str


def ny_rulemaking_run_id(version: str, *, limit: int | None = None) -> str:
    """Return a scoped NY State Register run id."""
    return f"{version}-limit-{limit}" if limit is not None else version


def extract_ny_state_register(
    store: CorpusArtifactStore,
    *,
    version: str,
    source_as_of: str | None = None,
    expression_date: date | str | None = None,
    limit: int | None = None,
    session: _Session | None = None,
    progress_stream: TextIO | None = None,
) -> NyRulemakingExtractReport:
    """Snapshot the DOS State Register issue index and linked issue PDFs."""
    run_id = ny_rulemaking_run_id(version, limit=limit)
    client = session or requests.Session()
    response = client.get(NY_STATE_REGISTER_URL, timeout=30)
    response.raise_for_status()
    index_bytes = response.content
    index_relative = "state-register/index.html"
    index_path = store.source_path("us-ny", DocumentClass.RULEMAKING, run_id, index_relative)
    index_sha = store.write_bytes(index_path, index_bytes)
    source_paths = [index_path]
    index_source_key = f"sources/us-ny/{DocumentClass.RULEMAKING.value}/{run_id}/{index_relative}"

    issues = _parse_state_register_issues(response.text)
    if limit is not None:
        issues = issues[:limit]
    source_as_of_text = source_as_of or version
    expression_date_text = _date_text(expression_date, version)
    root_path = "us-ny/rulemaking/state-register"
    items = [
        SourceInventoryItem(
            citation_path=root_path,
            source_url=NY_STATE_REGISTER_URL,
            source_path=index_source_key,
            source_format=NY_STATE_REGISTER_INDEX_SOURCE_FORMAT,
            sha256=index_sha,
            metadata={"kind": "collection", "issue_count": len(issues)},
        )
    ]
    records = [
        ProvisionRecord(
            jurisdiction="us-ny",
            document_class=DocumentClass.RULEMAKING.value,
            citation_path=root_path,
            id=deterministic_provision_id(root_path),
            heading="New York State Register",
            version=run_id,
            source_url=NY_STATE_REGISTER_URL,
            source_path=index_source_key,
            source_id="ny-dos-state-register",
            source_format=NY_STATE_REGISTER_INDEX_SOURCE_FORMAT,
            source_as_of=source_as_of_text,
            expression_date=expression_date_text,
            kind="collection",
            level=1,
            metadata={
                "source": "New York Department of State State Register",
                "issue_count": len(issues),
            },
        )
    ]
    notice_count = 0
    for ordinal, issue in enumerate(issues, start=1):
        pdf_response = client.get(issue.pdf_url, timeout=30)
        pdf_response.raise_for_status()
        actual_pdf_url = pdf_response.url or issue.pdf_url
        pdf_relative = f"state-register/{issue.token}.pdf"
        pdf_path = store.source_path("us-ny", DocumentClass.RULEMAKING, run_id, pdf_relative)
        pdf_sha = store.write_bytes(pdf_path, pdf_response.content)
        source_paths.append(pdf_path)
        source_key = f"sources/us-ny/{DocumentClass.RULEMAKING.value}/{run_id}/{pdf_relative}"
        citation_path = f"{root_path}/{issue.token}"
        notices = _parse_state_register_notices(pdf_response.content)
        notice_count += len(notices)
        items.append(
            SourceInventoryItem(
                citation_path=citation_path,
                source_url=actual_pdf_url,
                source_path=source_key,
                source_format=NY_STATE_REGISTER_SOURCE_FORMAT,
                sha256=pdf_sha,
                metadata={
                    "kind": "state_register_issue",
                    "page_url": issue.page_url,
                    "pdf_url": actual_pdf_url,
                    "notice_count": len(notices),
                },
            )
        )
        records.append(
            ProvisionRecord(
                jurisdiction="us-ny",
                document_class=DocumentClass.RULEMAKING.value,
                citation_path=citation_path,
                id=deterministic_provision_id(citation_path),
                heading=issue.title,
                version=run_id,
                source_url=actual_pdf_url,
                source_path=source_key,
                source_id="ny-dos-state-register",
                source_format=NY_STATE_REGISTER_SOURCE_FORMAT,
                source_as_of=source_as_of_text,
                expression_date=expression_date_text,
                parent_citation_path=root_path,
                parent_id=deterministic_provision_id(root_path),
                level=2,
                ordinal=ordinal,
                kind="issue",
                legal_identifier=issue.title,
                metadata={
                    "source": "New York Department of State State Register",
                    "page_url": issue.page_url,
                    "pdf_url": actual_pdf_url,
                    "notice_count": len(notices),
                },
            )
        )
        for notice_ordinal, notice in enumerate(notices, start=1):
            notice_path = f"{citation_path}/notice/{_slug(notice.notice_id)}"
            notice_metadata = {
                "source": "New York Department of State State Register",
                "kind": "rulemaking_notice",
                "issue": issue.title,
                "agency": notice.agency,
                "action_type": notice.action_type,
                "notice_id": notice.notice_id,
                "subject": notice.subject,
                "page_url": issue.page_url,
                "pdf_url": actual_pdf_url,
            }
            items.append(
                SourceInventoryItem(
                    citation_path=notice_path,
                    source_url=actual_pdf_url,
                    source_path=source_key,
                    source_format=NY_STATE_REGISTER_SOURCE_FORMAT,
                    sha256=pdf_sha,
                    metadata=notice_metadata,
                )
            )
            records.append(
                ProvisionRecord(
                    jurisdiction="us-ny",
                    document_class=DocumentClass.RULEMAKING.value,
                    citation_path=notice_path,
                    id=deterministic_provision_id(notice_path),
                    body=notice.body,
                    heading=notice.subject or notice.notice_id,
                    citation_label=notice.notice_id,
                    version=run_id,
                    source_url=actual_pdf_url,
                    source_path=source_key,
                    source_id="ny-dos-state-register",
                    source_format=NY_STATE_REGISTER_SOURCE_FORMAT,
                    source_as_of=source_as_of_text,
                    expression_date=expression_date_text,
                    parent_citation_path=citation_path,
                    parent_id=deterministic_provision_id(citation_path),
                    level=3,
                    ordinal=notice_ordinal,
                    kind="notice",
                    legal_identifier=notice.notice_id,
                    identifiers={"ny-state-register:notice-id": notice.notice_id},
                    metadata=notice_metadata,
                )
            )
        if progress_stream is not None:
            print(
                f"downloaded NY State Register issue {ordinal}: "
                f"{issue.title} ({len(notices)} notices)",
                file=progress_stream,
            )

    inventory_path = store.inventory_path("us-ny", DocumentClass.RULEMAKING, run_id)
    store.write_inventory(inventory_path, items)
    provisions_path = store.provisions_path("us-ny", DocumentClass.RULEMAKING, run_id)
    store.write_provisions(provisions_path, records)
    coverage = compare_provision_coverage(
        tuple(items),
        tuple(records),
        jurisdiction="us-ny",
        document_class=DocumentClass.RULEMAKING.value,
        version=run_id,
    )
    coverage_path = store.coverage_path("us-ny", DocumentClass.RULEMAKING, run_id)
    store.write_json(coverage_path, coverage.to_mapping())
    return NyRulemakingExtractReport(
        jurisdiction="us-ny",
        document_class=DocumentClass.RULEMAKING.value,
        issue_count=len(issues),
        notice_count=notice_count,
        provisions_written=len(records),
        inventory_path=inventory_path,
        provisions_path=provisions_path,
        coverage_path=coverage_path,
        coverage=coverage,
        source_paths=tuple(source_paths),
    )


def _parse_state_register_issues(html: str) -> list[_StateRegisterIssue]:
    soup = BeautifulSoup(html, "lxml")
    issues: list[_StateRegisterIssue] = []
    seen: set[str] = set()
    for article in soup.select("article.teaser--type--webny-document"):
        link = article.select_one('a[href^="/"]')
        title_link = article.select_one(".field-content a[href]") or link
        if link is None or title_link is None:
            continue
        page_url = urljoin(NY_DOS_BASE_URL, link.get("href") or "")
        title = _clean_text(title_link.get_text(" ", strip=True))
        if not title:
            continue
        token = _slug(title)
        if page_url in seen:
            continue
        seen.add(page_url)
        issues.append(
            _StateRegisterIssue(
                title=title,
                page_url=page_url,
                pdf_url=page_url,
                token=token,
            )
        )
    return issues


def _parse_state_register_notices(pdf_bytes: bytes) -> list[_StateRegisterNotice]:
    text = _pdf_text(pdf_bytes)
    if not text:
        return []
    marker_re = re.compile(
        r"(?m)^(NOTICE OF ADOPTION|PROPOSED RULE MAKING|EMERGENCY RULE MAKING|"
        r"NOTICE OF REVISED RULE MAKING|NOTICE OF WITHDRAWAL|NOTICE OF EXPIRATION|"
        r"NOTICE OF EMERGENCY ADOPTION)\s*$"
    )
    matches = list(marker_re.finditer(text))
    notices: list[_StateRegisterNotice] = []
    last_agency: str | None = None
    for index, match in enumerate(matches):
        start = _agency_start(text, match.start())
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        block = _clean_notice_block(text[start:end])
        if not block:
            continue
        notice_id_match = re.search(r"I\.D\. No\.\s+([A-Z0-9-]+)", block)
        if not notice_id_match:
            continue
        action_type = match.group(1)
        agency = _agency_for_notice(text[start:match.start()])
        if agency:
            last_agency = agency
        else:
            agency = last_agency
        subject = _subject_for_notice(text[match.end() : end])
        notices.append(
            _StateRegisterNotice(
                notice_id=notice_id_match.group(1),
                agency=agency,
                action_type=action_type,
                subject=subject,
                body=block,
            )
        )
    return notices


def _pdf_text(pdf_bytes: bytes) -> str:
    try:
        import fitz

        document = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return ""
    try:
        return "\n".join(page.get_text() for page in document)
    finally:
        document.close()


def _agency_start(text: str, marker_start: int) -> int:
    line_infos: list[tuple[int, str]] = []
    offset = 0
    for raw_line in text[:marker_start].splitlines(keepends=True):
        line_infos.append((offset, raw_line))
        offset += len(raw_line)
    agency_start: int | None = None
    collected = False
    collected_count = 0
    for start, line in reversed(line_infos[-12:]):
        cleaned = _clean_text(line)
        if not cleaned:
            if collected:
                break
            continue
        if cleaned in {"RULE MAKING ACTIVITIES", "Rule Making Activities"}:
            break
        if not _looks_like_agency_line(cleaned):
            if collected:
                break
            continue
        collected = True
        collected_count += 1
        agency_start = start
        if collected_count >= 3:
            break
    return agency_start if agency_start is not None else marker_start


def _agency_for_notice(text: str) -> str | None:
    lines = [
        _clean_text(line)
        for line in text.splitlines()
        if _clean_text(line) and _looks_like_agency_line(_clean_text(line))
    ]
    if not lines:
        return None
    return _clean_text(" ".join(lines[-3:]))


def _looks_like_agency_line(line: str) -> bool:
    if len(line) > 72 or line.endswith(".") or ":" in line:
        return False
    if line in {"Rule Making Activities", "RULE MAKING ACTIVITIES"}:
        return False
    agency_terms = {
        "Authority",
        "Board",
        "Commission",
        "Control",
        "Council",
        "Department",
        "Division",
        "Office",
        "Service",
        "Services",
        "Vehicles",
    }
    words = re.findall(r"[A-Za-z]+", line)
    allowed_lowercase = {"and", "for", "of", "the"}
    if any(word[0].islower() and word not in allowed_lowercase for word in words):
        return False
    return bool(set(words) & agency_terms)


def _subject_for_notice(text: str) -> str | None:
    lines = [_clean_text(line) for line in text.splitlines() if _clean_text(line)]
    skipped = {
        "NO HEARING(S) SCHEDULED",
        "HEARING(S) SCHEDULED",
        "NO HEARING SCHEDULED",
    }
    subject_lines: list[str] = []
    for line in lines:
        if line in skipped:
            continue
        if line.startswith("I.D. No."):
            break
        subject_lines.append(line)
        if len(subject_lines) >= 2:
            break
    return _clean_text(" ".join(subject_lines)) if subject_lines else None


def _clean_notice_block(value: str) -> str:
    lines = [_clean_text(line) for line in value.splitlines()]
    return "\n".join(line for line in lines if line)


def _date_text(value: date | str | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, date):
        return value.isoformat()
    return value


def _slug(value: str) -> str:
    lowered = value.strip().lower().replace("—", "-").replace("–", "-")
    return re.sub(r"[^a-z0-9]+", "-", lowered).strip("-") or "issue"


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()
