"""Offline source-discovery reporting for external citation inventories.

External citation lists are not corpus inputs. This module turns static URL
inventories into an operations report so agents can see which leads are worth
reviewing before creating source-first manifests.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from axiom_corpus.corpus.models import DocumentClass
from axiom_corpus.corpus.releases import ReleaseManifest


class SourceStatus(StrEnum):
    """Source eligibility classification for one external URL."""

    PRIMARY_OFFICIAL = "primary_official"
    OFFICIAL_BUT_NOT_CURRENT = "official_but_not_current"
    SECONDARY_MIRROR = "secondary_mirror"
    ANALYTICAL_OR_REPORT = "analytical_or_report"
    VENDOR_OR_PAYWALLED = "vendor_or_paywalled"
    UNKNOWN = "unknown"


class DiscoveryDisposition(StrEnum):
    """Operational action bucket for one external URL."""

    READY_FOR_MANIFEST = "ready_for_manifest"
    NEEDS_REVIEW = "needs_review"
    EXCLUDED_SECONDARY = "excluded_secondary"
    BLOCKED_VENDOR_ONLY = "blocked_vendor_only"


@dataclass(frozen=True)
class CanonicalUrl:
    raw_url: str
    canonical_url: str
    host: str
    fragment: str | None


@dataclass(frozen=True)
class SourceDiscoveryRow:
    raw_url: str
    canonical_url: str
    host: str
    source_list: str
    input_count: int
    source_status: SourceStatus
    disposition: DiscoveryDisposition
    document_class: str
    jurisdiction: str | None
    release_scope_present: bool
    fragment: str | None
    reason: str

    def to_mapping(self) -> dict[str, Any]:
        return {
            "raw_url": self.raw_url,
            "canonical_url": self.canonical_url,
            "host": self.host,
            "source_list": self.source_list,
            "input_count": self.input_count,
            "source_status": self.source_status.value,
            "disposition": self.disposition.value,
            "document_class": self.document_class,
            "jurisdiction": self.jurisdiction,
            "release_scope_present": self.release_scope_present,
            "fragment": self.fragment,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class SourceDiscoveryDomainRow:
    host: str
    url_count: int
    ready_for_manifest_count: int
    needs_review_count: int
    excluded_count: int
    release_scope_present_count: int
    source_status_counts: dict[str, int]
    disposition_counts: dict[str, int]
    document_class_counts: dict[str, int]
    jurisdiction_counts: dict[str, int]
    sample_urls: tuple[str, ...]

    def to_mapping(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "url_count": self.url_count,
            "ready_for_manifest_count": self.ready_for_manifest_count,
            "needs_review_count": self.needs_review_count,
            "excluded_count": self.excluded_count,
            "release_scope_present_count": self.release_scope_present_count,
            "source_status_counts": self.source_status_counts,
            "disposition_counts": self.disposition_counts,
            "document_class_counts": self.document_class_counts,
            "jurisdiction_counts": self.jurisdiction_counts,
            "sample_urls": list(self.sample_urls),
        }


@dataclass(frozen=True)
class SourceDiscoveryGroupRow:
    """Actionable group of ready source leads that should become one manifest scope."""

    group_key: str
    jurisdiction: str
    document_class: str
    source_family: str
    url_count: int
    input_count: int
    host_counts: dict[str, int]
    source_list_counts: dict[str, int]
    suggested_manifest_stem: str
    suggested_action: str
    sample_urls: tuple[str, ...]

    def to_mapping(self) -> dict[str, Any]:
        return {
            "group_key": self.group_key,
            "jurisdiction": self.jurisdiction,
            "document_class": self.document_class,
            "source_family": self.source_family,
            "url_count": self.url_count,
            "input_count": self.input_count,
            "host_counts": self.host_counts,
            "source_list_counts": self.source_list_counts,
            "suggested_manifest_stem": self.suggested_manifest_stem,
            "suggested_action": self.suggested_action,
            "sample_urls": list(self.sample_urls),
        }


@dataclass(frozen=True)
class SourceDiscoveryReport:
    generated_at: str
    source_name: str
    input_paths: tuple[Path, ...]
    raw_url_count: int
    invalid_url_count: int
    unique_url_count: int
    release_name: str | None
    release_scope_count: int
    rows: tuple[SourceDiscoveryRow, ...]
    domain_rows: tuple[SourceDiscoveryDomainRow, ...]
    group_rows: tuple[SourceDiscoveryGroupRow, ...]

    @property
    def ready_for_manifest_count(self) -> int:
        return sum(
            1
            for row in self.rows
            if row.disposition is DiscoveryDisposition.READY_FOR_MANIFEST
        )

    @property
    def needs_review_count(self) -> int:
        return sum(1 for row in self.rows if row.disposition is DiscoveryDisposition.NEEDS_REVIEW)

    @property
    def blocked_or_excluded_count(self) -> int:
        return sum(
            1
            for row in self.rows
            if row.disposition
            in (
                DiscoveryDisposition.EXCLUDED_SECONDARY,
                DiscoveryDisposition.BLOCKED_VENDOR_ONLY,
            )
        )

    @property
    def release_scope_present_count(self) -> int:
        return sum(1 for row in self.rows if row.release_scope_present)

    @property
    def ready_group_count(self) -> int:
        return len(self.group_rows)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "source_name": self.source_name,
            "input_paths": [str(path) for path in self.input_paths],
            "raw_url_count": self.raw_url_count,
            "invalid_url_count": self.invalid_url_count,
            "unique_url_count": self.unique_url_count,
            "release": self.release_name,
            "release_scope_count": self.release_scope_count,
            "ready_for_manifest_count": self.ready_for_manifest_count,
            "ready_group_count": self.ready_group_count,
            "needs_review_count": self.needs_review_count,
            "blocked_or_excluded_count": self.blocked_or_excluded_count,
            "release_scope_present_count": self.release_scope_present_count,
            "source_status_counts": _counter_mapping(
                Counter(row.source_status.value for row in self.rows)
            ),
            "disposition_counts": _counter_mapping(
                Counter(row.disposition.value for row in self.rows)
            ),
            "document_class_counts": _counter_mapping(
                Counter(row.document_class for row in self.rows)
            ),
            "jurisdiction_counts": _counter_mapping(
                Counter(row.jurisdiction or "unknown" for row in self.rows)
            ),
            "group_rows": [row.to_mapping() for row in self.group_rows],
            "domain_rows": [row.to_mapping() for row in self.domain_rows],
            "rows": [row.to_mapping() for row in self.rows],
            "corpus_source_policy": (
                "External citations are discovery leads only; selected documents "
                "must be re-fetched from primary official sources into corpus artifacts."
            ),
        }


@dataclass(frozen=True)
class _InputUrl:
    raw_url: str
    source_list: str
    canonical: CanonicalUrl


KNOWN_OFFICIAL_HOSTS: dict[str, str | None] = {
    "acf.gov": "us",
    "acf.hhs.gov": "us",
    "adminrules.idaho.gov": "us-id",
    "alisondb.legislature.state.al.us": "us-al",
    "app.leg.wa.gov": "us-wa",
    "apps.dor.ga.gov": "us-ga",
    "apps.legislature.ky.gov": "us-ky",
    "aspe.hhs.gov": "us",
    "azdor.gov": "us-az",
    "azleg.gov": "us-az",
    "capitol.hawaii.gov": "us-hi",
    "cdss.ca.gov": "us-ca",
    "cga.ct.gov": "us-ct",
    "code.dccouncil.gov": "us-dc",
    "code.wvlegislature.gov": "us-wv",
    "codes.ohio.gov": "us-oh",
    "coloradosos.gov": "us-co",
    "congress.gov": "us",
    "delcode.delaware.gov": "us-de",
    "docs.legis.wisconsin.gov": "us-wi",
    "dor.georgia.gov": "us-ga",
    "dor.mo.gov": "us-mo",
    "dor.ms.gov": "us-ms",
    "dor.sc.gov": "us-sc",
    "dor.wa.gov": "us-wa",
    "ecfr.gov": "us",
    "eclkc.ohs.acf.hhs.gov": "us",
    "federalregister.gov": "us",
    "files.hawaii.gov": "us-hi",
    "fns-prod.azureedge.net": "us",
    "fns-prod.azureedge.us": "us",
    "fns.usda.gov": "us",
    "forms.in.gov": "us-in",
    "ftb.ca.gov": "us-ca",
    "gencourt.state.nh.us": "us-nh",
    "govinfo.gov": "us",
    "headstart.gov": "us",
    "iga.in.gov": "us-in",
    "kslegislature.org": "us-ks",
    "ksrevenue.gov": "us-ks",
    "law.lis.virginia.gov": "us-va",
    "leg.colorado.gov": "us-co",
    "leg.mt.gov": "us-mt",
    "leginfo.legislature.ca.gov": "us-ca",
    "legis.ga.gov": "us-ga",
    "legis.iowa.gov": "us-ia",
    "legis.la.gov": "us-la",
    "legislature.idaho.gov": "us-id",
    "legislature.maine.gov": "us-me",
    "legislature.mi.gov": "us-mi",
    "legislature.vermont.gov": "us-vt",
    "mainelegislature.org": "us-me",
    "malegislature.gov": "us-ma",
    "marylandtaxes.gov": "us-md",
    "mass.gov": "us-ma",
    "medicaid.gov": "us",
    "mgaleg.maryland.gov": "us-md",
    "michigan.gov": "us-mi",
    "mtrevenue.gov": "us-mt",
    "ncdor.gov": "us-nc",
    "ncleg.gov": "us-nc",
    "nebraskalegislature.gov": "us-ne",
    "nmonesource.com": "us-nm",
    "nysenate.gov": "us-ny",
    "ocfs.ny.gov": "us-ny",
    "oregonlegislature.gov": "us-or",
    "osse.dc.gov": "us-dc",
    "otda.ny.gov": "us-ny",
    "portal.ct.gov": "us-ct",
    "revenue.alabama.gov": "us-al",
    "revenue.iowa.gov": "us-ia",
    "revenue.ky.gov": "us-ky",
    "revenue.louisiana.gov": "us-la",
    "revenue.nebraska.gov": "us-ne",
    "revenue.nh.gov": "us-nh",
    "revenue.pa.gov": "us-pa",
    "revenue.state.mn.us": "us-mn",
    "revenue.wi.gov": "us-wi",
    "revenuefiles.delaware.gov": "us-de",
    "revenuefiles.mt.gov": "us-mt",
    "revisor.mn.gov": "us-mn",
    "revisor.mo.gov": "us-mo",
    "rules.mt.gov": "us-mt",
    "scstatehouse.gov": "us-sc",
    "secure.ssa.gov": "us",
    "services.dpw.state.pa.us": "us-pa",
    "sos.state.co.us": "us-co",
    "ssa.gov": "us",
    "tax.colorado.gov": "us-co",
    "tax.idaho.gov": "us-id",
    "tax.illinois.gov": "us-il",
    "tax.iowa.gov": "us-ia",
    "tax.nd.gov": "us-nd",
    "tax.ny.gov": "us-ny",
    "tax.ohio.gov": "us-oh",
    "tax.ri.gov": "us-ri",
    "tax.utah.gov": "us-ut",
    "tax.vermont.gov": "us-vt",
    "tax.virginia.gov": "us-va",
    "tax.wv.gov": "us-wv",
    "tile.loc.gov": "us",
    "twc.texas.gov": "us-tx",
    "webserver.rilin.state.ri.us": "us-ri",
    "workingfamiliescredit.wa.gov": "us-wa",
}

STATE_HOST_SUBSTRINGS: dict[str, str] = {
    ".alabama.gov": "us-al",
    ".arkansas.gov": "us-ar",
    ".az.gov": "us-az",
    ".ca.gov": "us-ca",
    ".colorado.gov": "us-co",
    ".ct.gov": "us-ct",
    ".dc.gov": "us-dc",
    ".delaware.gov": "us-de",
    ".florida.gov": "us-fl",
    ".georgia.gov": "us-ga",
    ".hawaii.gov": "us-hi",
    ".idaho.gov": "us-id",
    ".illinois.gov": "us-il",
    ".in.gov": "us-in",
    ".iowa.gov": "us-ia",
    ".kansas.gov": "us-ks",
    ".kentucky.gov": "us-ky",
    ".louisiana.gov": "us-la",
    ".maine.gov": "us-me",
    ".maryland.gov": "us-md",
    ".mass.gov": "us-ma",
    ".michigan.gov": "us-mi",
    ".mn.gov": "us-mn",
    ".ms.gov": "us-ms",
    ".mo.gov": "us-mo",
    ".mt.gov": "us-mt",
    ".nebraska.gov": "us-ne",
    ".nevada.gov": "us-nv",
    ".nh.gov": "us-nh",
    ".nj.gov": "us-nj",
    ".ny.gov": "us-ny",
    ".ohio.gov": "us-oh",
    ".oklahoma.gov": "us-ok",
    ".oregon.gov": "us-or",
    ".pa.gov": "us-pa",
    ".sc.gov": "us-sc",
    ".texas.gov": "us-tx",
    ".utah.gov": "us-ut",
    ".vermont.gov": "us-vt",
    ".virginia.gov": "us-va",
    ".wa.gov": "us-wa",
    ".wv.gov": "us-wv",
    ".wisconsin.gov": "us-wi",
}

VENDOR_HOSTS = (
    "advance.lexis.com",
    "bloombergtax.com",
    "govt.westlaw.com",
    "lexis.com",
    "lexisnexis.com",
    "westlaw.com",
)
SECONDARY_HOSTS = (
    "casetext.com",
    "codes.findlaw.com",
    "efile.com",
    "elaws.us",
    "findlaw.com",
    "justia.com",
    "law.cornell.edu",
    "lawserver.com",
    "legiscan.com",
    "public.law",
    "regulations.justia.com",
    "snapscreener.com",
    "taxformfinder.org",
    "taxsim.nber.org",
    "zillionforms.com",
)
ANALYTICAL_HOSTS = (
    "atlantafed.org",
    "files.kff.org",
    "frac.org",
    "kff.org",
    "masslegalservices.org",
)
PRIVATE_OR_UNOWNED_HOSTS = (
    "assets-global.website-files.com",
    "docs.google.com",
)
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
}


def build_source_discovery_report(
    input_paths: tuple[str | Path, ...],
    *,
    release: ReleaseManifest | None = None,
    covered_source_urls: Iterable[str] | None = None,
    source_name: str = "policyengine-us",
    generated_at: str | None = None,
) -> SourceDiscoveryReport:
    """Build a source-discovery report from static URL-list files."""

    paths = tuple(Path(path) for path in input_paths)
    loaded = _load_input_urls(paths)
    grouped: dict[str, list[_InputUrl]] = defaultdict(list)
    invalid_url_count = 0
    for item in loaded:
        if item.canonical is None:
            invalid_url_count += 1
            continue
        grouped[item.canonical.canonical_url].append(
            _InputUrl(
                raw_url=item.raw_url,
                source_list=item.source_list,
                canonical=item.canonical,
            )
        )

    release_scopes = (
        {(scope.jurisdiction, scope.document_class) for scope in release.scopes}
        if release is not None
        else set()
    )
    covered_canonical_urls = (
        _covered_canonical_url_keys(covered_source_urls)
        if covered_source_urls is not None
        else None
    )
    rows = tuple(
        _build_row(
            canonical_url,
            items,
            release_scopes=release_scopes,
            covered_canonical_urls=covered_canonical_urls,
        )
        for canonical_url, items in sorted(grouped.items())
    )
    domain_rows = _build_domain_rows(rows)
    group_rows = _build_group_rows(rows)
    return SourceDiscoveryReport(
        generated_at=generated_at or datetime.now(UTC).isoformat(),
        source_name=source_name,
        input_paths=paths,
        raw_url_count=len(loaded),
        invalid_url_count=invalid_url_count,
        unique_url_count=len(rows),
        release_name=release.name if release is not None else None,
        release_scope_count=len(release_scopes),
        rows=rows,
        domain_rows=domain_rows,
        group_rows=group_rows,
    )


@dataclass(frozen=True)
class _LoadedInputUrl:
    raw_url: str
    source_list: str
    canonical: CanonicalUrl | None


def _load_input_urls(paths: tuple[Path, ...]) -> tuple[_LoadedInputUrl, ...]:
    rows: list[_LoadedInputUrl] = []
    for path in paths:
        source_list = _source_list_name(path)
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            rows.append(
                _LoadedInputUrl(
                    raw_url=stripped,
                    source_list=source_list,
                    canonical=canonicalize_url(stripped),
                )
            )
    return tuple(rows)


def canonicalize_url(raw_url: str) -> CanonicalUrl | None:
    """Normalize a URL enough to deduplicate static reference lists."""

    split = urlsplit(raw_url.strip())
    if split.scheme not in {"http", "https"} or not split.netloc:
        return None
    host = _normalize_host(split.hostname or "")
    if not host:
        return None
    netloc = host
    if split.port and split.port not in (80, 443):
        netloc = f"{host}:{split.port}"
    path = split.path or "/"
    if path != "/":
        path = path.rstrip("/")
    query = _normalize_query(split.query)
    canonical_url = urlunsplit((split.scheme.lower(), netloc, path, query, ""))
    return CanonicalUrl(
        raw_url=raw_url,
        canonical_url=canonical_url,
        host=host,
        fragment=split.fragment or None,
    )


def _covered_canonical_url_keys(urls: Iterable[str]) -> frozenset[str]:
    keys: set[str] = set()
    for url in urls:
        canonical = canonicalize_url(url)
        if canonical is None:
            continue
        keys.update(_canonical_url_equivalents(canonical.canonical_url))
    return frozenset(keys)


def _canonical_url_equivalents(canonical_url: str) -> tuple[str, ...]:
    """Return conservative URL variants that commonly identify the same page."""

    split = urlsplit(canonical_url)
    path = split.path or "/"
    variants = {canonical_url}
    if path not in {"", "/"}:
        name = path.rsplit("/", 1)[-1]
        if path.endswith("/index.html"):
            base_path = path.removesuffix("/index.html") or "/"
            variants.add(
                urlunsplit((split.scheme, split.netloc, base_path, split.query, ""))
            )
        elif "." not in name:
            variants.add(
                urlunsplit(
                    (
                        split.scheme,
                        split.netloc,
                        f"{path.rstrip('/')}/index.html",
                        split.query,
                        "",
                    )
                )
            )
    return tuple(sorted(variants))


def _normalize_query(query: str) -> str:
    pairs = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        lower_key = key.lower()
        if lower_key in TRACKING_QUERY_KEYS:
            continue
        if any(lower_key.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        pairs.append((key, value))
    return urlencode(sorted(pairs), doseq=True)


def _normalize_host(host: str) -> str:
    lower = host.lower().strip().rstrip(".")
    return lower[4:] if lower.startswith("www.") else lower


def _build_row(
    canonical_url: str,
    items: list[_InputUrl],
    *,
    release_scopes: set[tuple[str, str]],
    covered_canonical_urls: frozenset[str] | None,
) -> SourceDiscoveryRow:
    canonical = items[0].canonical
    host = canonical.host
    source_status = classify_source_status(host)
    document_class = infer_document_class(canonical_url, host)
    jurisdiction = infer_jurisdiction(host, canonical_url)
    release_scope_present = (
        canonical_url in covered_canonical_urls
        if covered_canonical_urls is not None
        else jurisdiction is not None and (jurisdiction, document_class) in release_scopes
    )
    disposition, reason = _disposition_and_reason(
        source_status=source_status,
        document_class=document_class,
        jurisdiction=jurisdiction,
    )
    return SourceDiscoveryRow(
        raw_url=items[0].raw_url,
        canonical_url=canonical_url,
        host=host,
        source_list=",".join(sorted({item.source_list for item in items})),
        input_count=len(items),
        source_status=source_status,
        disposition=disposition,
        document_class=document_class,
        jurisdiction=jurisdiction,
        release_scope_present=release_scope_present,
        fragment=canonical.fragment,
        reason=reason,
    )


def classify_source_status(host: str) -> SourceStatus:
    if _host_matches(host, VENDOR_HOSTS):
        return SourceStatus.VENDOR_OR_PAYWALLED
    if _host_matches(host, SECONDARY_HOSTS):
        return SourceStatus.SECONDARY_MIRROR
    if _host_matches(host, ANALYTICAL_HOSTS):
        return SourceStatus.ANALYTICAL_OR_REPORT
    if _host_matches(host, PRIVATE_OR_UNOWNED_HOSTS):
        return SourceStatus.UNKNOWN
    if _known_official_host(host) or host.endswith(".gov") or ".state." in host:
        return SourceStatus.PRIMARY_OFFICIAL
    return SourceStatus.UNKNOWN


def infer_document_class(url: str, host: str) -> str:
    text = f"{host} {url}".lower()
    if any(
        token in text
        for token in (
            "federalregister",
            "/register/",
            "state-register",
            "proposed-rule",
            "rulemaking",
        )
    ):
        return DocumentClass.RULEMAKING.value
    if any(
        token in text
        for token in (
            "adminrules",
            "administrative-code",
            "administrative-codes",
            "/cfr/",
            "ecfr",
            "regulation",
            "regulations",
            "/rules/",
            "txrules",
        )
    ):
        return DocumentClass.REGULATION.value
    if any(
        token in text
        for token in (
            "statute",
            "statutes",
            "uscode",
            "/usc/",
            "/code/",
            "codeofalabama",
            "delcode",
            "general-laws",
            "legislative/laws",
            "revised-statutes",
        )
    ):
        return DocumentClass.STATUTE.value
    if any(token in text for token in ("manual", "epolicy", "policy", "olmweb")):
        return DocumentClass.MANUAL.value
    if any(
        token in text
        for token in (
            "cola",
            "guidance",
            "memo",
            "notice",
            "acl",
            "im202",
            "im%202",
            "poverty-guidelines",
            "poverty_guidelines",
            "program-facts",
        )
    ):
        return DocumentClass.GUIDANCE.value
    if any(token in text for token in ("form", "forms", "instructions", ".xlsx", ".xls")):
        return DocumentClass.FORM.value
    return DocumentClass.OTHER.value


def infer_jurisdiction(host: str, url: str) -> str | None:
    known = _known_official_host(host)
    if known:
        return known
    for needle, jurisdiction in STATE_HOST_SUBSTRINGS.items():
        if needle in f".{host}":
            return jurisdiction
    if host.endswith(".gov"):
        return "us"
    if "dccouncil" in host or ".dc.gov" in host:
        return "us-dc"
    lower_url = url.lower()
    if "newyork" in lower_url or "/ny" in lower_url:
        return "us-ny"
    return None


def _disposition_and_reason(
    *,
    source_status: SourceStatus,
    document_class: str,
    jurisdiction: str | None,
) -> tuple[DiscoveryDisposition, str]:
    if source_status is SourceStatus.VENDOR_OR_PAYWALLED:
        return (
            DiscoveryDisposition.BLOCKED_VENDOR_ONLY,
            "vendor or paywalled endpoint; require official confirmation or license path",
        )
    if source_status in (SourceStatus.SECONDARY_MIRROR, SourceStatus.ANALYTICAL_OR_REPORT):
        return (
            DiscoveryDisposition.EXCLUDED_SECONDARY,
            "secondary source; use only to discover primary official documents",
        )
    if source_status is SourceStatus.PRIMARY_OFFICIAL and jurisdiction and document_class != "other":
        return (
            DiscoveryDisposition.READY_FOR_MANIFEST,
            "official/open candidate that can seed a source-first manifest",
        )
    return (
        DiscoveryDisposition.NEEDS_REVIEW,
        "needs source-status, jurisdiction, or document-class review before ingestion",
    )


def _build_domain_rows(rows: tuple[SourceDiscoveryRow, ...]) -> tuple[SourceDiscoveryDomainRow, ...]:
    grouped: dict[str, list[SourceDiscoveryRow]] = defaultdict(list)
    for row in rows:
        grouped[row.host].append(row)

    domain_rows = [
        SourceDiscoveryDomainRow(
            host=host,
            url_count=len(host_rows),
            ready_for_manifest_count=sum(
                row.disposition is DiscoveryDisposition.READY_FOR_MANIFEST
                for row in host_rows
            ),
            needs_review_count=sum(
                row.disposition is DiscoveryDisposition.NEEDS_REVIEW for row in host_rows
            ),
            excluded_count=sum(
                row.disposition
                in (
                    DiscoveryDisposition.EXCLUDED_SECONDARY,
                    DiscoveryDisposition.BLOCKED_VENDOR_ONLY,
                )
                for row in host_rows
            ),
            release_scope_present_count=sum(row.release_scope_present for row in host_rows),
            source_status_counts=_counter_mapping(
                Counter(row.source_status.value for row in host_rows)
            ),
            disposition_counts=_counter_mapping(
                Counter(row.disposition.value for row in host_rows)
            ),
            document_class_counts=_counter_mapping(
                Counter(row.document_class for row in host_rows)
            ),
            jurisdiction_counts=_counter_mapping(
                Counter(row.jurisdiction or "unknown" for row in host_rows)
            ),
            sample_urls=tuple(row.canonical_url for row in host_rows[:3]),
        )
        for host, host_rows in grouped.items()
    ]
    return tuple(
        sorted(
            domain_rows,
            key=lambda row: (
                -row.ready_for_manifest_count,
                -row.needs_review_count,
                -row.url_count,
                row.host,
            ),
        )
    )


def _build_group_rows(rows: tuple[SourceDiscoveryRow, ...]) -> tuple[SourceDiscoveryGroupRow, ...]:
    grouped: dict[tuple[str, str, str], list[SourceDiscoveryRow]] = defaultdict(list)
    for row in rows:
        if row.disposition is not DiscoveryDisposition.READY_FOR_MANIFEST:
            continue
        if row.release_scope_present or not row.jurisdiction:
            continue
        source_family = infer_source_family(row)
        grouped[(row.jurisdiction, row.document_class, source_family)].append(row)

    group_rows = [
        _source_discovery_group_row(
            jurisdiction=jurisdiction,
            document_class=document_class,
            source_family=source_family,
            rows=grouped_rows,
        )
        for (jurisdiction, document_class, source_family), grouped_rows in grouped.items()
    ]
    return tuple(
        sorted(
            group_rows,
            key=lambda row: (
                -row.input_count,
                -row.url_count,
                row.jurisdiction,
                row.document_class,
                row.source_family,
            ),
        )
    )


def _source_discovery_group_row(
    *,
    jurisdiction: str,
    document_class: str,
    source_family: str,
    rows: list[SourceDiscoveryRow],
) -> SourceDiscoveryGroupRow:
    group_key = f"{jurisdiction}/{document_class}/{source_family}"
    return SourceDiscoveryGroupRow(
        group_key=group_key,
        jurisdiction=jurisdiction,
        document_class=document_class,
        source_family=source_family,
        url_count=len(rows),
        input_count=sum(row.input_count for row in rows),
        host_counts=_counter_mapping(Counter(row.host for row in rows)),
        source_list_counts=_counter_mapping(
            Counter(
                source_list
                for row in rows
                for source_list in row.source_list.split(",")
                if source_list
            )
        ),
        suggested_manifest_stem=f"{jurisdiction}-{source_family.replace('_', '-')}",
        suggested_action=_suggested_group_action(document_class, source_family),
        sample_urls=tuple(
            row.canonical_url
            for row in sorted(rows, key=lambda row: (-row.input_count, row.canonical_url))[:5]
        ),
    )


def infer_source_family(row: SourceDiscoveryRow) -> str:
    """Return a stable grouping label for ready source-discovery leads."""
    text = f"{row.host} {row.canonical_url}".lower()
    if "sua-table" in text or ("utility" in text and "snap" in text):
        return "snap_utility_allowance_data"
    if "medicaid" in text and "eligibility-levels" in text:
        return "medicaid_chip_eligibility_levels"
    if "tax-parameters" in text:
        return "federal_tax_parameters"
    if "poverty-guidelines" in text or "poverty_guidelines" in text:
        return "poverty_guidelines"
    if "pir-form" in text:
        return "head_start_program_information_report"
    if "child-care" in text or "child_care" in text:
        return "child_care_subsidy"
    if row.document_class == DocumentClass.FORM.value:
        if _looks_like_tax_source(text):
            if _contains_any(text, ("eitc", "eic", "earned-income")):
                return "individual_income_tax_eitc_forms"
            if _contains_any(text, ("property-tax", "property_tax", "homestead", "rent")):
                return "property_tax_relief_forms"
            if _contains_any(
                text,
                (
                    "1040",
                    "ar1000",
                    "form-140",
                    "il-1040",
                    "individual",
                    "income",
                    "it540",
                    "n11",
                    "pit",
                ),
            ):
                return "individual_income_tax_forms"
            return "tax_forms"
        return "official_forms"
    if row.document_class == DocumentClass.GUIDANCE.value:
        if "snap" in text:
            return "snap_guidance"
        if _looks_like_tax_source(text):
            return "tax_guidance"
        return "official_guidance"
    if row.document_class == DocumentClass.MANUAL.value:
        return "manuals"
    return row.document_class


def _suggested_group_action(document_class: str, source_family: str) -> str:
    if source_family == "snap_utility_allowance_data":
        return "Create a current federal SNAP utility-allowance data manifest from the official workbook."
    if source_family == "medicaid_chip_eligibility_levels":
        return "Create a federal Medicaid/CHIP eligibility-levels guidance or data manifest."
    if source_family == "federal_tax_parameters":
        return "Create a federal tax-parameter data manifest; prefer workbook-aware extraction."
    if source_family == "individual_income_tax_forms":
        return "Manifest the current-year official tax booklet/forms first, then decide whether to add historical years."
    if source_family == "individual_income_tax_eitc_forms":
        return "Manifest the current-year official EITC form/instructions with the related return booklet if needed."
    if source_family == "property_tax_relief_forms":
        return "Manifest current official property-tax relief forms and instructions as one scope."
    if document_class == DocumentClass.FORM.value:
        return "Review the group and create one source-first manifest for the coherent current official form set."
    if document_class == DocumentClass.GUIDANCE.value:
        return "Review the group and create one source-first manifest for the coherent official guidance set."
    return "Review the group and create one source-first manifest when the documents share a policy scope."


def _looks_like_tax_source(text: str) -> bool:
    return _contains_any(
        text,
        (
            "1040",
            "azdor",
            "dor.",
            "mtrevenue",
            "ncdor",
            "pit-",
            "revenue",
            "tax",
            "taxation",
            "taxes",
            "/dor/",
            "/drs/",
        ),
    )


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)


def _known_official_host(host: str) -> str | None:
    if host in KNOWN_OFFICIAL_HOSTS:
        return KNOWN_OFFICIAL_HOSTS[host]
    for official_host, jurisdiction in KNOWN_OFFICIAL_HOSTS.items():
        if host.endswith(f".{official_host}"):
            return jurisdiction
    return None


def _host_matches(host: str, candidates: tuple[str, ...]) -> bool:
    return any(host == candidate or host.endswith(f".{candidate}") for candidate in candidates)


def _source_list_name(path: Path) -> str:
    name = path.stem
    if name.endswith("_references"):
        name = name.removesuffix("_references")
    return name


def _counter_mapping(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}
