"""Fast parallel statute crawler with R2 upload.

Usage:
    # Test single state
    uv run python -m arch.crawl us-oh --max-sections 100

    # Crawl all states
    uv run python -m arch.crawl --all

    # Dry run (no R2 upload)
    uv run python -m arch.crawl --all --dry-run

    # Download from Archive.org bulk data
    uv run python -m arch.crawl --archive us-ar us-co us-ga

Per-state dispatch
==================

State crawling is driven by two lookup tables, **not** by a class
hierarchy. Adding a state means adding an entry to each; there is no
subclassing of ``StateCrawler`` per jurisdiction.

1. ``atlas.sources.registry.get_all_configs()`` returns a
   ``SourceConfig`` per jurisdiction (``base_url``, ``toc_url_pattern``,
   ``codes``). This drives *where* to crawl.

2. ``SECTION_PATTERNS`` (below) or the YAML specs in
   ``atlas.sources.specs`` map a jurisdiction to a regex that identifies
   a "section page" vs. a navigation page. This drives *what to keep*
   during the recursive link walk in
   ``StateCrawler.discover_sections``.

Fallback order inside ``_get_section_pattern``:
    - spec YAML pattern for the jurisdiction (preferred);
    - hardcoded entry in ``SECTION_PATTERNS`` for the jurisdiction;
    - ``SECTION_PATTERNS["_default"]`` (a permissive catch-all that
      matches URLs containing ``section`` / ``§`` / ``sec`` / ``statute``
      followed by digits).

The default fallback is *deliberately loose* and will usually either
capture noise (nav pages that happen to contain the word "section") or
miss content entirely on sites with non-obvious URL shapes (postback
forms, embedded query IDs, JavaScript-rendered tables of contents).
When a new state produces 0 fetches or a huge pile of TOC pages, the
first thing to check is whether its pattern in ``SECTION_PATTERNS`` is
specific enough.

Known-broken jurisdictions (patterns either don't match real section
pages or the site requires more than a plain HTTP GET) are tracked in
``DATA_INVENTORY.md`` under "Known upstream issues".
"""

import asyncio
import hashlib
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin, urlparse

import click
import httpx
from bs4 import BeautifulSoup

from atlas.sources.registry import SourceConfig, get_all_configs
from atlas.sources.specs import get_section_pattern

# R2 config
R2_ENDPOINT = "https://010d8d7f3b423be5ce36c7a5a49e91e4.r2.cloudflarestorage.com"
R2_BUCKET = "atlas"

# States with bulk downloads available from Archive.org/Public.Resource.org
# Maps jurisdiction ID to Archive.org item identifier
# See: https://archive.org/details/govlaw
ARCHIVE_ORG_STATES: dict[str, str] = {
    "us-ga": "gov.ga.ocga.2018",  # Georgia OCGA
    "us-ky": "gov.ky.code",  # Kentucky Revised Statutes
    "us-nc": "gov.nc.code",  # North Carolina General Statutes
    "us-nd": "gov.nd.code",  # North Dakota Century Code
    "us-tn": "gov.tn.tca",  # Tennessee Code Annotated
    "us-vt": "gov.vt.code",  # Vermont Statutes
    "us-va": "gov.va.code",  # Virginia Code
    "us-wy": "gov.wy.code",  # Wyoming Statutes
    # Note: Colorado, Arkansas, Idaho, Mississippi have volumes split across
    # multiple archive.org items - not yet supported
}

# State-specific section URL patterns (regex)
# These identify what counts as a "section page" worth storing
# Many states put all sections in chapter pages, so we match chapters
SECTION_PATTERNS: dict[str, str] = {
    # === WORKING STATES ===
    "us-oh": r"/section-[\d.]+",
    "us-tx": r"/[A-Z]+\.\d+",
    "us-ny": r"/[A-Z]+/\d+",
    "us-fl": r"/statutes/\d+\.\d+",
    "us-nv": r"NRS-[\dA-Z]+\.html",  # Chapter pages
    "us-de": r"/c\d+/index\.html",  # Chapter pages
    "us-ia": r"/docs/code/\d+",  # Chapter pages
    "us-ky": r"/chapter\d+\.htm",  # Chapter pages
    "us-me": r"/statutes/\d+/",  # Maine title pages
    # === FIXED PATTERNS ===
    # Arizona: ARS section URLs
    "us-az": r"/ars/\d+/[\d\-]+\.htm",
    # California: section display pages (lawCode + sectionNum params)
    "us-ca": r"codes_displaySection\.xhtml\?.*sectionNum=|codes_displayText\.xhtml\?.*lawCode=",
    # Colorado: title/article/section structure
    "us-co": r"/crs\d{4}/.*title.*|/statutes.*article",
    # Illinois: ILCS section display pages
    "us-il": r"ilcs\d+\.asp\?.*Section|ilcs4\.asp\?.*ActID",
    # Arkansas: LexisNexis hosted - chapter level
    "us-ar": r"arcode/.*Default\.asp|arcode/.*\d+",
    # Hawaii: HRS section pages
    "us-hi": r"HRS_\d+-\d+\.htm|hrscurrent/.*\.htm",
    # Indiana: IC title/chapter structure
    "us-in": r"/ic/titles/\d+|/ic/\d+-\d+",
    # Kansas: chapter pages
    "us-ks": r"/statutes/chapters/ch\d+|/statutes/\d+-\d+",
    # Louisiana: law sections
    "us-la": r"Law\.aspx\?d=|Laws.*folder=",
    # Michigan: MCL sections
    "us-mi": r"MCL.*objectId=mcl-|/Laws/MCL/.*\.\d+",
    # Minnesota: chapter/section pages
    "us-mn": r"/statutes/cite/\d+\.\d+|/statutes.*chapter",
    # Mississippi: LexisNexis hosted
    "us-ms": r"mscode/.*Default\.asp|mscode/.*\d+",
    # Montana: MCA title/chapter/section
    "us-mt": r"/bills/mca/title.*section|mca_\d+",
    # Nebraska: statute display
    "us-ne": r"statute=\d+-\d+|/laws/statutes\.php",
    # New Hampshire: RSA chapter pages
    "us-nh": r"/rsa/html/.*\.htm|/rsa/.*\d+-\d+",
    # New Jersey: statute sections
    "us-nj": r"statutes.*\d+[A-Z]?:\d+|gateway\.dll/statutes/\d+",
    # New Mexico: NMSA sections
    "us-nm": r"nmsa.*nav\.do|nmsaid=\d+-\d+",
    # Oklahoma: OSCN document delivery
    "us-ok": r"DeliverDocument\.asp\?.*CiteID|oscn.*\d+",
    # Oregon: ORS chapter pages
    "us-or": r"/ors/ors\d+\.html|/ors\d+",
    # Pennsylvania: consolidated statutes
    "us-pa": r"view-statute\?.*ttl=\d+|/statutes.*sctn=",
    # Rhode Island: title/chapter/section structure
    "us-ri": r"/TITLE\d+/\d+-\d+|Statutes.*\.htm",
    # Connecticut: General Statutes - chapter pages (chap_XXX.htm)
    "us-ct": r"/pub/chap_\d+\.htm|/pub/title_\d+\.htm",
    # South Carolina: code sections
    "us-sc": r"/code/t\d+c\d+|codeoflaw.*section",
    # South Dakota: codified laws
    "us-sd": r"DisplayStatute\.aspx\?.*Statute=|Statute=\d+-\d+",
    # Utah: xcode sections
    "us-ut": r"/xcode/Title\d+/.*\.html|Chapter.*-S\d+",
    # Washington: RCW cite pages
    "us-wa": r"RCW/.*cite=\d+\.\d+|rcw.*\d+\.\d+",
    # Wisconsin: statute sections
    "us-wi": r"/document/statutes/\d+\.\d+|/statutes/\d+",
    # West Virginia: code sections
    "us-wv": r"code\.cfm\?.*chap=\d+.*art=|wvcode.*\d+",
    # === DEFAULT ===
    # Matches common section URL formats when no specific pattern
    "_default": r"(?:section|§|sec|statute)[\-_/]?\d+[\.\d]*",
}


def get_all_jurisdictions() -> list[str]:
    """Get list of all configured state jurisdictions."""
    configs = get_all_configs()
    return sorted([j for j in configs if j.startswith("us-") and j != "us"])


async def crawl_jurisdiction(
    jurisdiction: str,
    output_dir: Path | None = None,
    max_sections: int | None = None,
    concurrency: int = 20,
    delay: float = 0.1,
    dry_run: bool = False,
) -> dict:
    """Crawl a single jurisdiction and optionally save to disk.

    Args:
        jurisdiction: State code (e.g., 'us-ca')
        output_dir: If provided, save HTML files here instead of R2
        max_sections: Limit number of sections to fetch
        concurrency: Max concurrent requests
        delay: Delay between requests
        dry_run: If True, don't upload/save

    Returns:
        Dict with crawl statistics
    """
    configs = get_all_configs()
    if jurisdiction not in configs:
        raise ValueError(f"Unknown jurisdiction: {jurisdiction}")

    config = configs[jurisdiction]

    # Create crawler
    crawler = StateCrawler(config, concurrency, dry_run, delay)

    # Override upload if output_dir specified
    if output_dir and not dry_run:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Monkey-patch upload to save to disk instead
        def save_to_disk(url: str, html: str):
            # Create filename from URL (include query params and fragment)
            parsed = urlparse(url)
            path_parts = parsed.path.strip("/").replace("/", "_")
            # Include query params for sites that use them for section IDs
            # Replace slashes, equals, ampersands with safe chars
            query = (
                parsed.query.replace("/", "_").replace("=", "-").replace("&", "_")
                if parsed.query
                else ""
            )
            fragment = (
                parsed.fragment.replace("/", "_").replace(".", "-") if parsed.fragment else ""
            )

            # Build filename with all components
            parts = [path_parts] if path_parts else ["index"]
            if query:
                parts.append(query)
            if fragment:
                parts.append(fragment)
            filename = "_".join(parts) + ".html"

            # Sanitize: remove invalid filename chars (colons, etc)
            # Colons appear in embedded URLs like "docName=https://..."
            filename = re.sub(r'[:<>"|?*]', "-", filename)
            filename = re.sub(r"https?-__", "", filename)  # Remove "https--" prefix
            filename = re.sub(r"-+", "-", filename)  # Collapse multiple dashes

            # Truncate if too long (filesystem limit)
            if len(filename) > 200:
                url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
                filename = f"{path_parts[:100]}_{url_hash}.html"
                filename = re.sub(r'[:<>"|?*]', "-", filename)

            filepath = output_dir / filename
            filepath.write_text(html, encoding="utf-8")
            crawler.stats.bytes_uploaded += len(html.encode("utf-8"))

        crawler.upload_to_r2 = save_to_disk

    stats = await crawler.crawl(max_sections)

    return {
        "source": "html",
        "sections": stats.sections_fetched,
        "bytes": stats.bytes_fetched,
        "duration": stats.duration,
        "rate": stats.rate,
        "errors": stats.errors_count,
    }


async def download_from_archive_org(
    jurisdiction: str,
    output_dir: Path | None = None,
    dry_run: bool = False,
) -> dict:
    """Download bulk statute data from Archive.org.

    Archive.org hosts bulk downloads of state codes in various formats
    (RTF, HTML, ODT) from Public.Resource.org's collection.

    Returns:
        Dict with download stats
    """
    if jurisdiction not in ARCHIVE_ORG_STATES:
        available = ", ".join(sorted(ARCHIVE_ORG_STATES.keys()))
        raise ValueError(f"No Archive.org data for {jurisdiction}. Available: {available}")

    item_id = ARCHIVE_ORG_STATES[jurisdiction]
    output_dir = output_dir or Path(f"data/archive-org/{jurisdiction}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Archive.org metadata API - lists all files in an item
    metadata_url = f"https://archive.org/metadata/{item_id}"

    stats = {
        "jurisdiction": jurisdiction,
        "item_id": item_id,
        "files_downloaded": 0,
        "bytes_downloaded": 0,
        "errors": [],
    }

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        print(f"[{jurisdiction}] Fetching metadata for {item_id}...")

        try:
            resp = await client.get(metadata_url)
            resp.raise_for_status()
            metadata = resp.json()
        except Exception as e:
            stats["errors"].append(f"Failed to fetch metadata: {e}")
            return stats

        files = metadata.get("files", [])

        # Filter for content files (HTML, RTF, ODT - not metadata/thumbs)
        content_files = [
            f
            for f in files
            if any(
                f.get("name", "").lower().endswith(ext)
                for ext in [".html", ".htm", ".rtf", ".odt", ".xml", ".txt"]
            )
            and not f.get("name", "").startswith("__")
        ]

        print(f"[{jurisdiction}] Found {len(content_files)} content files")

        if dry_run:
            for f in content_files[:10]:
                print(f"  Would download: {f['name']} ({int(f.get('size', 0)) / 1024:.1f} KB)")
            if len(content_files) > 10:
                print(f"  ... and {len(content_files) - 10} more")
            return stats

        # Download files
        for f in content_files:
            fname = f["name"]
            fsize = int(f.get("size", 0))
            download_url = f"https://archive.org/download/{item_id}/{fname}"

            output_file = output_dir / fname
            if output_file.exists() and output_file.stat().st_size == fsize:
                print(f"  Skip (exists): {fname}")
                continue

            try:
                print(f"  Downloading: {fname} ({fsize / 1024:.1f} KB)")
                resp = await client.get(download_url)
                resp.raise_for_status()

                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_bytes(resp.content)

                stats["files_downloaded"] += 1
                stats["bytes_downloaded"] += len(resp.content)

            except Exception as e:
                stats["errors"].append(f"Failed {fname}: {e}")

        print(
            f"[{jurisdiction}] Done: {stats['files_downloaded']} files, "
            f"{stats['bytes_downloaded'] / 1024 / 1024:.1f} MB"
        )

    return stats


async def download_all_archive_org(
    jurisdictions: list[str] | None = None,
    output_dir: Path | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Download bulk data from Archive.org for multiple states."""
    jurisdictions = jurisdictions or list(ARCHIVE_ORG_STATES.keys())

    print(f"Downloading {len(jurisdictions)} states from Archive.org...")

    results = []
    for j in jurisdictions:
        try:
            stats = await download_from_archive_org(j, output_dir, dry_run)
            results.append(stats)
        except Exception as e:
            results.append({"jurisdiction": j, "error": str(e)})

    # Summary
    total_files = sum(r.get("files_downloaded", 0) for r in results)
    total_bytes = sum(r.get("bytes_downloaded", 0) for r in results)
    print("\nArchive.org download complete:")
    print(f"  Total files: {total_files}")
    print(f"  Total size: {total_bytes / 1024 / 1024:.1f} MB")

    return results


@dataclass
class CrawlStats:
    """Stats for a crawl job."""

    jurisdiction: str
    name: str
    codes: int = 0
    sections_discovered: int = 0
    sections_fetched: int = 0
    sections_failed: int = 0
    bytes_fetched: int = 0
    bytes_uploaded: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float = 0
    errors: list = field(default_factory=list)

    @property
    def duration(self) -> float:
        return (self.end_time or time.time()) - self.start_time

    @property
    def rate(self) -> float:
        if self.duration > 0:
            return self.sections_fetched / self.duration
        return 0

    @property
    def errors_count(self) -> int:
        return len(self.errors)


def get_r2_client():
    """Get R2 client using RF credentials file."""
    import json

    import boto3
    from botocore.config import Config

    creds_path = Path.home() / ".config" / "axiom-foundation" / "r2-credentials.json"
    with open(creds_path) as f:
        creds = json.load(f)

    return boto3.client(
        "s3",
        endpoint_url=creds["endpoint_url"],
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


class StateCrawler:
    """Async crawler for a single state."""

    def __init__(
        self,
        config: SourceConfig,
        max_concurrent: int = 20,  # Reduced from 50 to avoid rate limits
        dry_run: bool = False,
        delay_between_requests: float = 0.1,  # 100ms between requests
    ):
        self.config = config
        self.max_concurrent = max_concurrent
        self.dry_run = dry_run
        self.delay = delay_between_requests
        self.stats = CrawlStats(
            jurisdiction=config.jurisdiction,
            name=config.name,
            codes=len(config.codes),
        )
        self._r2 = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request = 0.0

    @property
    def r2(self):
        if self._r2 is None and not self.dry_run:
            self._r2 = get_r2_client()
        return self._r2

    def _get_headers(self) -> dict:
        """Get request headers that work for most sites."""
        return {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

    def _get_section_pattern(self) -> re.Pattern:
        """Get the section URL regex pattern for this state.

        Checks YAML specs first, falls back to hardcoded patterns.
        """
        # Check spec file first
        spec_pattern = get_section_pattern(self.config.jurisdiction)
        if spec_pattern:
            return re.compile(spec_pattern, re.IGNORECASE)

        # Fall back to hardcoded patterns
        pattern = SECTION_PATTERNS.get(self.config.jurisdiction, SECTION_PATTERNS["_default"])
        return re.compile(pattern, re.IGNORECASE)

    def _is_section_url(self, url: str) -> bool:
        """Check if a URL looks like a section page."""
        pattern = self._get_section_pattern()
        return bool(pattern.search(url))

    def _is_same_domain(self, url: str) -> bool:
        """Check if URL is on the same domain as base_url."""
        base_domain = urlparse(self.config.base_url).netloc
        url_domain = urlparse(url).netloc
        return url_domain == base_domain or url_domain == ""

    async def discover_sections(self, client: httpx.AsyncClient, max_depth: int = 3) -> list[str]:
        """Discover all section URLs via recursive BFS crawl.

        Starts from TOC pages and follows links within the domain
        up to max_depth levels, collecting section URLs.
        """
        section_urls: set[str] = set()
        visited: set[str] = set()

        # Start with TOC URLs for each code
        frontier: list[tuple[str, int]] = []  # (url, depth)

        for code_id in self.config.codes:
            if self.config.toc_url_pattern:
                # Handle different placeholder names in patterns
                toc_url = self.config.toc_url_pattern.format(code=code_id, title=code_id)
                if not toc_url.startswith("http"):
                    toc_url = f"{self.config.base_url}{toc_url}"
                frontier.append((toc_url, 0))
            else:
                # Fall back to base URL
                frontier.append((self.config.base_url, 0))

        print(
            f"  [{self.config.jurisdiction}] Starting discovery from {len(frontier)} TOC pages..."
        )

        while frontier:
            # Process current frontier in parallel
            current_batch = frontier[: self.max_concurrent]
            frontier = frontier[self.max_concurrent :]

            tasks = []
            for url, depth in current_batch:
                if url not in visited:
                    visited.add(url)
                    tasks.append(self._crawl_page_for_links(client, url, depth))

            if not tasks:
                continue

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    continue

                page_sections, child_links, depth = result
                section_urls.update(page_sections)

                # Add child links to frontier if within depth limit
                if depth < max_depth:
                    for link in child_links:
                        if link not in visited:
                            frontier.append((link, depth + 1))

            # Progress update
            if len(section_urls) % 100 == 0 and len(section_urls) > 0:
                print(f"  [{self.config.jurisdiction}] Found {len(section_urls)} sections...")

        self.stats.sections_discovered = len(section_urls)
        return list(section_urls)

    async def _crawl_page_for_links(
        self, client: httpx.AsyncClient, url: str, depth: int
    ) -> tuple[set[str], list[str], int]:
        """Crawl a page and extract section URLs and navigation links."""
        section_urls: set[str] = set()
        child_links: list[str] = []

        async with self._semaphore:
            try:
                resp = await client.get(url, timeout=30)
                if resp.status_code != 200:
                    return section_urls, child_links, depth

                soup = BeautifulSoup(resp.text, "html.parser")

                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    full_url = urljoin(url, href)

                    # Skip non-HTTP and external links
                    if not full_url.startswith("http"):
                        continue
                    if not self._is_same_domain(full_url):
                        continue

                    # Clean URL (remove fragments)
                    full_url = full_url.split("#")[0]

                    if self._is_section_url(full_url):
                        section_urls.add(full_url)
                    else:
                        # Add to child links for further crawling
                        # Only include navigation-like links
                        text = link.get_text(strip=True).lower()
                        if any(
                            x in href.lower() or x in text
                            for x in [
                                "title",
                                "chapter",
                                "article",
                                "part",
                                "division",
                                "subtitle",
                                "subchapter",
                                "code",
                                "revised",
                            ]
                        ):
                            child_links.append(full_url)

                return section_urls, child_links, depth

            except Exception as e:
                # Include jurisdiction so multi-state crawls are debuggable
                # when errors surface in the aggregate summary.
                self.stats.errors.append(
                    f"[{self.config.jurisdiction}] Crawl {url}: "
                    f"{type(e).__name__}: {e}"
                )
                return section_urls, child_links, depth

    async def _rate_limit(self):
        """Apply rate limiting between requests."""
        if self.delay > 0:
            now = time.time()
            elapsed = now - self._last_request
            if elapsed < self.delay:
                await asyncio.sleep(self.delay - elapsed)
            self._last_request = time.time()

    async def fetch_section(
        self, client: httpx.AsyncClient, url: str, max_retries: int = 3
    ) -> tuple[str, str | None]:
        """Fetch a single section with retry on rate limit."""
        await self._rate_limit()
        async with self._semaphore:
            for attempt in range(max_retries):
                try:
                    resp = await client.get(url, timeout=30)
                    if resp.status_code == 200:
                        self.stats.sections_fetched += 1
                        self.stats.bytes_fetched += len(resp.content)
                        return url, resp.text
                    elif resp.status_code == 429:
                        # Rate limited - exponential backoff
                        wait_time = (2**attempt) + (attempt * 0.5)
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        self.stats.sections_failed += 1
                        return url, None
                except Exception as e:
                    if attempt == max_retries - 1:
                        self.stats.sections_failed += 1
                        self.stats.errors.append(
                            f"[{self.config.jurisdiction}] Fetch {url}: "
                            f"{type(e).__name__}: {e}"
                        )
                    await asyncio.sleep(1)

            self.stats.sections_failed += 1
            return url, None

    def upload_to_r2(self, url: str, html: str) -> bool:
        """Upload section HTML to R2."""
        if self.dry_run:
            return True

        try:
            # Generate key from URL
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            path = urlparse(url).path.strip("/").replace("/", "_")
            key = f"{self.config.jurisdiction}/{path}_{url_hash}.html"

            self.r2.put_object(
                Bucket=R2_BUCKET,
                Key=key,
                Body=html.encode("utf-8"),
                ContentType="text/html",
            )
            self.stats.bytes_uploaded += len(html.encode("utf-8"))
            return True
        except Exception as e:
            self.stats.errors.append(
                f"[{self.config.jurisdiction}] R2 upload {url}: "
                f"{type(e).__name__}: {e}"
            )
            return False

    async def crawl(self, max_sections: int | None = None) -> CrawlStats:
        """Crawl all sections for this state."""
        async with httpx.AsyncClient(
            headers=self._get_headers(),
            follow_redirects=True,
            timeout=30,
        ) as client:
            # Discover sections
            print(f"  [{self.config.jurisdiction}] Discovering sections...")
            section_urls = await self.discover_sections(client)

            if max_sections:
                section_urls = section_urls[:max_sections]

            if not section_urls:
                # Most common cause: SECTION_PATTERNS entry doesn't match
                # real section URLs for this jurisdiction. Surface the
                # pattern so the failure is actionable in logs.
                pattern_repr = self._get_section_pattern().pattern
                print(
                    f"  [{self.config.jurisdiction}] No sections found — "
                    f"section pattern did not match any discovered links "
                    f"(pattern: {pattern_repr!r}). Check the entry in "
                    f"SECTION_PATTERNS or the YAML spec for this state."
                )
                self.stats.end_time = time.time()
                return self.stats

            print(f"  [{self.config.jurisdiction}] Fetching {len(section_urls)} sections...")

            # Fetch all sections concurrently
            tasks = [self.fetch_section(client, url) for url in section_urls]
            results = await asyncio.gather(*tasks)

            # Upload to R2
            for url, html in results:
                if html:
                    self.upload_to_r2(url, html)

        self.stats.end_time = time.time()
        return self.stats


async def crawl_state(
    jurisdiction: str,
    max_concurrent: int = 20,
    max_sections: int | None = None,
    dry_run: bool = False,
    delay: float = 0.1,
) -> CrawlStats:
    """Crawl a single state."""
    configs = get_all_configs()
    if jurisdiction not in configs:
        raise ValueError(f"Unknown jurisdiction: {jurisdiction}")

    config = configs[jurisdiction]
    crawler = StateCrawler(config, max_concurrent, dry_run, delay)
    return await crawler.crawl(max_sections)


async def crawl_all_states(
    max_concurrent_states: int = 20,
    max_concurrent_per_state: int = 20,
    max_sections_per_state: int | None = None,
    dry_run: bool = False,
    delay: float = 0.1,
) -> list[CrawlStats]:
    """Crawl all states in parallel."""
    configs = get_all_configs()
    jurisdictions = sorted([j for j in configs if j.startswith("us-") and j != "us"])

    print(f"Crawling {len(jurisdictions)} states...")
    print(f"  Concurrent states: {max_concurrent_states}")
    print(f"  Concurrent per state: {max_concurrent_per_state}")
    if max_sections_per_state:
        print(f"  Max sections per state: {max_sections_per_state}")
    print()

    semaphore = asyncio.Semaphore(max_concurrent_states)

    async def crawl_with_semaphore(jurisdiction: str) -> CrawlStats:
        async with semaphore:
            config = configs[jurisdiction]
            crawler = StateCrawler(config, max_concurrent_per_state, dry_run, delay)
            stats = await crawler.crawl(max_sections_per_state)
            print(
                f"  [{jurisdiction}] Done: {stats.sections_fetched} sections "
                f"in {stats.duration:.1f}s ({stats.rate:.1f}/s)"
            )
            return stats

    start = time.time()
    tasks = [crawl_with_semaphore(j) for j in jurisdictions]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Separate results from errors
    stats = []
    errors = []
    for r in results:
        if isinstance(r, CrawlStats):
            stats.append(r)
        else:
            errors.append(r)

    duration = time.time() - start

    # Summary
    print(f"\n{'=' * 60}")
    print("CRAWL COMPLETE")
    print(f"{'=' * 60}")
    print(f"States: {len(stats)} succeeded, {len(errors)} failed")
    print(f"Total duration: {duration:.1f}s ({duration / 60:.1f} minutes)")

    total_sections = sum(s.sections_fetched for s in stats)
    total_bytes = sum(s.bytes_fetched for s in stats)
    print(f"Total sections: {total_sections:,}")
    print(f"Total data: {total_bytes / 1024 / 1024:.1f} MB")
    print(f"Overall rate: {total_sections / duration:.1f} sections/second")

    if errors:
        print("\nErrors:")
        for e in errors[:10]:
            print(f"  {e}")

    return stats


@click.command()
@click.argument("jurisdiction", required=False, nargs=-1)
@click.option("--all", "crawl_all", is_flag=True, help="Crawl all states")
@click.option("--archive", is_flag=True, help="Download from Archive.org bulk data")
@click.option("--max-sections", type=int, help="Limit sections per state")
@click.option("--max-states", type=int, default=20, help="Concurrent states")
@click.option("--max-concurrent", type=int, default=20, help="Concurrent requests per state")
@click.option("--delay", type=float, default=0.1, help="Delay between requests (seconds)")
@click.option("--dry-run", is_flag=True, help="Don't upload to R2")
def main(
    jurisdiction: tuple[str, ...],
    crawl_all: bool,
    archive: bool,
    max_sections: int | None,
    max_states: int,
    max_concurrent: int,
    delay: float,
    dry_run: bool,
):
    """Crawl state statutes and upload to R2.

    Examples:

        # Test single state
        uv run python -m arch.crawl us-oh --max-sections 100 --dry-run

        # Crawl all states
        uv run python -m arch.crawl --all

        # Download from Archive.org (specific states)
        uv run python -m arch.crawl --archive us-ar us-co us-ga

        # Download all Archive.org states
        uv run python -m arch.crawl --archive --all
    """
    # Archive.org mode
    if archive:
        if crawl_all:
            # Download all states that have Archive.org data
            asyncio.run(download_all_archive_org(dry_run=dry_run))
        elif jurisdiction:
            # Download specific states
            asyncio.run(download_all_archive_org(list(jurisdiction), dry_run=dry_run))
        else:
            # List available states
            click.echo("Archive.org bulk data available for:")
            for j in sorted(ARCHIVE_ORG_STATES.keys()):
                click.echo(f"  {j}")
            click.echo("\nUsage: uv run python -m arch.crawl --archive us-ar us-co")
        return

    # Web crawler mode
    if crawl_all:
        asyncio.run(
            crawl_all_states(
                max_concurrent_states=max_states,
                max_concurrent_per_state=max_concurrent,
                max_sections_per_state=max_sections,
                dry_run=dry_run,
                delay=delay,
            )
        )
    elif jurisdiction and len(jurisdiction) == 1:
        stats = asyncio.run(
            crawl_state(
                jurisdiction[0],
                max_concurrent=max_concurrent,
                max_sections=max_sections,
                dry_run=dry_run,
                delay=delay,
            )
        )
        print(f"\n{stats.name}:")
        print(f"  Sections discovered: {stats.sections_discovered}")
        print(f"  Sections fetched: {stats.sections_fetched}")
        print(f"  Sections failed: {stats.sections_failed}")
        print(f"  Data fetched: {stats.bytes_fetched / 1024:.1f} KB")
        print(f"  Duration: {stats.duration:.1f}s")
        print(f"  Rate: {stats.rate:.1f} sections/second")
        if stats.errors:
            print(f"  Errors: {len(stats.errors)}")
            for e in stats.errors[:5]:
                print(f"    {e}")
    elif jurisdiction:
        # Multiple jurisdictions - crawl each
        for j in jurisdiction:
            stats = asyncio.run(
                crawl_state(
                    j,
                    max_concurrent=max_concurrent,
                    max_sections=max_sections,
                    dry_run=dry_run,
                    delay=delay,
                )
            )
            print(f"\n{stats.name}: {stats.sections_fetched} sections in {stats.duration:.1f}s")
    else:
        click.echo("Specify a jurisdiction or use --all")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
