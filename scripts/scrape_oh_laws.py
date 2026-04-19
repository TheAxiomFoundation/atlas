"""Scrape the Ohio Revised Code from codes.ohio.gov.

Source layout
-------------
Ohio serves the Revised Code as static HTML (not a SPA). The root at
``https://codes.ohio.gov/ohio-revised-code`` links to 32 top-level titles
(e.g. ``title-1``, ``title-3``, ...) plus one ``general-provisions`` bucket.
Each title/bucket page links to chapter pages
(``/ohio-revised-code/chapter-{N}``), and each chapter page links to
individual section pages (``/ohio-revised-code/section-{N}.{N}``).

Each section page contains:

- ``<h1>Section {number} <span class='codes-separator'>|</span> {heading}.</h1>``
- ``<section class="laws-body"><span><p>...</p><p>...</p></span></section>``

Missing / repealed sections still return HTTP 200 but omit
``<section class="laws-body">``; we treat those as skips.

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state oh`` expects.

Usage
-----
::

    uv run python scripts/scrape_oh_laws.py --out /tmp/rules-us-oh
    uv run python scripts/scrape_oh_laws.py --out /tmp/rules-us-oh --chapters 1,5747
"""

from __future__ import annotations

import argparse
import html as _html
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

BASE = "https://codes.ohio.gov"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> tuple[int, str]:
    """GET a URL. Return (status, body). Treat 404 as (404, '')."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return (resp.status, resp.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return (404, "")
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|li)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# Each title page links to its chapters; general-provisions is the bucket
# for low-numbered chapters (1, 3, 5, 7, 9).
_TITLE_LINK = re.compile(
    r'href="/?ohio-revised-code/(title-[0-9]+|general-provisions)"',
)
_CHAPTER_LINK = re.compile(
    r'href="(?:/?ohio-revised-code/)?chapter-(?P<chapter>[0-9A-Za-z.-]+?)"',
)
_SECTION_LINK = re.compile(
    r'href="(?:/?ohio-revised-code/)?section-(?P<section>[0-9A-Za-z.-]+?)"',
)

# Section page extractors.
_H1_RE = re.compile(
    r"<h1>\s*Section\s+(?P<num>[0-9A-Za-z.-]+)\s*"
    r"<span[^>]*class=['\"]codes-separator['\"][^>]*>\|</span>\s*"
    r"(?P<heading>.*?)</h1>",
    re.DOTALL,
)
_BODY_RE = re.compile(
    r'<section\s+class="laws-body">(?P<body>.*?)</section>',
    re.DOTALL,
)


def list_titles() -> list[str]:
    """Return title slugs (``title-1``, ``general-provisions``, ...)."""
    _, html = _http_get(f"{BASE}/ohio-revised-code")
    seen = []
    out = []
    for m in _TITLE_LINK.finditer(html):
        slug = m.group(1)
        if slug in seen:
            continue
        seen.append(slug)
        out.append(slug)
    return out


def list_chapters_in_title(title_slug: str) -> list[str]:
    """Return chapter tokens (``1``, ``5747``, ``1301``) for a title."""
    _, html = _http_get(f"{BASE}/ohio-revised-code/{title_slug}")
    seen = set()
    chapters: list[str] = []
    for m in _CHAPTER_LINK.finditer(html):
        ch = m.group("chapter")
        if ch in seen:
            continue
        seen.add(ch)
        chapters.append(ch)
    return chapters


def list_sections_in_chapter(chapter: str) -> list[str]:
    """Return section numbers (``1.01``, ``5747.01``) for a chapter."""
    status, html = _http_get(f"{BASE}/ohio-revised-code/chapter-{chapter}")
    if status != 200 or not html:
        return []
    seen = set()
    sections: list[str] = []
    for m in _SECTION_LINK.finditer(html):
        sec = m.group("section")
        # Chapter page link target is always the section for that chapter;
        # chapter=1 -> section 1.01 etc. Keep only sections whose chapter
        # prefix matches (guards against cross-chapter reference links).
        prefix = sec.split(".")[0] if "." in sec else sec
        if prefix != chapter:
            continue
        if sec in seen:
            continue
        seen.add(sec)
        sections.append(sec)
    return sections


def parse_section_page(html: str) -> tuple[str, str] | None:
    """Return (heading, body) or None if the page has no statute body."""
    body_m = _BODY_RE.search(html)
    if not body_m:
        return None
    head_m = _H1_RE.search(html)
    if head_m:
        heading = _clean_text(head_m.group("heading"))
        heading = heading.rstrip(".").strip()
    else:
        heading = ""
    body = _clean_text(body_m.group("body"))
    # Drop the trailing "Last updated ..." notice if present.
    body = re.sub(r"Last updated [^\n]+$", "", body).strip()
    return (heading, body)


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"R.C. \u00a7 {section}"
    safe_section = section.replace(".", "_").replace("-", "_")
    eid = f"sec_{chapter}_{safe_section}"
    paras = [p for p in re.split(r"\n\n+|(?<=\n)", body) if p.strip()]
    if not paras:
        paras = [body] if body.strip() else []
    paras_xml = "\n            ".join(
        f"<p>{xml_escape(p.strip())}</p>" for p in paras
    ) or "<p/>"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="{AKN_NS}">
  <act name="section">
    <meta>
      <identification source="#axiom">
        <FRBRWork>
          <FRBRthis value="/akn/us-oh/act/rc/{section}"/>
          <FRBRuri value="/akn/us-oh/act/rc/{section}"/>
          <FRBRauthor href="#oh-lsc"/>
          <FRBRcountry value="us-oh"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="RC"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-oh/act/rc/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-oh/act/rc/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-oh/act/rc/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-oh/act/rc/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="oh-lsc" href="https://codes.ohio.gov" showAs="Ohio Legislative Service Commission"/>
        <TLCOrganization eId="axiom" href="https://axiom-foundation.org" showAs="Axiom Foundation"/>
      </references>
    </meta>
    <body>
      <section eId="{eid}">
        <num>{xml_escape(citation)}</num>
        <heading>{xml_escape(heading or f"Section {section}")}</heading>
        <content>
            {paras_xml}
        </content>
      </section>
    </body>
  </act>
</akomaNtoso>
"""


def scrape_section(
    chapter: str, section: str, out_root: Path
) -> tuple[bool, bool]:
    """Scrape one section. Return (ok, skipped)."""
    url = f"{BASE}/ohio-revised-code/section-{section}"
    try:
        status, html = _http_get(url)
    except RuntimeError as exc:
        print(f"  WARN sec {section}: {exc}", file=sys.stderr)
        return (False, True)
    if status == 404 or not html:
        return (False, True)
    parsed = parse_section_page(html)
    if parsed is None:
        return (False, True)
    heading, body = parsed
    if not body:
        return (False, True)
    xml = build_akn_xml(chapter, section, heading, body)
    safe_section = section.replace("/", "_")
    dest = (
        out_root
        / "statutes"
        / f"ch-{chapter}"
        / f"ch-{chapter}-sec-{safe_section}.xml"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(xml, encoding="utf-8")
    return (True, False)


def scrape_chapter(
    chapter: str, out_root: Path
) -> tuple[int, int, str]:
    """Scrape every section in a chapter. Return (ok, skipped, chapter)."""
    try:
        sections = list_sections_in_chapter(chapter)
    except RuntimeError as exc:
        print(f"  WARN ch-{chapter}: {exc}", file=sys.stderr)
        return (0, 0, chapter)
    ok = 0
    skipped = 0
    for section in sections:
        got, skip = scrape_section(chapter, section, out_root)
        if got:
            ok += 1
        elif skip:
            skipped += 1
    return (ok, skipped, chapter)


def discover_all_chapters() -> list[str]:
    """Crawl the root + each title to collect every chapter token."""
    titles = list_titles()
    chapters: list[str] = []
    seen: set[str] = set()
    for t in titles:
        try:
            for ch in list_chapters_in_title(t):
                if ch not in seen:
                    seen.add(ch)
                    chapters.append(ch)
        except RuntimeError as exc:
            print(f"  WARN title {t}: {exc}", file=sys.stderr)
    # Sort numerically when possible.
    def key(c: str) -> tuple[int, str]:
        m = re.match(r"^(\d+)", c)
        return (int(m.group(1)) if m else 10**9, c)
    chapters.sort(key=key)
    return chapters


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-oh"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '1,5747').",
    )
    parser.add_argument("--workers", type=int, default=6,
                        help="Parallel chapter fetches.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N chapters.")
    args = parser.parse_args(argv)

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)

    if args.chapters:
        chapters = [c.strip() for c in args.chapters.split(",") if c.strip()]
    else:
        chapters = discover_all_chapters()

    if args.limit:
        chapters = chapters[: args.limit]
    print(f"Scraping {len(chapters)} chapters", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(scrape_chapter, c, args.out): c for c in chapters}
        for fut in as_completed(futures):
            ok, skipped, ch = fut.result()
            total_ok += ok
            total_skipped += skipped
            if ok + skipped > 0:
                elapsed = (time.time() - started) / 60
                print(
                    f"  ch-{ch}: {ok} ok, {skipped} skip  "
                    f"(running: {total_ok} ok / {total_skipped} skip, "
                    f"{elapsed:.1f} min)",
                    flush=True,
                )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE \u2014 {total_ok} sections scraped, "
        f"{total_skipped} skipped, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
