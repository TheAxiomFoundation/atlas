"""Scrape the North Carolina General Statutes from ncleg.gov.

Source layout
-------------
Each chapter is served as one HTML file at
``https://www.ncleg.gov/EnactedLegislation/Statutes/HTML/ByChapter/Chapter_{N}.html``,
where ``{N}`` is 1, 1A, 2, ... . A chapter page contains all sections as
``<p><span>&sect; N-N. &nbsp;Heading.</span></p>`` markers followed by
body paragraphs. Section headers can be distinguished from inline
``&sect;`` references because they are the first child of a ``<p>``.

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{N}/ch-{N}-sec-{section}.xml``.

Usage
-----
::

    uv run python scripts/scrape_nc_laws.py --out /tmp/rules-us-nc
    uv run python scripts/scrape_nc_laws.py --out /tmp/rules-us-nc --chapters 1,105
"""

from __future__ import annotations

import argparse
import html as _html
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

BASE = "https://www.ncleg.gov/EnactedLegislation/Statutes/HTML/ByChapter"
TOC_URL = "https://www.ncleg.gov/Laws/GeneralStatutesTOC"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def list_chapter_tokens() -> list[str]:
    """Return chapter identifiers (e.g. '1', '1A', '105') from the TOC."""
    html = _http_get(TOC_URL)
    # Links look like href="/EnactedLegislation/Statutes/HTML/ByChapter/Chapter_1.html"
    tokens = set(
        re.findall(
            r"/EnactedLegislation/Statutes/HTML/ByChapter/Chapter_([A-Za-z0-9]+)\.html",
            html,
        )
    )
    # Sort naturally: numeric part then alpha suffix.
    def key(t: str) -> tuple[int, str]:
        m = re.match(r"(\d+)([A-Za-z]*)", t)
        return (int(m.group(1)), m.group(2)) if m else (999999, t)

    return sorted(tokens, key=key)


def _clean_text(s: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# Section header pattern: a <p> whose first <span> starts with "§ N-N. Heading."
# Using the raw HTML entity `&sect;` because NC emits that rather than §.
# Section numbers can include dots (1-339.1) and colons, so the section
# capture is greedy until a trailing "." followed by whitespace — that's
# the delimiter between section id and heading.
_HEADER_RE = re.compile(
    r'<p[^>]*>\s*<span[^>]*>&sect;\s+'
    r'(?P<section>[0-9A-Za-z][0-9A-Za-z.\-:]*?)\.\s+'
    r'(?:&nbsp;\s*)*(?P<heading>.*?)</span>',
    re.DOTALL | re.IGNORECASE,
)


def split_sections(html: str, chapter_token: str) -> list[tuple[str, str, str]]:
    """Return ``(section_number, heading, body)`` for each section."""
    starts = list(_HEADER_RE.finditer(html))
    out: list[tuple[str, str, str]] = []
    for i, m in enumerate(starts):
        section = m.group("section")
        heading = _clean_text(m.group("heading")).rstrip(".")
        body_start = m.end()
        body_end = starts[i + 1].start() if i + 1 < len(starts) else len(html)
        slab = html[body_start:body_end]
        # Strip trailing chapter artifacts like the next section's <p> opener.
        # Also drop any "Editor's Note" or cite-history paragraphs — NC uses
        # italic-wrapped parentheticals at the end of each section.
        slab = re.sub(
            r"<p[^>]*>\s*<span[^>]*>\s*\(.*?\)\s*</span>\s*</p>\s*$",
            "",
            slab,
            flags=re.DOTALL,
        )
        body = _clean_text(slab)
        out.append((section, heading, body))
    return out


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"G.S. {section}"
    safe_section = section.replace(".", "_").replace("-", "_")
    eid = f"sec_{chapter}_{safe_section}"
    paras = [p for p in re.split(r"\n\n+", body) if p.strip()]
    paras_xml = "\n            ".join(
        f"<p>{xml_escape(p)}</p>" for p in paras
    ) or "<p/>"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="{AKN_NS}">
  <act name="section">
    <meta>
      <identification source="#axiom">
        <FRBRWork>
          <FRBRthis value="/akn/us-nc/act/gs/{section}"/>
          <FRBRuri value="/akn/us-nc/act/gs/{section}"/>
          <FRBRdate date="2026-01-01" name="enacted"/>
          <FRBRauthor href="#nc-legislature"/>
          <FRBRcountry value="us-nc"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="GS"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-nc/act/gs/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-nc/act/gs/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-nc/act/gs/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-nc/act/gs/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="nc-legislature" href="https://www.ncleg.gov" showAs="North Carolina General Assembly"/>
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


def scrape_chapter(
    chapter_token: str, out_root: Path
) -> tuple[int, int, str]:
    url = f"{BASE}/Chapter_{chapter_token}.html"
    try:
        html = _http_get(url)
    except RuntimeError as exc:
        print(f"  WARN ch-{chapter_token}: {exc}", file=sys.stderr)
        return (0, 0, chapter_token)

    sections = split_sections(html, chapter_token)
    ok = 0
    skipped = 0
    for section, heading, body in sections:
        if not body:
            skipped += 1
            continue
        safe = section.replace("/", "_")
        dest = (
            out_root
            / "statutes"
            / f"ch-{chapter_token}"
            / f"ch-{chapter_token}-sec-{safe}.xml"
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(
            build_akn_xml(chapter_token, section, heading, body),
            encoding="utf-8",
        )
        ok += 1
    return (ok, skipped, chapter_token)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-nc"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '1,1A,105').",
    )
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args(argv)

    chapter_filter: set[str] | None = None
    if args.chapters:
        chapter_filter = {c.strip() for c in args.chapters.split(",") if c.strip()}

    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    chapters = list_chapter_tokens()
    if chapter_filter:
        chapters = [c for c in chapters if c in chapter_filter]
    print(f"Scraping {len(chapters)} chapter(s)", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(scrape_chapter, c, args.out): c for c in chapters}
        for fut in as_completed(futures):
            ok, skipped, ch = fut.result()
            total_ok += ok
            total_skipped += skipped
            elapsed = (time.time() - started) / 60
            print(
                f"  ch-{ch}: {ok} ok, {skipped} skip  "
                f"(running: {total_ok} ok, {elapsed:.1f} min)",
                flush=True,
            )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE — {total_ok} sections scraped, "
        f"{total_skipped} skipped, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
