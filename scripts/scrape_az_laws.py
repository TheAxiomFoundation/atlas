"""Scrape the Arizona Revised Statutes from azleg.gov.

Source layout
-------------
Each title's TOC lives at
``https://www.azleg.gov/arsDetail?title={title}``, which links to
section pages via a /viewdocument wrapper — the actual files are
``https://www.azleg.gov/ars/{title}/{file}.htm``. The filename is the
zero-padded section number within the title. Each section page is a
tiny static HTML doc with exactly one ``<p>`` header line
(``<font color=GREEN>1-101</font>. <font color=PURPLE><u>Heading</u></font>``)
followed by body paragraphs.

Output
------
AKN-3.0 XML at ``{out}/statutes/ch-{title}/ch-{title}-sec-{section}.xml``.

Usage
-----
::

    uv run python scripts/scrape_az_laws.py --out /tmp/rules-us-az
    uv run python scripts/scrape_az_laws.py --out /tmp/rules-us-az --titles 36,43
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

BASE = "https://www.azleg.gov"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> str | None:
    """GET a URL; returns None on 404 so missing titles/sections skip."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(min(8.0, 2.0**attempt))
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def list_section_urls_for_title(title: int) -> list[str]:
    """Return section page URLs for a given AZ title."""
    html = _http_get(f"{BASE}/arsDetail?title={title}")
    if html is None:
        return []
    # Links look like /viewdocument/?docName=https://www.azleg.gov/ars/1/00101.htm
    # but also appear directly in some pages. Normalize to canonical form.
    urls = set()
    for m in re.finditer(
        r'href="(?:[^"]*?docName=)?(https?://www\.azleg\.gov/ars/(\d+)/(\d+)\.htm)"',
        html,
    ):
        url, t, _sec = m.group(1), m.group(2), m.group(3)
        if int(t) == title:
            urls.add(url)
    return sorted(urls)


def _clean_text(s: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


_HEADER_RE = re.compile(
    r'<font\s+color\s*=\s*"?GREEN"?\s*>\s*(?P<section>[\w.\-]+)\s*</font>\s*\.?\s*'
    r'<font\s+color\s*=\s*"?PURPLE"?\s*>\s*<u>\s*(?P<heading>.*?)\s*</u>\s*</font>',
    re.DOTALL | re.IGNORECASE,
)


def parse_section(html: str) -> tuple[str, str, str] | None:
    """Extract ``(section, heading, body)`` from one section page.

    Returns None if the header line can't be parsed (e.g. repealed
    sections with empty bodies that AZ publishes as placeholder pages).
    """
    m = _HEADER_RE.search(html)
    if not m:
        return None
    section = m.group("section").strip()
    heading = _clean_text(m.group("heading"))
    body_start = m.end()
    # Body is the rest until </BODY>.
    body_html = html[body_start:]
    body_html = re.sub(r"</body>.*", "", body_html, flags=re.IGNORECASE | re.DOTALL)
    body = _clean_text(body_html)
    # Trim lone periods / pure-whitespace openers.
    body = body.lstrip(".").strip()
    return section, heading, body


def build_akn_xml(
    title: str, section: str, heading: str, body: str
) -> str:
    citation = f"A.R.S. § {section}"
    safe_section = section.replace(".", "_").replace("-", "_")
    eid = f"sec_{title}_{safe_section}"
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
          <FRBRthis value="/akn/us-az/act/ars/{section}"/>
          <FRBRuri value="/akn/us-az/act/ars/{section}"/>
          <FRBRauthor href="#az-legislature"/>
          <FRBRcountry value="us-az"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="ARS"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-az/act/ars/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-az/act/ars/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-az/act/ars/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-az/act/ars/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="az-legislature" href="https://www.azleg.gov" showAs="Arizona State Legislature"/>
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


def scrape_section(url: str, title: int, out_root: Path) -> tuple[bool, str]:
    html = _http_get(url)
    if html is None:
        return (False, "404")
    parsed = parse_section(html)
    if parsed is None:
        return (False, "no header")
    section, heading, body = parsed
    if not body:
        return (False, "empty body")
    safe = section.replace("/", "_")
    dest = (
        out_root
        / "statutes"
        / f"ch-{title}"
        / f"ch-{title}-sec-{safe}.xml"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        build_akn_xml(str(title), section, heading, body), encoding="utf-8"
    )
    return (True, section)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-az"))
    parser.add_argument(
        "--titles",
        default="",
        help="Comma-separated title numbers (default: scan 1-49).",
    )
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args(argv)

    title_filter: list[int]
    if args.titles:
        title_filter = [int(t.strip()) for t in args.titles.split(",") if t.strip()]
    else:
        # AZ titles run 1-49 with a couple of gaps (2, 37 missing per the TOC).
        title_filter = list(range(1, 50))

    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    total_ok = 0
    total_failed = 0

    for title in title_filter:
        urls = list_section_urls_for_title(title)
        if not urls:
            print(f"  title {title}: 0 sections (skipping)", flush=True)
            continue
        ok = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(scrape_section, u, title, args.out): u for u in urls
            }
            for fut in as_completed(futures):
                ok_flag, _msg = fut.result()
                if ok_flag:
                    ok += 1
                else:
                    failed += 1
        total_ok += ok
        total_failed += failed
        elapsed = (time.time() - started) / 60
        print(
            f"  title {title}: {ok} ok, {failed} skip  "
            f"(running: {total_ok} ok, {elapsed:.1f} min)",
            flush=True,
        )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE — {total_ok} sections scraped, "
        f"{total_failed} skipped, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
