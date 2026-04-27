"""Scrape Illinois Compiled Statutes from ilga.gov's bulk FTP tree.

Output
------
AKN-3.0 XML files into ``{out}/statutes/ch-{chapter}/ch-{chapter}-act-{act}-{section}.xml``
shaped so ``ingest_state_laws.py --state il --repo-dir {out}`` can walk
them without any IL-specific logic.

ILCS organization
-----------------
Citations in Illinois take the form ``(35 ILCS 155/2)``:

* **35** — chapter (topic group, e.g. 35 = Revenue)
* **155** — act within that chapter
* **2**  — section within that act

The FTP layout mirrors this as::

    /ftp/ILCS/Ch {chapter:04d}/Act {act:04d}/{chapter:04d}{act:04d}0K{section}.html

Each section is one tidy HTML file with a table of ``<code><font>`` lines;
the first line carries the ILCS citation, the second is
``Sec. {n}. {heading}.``, and the rest are body paragraphs joined by ``<br>``.

Usage
-----
::

    uv run python scripts/scrape_il_laws.py --out /tmp/rules-us-il
    uv run python scripts/scrape_il_laws.py --out /tmp/rules-us-il --chapters 35,215
    uv run python scripts/scrape_il_laws.py --out /tmp/rules-us-il --limit 10
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

BASE = "https://www.ilga.gov/ftp/ILCS"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


# --- Fetch helpers --------------------------------------------------------


def _http_get(url: str, retries: int = 3) -> str:
    """GET a URL as text with basic retries on transient upstream errors."""
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


def _list_ftp_dir(url: str) -> list[tuple[str, str, bool]]:
    """Return ``(href, label, is_dir)`` tuples for children of an IIS dir listing."""
    html = _http_get(url)
    out: list[tuple[str, str, bool]] = []
    for m in re.finditer(r'<A HREF="([^"]+)">([^<]+)</A>', html):
        href, label = m.group(1), m.group(2)
        if label.startswith("["):  # "[To Parent Directory]"
            continue
        is_dir = href.endswith("/")
        out.append((href, label, is_dir))
    return out


# --- Parsing --------------------------------------------------------------


# Matches the header line: "(35 ILCS 155/2)" or "(35 ILCS 155/Art. 2)" etc.
_ILCS_RE = re.compile(
    r"\(\s*(?P<chapter>\d+)\s+ILCS\s+(?P<act>\d+(?:[-.]\d+)?)\s*/\s*(?P<section>[^)\s]+)\s*\)"
)

# "Sec. 2." or "Sec. 2-5." — section identifier at start of body.
_SEC_RE = re.compile(r"Sec\.\s*(?P<section>[\w.\-]+?)\s*\.")


def _clean_text(s: str) -> str:
    """Strip HTML tags and normalize whitespace in a raw fragment."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|code)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    # Collapse runs of spaces/tabs; preserve newlines for paragraph shaping.
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


def parse_section_html(html: str) -> tuple[str, str, str, str] | None:
    """Return ``(chapter, act, section, heading, body)`` or None on parse fail."""
    m = _ILCS_RE.search(html)
    if not m:
        return None
    chapter = m.group("chapter")
    act = m.group("act")
    section = m.group("section")
    # Everything after the (Ch. X, par. Y) parenthetical is the section body.
    # Find "Sec. X." — the first such occurrence is the section's heading line.
    after_header = html[m.end() :]
    sec_m = _SEC_RE.search(after_header)
    heading = ""
    body_start = after_header
    if sec_m:
        # Heading text = the text immediately after "Sec. X." until a <br>.
        tail = after_header[sec_m.end() :]
        br = re.search(r"<br", tail, re.IGNORECASE)
        heading_raw = tail[: br.start()] if br else tail[:200]
        heading = _clean_text(heading_raw).rstrip(".").strip()
        body_start = tail[br.end() :] if br else tail

    # Body: everything from there, stripped; trim trailing "(Source: ..." marker
    body = _clean_text(body_start)
    body = re.sub(r"\(\s*Source:[^)]*\)\s*$", "", body).strip()
    return chapter, act, section, heading, body


# --- XML emission ---------------------------------------------------------


def build_akn_xml(
    chapter: str, act: str, section: str, heading: str, body: str
) -> str:
    """Emit an AKN-3.0 document matching the existing NY/CA file shape."""
    citation = f"{chapter} ILCS {act}/{section}"
    eid = f"sec_{chapter}_{act}_{section}".replace(".", "_").replace("-", "_")
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
          <FRBRthis value="/akn/us-il/act/ilcs/{chapter}-{act}-{section}"/>
          <FRBRuri value="/akn/us-il/act/ilcs/{chapter}-{act}-{section}"/>
          <FRBRauthor href="#il-legislature"/>
          <FRBRcountry value="us-il"/>
          <FRBRnumber value="{chapter}-{act}-{section}"/>
          <FRBRname value="ILCS"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-il/act/ilcs/{chapter}-{act}-{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-il/act/ilcs/{chapter}-{act}-{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-il/act/ilcs/{chapter}-{act}-{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-il/act/ilcs/{chapter}-{act}-{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="il-legislature" href="https://www.ilga.gov" showAs="Illinois General Assembly"/>
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


# --- Walker ---------------------------------------------------------------


def iter_chapters() -> list[tuple[int, str]]:
    """Return ``(chapter_int, href)`` for every chapter directory."""
    out: list[tuple[int, str]] = []
    for href, label, is_dir in _list_ftp_dir(f"{BASE}/"):
        if not is_dir:
            continue
        m = re.match(r"Ch\s+(\d+)", label)
        if m:
            out.append((int(m.group(1)), href))
    return sorted(out)


def iter_acts(chapter_href: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    for href, label, is_dir in _list_ftp_dir(
        f"https://www.ilga.gov{chapter_href}"
    ):
        if not is_dir:
            continue
        m = re.match(r"Act\s+(\d+)", label)
        if m:
            out.append((int(m.group(1)), href))
    return sorted(out)


def iter_sections(act_href: str) -> list[tuple[str, str]]:
    """Return ``(section_number, file_url)`` for each section HTML file."""
    out: list[tuple[str, str]] = []
    for href, label, is_dir in _list_ftp_dir(
        f"https://www.ilga.gov{act_href}"
    ):
        if is_dir:
            continue
        m = re.match(r"^\d{9}K(?P<sec>[\w.\-]+)\.html$", label)
        if m:
            out.append(
                (m.group("sec"), f"https://www.ilga.gov{href}")
            )
    return sorted(out)


# --- Main -----------------------------------------------------------------


def scrape_section(url: str, out_root: Path) -> tuple[bool, str]:
    """Fetch one section file, parse, write AKN XML. Return (ok, message)."""
    try:
        html = _http_get(url)
    except RuntimeError as exc:
        return (False, f"fetch: {exc}")

    parsed = parse_section_html(html)
    if parsed is None:
        return (False, "no ILCS header found")
    chapter, act, section, heading, body = parsed

    # Skip empty bodies — usually "repealed" section markers without content.
    if not body:
        return (False, f"empty body ({chapter} ILCS {act}/{section})")

    xml = build_akn_xml(chapter, act, section, heading, body)
    dest = (
        out_root
        / "statutes"
        / f"ch-{chapter}"
        / f"ch-{chapter}-act-{act}-sec-{section}.xml"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(xml, encoding="utf-8")
    return (True, f"{chapter} ILCS {act}/{section}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--out", type=Path, default=Path("/tmp/rules-us-il"),
        help="Output root; writes {out}/statutes/ch-*/*.xml",
    )
    parser.add_argument(
        "--chapters", default="",
        help="Comma-separated chapter numbers to scrape (default all).",
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N sections (for testing).")
    parser.add_argument("--workers", type=int, default=8,
                        help="Parallel section fetches (default 8).")
    args = parser.parse_args(argv)

    chapter_filter: set[int] = set()
    if args.chapters:
        chapter_filter = {int(c) for c in args.chapters.split(",")}

    started = time.time()
    ok = 0
    failed = 0
    seen = 0
    args.out.mkdir(parents=True, exist_ok=True)

    chapters = iter_chapters()
    if chapter_filter:
        chapters = [c for c in chapters if c[0] in chapter_filter]
    print(f"Scraping {len(chapters)} chapter(s)", flush=True)

    for chapter_num, chapter_href in chapters:
        print(f"\n--- Chapter {chapter_num} ---", flush=True)
        try:
            acts = iter_acts(chapter_href)
        except RuntimeError as exc:
            print(f"  WARN: chapter list failed: {exc}", file=sys.stderr)
            continue
        for act_num, act_href in acts:
            try:
                sections = iter_sections(act_href)
            except RuntimeError as exc:
                print(f"  WARN: act list failed: {exc}", file=sys.stderr)
                continue
            if not sections:
                continue
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                futures = {
                    ex.submit(scrape_section, url, args.out): sec
                    for sec, url in sections
                }
                for fut in as_completed(futures):
                    seen += 1
                    ok_flag, msg = fut.result()
                    if ok_flag:
                        ok += 1
                    else:
                        failed += 1
                        if failed <= 20:
                            print(f"  skip {futures[fut]}: {msg}",
                                  file=sys.stderr)
                    if args.limit and seen >= args.limit:
                        break
            if args.limit and seen >= args.limit:
                break
            elapsed = time.time() - started
            print(
                f"  Ch {chapter_num} Act {act_num}: {ok} ok, {failed} skip, "
                f"{elapsed/60:.1f} min",
                flush=True,
            )
        if args.limit and seen >= args.limit:
            break

    elapsed = time.time() - started
    print(
        f"\nDONE — {ok} sections scraped, {failed} skipped, "
        f"{elapsed/60:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
