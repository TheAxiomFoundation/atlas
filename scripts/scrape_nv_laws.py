"""Scrape the Nevada Revised Statutes from leg.state.nv.us.

Source layout
-------------
The NRS is served one page per chapter at
``https://www.leg.state.nv.us/NRS/NRS-{chapter}.html`` (e.g. ``NRS-244.html``).
Each page is a Word-exported HTML bundle where each section starts with::

    <p class="SectBody">
      <span class="Empty">      <a name=NRS244Sec010></a>NRS </span>
      <span class="Section">244.010</span>
      <span class="Empty">  </span>
      <span class="Leadline">Minimum number of county commissioners.</span>
      <span class="Empty">  </span>
      {body paragraphs...}
    </p>
    ...
    <p class="SourceLine">(Added to NRS by 1971, ...)</p>

Output
------
AKN-3.0 XML files at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state nv`` expects.

Usage
-----
::

    uv run python scripts/scrape_nv_laws.py --out /tmp/rules-us-nv
    uv run python scripts/scrape_nv_laws.py --out /tmp/rules-us-nv --chapters 244
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

BASE = "https://www.leg.state.nv.us/NRS"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 3) -> str:
    """GET a URL decoded as cp1252 (NV pages are Windows-1252)."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("cp1252", errors="replace")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(8.0, 2.0**attempt))
                continue
    raise RuntimeError(f"failed to fetch {url}: {last_exc}")


def list_chapter_files() -> list[str]:
    """Return the list of ``NRS-*.html`` filenames from the index page."""
    html = _http_get(f"{BASE}/")
    return sorted(set(re.findall(r'href="(NRS-[^"]+\.html)"', html)))


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# Each section's body is bracketed: starts at <a name=NRS{chapter}Sec{sec}>,
# ends at the next such anchor or at the chapter's sentinel </body>.
_SECTION_ANCHOR = re.compile(
    r"<a\s+name=NRS(?P<chapter>[0-9A-Z]+)Sec(?P<section>[0-9A-Za-z]+)>\s*</a>"
)


def split_sections(html: str) -> list[tuple[str, str, str, str]]:
    """Return ``(chapter, section, heading, body)`` for each section in page."""
    anchors = list(_SECTION_ANCHOR.finditer(html))
    sections: list[tuple[str, str, str, str]] = []
    for i, m in enumerate(anchors):
        chapter = m.group("chapter")
        section = m.group("section")
        # Map NRS244Sec010 → section number "244.010".
        # We take the text between this anchor and the next anchor (or EOF).
        start = m.end()
        end = anchors[i + 1].start() if i + 1 < len(anchors) else len(html)
        slab = html[start:end]

        # Section number is inside <span class="Section">...</span> near start.
        sec_m = re.search(
            r'<span\s+class="Section"[^>]*>\s*([^<]+?)\s*</span>', slab
        )
        section_num = _clean_text(sec_m.group(1)) if sec_m else section

        # Heading: first <span class="Leadline">...</span> or
        # <span class="COLeadline">...</span>.
        head_m = re.search(
            r'<span\s+class="(?:Leadline|COLeadline)"[^>]*>(.*?)</span>',
            slab,
            re.DOTALL,
        )
        heading = _clean_text(head_m.group(1)) if head_m else ""
        heading = heading.rstrip(".")

        # Body: strip the Section span + Leadline span + SourceLine paragraphs.
        body_html = slab
        if sec_m:
            body_html = body_html[sec_m.end() :]
        if head_m:
            # Only drop the first Leadline span
            body_html = re.sub(
                r'<span\s+class="(?:Leadline|COLeadline)"[^>]*>.*?</span>',
                "",
                body_html,
                count=1,
                flags=re.DOTALL,
            )
        # Drop SourceLine paragraphs — cite history, not body text.
        body_html = re.sub(
            r'<p\s+class="SourceLine"[^>]*>.*?</p>',
            "",
            body_html,
            flags=re.DOTALL,
        )
        body = _clean_text(body_html)
        # Trim leading trailing "—" or em-dashes that Word left around.
        body = body.strip().strip("—–-").strip()

        sections.append((chapter, section_num, heading, body))
    return sections


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    citation = f"NRS {section}"
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
          <FRBRthis value="/akn/us-nv/act/nrs/{section}"/>
          <FRBRuri value="/akn/us-nv/act/nrs/{section}"/>
          <FRBRdate date="2026-01-01" name="enacted"/>
          <FRBRauthor href="#nv-legislature"/>
          <FRBRcountry value="us-nv"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="NRS"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-nv/act/nrs/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-nv/act/nrs/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-nv/act/nrs/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-nv/act/nrs/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="nv-legislature" href="https://www.leg.state.nv.us" showAs="Nevada Legislature"/>
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


def _chapter_key(filename: str) -> str:
    """Extract chapter token from ``NRS-244.html`` → ``244``."""
    m = re.match(r"NRS-(.+)\.html$", filename)
    return m.group(1) if m else filename


def scrape_chapter(
    filename: str, out_root: Path, chapter_filter: set[str] | None
) -> tuple[int, int, str]:
    """Scrape one chapter page. Return (ok, skipped, chapter_token)."""
    chapter_token = _chapter_key(filename)
    if chapter_filter and chapter_token not in chapter_filter:
        return (0, 0, chapter_token)

    try:
        html = _http_get(f"{BASE}/{filename}")
    except RuntimeError as exc:
        print(f"  WARN {chapter_token}: {exc}", file=sys.stderr)
        return (0, 0, chapter_token)

    sections = split_sections(html)
    ok = 0
    skipped = 0
    for chapter, section, heading, body in sections:
        if not body:
            skipped += 1
            continue
        xml = build_akn_xml(chapter, section, heading, body)
        safe_section = section.replace("/", "_")
        dest = (
            out_root
            / "statutes"
            / f"ch-{chapter_token}"
            / f"ch-{chapter_token}-sec-{safe_section}.xml"
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(xml, encoding="utf-8")
        ok += 1
    return (ok, skipped, chapter_token)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-nv"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '244,244A').",
    )
    parser.add_argument("--workers", type=int, default=6,
                        help="Parallel chapter fetches.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N chapter pages.")
    args = parser.parse_args(argv)

    chapter_filter: set[str] | None = None
    if args.chapters:
        chapter_filter = {c.strip() for c in args.chapters.split(",") if c.strip()}

    started = time.time()
    args.out.mkdir(parents=True, exist_ok=True)
    files = list_chapter_files()
    if args.limit:
        files = files[: args.limit]
    print(f"Scraping {len(files)} chapter pages", flush=True)

    total_ok = 0
    total_skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_chapter, f, args.out, chapter_filter): f for f in files
        }
        for fut in as_completed(futures):
            ok, skipped, ch = fut.result()
            total_ok += ok
            total_skipped += skipped
            if ok + skipped > 0:
                elapsed = (time.time() - started) / 60
                print(
                    f"  ch-{ch}: {ok} ok, {skipped} skip  "
                    f"(running: {total_ok} ok / {total_skipped} skip, {elapsed:.1f} min)",
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
