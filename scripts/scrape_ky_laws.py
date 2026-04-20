"""Scrape the Kentucky Revised Statutes from apps.legislature.ky.gov.

Source layout
-------------
KY serves statutes as PDFs (not HTML). The discovery path is:

1. Master TOC at ``chapter.aspx?Chapter=1`` — despite the query string, this
   returns the *index of all chapters* across every title, one anchor per
   chapter: ``<a class="chapter" href="chapter.aspx?id={chapter_id}">CHAPTER
   {chapter_token} {heading}</a>``. The ``chapter_token`` is the human-readable
   chapter number (``1``, ``2``, ``6A``, ``11A``, ``141``, ``635`` ...) while
   ``chapter_id`` is an opaque database id used by the site.
2. A chapter page at ``chapter.aspx?id={chapter_id}`` lists its sections inside
   ``<span class="bod">``. Each entry is::

       <a class="statute" href="statute.aspx?id={section_id}">
         .010  Heading text here.
       </a>

   where ``.010`` is the section number within the chapter (the full KRS
   citation is ``{chapter_token}{.section}`` -- e.g. ``1.010``).
3. ``statute.aspx?id={section_id}`` returns a PDF (Content-Type
   ``application/PDF``, filename like ``KRS1_010(K).pdf``). ``pdftotext
   -layout`` yields clean text whose first line is
   ``{chapter}.{section} {heading}.`` followed by the body, ending in
   ``Effective: ...`` and ``History: ...`` trailer lines we drop like NV's
   SourceLine.

Output
------
AKN-3.0 XML at ``{out}/statutes/ch-{chapter}/ch-{chapter}-sec-{section}.xml``,
shape matching what ``ingest_state_laws.py --state ky`` expects.

Usage
-----
::

    uv run python scripts/scrape_ky_laws.py --out /tmp/rules-us-ky
    uv run python scripts/scrape_ky_laws.py --out /tmp/rules-us-ky --chapters 1,2
"""

from __future__ import annotations

import argparse
import html as _html
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

BASE = "https://apps.legislature.ky.gov/law/statutes"
UA = "Mozilla/5.0 (compatible; axiom-scraper/0.1; +https://axiom-foundation.org)"
AKN_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"


def _http_get(url: str, retries: int = 5) -> bytes | None:
    """GET a URL as bytes; returns None on 404/redirect-to-gone or giveup.

    KY's IIS host is sometimes slow and occasionally returns transient
    5xx errors. Soft-fail like the AZ scraper (return None) instead of
    raising so one flaky section doesn't kill a whole run.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=45) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code in (404, 307, 410):
                return None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        if attempt < retries:
            time.sleep(min(12.0, 2.0**attempt))
    print(f"  WARN skip {url}: {last_exc}", file=sys.stderr, flush=True)
    return None


def _http_get_text(url: str, retries: int = 5) -> str | None:
    data = _http_get(url, retries=retries)
    if data is None:
        return None
    return data.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

_CHAPTER_ANCHOR = re.compile(
    r'<a[^>]*class="chapter"[^>]*href="chapter\.aspx\?id=(?P<cid>\d+)"[^>]*>'
    r"\s*CHAPTER\s+(?P<token>\S+)\s+(?P<heading>[^<]*?)\s*</a>",
    re.DOTALL | re.IGNORECASE,
)


def list_chapters() -> list[tuple[str, str]]:
    """Return ``[(chapter_token, chapter_id), ...]`` from the master TOC."""
    html = _http_get_text(f"{BASE}/chapter.aspx?Chapter=1")
    if html is None:
        return []
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for m in _CHAPTER_ANCHOR.finditer(html):
        token = m.group("token").strip().upper()
        cid = m.group("cid")
        if token in seen:
            continue
        seen.add(token)
        out.append((token, cid))
    return out


_SECTION_ANCHOR = re.compile(
    r'<a[^>]*class="statute"[^>]*href="statute\.aspx\?id=(?P<sid>\d+)"[^>]*>'
    r"\s*(?P<sec>\.\S+)\s+(?P<heading>.*?)\s*</a>",
    re.DOTALL | re.IGNORECASE,
)


def list_sections(chapter_id: str) -> list[tuple[str, str, str]]:
    """Return ``[(section_dot, section_id, heading), ...]`` for a chapter.

    ``section_dot`` is e.g. ``.010``. The full citation is built by the
    caller as ``{chapter_token}{section_dot}`` -- e.g. ``1.010``.
    """
    html = _http_get_text(f"{BASE}/chapter.aspx?id={chapter_id}")
    if html is None:
        return []
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for m in _SECTION_ANCHOR.finditer(html):
        sec = m.group("sec").strip()
        sid = m.group("sid")
        heading = _clean_text(m.group("heading")).rstrip(".")
        if sid in seen:
            continue
        seen.add(sid)
        out.append((sec, sid, heading))
    return out


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------


def _clean_text(s: str) -> str:
    """Strip HTML, normalize whitespace (for headings from chapter page)."""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</(p|div|tr|td|span)>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s).strip()
    return s


# Trailer lines in the PDF that start the "publication history" block. Once we
# see one of these at the start of a line, we drop everything from there on.
_TRAILER_PREFIX = re.compile(
    r"^\s*(Effective:|History:|Legislative Research Commission Note|"
    r"Catchline at repeal:)",
    re.IGNORECASE,
)


def _pdftotext(data: bytes) -> str | None:
    """Run ``pdftotext -layout`` on a PDF byte blob."""
    if shutil.which("pdftotext") is None:
        raise RuntimeError(
            "pdftotext not found; install poppler (brew install poppler)"
        )
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
        tmp.write(data)
        tmp.flush()
        try:
            proc = subprocess.run(
                ["pdftotext", "-layout", tmp.name, "-"],
                check=False,
                capture_output=True,
                timeout=60,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            print(f"  WARN pdftotext failed: {exc}", file=sys.stderr)
            return None
    if proc.returncode != 0:
        # Some PDFs have corrupt trailers but pdftotext still prints to stdout.
        if not proc.stdout:
            return None
    return proc.stdout.decode("utf-8", errors="replace")


def parse_pdf_body(
    text: str, chapter: str, section_dot: str
) -> tuple[str, str]:
    """Return ``(heading, body)`` extracted from PDF text.

    The PDF opens with ``{chapter}{section_dot} {heading}.`` on the first
    non-empty line, followed by body paragraphs. ``Effective:`` /
    ``History:`` / ``Legislative Research Commission Note`` lines at the
    tail are history metadata -- drop them (mirrors NV's SourceLine).
    """
    lines = text.splitlines()
    # Find the first non-empty line for the heading
    heading = ""
    body_start = 0
    num_prefix = f"{chapter}{section_dot}"  # e.g. "1.010"
    for i, raw in enumerate(lines):
        ln = raw.strip()
        if not ln:
            continue
        # Expected: "1.010 Legislative intent ..." but tolerate just the
        # heading if the PDF rendered the number elsewhere.
        if ln.startswith(num_prefix):
            heading = ln[len(num_prefix):].strip()
        else:
            heading = ln
        heading = heading.rstrip(".").strip()
        body_start = i + 1
        break

    # Trim trailer from first Effective/History/LRC line onward.
    kept: list[str] = []
    for raw in lines[body_start:]:
        if _TRAILER_PREFIX.match(raw):
            break
        kept.append(raw)

    # Collapse body: pdftotext -layout preserves intra-paragraph line wraps;
    # treat blank lines as paragraph separators.
    paragraphs: list[str] = []
    buf: list[str] = []
    for raw in kept:
        if raw.strip():
            buf.append(raw.strip())
        else:
            if buf:
                paragraphs.append(" ".join(buf))
                buf = []
    if buf:
        paragraphs.append(" ".join(buf))
    # Further squeeze whitespace inside each paragraph.
    paragraphs = [re.sub(r"\s+", " ", p).strip() for p in paragraphs]
    paragraphs = [p for p in paragraphs if p]
    body = "\n\n".join(paragraphs)
    return heading, body


# ---------------------------------------------------------------------------
# AKN serialization
# ---------------------------------------------------------------------------


def build_akn_xml(
    chapter: str, section: str, heading: str, body: str
) -> str:
    """Build AKN-3.0 XML for one KRS section. ``section`` like ``1.010``."""
    citation = f"KRS {section}"
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
          <FRBRthis value="/akn/us-ky/act/krs/{section}"/>
          <FRBRuri value="/akn/us-ky/act/krs/{section}"/>
          <FRBRauthor href="#ky-legislature"/>
          <FRBRcountry value="us-ky"/>
          <FRBRnumber value="{section}"/>
          <FRBRname value="KRS"/>
        </FRBRWork>
        <FRBRExpression>
          <FRBRthis value="/akn/us-ky/act/krs/{section}/eng@2026-01-01"/>
          <FRBRuri value="/akn/us-ky/act/krs/{section}/eng@2026-01-01"/>
          <FRBRdate date="2026-01-01" name="publication"/>
          <FRBRauthor href="#axiom"/>
          <FRBRlanguage language="eng"/>
        </FRBRExpression>
        <FRBRManifestation>
          <FRBRthis value="/akn/us-ky/act/krs/{section}/eng@2026-01-01/main.xml"/>
          <FRBRuri value="/akn/us-ky/act/krs/{section}/eng@2026-01-01/main.xml"/>
          <FRBRdate date="2026-01-01" name="generation"/>
          <FRBRauthor href="#axiom"/>
        </FRBRManifestation>
      </identification>
      <references source="#axiom">
        <TLCOrganization eId="ky-legislature" href="https://legislature.ky.gov" showAs="Kentucky General Assembly"/>
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


# ---------------------------------------------------------------------------
# Scrape driver
# ---------------------------------------------------------------------------


def scrape_section(
    chapter: str,
    section_dot: str,
    section_id: str,
    toc_heading: str,
    out_root: Path,
) -> tuple[bool, str]:
    """Fetch one section PDF, extract text, write AKN XML.

    Returns ``(wrote, msg)``.
    """
    section = f"{chapter}{section_dot}"  # e.g. "1.010"
    pdf = _http_get(f"{BASE}/statute.aspx?id={section_id}")
    if pdf is None:
        return (False, "404")
    if not pdf.startswith(b"%PDF"):
        return (False, "non-pdf")
    text = _pdftotext(pdf)
    if text is None:
        return (False, "pdftotext-failed")
    heading, body = parse_pdf_body(text, chapter, section_dot)
    if not heading:
        heading = toc_heading
    if not body:
        return (False, "empty body")
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
    return (True, section)


def scrape_chapter(
    chapter: str,
    chapter_id: str,
    out_root: Path,
    workers: int,
) -> tuple[int, int]:
    """Scrape every section in a chapter. Returns ``(ok, skipped)``."""
    sections = list_sections(chapter_id)
    if not sections:
        return (0, 0)
    ok = 0
    skipped = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(
                scrape_section, chapter, sec, sid, heading, out_root
            ): sec
            for sec, sid, heading in sections
        }
        for fut in as_completed(futures):
            wrote, _msg = fut.result()
            if wrote:
                ok += 1
            else:
                skipped += 1
    return (ok, skipped)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("/tmp/rules-us-ky"))
    parser.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter tokens (e.g. '1,2,6A').",
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Parallel section fetches."
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Stop after N chapters."
    )
    args = parser.parse_args(argv)

    chapter_filter: set[str] | None = None
    if args.chapters:
        chapter_filter = {c.strip().upper() for c in args.chapters.split(",") if c.strip()}

    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    chapters = list_chapters()
    if chapter_filter:
        chapters = [c for c in chapters if c[0] in chapter_filter]
    if args.limit:
        chapters = chapters[: args.limit]
    print(f"Scraping {len(chapters)} chapters (workers={args.workers})", flush=True)

    total_ok = 0
    total_skipped = 0
    for token, cid in chapters:
        ok, skipped = scrape_chapter(token, cid, args.out, args.workers)
        total_ok += ok
        total_skipped += skipped
        elapsed = (time.time() - started) / 60
        print(
            f"  ch-{token}: {ok} ok, {skipped} skip  "
            f"(running: {total_ok} ok / {total_skipped} skip, {elapsed:.1f} min)",
            flush=True,
        )

    elapsed = (time.time() - started) / 60
    print(
        f"\nDONE - {total_ok} sections scraped, "
        f"{total_skipped} skipped, {elapsed:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
