"""Parsers for California regulations published as DOCX.

Currently handles CDSS Manual of Policies and Procedures (MPP) Division 63
(CalFresh / Food Stamps). MPP is published by CDSS at:

    https://www.cdss.ca.gov/inforesources/Rules-Regulations/
      Legislation-and-Regulations/CalWORKs-CalFresh-Regulations/CalFresh-Regulations

as a stack of Microsoft Word ``.docx`` files, one per section group.

The DOCX structure is consistent across files: a table of contents, then
body content punctuated by running page headers and footers. Sections are
numbered ``63-XXX`` at the top level, with subsections ``.N`` / ``.NN`` /
``.NNN`` etc. Some subsections carry MR/QR variant markers reflecting the
historical Monthly Reporting vs Quarterly Reporting policy regimes.

This module is intentionally conservative: it extracts top-level section
boundaries and first-level subsections (``.N``). Deeper nesting,
MR/QR variant resolution, and table-aware parsing are explicit follow-on
work captured in the encoding playbook.
"""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from xml.etree import ElementTree as ET

WORD_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"

# Matches paragraphs that look like a top-level section header:
#   "63-300 APPLICATION PROCESS" / "63-301 APPLICATION PROCESSING TIME STANDARDS"
# Excludes running headers like "63-300 (Cont.)" or "63-300 APPLICATION PROCESS (Continued) 63-300".
_SECTION_HEADER_RE = re.compile(
    r"^(?P<num>63-\d+(?:\.\d+)?)\s+(?P<title>[A-Z][A-Za-z0-9 ,/&\-:'()]+?)\s*$"
)

# Matches subsection markers. MPP uses two patterns:
#   (a) ".1 General Process"  → title only, body in following paragraphs
#   (b) ".31 The CWD shall not deny eligibility..." → entire subsection content
#       on one line, no separate body
# We accept both by matching just the prefix ``.<digits>\s+`` and treating
# whatever follows as the heading text. Whether the line ends with a period
# or a title-cased word is irrelevant — the parser doesn't need to know.
_SUBSECTION_HEADER_RE = re.compile(
    r"^\.(?P<num>\d{1,4})\s+(?P<title>\S.*)$"
)

# Patterns that indicate page-footer or running-header noise.
_NOISE_PATTERNS = (
    re.compile(r"^CALIFORNIA-DSS-MANUAL-FS$"),
    re.compile(r"^MANUAL LETTER NO\.\s+FS-\d{2}-\d{2}\s+Effective\s+\d+/\d+/\d+$", re.I),
    re.compile(r"^Page\s+\d+$", re.I),
    re.compile(r"^Regulations$"),
)

# Running headers within the body that repeat on every page boundary:
#   "63-300 APPLICATION PROCESS (Continued) 63-300"
#   "63-300 (Cont.) APPLICATION PROCESS"
_RUNNING_HEADER_RE = re.compile(
    r"^63-\d+(?:\.\d+)?\s+.*\((?:Cont(?:inued|\.))\).*$",
    re.I,
)


@dataclass(frozen=True)
class MppParagraph:
    """One non-empty paragraph from a DOCX, after run-merging and whitespace cleanup."""

    text: str
    index: int


@dataclass(frozen=True)
class MppSection:
    """A top-level MPP section (``63-XXX``) parsed from a DOCX.

    ``subsections`` carries first-level subsections (``.N``). Each subsection's
    body is the concatenated text of paragraphs between its header and the next
    header. Deeper nesting (``.NN`` and below) is currently not resolved — the
    text is preserved in the parent subsection body.
    """

    num: str
    title: str
    subsections: tuple[MppSubsection, ...]
    source_file: str


@dataclass(frozen=True)
class MppSubsection:
    """A first-level subsection within an MPP section."""

    num: str  # bare digit string like "1", "21" — no leading dot
    title: str
    body: str
    parent_num: str  # e.g. "63-301"


def extract_paragraphs(docx_bytes: bytes) -> tuple[MppParagraph, ...]:
    """Parse a DOCX byte string into a sequence of non-empty paragraphs.

    Implementation mirrors ``axiom_corpus.corpus.states._docx_paragraphs``
    so MPP doesn't take a dependency on a private function in another module.
    Worth promoting both to a shared utility module in a follow-up.
    """
    with zipfile.ZipFile(io.BytesIO(docx_bytes)) as archive:
        root = ET.fromstring(archive.read("word/document.xml"))
    paragraphs: list[MppParagraph] = []
    index = 0
    for p in root.iter(f"{{{WORD_NS}}}p"):
        parts: list[str] = []
        for node in p.iter():
            if node.tag == f"{{{WORD_NS}}}t" and node.text:
                parts.append(node.text)
            elif node.tag in {f"{{{WORD_NS}}}tab", f"{{{WORD_NS}}}br"}:
                parts.append(" ")
        text = _clean_text("".join(parts))
        if not text:
            continue
        index += 1
        paragraphs.append(MppParagraph(text=text, index=index))
    if not paragraphs:
        raise ValueError("DOCX word/document.xml has no text paragraphs")
    return tuple(paragraphs)


def parse_mpp_sections(
    paragraphs: tuple[MppParagraph, ...],
    *,
    source_file: str,
    expected_sections: tuple[str, ...] = (),
) -> tuple[MppSection, ...]:
    """Walk paragraphs and split into MPP sections + first-level subsections.

    ``expected_sections`` is a hint from the manifest about which top-level
    section numbers should appear in this DOCX. It's used only to skip the
    TOC at the top of the file: parsing starts at the first paragraph that
    matches one of the expected section headers (after running-header
    deduplication). If empty, parsing starts at the first paragraph that
    looks like any ``63-XXX`` section header.
    """
    body_start = _find_body_start(paragraphs, expected_sections)
    body = paragraphs[body_start:]

    sections: list[MppSection] = []
    seen_section_nums: set[str] = set()
    current_section_num: str | None = None
    current_section_title: str | None = None
    current_subsections: list[MppSubsection] = []

    current_sub_num: str | None = None
    current_sub_title: str | None = None
    current_sub_body: list[str] = []

    def flush_subsection() -> None:
        nonlocal current_sub_num, current_sub_title, current_sub_body
        if current_sub_num is None or current_section_num is None:
            return
        body_text = _clean_text(" ".join(current_sub_body))
        if body_text or current_sub_title:
            current_subsections.append(
                MppSubsection(
                    num=current_sub_num,
                    title=current_sub_title or "",
                    body=body_text,
                    parent_num=current_section_num,
                )
            )
        current_sub_num = None
        current_sub_title = None
        current_sub_body = []

    def flush_section() -> None:
        nonlocal current_section_num, current_section_title, current_subsections
        flush_subsection()
        if current_section_num is None:
            return
        sections.append(
            MppSection(
                num=current_section_num,
                title=current_section_title or "",
                subsections=tuple(current_subsections),
                source_file=source_file,
            )
        )
        current_section_num = None
        current_section_title = None
        current_subsections = []

    for para in body:
        text = para.text
        if _is_noise(text):
            continue
        if _RUNNING_HEADER_RE.match(text):
            continue

        section_match = _SECTION_HEADER_RE.match(text)
        if section_match:
            section_num = section_match.group("num")
            # MPP DOCX files repeat the section header on every page as a
            # running header. The first occurrence is real; the rest are noise.
            # Don't restart the section on every page — keep the in-progress
            # subsection state and skip the line.
            if section_num in seen_section_nums:
                continue
            flush_section()
            seen_section_nums.add(section_num)
            current_section_num = section_num
            # Strip trailing duplicate section number ("APPLICATION PROCESS 63-300"
            # — the title comes paired with a page-header repeat of the number).
            raw_title = section_match.group("title").strip()
            # Also strip a trailing "Regulations" word picked up from page footers.
            raw_title = re.sub(r"\s+Regulations\s*$", "", raw_title, flags=re.I).strip()
            current_section_title = re.sub(
                rf"\s+{re.escape(current_section_num)}\s*$", "", raw_title
            ).strip()
            continue

        subsection_match = _SUBSECTION_HEADER_RE.match(text)
        if subsection_match and current_section_num is not None:
            flush_subsection()
            current_sub_num = subsection_match.group("num")
            current_sub_title = subsection_match.group("title").strip()
            continue

        # Body text. Attach to current subsection, or buffer pending section
        # prose (rare — most sections start with a subsection marker).
        if current_sub_num is not None:
            current_sub_body.append(text)

    flush_section()
    return tuple(sections)


def _find_body_start(
    paragraphs: tuple[MppParagraph, ...],
    expected_sections: tuple[str, ...],
) -> int:
    """Locate the first body paragraph after the TOC.

    The TOC contains paragraphs like ``"Application Process      63-300"``
    (section number at the end). The body contains paragraphs like
    ``"63-300 APPLICATION PROCESS"`` (section number at the start, all-caps title).
    We skip until we see the body form.
    """
    for i, para in enumerate(paragraphs):
        match = _SECTION_HEADER_RE.match(para.text)
        if not match:
            continue
        if expected_sections and match.group("num") not in expected_sections:
            continue
        # Heuristic: TOC entries don't have all-caps titles after the number.
        # Body section headers do.
        title = match.group("title").strip()
        if title.isupper():
            return i
    # Fallback: start at paragraph 0 if we didn't find a body marker.
    return 0


def _is_noise(text: str) -> bool:
    return any(pat.match(text) for pat in _NOISE_PATTERNS)


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
