"""Base class for legal document converters.

This module provides the abstract base class and registry for converters that
transform legal documents from various source formats into Akoma Ntoso, the
OASIS standard for legislative and legal documents.

Supported source formats:
- USLM: US Legislation Markup (US Code)
- CLML: Crown Legislation Markup Language (UK)
- Formex: EU Official Journal format
- HTML: Scraped state statute HTML (various)
- DC-law: District of Columbia law XML (Akoma Ntoso based)

Usage:
    from atlas.converters.base import LegalDocConverter, register_converter, get_converter

    @register_converter
    class MyConverter(LegalDocConverter):
        jurisdiction = "us-ca"
        source_format = "html"
        doc_type = "statute"

        def fetch(self, citation: str) -> bytes:
            ...

        def parse(self, raw: bytes, source_url: str = "") -> AkomaNtoso:
            ...

    # Get a converter
    converter = get_converter("us-ca", "html")
    doc = converter.convert("CA RTC 17041")
"""

from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from typing import Iterator
from uuid import uuid4, uuid5, NAMESPACE_URL

from pydantic import BaseModel, Field


# -----------------------------------------------------------------------------
# Akoma Ntoso Models (simplified for arch pipeline)
# -----------------------------------------------------------------------------


class AknSubsection(BaseModel):
    """A subsection/paragraph within a section.

    Corresponds to Akoma Ntoso hierarchical elements:
    - subsection, paragraph, subparagraph, clause, subclause, etc.
    """

    id: str = Field(..., description="Unique identifier (eId in AKN)")
    identifier: str = Field(..., description="Display identifier (a, 1, A, etc.)")
    text: str = Field(..., description="Text content")
    heading: str | None = Field(None, description="Heading if present")
    children: list["AknSubsection"] = Field(
        default_factory=list, description="Nested subsections"
    )

    model_config = {"extra": "forbid"}


class AknSection(BaseModel):
    """A section within a legal document.

    Corresponds to Akoma Ntoso <section> element.
    """

    id: str = Field(..., description="Unique identifier (eId in AKN)")
    jurisdiction: str = Field(..., description="Jurisdiction code (us, us-oh, uk, etc.)")
    doc_type: str = Field(..., description="Document type (statute, regulation, etc.)")
    title: str = Field(..., description="Section heading/title")
    text: str = Field(..., description="Full text content")
    subsections: list[AknSubsection] = Field(
        default_factory=list, description="Hierarchical subsections"
    )

    # Metadata
    enacted_date: date | None = Field(None, description="Date enacted")
    effective_date: date | None = Field(None, description="Effective date")
    source_url: str = Field(..., description="URL to official source")

    # Hierarchy (optional, varies by jurisdiction)
    division: str | None = Field(None, description="Division/Subtitle")
    part: str | None = Field(None, description="Part")
    chapter: str | None = Field(None, description="Chapter")
    subchapter: str | None = Field(None, description="Subchapter")
    article: str | None = Field(None, description="Article")

    model_config = {"extra": "forbid"}


class AkomaNtoso(BaseModel):
    """An Akoma Ntoso document representation.

    This is a simplified Pydantic model for the OASIS Akoma Ntoso standard,
    focused on the fields needed for the arch pipeline.

    The full AKN spec includes:
    - FRBR identification (Work, Expression, Manifestation, Item)
    - Temporal versioning
    - Lifecycle events
    - Cross-references and modifications

    This model captures the essential structure for conversion and ingestion.
    """

    uri: str = Field(..., description="FRBR Work URI (e.g., /us/statute/26/32)")
    jurisdiction: str = Field(..., description="Jurisdiction code")
    doc_type: str = Field(..., description="Document type")
    source_format: str = Field(..., description="Original format (uslm, clml, html)")
    source_url: str = Field(..., description="URL to official source")
    sections: list[AknSection] = Field(
        default_factory=list, description="Document sections"
    )

    # Metadata
    title: str | None = Field(None, description="Document title")
    published_date: date | None = Field(None, description="Publication date")
    retrieved_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When fetched",
    )

    model_config = {"extra": "forbid"}


# -----------------------------------------------------------------------------
# Converter Registry
# -----------------------------------------------------------------------------

CONVERTERS: dict[str, type["LegalDocConverter"]] = {}


def register_converter(cls: type["LegalDocConverter"]) -> type["LegalDocConverter"]:
    """Decorator to register a converter class.

    The converter is registered with key "{jurisdiction}:{source_format}".

    Usage:
        @register_converter
        class MyConverter(LegalDocConverter):
            jurisdiction = "us"
            source_format = "uslm"
            ...
    """
    key = f"{cls.jurisdiction}:{cls.source_format}"
    CONVERTERS[key] = cls
    return cls


def get_converter(
    jurisdiction: str, source_format: str | None = None
) -> "LegalDocConverter | None":
    """Get a converter instance for a jurisdiction and format.

    Args:
        jurisdiction: Jurisdiction code (e.g., "us", "us-oh", "uk")
        source_format: Source format (e.g., "uslm", "html", "clml")
            If None, returns the first converter for that jurisdiction.

    Returns:
        Converter instance or None if not found.
    """
    if source_format:
        key = f"{jurisdiction}:{source_format}"
        converter_cls = CONVERTERS.get(key)
        if converter_cls:
            return converter_cls()
        return None

    # Find any converter for this jurisdiction
    for key, converter_cls in CONVERTERS.items():
        if key.startswith(f"{jurisdiction}:"):
            return converter_cls()
    return None


# -----------------------------------------------------------------------------
# Base Converter Class
# -----------------------------------------------------------------------------


def _deterministic_id(citation_path: str) -> str:
    """Generate deterministic UUID from citation path for idempotent upserts."""
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


class LegalDocConverter(ABC):
    """Base class for converting legal documents to Akoma Ntoso.

    Subclasses must implement:
    - fetch(): Download raw source document
    - parse(): Parse raw bytes to AkomaNtoso model

    Subclasses must set class attributes:
    - jurisdiction: Jurisdiction code (e.g., "us", "us-oh", "uk", "ca")
    - source_format: Source format (e.g., "uslm", "html", "clml", "formex")
    - doc_type: Document type (e.g., "statute", "regulation", "guidance")

    The convert() method chains fetch -> parse for convenience.
    The to_rules() method converts AkomaNtoso to akn.rules dicts for DB insert.
    """

    # Subclasses must set these
    jurisdiction: str = ""  # e.g., "us", "us-oh", "uk", "ca", "nz", "eu"
    source_format: str = ""  # e.g., "uslm", "clml", "html", "formex"
    doc_type: str = ""  # e.g., "statute", "regulation", "guidance", "manual"

    @abstractmethod
    def fetch(self, citation: str) -> bytes:
        """Fetch raw source document by citation.

        Args:
            citation: Citation string (format depends on jurisdiction)
                e.g., "26 USC 32", "Cal. RTC 17041", "OH 5747.02"

        Returns:
            Raw bytes of the source document (XML, HTML, etc.)
        """
        pass

    @abstractmethod
    def parse(self, raw: bytes, source_url: str = "") -> AkomaNtoso:
        """Parse raw document to AkomaNtoso model.

        Args:
            raw: Raw bytes from fetch()
            source_url: URL the document was fetched from

        Returns:
            AkomaNtoso document model
        """
        pass

    def convert(self, citation: str) -> AkomaNtoso:
        """Full pipeline: fetch -> parse -> return AKN.

        Args:
            citation: Citation string to fetch and parse

        Returns:
            Parsed AkomaNtoso document
        """
        raw = self.fetch(citation)
        return self.parse(raw)

    def to_rules(self, akn: AkomaNtoso) -> Iterator[dict]:
        """Convert AkomaNtoso to akn.rules dictionaries for DB insert.

        Default implementation flattens sections and subsections into
        rule records matching the akn.rules schema.

        Subclasses can override for custom conversion logic.

        Args:
            akn: Parsed AkomaNtoso document

        Yields:
            Dictionaries matching akn.rules table schema
        """
        for section in akn.sections:
            yield from self._section_to_rules(section)

    def _section_to_rules(
        self,
        section: AknSection,
        parent_id: str | None = None,
    ) -> Iterator[dict]:
        """Convert a section to rule dictionaries.

        Args:
            section: AknSection to convert
            parent_id: Parent rule ID for hierarchy

        Yields:
            Rule dictionaries for section and all subsections
        """
        # Use section.id as citation_path
        citation_path = section.id
        section_id = _deterministic_id(citation_path)

        yield {
            "id": section_id,
            "jurisdiction": section.jurisdiction,
            "doc_type": section.doc_type,
            "parent_id": parent_id,
            "level": 0,
            "ordinal": None,  # Could parse from section number
            "heading": section.title,
            "body": section.text,
            "effective_date": (
                section.effective_date.isoformat() if section.effective_date else None
            ),
            "source_url": section.source_url,
            "source_path": None,
            "citation_path": citation_path,
            "rac_path": None,
            "has_rac": False,
        }

        # Recursively yield subsections
        yield from self._subsections_to_rules(
            section.subsections,
            parent_id=section_id,
            level=1,
            parent_path=citation_path,
            jurisdiction=section.jurisdiction,
            doc_type=section.doc_type,
        )

    def _subsections_to_rules(
        self,
        subsections: list[AknSubsection],
        parent_id: str,
        level: int,
        parent_path: str,
        jurisdiction: str,
        doc_type: str,
    ) -> Iterator[dict]:
        """Convert subsections to rule dictionaries recursively.

        Args:
            subsections: List of AknSubsection to convert
            parent_id: Parent rule UUID
            level: Nesting level (1 = direct child of section)
            parent_path: Parent citation path
            jurisdiction: Jurisdiction code
            doc_type: Document type

        Yields:
            Rule dictionaries for each subsection and descendants
        """
        for i, sub in enumerate(subsections):
            citation_path = sub.id  # Use the subsection's id as citation_path
            sub_id = _deterministic_id(citation_path)

            yield {
                "id": sub_id,
                "jurisdiction": jurisdiction,
                "doc_type": doc_type,
                "parent_id": parent_id,
                "level": level,
                "ordinal": i + 1,
                "heading": sub.heading,
                "body": sub.text,
                "effective_date": None,
                "source_url": None,
                "source_path": None,
                "citation_path": citation_path,
                "rac_path": None,
                "has_rac": False,
            }

            # Recurse for children
            if sub.children:
                yield from self._subsections_to_rules(
                    sub.children,
                    parent_id=sub_id,
                    level=level + 1,
                    parent_path=citation_path,
                    jurisdiction=jurisdiction,
                    doc_type=doc_type,
                )
