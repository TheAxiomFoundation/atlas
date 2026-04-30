"""Source-first corpus models.

The corpus contract is official source provenance plus normalized provisions.
External interchange formats are not the storage model for ingestion coverage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Self

import yaml  # type: ignore[import-untyped]


class DocumentClass(str, Enum):
    """Policy document class for corpus records."""

    STATUTE = "statute"
    REGULATION = "regulation"
    GUIDANCE = "guidance"
    POLICY = "policy"
    MANUAL = "manual"
    FORM = "form"
    RULEMAKING = "rulemaking"
    OTHER = "other"


@dataclass(frozen=True)
class SourceInventoryItem:
    """One expected normalized provision from an official source snapshot."""

    citation_path: str
    source_url: str | None = None
    source_path: str | None = None
    source_format: str | None = None
    sha256: str | None = None
    metadata: dict[str, Any] | None = None

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        return cls(
            citation_path=str(data["citation_path"]),
            source_url=data.get("source_url"),
            source_path=data.get("source_path"),
            source_format=data.get("source_format"),
            sha256=data.get("sha256"),
            metadata=data.get("metadata"),
        )

    def to_mapping(self) -> dict[str, Any]:
        out: dict[str, Any] = {"citation_path": self.citation_path}
        if self.source_url:
            out["source_url"] = self.source_url
        if self.source_path:
            out["source_path"] = self.source_path
        if self.source_format:
            out["source_format"] = self.source_format
        if self.sha256:
            out["sha256"] = self.sha256
        if self.metadata:
            out["metadata"] = self.metadata
        return out


@dataclass(frozen=True)
class ProvisionRecord:
    """Normalized corpus provision ready for indexing or database import."""

    jurisdiction: str
    document_class: str
    citation_path: str
    body: str | None = None
    id: str | None = None
    heading: str | None = None
    citation_label: str | None = None
    version: str | None = None
    source_url: str | None = None
    source_path: str | None = None
    source_id: str | None = None
    source_format: str | None = None
    source_document_id: str | None = None
    source_as_of: str | None = None
    expression_date: str | None = None
    parent_citation_path: str | None = None
    parent_id: str | None = None
    level: int | None = None
    ordinal: int | None = None
    kind: str | None = None
    language: str | None = "en"
    legal_identifier: str | None = None
    identifiers: dict[str, str] | None = None
    rulespec_path: str | None = None
    has_rulespec: bool | None = None
    metadata: dict[str, Any] | None = None

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        body_value = data.get("body")
        identifiers_value = data.get("identifiers")
        return cls(
            jurisdiction=str(data["jurisdiction"]),
            document_class=str(data["document_class"]),
            citation_path=str(data["citation_path"]),
            body=str(body_value) if body_value is not None else None,
            id=data.get("id"),
            heading=data.get("heading"),
            citation_label=data.get("citation_label"),
            version=data.get("version"),
            source_url=data.get("source_url"),
            source_path=data.get("source_path"),
            source_id=data.get("source_id"),
            source_format=data.get("source_format"),
            source_document_id=data.get("source_document_id"),
            source_as_of=data.get("source_as_of"),
            expression_date=data.get("expression_date"),
            parent_citation_path=data.get("parent_citation_path"),
            parent_id=data.get("parent_id"),
            level=data.get("level"),
            ordinal=data.get("ordinal"),
            kind=data.get("kind"),
            language=data.get("language", "en"),
            legal_identifier=data.get("legal_identifier"),
            identifiers=(
                {str(key): str(value) for key, value in identifiers_value.items()}
                if isinstance(identifiers_value, dict)
                else None
            ),
            rulespec_path=data.get("rulespec_path"),
            has_rulespec=data.get("has_rulespec"),
            metadata=data.get("metadata"),
        )

    def to_mapping(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "jurisdiction": self.jurisdiction,
            "document_class": self.document_class,
            "citation_path": self.citation_path,
            "body": self.body,
        }
        optional: dict[str, Any] = {
            "id": self.id,
            "heading": self.heading,
            "citation_label": self.citation_label,
            "version": self.version,
            "source_url": self.source_url,
            "source_path": self.source_path,
            "source_id": self.source_id,
            "source_format": self.source_format,
            "source_document_id": self.source_document_id,
            "source_as_of": self.source_as_of,
            "expression_date": self.expression_date,
            "parent_citation_path": self.parent_citation_path,
            "parent_id": self.parent_id,
            "level": self.level,
            "ordinal": self.ordinal,
            "kind": self.kind,
            "language": self.language,
            "legal_identifier": self.legal_identifier,
            "identifiers": self.identifiers,
            "rulespec_path": self.rulespec_path,
            "has_rulespec": self.has_rulespec,
            "metadata": self.metadata,
        }
        out.update({key: value for key, value in optional.items() if value is not None})
        return out

    def to_supabase_row(self) -> dict[str, Any]:
        """Return the current `corpus.provisions` REST shape."""
        from axiom_corpus.corpus.supabase import provision_to_supabase_row

        return provision_to_supabase_row(self)


@dataclass(frozen=True)
class CorpusSource:
    """Configured upstream source for a corpus ingest run."""

    source_id: str
    jurisdiction: str
    document_class: str
    adapter: str
    source_url: str | None = None
    version: str | None = None
    metadata: dict[str, Any] | None = None

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        return cls(
            source_id=str(data["source_id"]),
            jurisdiction=str(data["jurisdiction"]),
            document_class=str(data["document_class"]),
            adapter=str(data["adapter"]),
            source_url=data.get("source_url"),
            version=data.get("version"),
            metadata=data.get("metadata"),
        )


@dataclass(frozen=True)
class CorpusManifest:
    """Manifest for source-first corpus ingestion."""

    version: str
    sources: tuple[CorpusSource, ...]

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> Self:
        return cls(
            version=str(data["version"]),
            sources=tuple(CorpusSource.from_mapping(row) for row in data.get("sources", [])),
        )

    @classmethod
    def load(cls, path: str | Path) -> Self:
        data = yaml.safe_load(Path(path).read_text())
        if not isinstance(data, dict):
            raise ValueError("manifest must be a YAML mapping")
        return cls.from_mapping(data)

    def require_unique_sources(self) -> None:
        seen: set[str] = set()
        for source in self.sources:
            if source.source_id in seen:
                raise ValueError(f"duplicate source_id: {source.source_id}")
            seen.add(source.source_id)

    def to_json(self) -> str:
        return json.dumps(
            {
                "version": self.version,
                "sources": [
                    {
                        "source_id": source.source_id,
                        "jurisdiction": source.jurisdiction,
                        "document_class": source.document_class,
                        "adapter": source.adapter,
                        "source_url": source.source_url,
                        "version": source.version,
                        "metadata": source.metadata,
                    }
                    for source in self.sources
                ],
            },
            indent=2,
            sort_keys=True,
        )
