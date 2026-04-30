"""Filesystem/object-storage layout for source-first corpus artifacts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from axiom_corpus.corpus.models import DocumentClass, ProvisionRecord, SourceInventoryItem


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_segment(value: str) -> str:
    cleaned = value.strip().strip("/")
    cleaned = cleaned.replace("\\", "-").replace(":", "-")
    if cleaned in {"", ".", ".."} or "/" in cleaned:
        raise ValueError(f"unsafe path segment: {value!r}")
    return cleaned


class CorpusArtifactStore:
    """Durable artifact layout.

    The same key shape can be used locally or in R2:

    sources/{jurisdiction}/{document_class}/{run_id}/...
    inventory/{jurisdiction}/{document_class}/{run_id}.json
    provisions/{jurisdiction}/{document_class}/{version}.jsonl
    coverage/{jurisdiction}/{document_class}/{version}.json
    exports/{format}/{jurisdiction}/{document_class}/{version}/...
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def source_path(
        self,
        jurisdiction: str,
        document_class: DocumentClass | str,
        run_id: str,
        relative_name: str,
    ) -> Path:
        doc_class = (
            document_class.value if isinstance(document_class, DocumentClass) else document_class
        )
        parts = [safe_segment(part) for part in relative_name.split("/") if part]
        return self.root.joinpath(
            "sources",
            safe_segment(jurisdiction),
            safe_segment(doc_class),
            safe_segment(run_id),
            *parts,
        )

    def inventory_path(
        self,
        jurisdiction: str,
        document_class: DocumentClass | str,
        run_id: str,
    ) -> Path:
        doc_class = (
            document_class.value if isinstance(document_class, DocumentClass) else document_class
        )
        return (
            self.root
            / "inventory"
            / safe_segment(jurisdiction)
            / safe_segment(doc_class)
            / f"{safe_segment(run_id)}.json"
        )

    def provisions_path(
        self,
        jurisdiction: str,
        document_class: DocumentClass | str,
        version: str,
    ) -> Path:
        doc_class = (
            document_class.value if isinstance(document_class, DocumentClass) else document_class
        )
        return (
            self.root
            / "provisions"
            / safe_segment(jurisdiction)
            / safe_segment(doc_class)
            / f"{safe_segment(version)}.jsonl"
        )

    def coverage_path(
        self,
        jurisdiction: str,
        document_class: DocumentClass | str,
        version: str,
    ) -> Path:
        doc_class = (
            document_class.value if isinstance(document_class, DocumentClass) else document_class
        )
        return (
            self.root
            / "coverage"
            / safe_segment(jurisdiction)
            / safe_segment(doc_class)
            / f"{safe_segment(version)}.json"
        )

    def export_path(
        self,
        export_format: str,
        jurisdiction: str,
        document_class: DocumentClass | str,
        version: str,
        relative_name: str,
    ) -> Path:
        doc_class = (
            document_class.value if isinstance(document_class, DocumentClass) else document_class
        )
        parts = [safe_segment(part) for part in relative_name.split("/") if part]
        return self.root.joinpath(
            "exports",
            safe_segment(export_format),
            safe_segment(jurisdiction),
            safe_segment(doc_class),
            safe_segment(version),
            *parts,
        )

    def write_bytes(self, path: Path, data: bytes) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)
        tmp_path.replace(path)
        return sha256_bytes(data)

    def write_text(self, path: Path, text: str) -> str:
        return self.write_bytes(path, text.encode("utf-8"))

    def write_json(self, path: Path, data: dict[str, Any]) -> str:
        return self.write_text(path, json.dumps(data, indent=2, sort_keys=True) + "\n")

    def write_inventory(
        self,
        path: Path,
        items: Iterable[SourceInventoryItem],
    ) -> str:
        return self.write_json(path, {"items": [item.to_mapping() for item in items]})

    def write_provisions(
        self,
        path: Path,
        records: Iterable[ProvisionRecord],
    ) -> str:
        lines = [json.dumps(record.to_mapping(), sort_keys=True) for record in records]
        return self.write_text(path, "\n".join(lines) + ("\n" if lines else ""))

    def iter_provision_files(
        self,
        jurisdiction: str | None = None,
        document_class: DocumentClass | str | None = None,
        version: str | None = None,
    ) -> Iterator[Path]:
        base = self.root / "provisions"
        if jurisdiction:
            base = base / jurisdiction
        if document_class:
            doc_class = (
                document_class.value
                if isinstance(document_class, DocumentClass)
                else document_class
            )
            base = base / doc_class
        if version:
            yield base / f"{version}.jsonl"
            return
        if not base.exists():
            return
        yield from sorted(base.rglob("*.jsonl"))
