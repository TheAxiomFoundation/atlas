"""JSON artifact readers for corpus inventory and provision records."""

from __future__ import annotations

import json
from pathlib import Path

from axiom_corpus.corpus.models import ProvisionRecord, SourceInventoryItem


def load_source_inventory(path: str | Path) -> tuple[SourceInventoryItem, ...]:
    data = json.loads(Path(path).read_text())
    rows = data.get("items", data if isinstance(data, list) else [])
    return tuple(SourceInventoryItem.from_mapping(row) for row in rows)


def load_provisions(path: str | Path) -> tuple[ProvisionRecord, ...]:
    records: list[ProvisionRecord] = []
    p = Path(path)
    if not p.exists():
        return ()
    for line in p.read_text().splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        records.append(ProvisionRecord.from_mapping(data))
    return tuple(records)
