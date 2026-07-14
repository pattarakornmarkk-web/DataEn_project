"""Landing-file writer — deterministic bytes for deterministic replay.

Same GeneratedFile => identical file content, always: fixed field order (contract
order, then extras sorted, _ingestion_order last), sorted JSON keys, no timestamps
of its own. Paths follow conf/sources.yml (path or per-emitter path).
"""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from retail_lakehouse.config import loader
from retail_lakehouse.scenarios.generator import GeneratedFile


def _field_order(records: list[dict], contract_columns: list[str]) -> list[str]:
    seen = {k for r in records for k in r}
    extras = sorted(seen - set(contract_columns) - {"_ingestion_order"})
    return [c for c in contract_columns if c in seen] + extras + ["_ingestion_order"]


def render_file(file: GeneratedFile, fmt: str, contract_columns: list[str]) -> bytes:
    fields = _field_order(file.records, contract_columns)
    if fmt == "csv":
        buffer = io.StringIO()
        w = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for record in file.records:
            w.writerow({k: ("" if record.get(k) is None else record.get(k)) for k in fields})
        return buffer.getvalue().encode()
    # JSONL values are ALL strings (or null) — the landing contract is stringly typed,
    # so bronze's explicit string schema reads every field losslessly.
    lines = [
        json.dumps(
            {k: (None if record.get(k) is None else str(record.get(k))) for k in fields},
            sort_keys=True,
        )
        for record in file.records
    ]
    return ("\n".join(lines) + "\n").encode()


def write_files(files: list[GeneratedFile], base_path: str) -> list[str]:
    """Write generated files under base_path (the landing volume). Returns paths written."""
    sources = loader.load_sources()
    contracts = loader.load_contracts()["source_contracts"]
    written = []
    for file in files:
        config = sources[file.source]
        rel = config["emitters"][file.emitter] if file.emitter else config["path"]
        target = Path(base_path) / rel / file.file_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(
            render_file(file, config["format"], list(contracts[file.source]["columns"]))
        )
        written.append(str(target))
    return sorted(written)
