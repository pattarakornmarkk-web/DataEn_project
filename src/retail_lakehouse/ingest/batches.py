"""Batch-level ingestion semantics: replay identity, lateness, CDC prep, file hooks.

Everything here is pure and deterministic. Production file discovery/exactly-once is
Auto Loader's job (checkpoint); these primitives give the emulator, tests, and
integration assertions the SAME definitions of "same batch", "late", and "CDC-shaped".
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import posixpath
from dataclasses import dataclass
from datetime import datetime, timedelta

from retail_lakehouse.ingest.filenames import parse_batch_metadata

IN_WINDOW = "in_window"
BEYOND_WINDOW = "beyond_window"

_FORMAT_EXTENSIONS = {"csv": {"csv"}, "json": {"json", "jsonl"}}


def classify_lateness(event_ts: datetime, as_of: datetime, window_days: int) -> str:
    """Lateness label per the dedup-window ADR: flagged, never dropped.

    Boundary is INCLUSIVE: an event exactly window_days old is still in_window.
    Future events are in_window here — future timestamps are plausibility's concern.
    """
    return IN_WINDOW if as_of - event_ts <= timedelta(days=window_days) else BEYOND_WINDOW


def file_identity(file_path: str) -> str:
    """Idempotency key for a landed file — normalized full path (Auto Loader semantics)."""
    return posixpath.normpath(str(file_path).strip())


def batch_fingerprint(records: list[dict]) -> str:
    """Order-insensitive content hash of a batch — replay detection for tests/emulator."""
    canonical = sorted(json.dumps(record, sort_keys=True, default=str) for record in records)
    return hashlib.sha256("\n".join(canonical).encode()).hexdigest()


@dataclass(frozen=True)
class CdcSpec:
    """CDC shape resolved from entity + source contracts (bind once, use everywhere)."""

    keys: tuple[str, ...]
    sequence_by: tuple[str, ...]
    tiebreak: str
    op_column: str | None
    delete_payload: str | None


def bind_cdc_spec(entity_contract: dict, source_contract: dict) -> CdcSpec:
    return CdcSpec(
        keys=tuple(entity_contract["keys"]),
        sequence_by=tuple(entity_contract["sequence_by"]),
        tiebreak=entity_contract["tiebreak"],
        op_column=source_contract.get("op_column"),
        delete_payload=source_contract.get("delete_payload"),
    )


def prepare_cdc_metadata(record: dict, spec: CdcSpec, default_op: str = "U") -> dict:
    """Attach normalized CDC metadata: _op and the full ordering tuple (ADR-0009).

    _sequence = sequence_by values + tiebreak value — the ONE ordering key that
    transform/cdc.py and transform/scd2.py consume; they never re-read contracts.
    Input record is not mutated.
    """
    op = record.get(spec.op_column) if spec.op_column else None
    sequence = tuple(record.get(col) for col in (*spec.sequence_by, spec.tiebreak))
    return {
        **record,
        "_op": (str(op).strip() if op is not None else default_op),
        "_sequence": sequence,
    }


def validate_file(
    source_name: str, source_config: dict, file_name: str, emitter: str | None = None
) -> list[str]:
    """File-level validation hooks — run BEFORE any row reaches the DQ engine.

    Returns a list of issues (empty = accept). Never raises: a bad file is a
    routing decision (reject/alert), not a crash.
    """
    issues: list[str] = []
    meta = parse_batch_metadata(file_name)

    if meta.is_malformed:
        issues.append(f"malformed filename: {file_name!r}")
    elif meta.source != source_name:
        issues.append(f"filename source {meta.source!r} does not match {source_name!r}")

    pattern = source_config.get("filename_pattern")
    if pattern and not fnmatch.fnmatch(file_name, pattern + ".*"):
        issues.append(f"filename does not match declared pattern {pattern!r}")

    fmt = source_config.get("format")
    allowed = _FORMAT_EXTENSIONS.get(fmt, set())
    if meta.extension is not None and meta.extension not in allowed:
        issues.append(f"extension {meta.extension!r} not valid for format {fmt!r}")

    declared_emitters = source_config.get("emitters") or {}
    if declared_emitters:
        if emitter is None:
            issues.append(f"source {source_name!r} requires an emitter")
        elif emitter not in declared_emitters:
            issues.append(f"undeclared emitter {emitter!r} for source {source_name!r}")
    elif emitter is not None:
        issues.append(f"source {source_name!r} declares no emitters")

    return issues
