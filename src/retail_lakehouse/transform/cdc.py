"""CDC normalization — pure oracle (unit spec §1.3, ADR-0004/0006/0009).

Semantic contract:
  - input: contract-coerced records carrying _op/_sequence (ingest.batches.prepare_cdc_metadata)
    or raw records + CdcSpec (this module will prepare them)
  - snapshot rows become synthetic upserts (op=U) — ADR-0004 unification
  - unify asserts both branches carry the IDENTICAL payload column set
  - replayed events (same key + sequence + op + payload) are dropped and counted
  - ordering-tie with different content: deterministic winner by canonical payload,
    losers surface as conflicts (never silently overwritten)
  - orphan deletes (D with no prior upsert in-stream and key unknown to the target)
    are TAGGED AND HELD, never applied, never dropped — ADR-0006
  - output events are sorted by (key, sequence): "late arriving" ceases to exist
    here; slotting into history is scd2's job
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from retail_lakehouse.config.contracts import ConfigError
from retail_lakehouse.ingest.batches import CdcSpec, prepare_cdc_metadata
from retail_lakehouse.transform.dedup import canonical_payload, sequence_sort_key

OP_UPSERT = "U"
OP_DELETE = "D"
_META_FIELDS = ("_op", "_sequence")


@dataclass(frozen=True)
class CdcEvent:
    key: tuple
    op: str  # OP_UPSERT | OP_DELETE (source "I" normalizes to upsert)
    sequence: tuple  # raw sequence values (sequence_by + tiebreak, ADR-0009)
    payload: dict  # business columns only — no op/meta fields
    is_orphan_delete: bool = False

    @property
    def sort_key(self) -> tuple:
        return (sequence_sort_key(self.key), sequence_sort_key(self.sequence))

    def content_identity(self) -> tuple:
        return (self.key, self.op, self.sequence, canonical_payload(self.payload))


@dataclass(frozen=True)
class NormalizedCdcStream:
    events: tuple[CdcEvent, ...]  # apply these, in (key, sequence) order
    held_orphans: tuple[CdcEvent, ...]  # ADR-0006: tagged, held, not applied
    conflicts: tuple[CdcEvent, ...]  # ordering-tie losers (flagged)
    replays_dropped: int


def _to_event(record: dict, spec: CdcSpec) -> CdcEvent:
    prepared = (
        record if "_op" in record and "_sequence" in record else prepare_cdc_metadata(record, spec)
    )
    op = OP_DELETE if prepared["_op"] == OP_DELETE else OP_UPSERT  # I -> upsert
    # Payload = business STATE only. Op, sequence_by, and tiebreak columns are
    # identity/ordering, not state — including them would defeat no-change detection.
    excluded = {*_META_FIELDS, spec.op_column, *spec.sequence_by, spec.tiebreak}
    key = tuple(prepared.get(col) for col in spec.keys)
    payload = {col: value for col, value in prepared.items() if col not in excluded}
    return CdcEvent(key=key, op=op, sequence=tuple(prepared["_sequence"]), payload=payload)


def to_events(records: list[dict], spec: CdcSpec) -> list[CdcEvent]:
    """CDC-source records -> events (op read from the record's op column)."""
    return [_to_event(record, spec) for record in records]


def snapshot_to_events(records: list[dict], spec: CdcSpec) -> list[CdcEvent]:
    """Snapshot rows -> synthetic upsert events (ADR-0004). Op column, if any, ignored."""
    events = []
    for record in records:
        clean = {col: value for col, value in record.items() if col != spec.op_column}
        events.append(
            _to_event(
                {
                    **clean,
                    "_op": OP_UPSERT,
                    "_sequence": tuple(
                        record.get(col) for col in (*spec.sequence_by, spec.tiebreak)
                    ),
                },
                spec,
            )
        )
    return events


def unify_streams(*streams: list[CdcEvent]) -> list[CdcEvent]:
    """Union of event streams. Both branches MUST produce the identical payload schema."""
    events = [event for stream in streams for event in stream]
    schemas = {tuple(sorted(event.payload)) for event in events}
    if len(schemas) > 1:
        raise ConfigError(f"unify_streams: branches disagree on payload schema: {schemas}")
    return events


def normalize(events: list[CdcEvent], known_keys: frozenset = frozenset()) -> NormalizedCdcStream:
    """Replay-drop, conflict-resolve, orphan-hold, and order the stream.

    known_keys: entity keys already present in the target dimension — a delete for
    one of these is legitimate even with no in-stream upsert.
    """
    # 1) exact replays: identical (key, op, sequence, payload) collapses to one
    seen: set[tuple] = set()
    unique: list[CdcEvent] = []
    replays = 0
    for event in sorted(events, key=lambda e: (e.sort_key, e.op, canonical_payload(e.payload))):
        identity = event.content_identity()
        if identity in seen:
            replays += 1
        else:
            seen.add(identity)
            unique.append(event)

    # 2) ordering-tie conflicts: same (key, sequence), different content —
    #    deterministic winner = max canonical payload; losers flagged
    by_slot: dict[tuple, list[CdcEvent]] = {}
    for event in unique:
        by_slot.setdefault((event.key, event.sequence), []).append(event)
    conflicts: list[CdcEvent] = []
    resolved: list[CdcEvent] = []
    for slot_events in by_slot.values():
        winner = max(slot_events, key=lambda e: (e.op, canonical_payload(e.payload)))
        resolved.append(winner)
        conflicts.extend(e for e in slot_events if e is not winner)

    # 3) orphan deletes: D before any upsert for an unknown key -> tag + hold (ADR-0006)
    resolved.sort(key=lambda e: e.sort_key)
    upserted: set[tuple] = set()
    applied: list[CdcEvent] = []
    orphans: list[CdcEvent] = []
    for event in resolved:
        if event.op == OP_DELETE and event.key not in upserted and event.key not in known_keys:
            orphans.append(replace(event, is_orphan_delete=True))
            continue
        if event.op == OP_UPSERT:
            upserted.add(event.key)
        applied.append(event)

    return NormalizedCdcStream(
        events=tuple(applied),
        held_orphans=tuple(orphans),
        conflicts=tuple(sorted(conflicts, key=lambda e: e.sort_key)),
        replays_dropped=replays,
    )
