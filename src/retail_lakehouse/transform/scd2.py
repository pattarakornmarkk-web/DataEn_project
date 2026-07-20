"""SCD Type 2 — the pure ORACLE (ADR-0002; unit spec §1.4).

This implementation DEFINES correct SCD2 semantics for the platform; the Spark
compiler must reproduce it on the same fixtures (differential tests). An
auto_cdc_flow cross-check against the same canonical relation is the v1.1 path.

Semantic contract:
  - effective dates come from the SEQUENCE tuple (sequence_by + tiebreak, ADR-0009) —
    never processing time
  - apply = per-key EVENT MERGE + REBUILD: existing versions are re-expressed as
    states, merged with incoming events, no-change states collapsed by content hash,
    then re-materialized. Late-arriving corrections and replays are handled by
    construction, not by special cases.
  - deletes are TOMBSTONES: they close the chain (no current row) and stay in
    history so replaying the delete is a no-op. Never a physical row removal.
  - invariants (validate_history): per key, intervals contiguous and non-overlapping,
    exactly one current version unless the last state is a tombstone, current rows
    have valid_to None.
  - deterministic: output sorted by (key, valid_from); same inputs => identical output.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from retail_lakehouse.transform.cdc import OP_DELETE, CdcEvent
from retail_lakehouse.transform.ordering import canonical_payload, sequence_sort_key


@dataclass(frozen=True)
class Version:
    key: tuple
    attributes: dict | None  # None on tombstones
    valid_from: tuple  # sequence tuple
    valid_to: tuple | None  # None = open interval
    is_current: bool
    is_deleted: bool = False


def record_hash(payload: dict, exclude: tuple[str, ...] = ()) -> str:
    """Content identity for no-change detection (Spark layer will use xxhash64 —
    differential tests compare equality VERDICTS, not hash values)."""
    content = {col: value for col, value in payload.items() if col not in exclude}
    return hashlib.sha256(canonical_payload(content).encode()).hexdigest()


def _states_from_history(history: list[Version]) -> dict[tuple, dict[tuple, tuple]]:
    """key -> {sequence -> (content_hash, attributes|None, deleted)}."""
    states: dict[tuple, dict[tuple, tuple]] = {}
    for version in history:
        content = (
            ("__tombstone__", None, True)
            if version.is_deleted
            else (record_hash(version.attributes), version.attributes, False)
        )
        states.setdefault(version.key, {})[version.valid_from] = content
    return states


def apply_scd2(history: list[Version], events: list[CdcEvent]) -> tuple[Version, ...]:
    """Merge normalized CDC events into existing history; return the NEW full history.

    Events must come from cdc.normalize (orphans already held out). Pure function.
    """
    states = _states_from_history(list(history))
    for event in events:
        content = (
            ("__tombstone__", None, True)
            if event.op == OP_DELETE
            else (record_hash(event.payload), event.payload, False)
        )
        key_states = states.setdefault(event.key, {})
        existing = key_states.get(event.sequence)
        if existing is not None and existing[0] != content[0]:
            # same slot, different content: correction wins deterministically —
            # incoming beats stored only if its canonical content is greater
            stored_payload = canonical_payload(existing[1] or {})
            incoming_payload = canonical_payload(content[1] or {})
            if incoming_payload <= stored_payload:
                continue
        key_states[event.sequence] = content

    rebuilt: list[Version] = []
    for key in sorted(states, key=sequence_sort_key):
        ordered = sorted(states[key].items(), key=lambda item: sequence_sort_key(item[0]))
        # collapse no-change states (identical content as immediate predecessor)
        collapsed: list[tuple[tuple, tuple]] = []
        for sequence, content in ordered:
            if collapsed and collapsed[-1][1][0] == content[0]:
                continue
            collapsed.append((sequence, content))
        last_index = len(collapsed) - 1
        for index, (sequence, (_, attributes, deleted)) in enumerate(collapsed):
            valid_to = collapsed[index + 1][0] if index < last_index else None
            rebuilt.append(
                Version(
                    key=key,
                    attributes=dict(attributes) if attributes is not None else None,
                    valid_from=sequence,
                    valid_to=valid_to,
                    is_current=(index == last_index and not deleted),
                    is_deleted=deleted,
                )
            )
    return tuple(rebuilt)


def current_rows(history: tuple[Version, ...]) -> tuple[Version, ...]:
    """The BI-facing view: current, non-deleted versions only."""
    return tuple(v for v in history if v.is_current)


def validate_history(history: tuple[Version, ...]) -> list[str]:
    """SCD2 invariants; returns violations (empty = healthy). Shared with audit checks."""
    violations: list[str] = []
    by_key: dict[tuple, list[Version]] = {}
    for version in history:
        by_key.setdefault(version.key, []).append(version)

    for key, versions in by_key.items():
        versions.sort(key=lambda v: sequence_sort_key(v.valid_from))
        currents = [v for v in versions if v.is_current]
        last = versions[-1]
        expected_currents = 0 if last.is_deleted else 1
        if len(currents) != expected_currents:
            violations.append(f"{key}: expected {expected_currents} current, found {len(currents)}")
        for earlier, later in zip(versions, versions[1:], strict=False):
            if earlier.valid_to != later.valid_from:
                violations.append(
                    f"{key}: interval gap/overlap between "
                    f"{earlier.valid_from} and {later.valid_from}"
                )
        if last.valid_to is not None:
            violations.append(f"{key}: last version must be open-ended")
        for version in versions[:-1]:
            if version.is_current:
                violations.append(f"{key}: non-terminal version marked current")
    return violations
