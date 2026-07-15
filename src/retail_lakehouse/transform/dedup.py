"""Keyed deduplication — pure oracle (unit spec §1.2, ADR-0009).

Semantic contract:
  - winner per key = MAX by (sequence_by values, tiebreak value), None-safe
    (highest wins — ADR-0009's "descending" rule expressed as max-selection)
  - losers are CLASSIFIED, never silently discarded:
      exact_duplicates : payload identical to the winner (replay delivery)
      superseded       : older version by the ordering
      conflicts        : full ordering tie with a DIFFERENT payload — winner chosen
                         deterministically by canonical payload, losers flagged
  - order independent: any permutation of the input yields the identical result
  - stable output ordering: survivors sorted by key projection
"""

from __future__ import annotations

import json
from dataclasses import dataclass


def sequence_sort_key(values: tuple) -> tuple:
    """None-safe total ordering for a sequence tuple: None sorts below any value."""
    return tuple((value is not None, value) for value in values)


def canonical_payload(record: dict) -> str:
    """Deterministic content identity for a record (order-insensitive, type-tolerant)."""
    return json.dumps(record, sort_keys=True, default=str)


@dataclass(frozen=True)
class DedupResult:
    survivors: tuple[dict, ...]
    exact_duplicates: tuple[dict, ...]
    superseded: tuple[dict, ...]
    conflicts: tuple[dict, ...]

    @property
    def input_count(self) -> int:
        return (
            len(self.survivors)
            + len(self.exact_duplicates)
            + len(self.superseded)
            + len(self.conflicts)
        )


def _ordering(record: dict, sequence_by: list[str], tiebreak: str) -> tuple:
    return sequence_sort_key(tuple(record.get(col) for col in (*sequence_by, tiebreak)))


def dedup_by_key(
    records: list[dict], keys: list[str], sequence_by: list[str], tiebreak: str
) -> DedupResult:
    """Deterministic keyed dedup. Pure: input records are never mutated."""
    groups: dict[tuple, list[dict]] = {}
    for record in records:
        key = tuple(str(record.get(col)) if record.get(col) is not None else None for col in keys)
        groups.setdefault(key, []).append(record)

    survivors: list[tuple[tuple, dict]] = []
    exact_duplicates: list[dict] = []
    superseded: list[dict] = []
    conflicts: list[dict] = []

    for key, group in groups.items():
        # Winner: max ordering; ordering ties resolved by canonical payload (documented).
        winner = max(
            group, key=lambda r: (_ordering(r, sequence_by, tiebreak), canonical_payload(r))
        )
        winner_ordering = _ordering(winner, sequence_by, tiebreak)
        winner_payload = canonical_payload(winner)
        survivors.append((key, winner))
        for record in group:
            if record is winner:
                continue
            if canonical_payload(record) == winner_payload:
                exact_duplicates.append(record)
            elif _ordering(record, sequence_by, tiebreak) == winner_ordering:
                conflicts.append(record)
            else:
                superseded.append(record)

    survivors.sort(key=lambda pair: sequence_sort_key(pair[0]))
    return DedupResult(
        survivors=tuple(record for _, record in survivors),
        exact_duplicates=tuple(sorted(exact_duplicates, key=canonical_payload)),
        superseded=tuple(sorted(superseded, key=canonical_payload)),
        conflicts=tuple(sorted(conflicts, key=canonical_payload)),
    )
