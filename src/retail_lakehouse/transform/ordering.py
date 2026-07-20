"""Ordering and content-identity primitives shared across the transform layer.

Two rules the whole platform depends on:

  sequence_sort_key  — None-safe total ordering (None sorts below any value), the
                       oracle counterpart of Spark's explicit asc_nulls_first /
                       desc_nulls_last. Used by CDC ordering, SCD2 interval
                       rebuild, funnel session ordering, and aggregate grouping.
  canonical_payload  — deterministic content identity for a record, used for
                       replay detection and no-change collapse.

Ordering keys come from the entity contract: (sequence_by..., tiebreak) — ADR-0009.
"""

from __future__ import annotations

import json


def sequence_sort_key(values: tuple) -> tuple:
    """None-safe total ordering for a sequence tuple: None sorts below any value."""
    return tuple((value is not None, value) for value in values)


def canonical_payload(record: dict) -> str:
    """Deterministic content identity for a record (order-insensitive, type-tolerant)."""
    return json.dumps(record, sort_keys=True, default=str)
