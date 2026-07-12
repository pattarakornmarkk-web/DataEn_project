"""Hand-rolled SCD2 — kept as the tested ORACLE for auto_cdc_flow (ADR-0002, unit spec §1.4).

Invariants this implementation must satisfy (and the integration cross-check asserts):
- exactly one current version per key
- version intervals contiguous, non-overlapping
- effective dates from the SEQUENCE column, never processing time
- replay of an identical batch adds zero rows
Scope is frozen to the documented scenario set (review item R7).
"""

from pyspark.sql import DataFrame


def apply_scd2(
    target: DataFrame, changes: DataFrame, keys: list[str], sequence_col: str
) -> DataFrame:
    """Return the new full history state after applying a change batch."""
    raise NotImplementedError


def add_record_hash(df: DataFrame, non_key_cols: list[str]) -> DataFrame:
    """xxhash64 over coalesced non-key columns (no-change detection)."""
    raise NotImplementedError
