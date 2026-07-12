"""Standing invariants — SHARED by tests/data_quality and the post-run job task.

- conservation: bronze = silver + quarantine per _batch_date
- exactly one current version per SCD2 key
- no overlapping/gapped version intervals
- quarantine rate below threshold per source
- no future _ingest_ts
- fct_sales revenue equals independent recomputation from silver
- audit-absence canary (R4): audit rows exist for every pipeline run
"""

from pyspark.sql import DataFrame


def check_conservation(bronze: DataFrame, silver: DataFrame, quarantine: DataFrame) -> DataFrame:
    """Per-batch counts; returns violations (empty = pass)."""
    raise NotImplementedError


def check_exactly_one_current(hist: DataFrame, keys: list[str]) -> DataFrame:
    raise NotImplementedError


def check_interval_integrity(hist: DataFrame, keys: list[str]) -> DataFrame:
    """Contiguity + non-overlap of __START_AT/__END_AT per key."""
    raise NotImplementedError
