"""Keyed deduplication (unit spec §1.2).

Contract: keep latest by sequence column; ties broken by the contract-declared
tiebreak column, descending (ADR-0009); stable under input order shuffling;
total on empty/single-row input.
"""

from pyspark.sql import DataFrame


def dedup_by_key(df: DataFrame, keys: list[str], sequence_col: str, tiebreak_col: str) -> DataFrame:
    raise NotImplementedError
