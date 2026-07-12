"""Currency normalization (THB -> USD). Original amount + currency always retained."""

from pyspark.sql import DataFrame


def normalize_currency(df: DataFrame, amount_col: str, currency_col: str) -> DataFrame:
    """Add normalized amount column; applied exactly once (idempotent on re-call)."""
    raise NotImplementedError
