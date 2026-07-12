"""Valid/quarantine partition — conservation by construction.

Contract: every input row appears exactly once across (valid, quarantine).
Valid rows are typed via try_cast against the contract; quarantine rows keep
original string values + reason array + lineage.
"""

from pyspark.sql import DataFrame


def split_valid(df: DataFrame, contract: dict) -> DataFrame:
    """Rows with empty reason, cast to contract types."""
    raise NotImplementedError


def split_quarantine(df: DataFrame) -> DataFrame:
    """Rows with non-empty reason, original values preserved."""
    raise NotImplementedError
