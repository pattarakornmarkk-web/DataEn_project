"""Reason aggregation — the packaged melt/unpivot pattern (survivor from fw_Mark.py).

Collapses _is_* flag columns into a per-row `reason` array of violated rule names.
"""

from pyspark.sql import DataFrame


def get_reason(df: DataFrame) -> DataFrame:
    """Melt flag columns -> reason array. Exact set semantics: no duplicate reasons."""
    raise NotImplementedError
