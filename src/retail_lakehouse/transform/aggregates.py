"""Daily aggregates + the closure-safe MV factory (unit spec §1.6).

Contracts: conservation (sum of aggregate = sum of input, exactly); null dimensions
go to an explicit UNKNOWN bucket, never dropped; factory-generated functions bind
their segment via default argument (the regression fix for the original loop-closure bug).
"""

from pyspark.sql import DataFrame


def sales_by_dimension_daily(fct: DataFrame, dim_cols: list[str]) -> DataFrame:
    raise NotImplementedError


def make_segment_aggregate(segment_col: str, segment_value: str):
    """Factory returning a segment-filtered aggregate function. Late-binding-safe."""
    raise NotImplementedError
