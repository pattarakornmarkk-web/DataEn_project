"""Session funnel attribution (activity -> order conversion, unit spec §1.6).

Attribution window boundary behavior is part of the contract — tested at the exact edge.
"""

from pyspark.sql import DataFrame


def attribute_conversions(activity: DataFrame, orders: DataFrame, window_hours: int) -> DataFrame:
    raise NotImplementedError
