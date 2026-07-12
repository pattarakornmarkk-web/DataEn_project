"""Standard lineage column set for all bronze tables.

Adds: _ingest_ts, _source_file, _batch_date. Blueprint ref: Bronze Design §4.
"""

from pyspark.sql import DataFrame


def with_lineage_columns(df: DataFrame) -> DataFrame:
    """Append the standard lineage columns from file metadata + processing time."""
    raise NotImplementedError
