"""Fixture adapter — one dataset, two paths (approved _row_id strategy).

Records get a _row_id BEFORE the paths split, so oracle and Spark rows align by
identity even when content is fully duplicated. Schemas are always explicit.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from retail_lakehouse.spark.types import ROW_ID, raw_schema, typed_schema


def with_row_ids(records: list[dict]) -> list[dict]:
    return [{ROW_ID: i, **record} for i, record in enumerate(records)]


def to_raw_dataframe(spark, records: list[dict], columns: list[str]):
    """Bronze-shaped input: every business column as string; records carry _row_id."""
    schema = raw_schema(columns)
    rows = [tuple(r.get(f.name) for f in schema.fields) for r in records]
    return spark.createDataFrame(rows, schema)


def to_typed_dataframe(spark, records: list[dict], column_types: dict[str, str]):
    """Contract-coerced input; records carry _row_id."""
    schema = typed_schema(column_types)
    rows = [tuple(_to_spark_value(r.get(f.name)) for f in schema.fields) for r in records]
    return spark.createDataFrame(rows, schema)


def _to_spark_value(value):
    # Spark TimestampType is tz-less, interpreted via session TZ (pinned UTC):
    # aware-UTC oracle values are handed over as naive UTC.
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def normalize_value(value):
    """Spark -> comparison domain: naive timestamps become aware UTC; exact Decimals."""
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, list):
        return [normalize_value(v) for v in value]
    return value


def collect_normalized(df, sort_by_row_id: bool = True) -> list[dict]:
    """DataFrame -> list[dict], values normalized.

    sort_by_row_id=True (default) gives deterministic row alignment for content
    comparisons; pass False when the DataFrame's OWN ordering is what's under test.
    """
    rows = [{k: normalize_value(v) for k, v in row.asDict().items()} for row in df.collect()]
    if sort_by_row_id and rows and ROW_ID in rows[0]:
        rows.sort(key=lambda r: r[ROW_ID])
    return rows
