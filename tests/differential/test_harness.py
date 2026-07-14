"""Phase 6A end-to-end harness verification — no compiler logic yet.

Proves: Spark session on this runner, explicit schema mapping (spark/types.py),
adapter round-trips, normalizers (Decimal / timestamp / date / boolean / null),
and the two ordering primitives every compiler will rely on.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from retail_lakehouse.dq.engine import coerce_value
from retail_lakehouse.transform.dedup import sequence_sort_key
from tests.differential import adapter, comparators

pytestmark = [pytest.mark.unit, pytest.mark.spark]

COLUMN_TYPES = {
    "id": "bigint",
    "name": "string",
    "price": "decimal(10,2)",
    "born": "date",
    "seen_ts": "timestamp",
    "active": "boolean",
}
RAW_RECORDS = [
    {
        "id": "42",
        "name": "n1",
        "price": "2.78",
        "born": "1990-05-01",
        "seen_ts": "2026-06-06T08:00:00Z",
        "active": "true",
    },
    {
        "id": "43",
        "name": None,
        "price": "20.00",
        "born": None,
        "seen_ts": "2026-06-06T09:30:00Z",
        "active": "N",
    },
    {
        "id": None,
        "name": "dup",
        "price": None,
        "born": "2001-01-01",
        "seen_ts": None,
        "active": None,
    },
    {
        "id": None,
        "name": "dup",
        "price": None,
        "born": "2001-01-01",
        "seen_ts": None,
        "active": None,
    },  # exact duplicate: _row_id must still separate them
]


def _oracle_typed(records):
    return [
        {
            adapter.ROW_ID: r[adapter.ROW_ID],
            **{col: coerce_value(r.get(col), ctype) for col, ctype in COLUMN_TYPES.items()},
        }
        for r in records
    ]


def test_typed_round_trip_matches_oracle_coercion(spark):
    records = adapter.with_row_ids(RAW_RECORDS)
    oracle_rows = _oracle_typed(records)
    df = adapter.to_typed_dataframe(spark, oracle_rows, COLUMN_TYPES)
    comparators.assert_rows_equivalent(
        oracle_rows, adapter.collect_normalized(df), case="typed round-trip"
    )


def test_decimal_precision_survives_exactly(spark):
    records = adapter.with_row_ids([{"price": "2.78"}, {"price": "20.00"}])
    typed = [{**r, "price": Decimal(r["price"])} for r in records]
    rows = adapter.collect_normalized(
        adapter.to_typed_dataframe(spark, typed, {"price": "decimal(10,2)"})
    )
    assert [r["price"] for r in rows] == [Decimal("2.78"), Decimal("20.00")]
    assert all(isinstance(r["price"], Decimal) for r in rows)


def test_timestamp_normalization_is_lossless(spark):
    aware = datetime(2026, 6, 6, 8, 0, tzinfo=timezone.utc)
    rows = adapter.collect_normalized(
        adapter.to_typed_dataframe(
            spark, adapter.with_row_ids([{"ts": aware}]), {"ts": "timestamp"}
        )
    )
    assert rows[0]["ts"] == aware  # aware -> naive-UTC -> aware, byte-equal


def test_null_ordering_primitive_matches_oracle(spark):
    """asc_nulls_first == oracle sequence_sort_key ascending — the compiler's bedrock."""
    values = ["t2", None, "t1", "t3", None]
    records = adapter.with_row_ids([{"seq": v} for v in values])
    oracle_order = [
        r[adapter.ROW_ID] for r in sorted(records, key=lambda r: sequence_sort_key((r["seq"],)))
    ]
    df = adapter.to_raw_dataframe(spark, records, ["seq"])
    spark_order = [
        r[adapter.ROW_ID]
        for r in adapter.collect_normalized(
            df.orderBy(F.asc_nulls_first("seq"), F.asc(adapter.ROW_ID)),
            sort_by_row_id=False,  # the DataFrame's own ordering is what's under test
        )
    ]
    # oracle sort is stable; enforce same explicit secondary key for the comparison
    assert spark_order == oracle_order


def test_desc_nulls_last_means_nulls_lose_max_selection(spark):
    records = adapter.with_row_ids([{"seq": None}, {"seq": "t1"}])
    df = adapter.to_raw_dataframe(spark, records, ["seq"])
    winner = df.orderBy(F.desc_nulls_last("seq")).first()
    assert winner["seq"] == "t1"  # real value beats null, both worlds


def test_date_and_boolean_round_trip(spark):
    rows = adapter.collect_normalized(
        adapter.to_typed_dataframe(
            spark,
            adapter.with_row_ids([{"d": date(2026, 7, 6), "b": False}]),
            {"d": "date", "b": "boolean"},
        )
    )
    assert rows[0]["d"] == date(2026, 7, 6) and rows[0]["b"] is False
