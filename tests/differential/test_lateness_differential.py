"""Lateness flag: Spark column vs ingest.batches.classify_lateness (the oracle).

Facts carry `_lateness` so beyond-window events are visible and countable — the
design has always been "flag, never drop". The boundary is the interesting part:
an event exactly `window_days` old is still in_window in both worlds.
"""

from datetime import datetime, timedelta, timezone

import pytest

from retail_lakehouse.ingest import batches
from retail_lakehouse.spark.declarations import lateness_column
from tests.differential import adapter, comparators

pytestmark = [pytest.mark.unit, pytest.mark.spark]

AS_OF = datetime(2026, 6, 13, 8, 0, 0, tzinfo=timezone.utc)
WINDOW = 7

OFFSETS = [
    timedelta(days=0),  # now
    timedelta(days=3),  # comfortably inside
    timedelta(days=7),  # exact boundary — inclusive
    timedelta(days=7, seconds=1),  # one second past the boundary
    timedelta(days=30),  # far beyond
    timedelta(days=-1),  # future event: lateness is not plausibility's job
]


def test_lateness_flag_matches_oracle_at_every_offset(spark):
    events = [{"event_ts": AS_OF - offset} for offset in OFFSETS]
    records = adapter.with_row_ids(events)

    oracle_rows = [
        {
            adapter.ROW_ID: r[adapter.ROW_ID],
            "_lateness": batches.classify_lateness(r["event_ts"], AS_OF, WINDOW),
        }
        for r in records
    ]

    df = adapter.to_typed_dataframe(spark, records, {"event_ts": "timestamp"})
    spark_rows = [
        {adapter.ROW_ID: r[adapter.ROW_ID], "_lateness": r["_lateness"]}
        for r in adapter.collect_normalized(
            df.withColumn("_lateness", lateness_column("event_ts", AS_OF, WINDOW))
        )
    ]

    comparators.assert_rows_equivalent(oracle_rows, spark_rows, case="lateness flag")


def test_boundary_is_inclusive_and_nothing_is_dropped(spark):
    records = adapter.with_row_ids(
        [{"event_ts": AS_OF - timedelta(days=WINDOW)}, {"event_ts": AS_OF - timedelta(days=8)}]
    )
    df = adapter.to_typed_dataframe(spark, records, {"event_ts": "timestamp"})
    rows = adapter.collect_normalized(
        df.withColumn("_lateness", lateness_column("event_ts", AS_OF, WINDOW))
    )
    assert [r["_lateness"] for r in rows] == [batches.IN_WINDOW, batches.BEYOND_WINDOW]
    assert len(rows) == len(records)  # flagged, never filtered
