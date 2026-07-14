"""Unit spec §1.6 — aggregations, conservation, and the closure-bug regression test."""

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from retail_lakehouse.transform import aggregates, funnel

pytestmark = pytest.mark.unit


def _row(region, day, amount):
    return {"region": region, "sale_date": day, "amount_base": amount}


ROWS = [
    _row("N", "d1", "10.00"),
    _row("N", "d1", "5.50"),
    _row("N", "d2", "1.00"),
    _row("S", "d1", "20.00"),
    _row("S", "d2", "3.25"),
    _row(None, "d1", "7.00"),
    _row("N", "d1", "2.25"),
    _row("S", "d2", "0.75"),
]


def test_conservation_sum_aggregate_equals_sum_input():
    result = aggregates.sales_by_dimension_daily(ROWS, dims=["region"])
    assert result.conserved
    assert sum((r.total for r in result.rows), Decimal("0")) == Decimal("49.75")


def test_null_dimension_goes_to_unknown_bucket():
    result = aggregates.sales_by_dimension_daily(ROWS, dims=["region"])
    unknown = next(r for r in result.rows if dict(r.group)["region"] == aggregates.UNKNOWN)
    assert unknown.total == Decimal("7.00")  # never silently dropped


def test_group_count_exact():
    result = aggregates.sales_by_dimension_daily(ROWS, dims=["region"])
    assert len(result.rows) == 5  # (d1,N)(d1,S)(d1,UNKNOWN)(d2,N)(d2,S)
    d1_n = next(r for r in result.rows if dict(r.group) == {"sale_date": "d1", "region": "N"})
    assert (d1_n.total, d1_n.row_count) == (Decimal("17.75"), 3)


def test_factory_generated_functions_filter_own_segment():
    make = aggregates.make_segment_aggregate
    # built in a loop, exactly like the original agg_sales.py bug
    factories = {seg: make(["sale_date"], "amount_base", "region", seg) for seg in ["N", "S"]}
    north = factories["N"](ROWS)
    assert all(dict(r.group).get("sale_date") for r in north.rows)
    assert north.input_total == Decimal("18.75")  # only N rows


def test_factory_outputs_differ_between_segments():  # the closure-bug regression
    make = aggregates.make_segment_aggregate
    factories = [make(["sale_date"], "amount_base", "region", seg) for seg in ["N", "S"]]
    north, south = (f(ROWS) for f in factories)
    assert north != south
    assert factories[0].segment == ("region", "N") and factories[1].segment == ("region", "S")


def test_deterministic_grouping_and_replay_safety():
    baseline = aggregates.sales_by_dimension_daily(ROWS, dims=["region"])
    assert baseline == aggregates.sales_by_dimension_daily(ROWS, dims=["region"])  # replay
    shuffled = ROWS[:]
    random.Random(4).shuffle(shuffled)
    assert aggregates.sales_by_dimension_daily(shuffled, dims=["region"]) == baseline


def test_empty_input_conserves_zero():
    result = aggregates.aggregate_sum([], group_by=["region"], amount_col="amount_base")
    assert result.rows == () and result.conserved and result.input_total == Decimal("0")


def test_funnel_attribution_window_boundary():
    cart_ts = datetime(2026, 6, 6, 10, 0, tzinfo=timezone.utc)
    activity = [
        {
            "session_id": "s1",
            "customer_id": 1,
            "activity_type": "ADD_TO_CART",
            "activity_ts": cart_ts,
        },
        {
            "session_id": "s2",
            "customer_id": 2,
            "activity_type": "ADD_TO_CART",
            "activity_ts": cart_ts,
        },
    ]
    exactly_at_edge = {"customer_id": 1, "order_id": 11, "event_ts": cart_ts + timedelta(hours=24)}
    one_second_past = {
        "customer_id": 2,
        "order_id": 22,
        "event_ts": cart_ts + timedelta(hours=24, seconds=1),
    }
    result = funnel.attribute_conversions(activity, [exactly_at_edge, one_second_past], 24)
    by_session = {s.session_id: s for s in result.sessions}
    assert by_session["s1"].converted  # inclusive boundary
    assert not by_session["s2"].converted
