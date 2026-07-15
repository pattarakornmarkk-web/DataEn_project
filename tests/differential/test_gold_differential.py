"""Gold compiler vs oracle: native paths get differentials, oracle-in-executor
paths get parity checks (same code, so equality is a wiring test, not a proof)."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from retail_lakehouse.spark import gold_compiler
from retail_lakehouse.transform import aggregates as agg_oracle
from retail_lakehouse.transform import funnel as funnel_oracle
from retail_lakehouse.transform import revenue as revenue_oracle
from retail_lakehouse.transform.fx import FxRates
from tests.differential import adapter

pytestmark = [pytest.mark.unit, pytest.mark.spark]

RATES = FxRates.from_records(
    [{"currency": "THB", "effective_date": date(2026, 1, 1), "per_base": "35"}]
)
TS = datetime(2026, 6, 6, 12, 0, tzinfo=timezone.utc)

EVENT_TYPES = {
    "order_id": "bigint",
    "event_type": "string",
    "customer_id": "bigint",
    "store_id": "int",
    "product_id": "bigint",
    "quantity": "int",
    "unit_price": "string",
    "currency": "string",
    "event_ts": "timestamp",
}


def _event(oid, etype, qty=1, price="10.00", ccy="USD", hours=0, cust=1, prod=7):
    return {
        "order_id": oid,
        "event_type": etype,
        "customer_id": cust,
        "store_id": 1,
        "product_id": prod,
        "quantity": qty,
        "unit_price": price,
        "currency": ccy,
        "event_ts": TS.replace(hour=12 + hours),
    }


EVENTS = [
    _event(101, "PLACED"),
    _event(101, "PAID", qty=2),
    _event(102, "PAID"),
    _event(102, "CANCELLED", hours=1),
    _event(103, "PLACED"),
    _event(104, "PAID", price="700.00", ccy="THB"),
]


class TestRevenueParity:
    def test_totals_and_lines_match_oracle(self, spark):
        df = adapter.to_typed_dataframe(spark, adapter.with_row_ids(EVENTS), EVENT_TYPES)
        lines = adapter.collect_normalized(
            gold_compiler.revenue_lines(df, RATES), sort_by_row_id=False
        )
        oracle = revenue_oracle.compute_line_revenue(
            revenue_oracle.resolve_order_lifecycle(EVENTS), RATES
        )
        assert {r["order_id"] for r in lines} == {ln.order_id for ln in oracle.lines}
        spark_total = sum(r["amount_base"] for r in lines)
        assert spark_total == oracle.total_base == Decimal("40.00")
        thb = next(r for r in lines if r["order_id"] == 104)
        assert (thb["amount_local"], thb["amount_base"]) == (Decimal("700.00"), Decimal("20.00"))


class TestAggregateDifferential:
    ROWS = [
        {"region": "N", "sale_date": "d1", "amount_base": "10.00"},
        {"region": "N", "sale_date": "d1", "amount_base": "5.50"},
        {"region": "S", "sale_date": "d2", "amount_base": "3.25"},
        {"region": None, "sale_date": "d1", "amount_base": "7.00"},
    ]

    def test_matches_oracle_groups_and_totals(self, spark):
        typed = [{**r, "amount_base": Decimal(r["amount_base"])} for r in self.ROWS]
        df = adapter.to_typed_dataframe(
            spark,
            adapter.with_row_ids(typed),
            {"region": "string", "sale_date": "string", "amount_base": "decimal(18,2)"},
        )
        spark_rows = {
            (r["sale_date"], r["region"]): (r["total"], r["row_count"])
            for r in adapter.collect_normalized(
                gold_compiler.daily_aggregate(df, ["sale_date", "region"]), sort_by_row_id=False
            )
        }
        oracle = agg_oracle.aggregate_sum(self.ROWS, ["sale_date", "region"], "amount_base")
        oracle_rows = {
            (dict(row.group)["sale_date"], dict(row.group)["region"]): (row.total, row.row_count)
            for row in oracle.rows
        }
        assert spark_rows == oracle_rows
        assert sum(t for t, _ in spark_rows.values()) == oracle.input_total


class TestAsOfJoin:
    def test_sale_joins_product_version_valid_at_sale_date(self, spark):
        lines = adapter.to_typed_dataframe(
            spark,
            adapter.with_row_ids(
                [
                    {"product_id": 7, "sale_date": date(2026, 3, 1)},
                    {"product_id": 7, "sale_date": date(2026, 7, 1)},
                ]
            ),
            {"product_id": "bigint", "sale_date": "date"},
        )
        hist = adapter.to_typed_dataframe(
            spark,
            adapter.with_row_ids(
                [
                    {
                        "product_id": 7,
                        "effective_date": date(2026, 1, 1),
                        "_to_effective_date": date(2026, 6, 1),
                        "_is_terminal": False,
                        "category": "OLD",
                        "unit_price": Decimal("10.00"),
                    },
                    {
                        "product_id": 7,
                        "effective_date": date(2026, 6, 1),
                        "_to_effective_date": None,
                        "_is_terminal": True,
                        "category": "NEW",
                        "unit_price": Decimal("12.00"),
                    },
                ]
            ),
            {
                "product_id": "bigint",
                "effective_date": "date",
                "_to_effective_date": "date",
                "_is_terminal": "boolean",
                "category": "string",
                "unit_price": "decimal(10,2)",
            },
        )
        rows = adapter.collect_normalized(gold_compiler.with_product_asof(lines, hist))
        by_date = {r["sale_date"]: r["category"] for r in rows}
        assert by_date == {date(2026, 3, 1): "OLD", date(2026, 7, 1): "NEW"}


class TestFunnelParity:
    def test_sessions_match_oracle(self, spark):
        activity = [
            {
                "session_id": "s1",
                "customer_id": 1,
                "activity_type": "ADD_TO_CART",
                "activity_ts": TS,
            },
            {"session_id": "s2", "customer_id": 2, "activity_type": "PAGE_VIEW", "activity_ts": TS},
            {
                "session_id": "s3",
                "customer_id": None,
                "activity_type": "ADD_TO_CART",
                "activity_ts": TS,
            },
        ]
        orders = [{"customer_id": 1, "order_id": 900, "event_ts": TS.replace(hour=13)}]
        activity_df = adapter.to_typed_dataframe(
            spark,
            adapter.with_row_ids(activity),
            {
                "session_id": "string",
                "customer_id": "bigint",
                "activity_type": "string",
                "activity_ts": "timestamp",
            },
        )
        orders_df = adapter.to_typed_dataframe(
            spark,
            adapter.with_row_ids(orders),
            {"customer_id": "bigint", "order_id": "bigint", "event_ts": "timestamp"},
        )
        spark_rows = {
            r["session_id"]: (r["stage"], r["converted"])
            for r in adapter.collect_normalized(
                gold_compiler.funnel_sessions(activity_df, orders_df, 24), sort_by_row_id=False
            )
        }
        oracle = funnel_oracle.attribute_conversions(activity, orders, 24)
        oracle_rows = {s.session_id: (s.stage, s.converted) for s in oracle.sessions}
        assert spark_rows == oracle_rows
        assert spark_rows["s1"] == (funnel_oracle.STAGE_CONVERTED, True)
        assert spark_rows["s3"][1] is False  # anonymous never converts
