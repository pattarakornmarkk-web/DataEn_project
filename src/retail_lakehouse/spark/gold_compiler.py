"""Gold compiler — two mechanisms per the approved slice-7 design:

ORACLE-IN-EXECUTOR (applyInPandas / cogroup): revenue lifecycle and funnel
attribution run the ACTUAL oracle per group inside executors — zero translation
risk for stateful logic; money travels as strings and is cast to decimal once.

NATIVE + DIFFERENTIAL: daily aggregates (UNKNOWN bucketing, exact decimal sums)
and the as-of SCD2 product join — simple relational shapes with differential tests.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail_lakehouse.spark.scd2_compiler import TERMINAL_COL
from retail_lakehouse.transform import funnel as funnel_oracle
from retail_lakehouse.transform import revenue as revenue_oracle
from retail_lakehouse.transform.aggregates import UNKNOWN
from retail_lakehouse.transform.fx import FxRates

_REVENUE_SCHEMA = (
    "order_id long, customer_id long, store_id int, product_id long, sale_date date, "
    "quantity int, currency string, amount_local string, amount_base string"
)
_FUNNEL_SCHEMA = (
    "session_id string, customer_id long, stage int, converted boolean, "
    "cart_regressed boolean, session_date date"
)


def revenue_lines(order_events: DataFrame, rates: FxRates) -> DataFrame:
    """Oracle-in-executor: lifecycle resolution + FX per order group (ADR-0005 semantics)."""

    def resolve(pdf):
        import pandas as pd

        events = pdf.to_dict("records")
        resolutions = revenue_oracle.resolve_order_lifecycle(events)
        result = revenue_oracle.compute_line_revenue(resolutions, rates)
        paid = {r.order_id: r.paid_event for r in resolutions if r.paid_event}
        rows = [
            {
                "order_id": line.order_id,
                "customer_id": paid[line.order_id].get("customer_id"),
                "store_id": paid[line.order_id].get("store_id"),
                "product_id": paid[line.order_id].get("product_id"),
                "sale_date": paid[line.order_id]["event_ts"].date(),
                "quantity": line.quantity,
                "currency": line.currency,
                "amount_local": str(line.amount_local),
                "amount_base": str(line.amount_base),
            }
            for line in result.lines
        ]
        return pd.DataFrame(rows, columns=[c.split()[0] for c in _REVENUE_SCHEMA.split(", ")])

    lines = order_events.groupBy("order_id").applyInPandas(resolve, schema=_REVENUE_SCHEMA)
    return lines.withColumn(
        "amount_local", F.col("amount_local").try_cast("decimal(18,2)")
    ).withColumn("amount_base", F.col("amount_base").try_cast("decimal(18,2)"))


def with_product_asof(lines: DataFrame, product_hist: DataFrame) -> DataFrame:
    """As-of SCD2 join: product attributes valid AT the sale date (never just current)."""
    hist = product_hist.select(
        F.col("product_id").alias("_p_id"),
        F.col("effective_date").alias("_p_from"),
        F.col("_to_effective_date").alias("_p_to"),
        F.col(TERMINAL_COL).alias("_p_open"),
        F.col("category"),
        F.col("unit_price").alias("list_price"),
    )
    condition = (
        (F.col("product_id") == F.col("_p_id"))
        & (F.col("sale_date") >= F.col("_p_from"))
        & (F.col("_p_open") | (F.col("sale_date") < F.col("_p_to")))
    )
    return lines.join(hist, condition, "left").drop("_p_id", "_p_from", "_p_to", "_p_open")


def daily_aggregate(df: DataFrame, dims: list[str], amount_col: str = "amount_base") -> DataFrame:
    """Native aggregate with oracle semantics: UNKNOWN bucketing, exact decimal sums."""
    groups = [F.coalesce(F.col(d).cast("string"), F.lit(UNKNOWN)).alias(d) for d in dims]
    return (
        df.groupBy(*groups)
        .agg(F.sum(amount_col).alias("total"), F.count(F.lit(1)).alias("row_count"))
        .orderBy(*[F.asc_nulls_first(d) for d in dims])
    )


def funnel_sessions(activity: DataFrame, paid_orders: DataFrame, window_hours: int) -> DataFrame:
    """Oracle-in-executor: session funnel per customer cogroup (anonymous included)."""

    def attribute(key, activity_pdf, orders_pdf):
        import pandas as pd

        result = funnel_oracle.attribute_conversions(
            activity_pdf.to_dict("records"), orders_pdf.to_dict("records"), window_hours
        )
        rows = [
            {
                "session_id": s.session_id,
                "customer_id": s.customer_id,
                "stage": s.stage,
                "converted": s.converted,
                "cart_regressed": s.cart_regressed,
                "session_date": (s.last_cart_add_ts or activity_pdf["activity_ts"].min()).date(),
            }
            for s in result.sessions
        ]
        return pd.DataFrame(rows, columns=[c.split()[0] for c in _FUNNEL_SCHEMA.split(", ")])

    orders = paid_orders.select("customer_id", "order_id", "event_ts")
    return (
        activity.groupBy("customer_id")
        .cogroup(orders.groupBy("customer_id"))
        .applyInPandas(attribute, schema=_FUNNEL_SCHEMA)
    )
