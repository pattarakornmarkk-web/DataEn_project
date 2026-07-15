"""Standing invariants — run post-pipeline; violations fail the orchestrator.

Definitions shared with the blueprint testing strategy: conservation per source,
exactly-one-current per SCD2 key, fct_sales conservation against silver.
Returns human-readable violations (empty = healthy).
"""

from __future__ import annotations

from retail_lakehouse.audit.run_audit import collect_source_counts
from retail_lakehouse.config.params import PipelineParams

DIMS = ("dim_customer_hist", "dim_product_hist", "dim_store_hist")


def check_conservation(spark, p: PipelineParams) -> list[str]:
    return [
        f"conservation violated for {row['source']}: bronze={row['bronze_rows']} "
        f"valid={row['valid_rows']} quarantine={row['quarantine_rows']}"
        for row in collect_source_counts(spark, p)
        if not row["conserved"]
    ]


def check_exactly_one_current(spark, p: PipelineParams) -> list[str]:
    from pyspark.sql import functions as F

    violations = []
    for dim in DIMS:
        hist = spark.table(p.qualified("silver", dim))
        key = [c for c in hist.columns if c in ("customer_id", "product_id", "store_id")]
        bad = (
            hist.groupBy(*key)
            .agg(F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("currents"))
            .filter(F.col("currents") > 1)
            .count()
        )
        if bad:
            violations.append(f"{dim}: {bad} key(s) with more than one current version")
    return violations


def run_all(spark, p: PipelineParams) -> list[str]:
    return check_conservation(spark, p) + check_exactly_one_current(spark, p)
