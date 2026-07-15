"""ops.pipeline_runs + ops.dq_results — computed DIRECTLY from tables (approved
slice-7 design: no event-log dependency on the critical path).

Every row carries git_sha + deployed_by: any number in ops traces to a commit.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

from retail_lakehouse.config import loader
from retail_lakehouse.config.params import PipelineParams

RUNS_DDL = (
    "run_id STRING, run_ts TIMESTAMP, environment STRING, git_sha STRING, "
    "deployed_by STRING, source STRING, bronze_rows BIGINT, valid_rows BIGINT, "
    "quarantine_rows BIGINT, conserved BOOLEAN"
)
DQ_DDL = (
    "run_id STRING, run_ts TIMESTAMP, source STRING, reason STRING, "
    "violations BIGINT, git_sha STRING"
)


def run_identity() -> tuple[str, datetime]:
    run_id = os.environ.get("DATABRICKS_JOB_RUN_ID") or f"local-{uuid.uuid4().hex[:12]}"
    return run_id, datetime.now(timezone.utc)


def ensure_ops_tables(spark, p: PipelineParams) -> None:
    ops = f"{p.catalog}.{p.ops_schema}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ops}.pipeline_runs ({RUNS_DDL})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ops}.dq_results ({DQ_DDL})")


def collect_source_counts(spark, p: PipelineParams) -> list[dict]:
    rows = []
    for source in loader.load_sources():
        bronze = spark.table(p.qualified("bronze", f"{source}_raw")).count()
        valid = spark.table(p.qualified("silver", f"{source}_valid")).count()
        quarantine = spark.table(p.qualified("silver", f"{source}_quarantine")).count()
        rows.append(
            {
                "source": source,
                "bronze_rows": bronze,
                "valid_rows": valid,
                "quarantine_rows": quarantine,
                "conserved": bronze == valid + quarantine,
            }
        )
    return rows


def write_run_audit(spark, p: PipelineParams) -> list[dict]:
    run_id, run_ts = run_identity()
    ensure_ops_tables(spark, p)
    counts = collect_source_counts(spark, p)
    ops = f"{p.catalog}.{p.ops_schema}"

    runs = [
        {
            "run_id": run_id,
            "run_ts": run_ts,
            "environment": p.environment_tag,
            "git_sha": p.git_sha,
            "deployed_by": p.deployed_by,
            **row,
        }
        for row in counts
    ]
    spark.createDataFrame(runs, schema=RUNS_DDL).write.mode("append").saveAsTable(
        f"{ops}.pipeline_runs"
    )

    dq_rows = []
    for source in loader.load_sources():
        quarantine = spark.table(p.qualified("silver", f"{source}_quarantine"))
        from pyspark.sql import functions as F

        for row in (
            quarantine.select(F.explode("reason").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        ):
            dq_rows.append(
                {
                    "run_id": run_id,
                    "run_ts": run_ts,
                    "source": source,
                    "reason": row["reason"],
                    "violations": row["count"],
                    "git_sha": p.git_sha,
                }
            )
    if dq_rows:
        spark.createDataFrame(dq_rows, schema=DQ_DDL).write.mode("append").saveAsTable(
            f"{ops}.dq_results"
        )
    return counts
