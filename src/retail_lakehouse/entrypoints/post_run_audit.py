"""post-run-audit entry point: table counts -> ops.pipeline_runs + ops.dq_results."""

from __future__ import annotations

import sys

from retail_lakehouse.config import params


def main() -> None:
    from pyspark.sql import SparkSession

    p = params.from_task_args(sys.argv[1:])  # R1 guard before any write
    spark = SparkSession.builder.getOrCreate()
    from retail_lakehouse.audit.run_audit import write_run_audit

    counts = write_run_audit(spark, p)
    for row in counts:
        print(
            f"{row['source']:20} bronze={row['bronze_rows']:>7} valid={row['valid_rows']:>7} "
            f"quarantine={row['quarantine_rows']:>5} conserved={row['conserved']}"
        )
    # quarantine reason breakdown — the DQ evidence line for scenario runs
    from pyspark.sql import functions as F

    from retail_lakehouse.config import loader

    for source in loader.load_sources():
        quarantine = spark.table(p.qualified("silver", f"{source}_quarantine"))
        for row in quarantine.select(F.explode("reason").alias("r")).groupBy("r").count().collect():
            print(f"  reason {source}.{row['r']}: {row['count']}")
