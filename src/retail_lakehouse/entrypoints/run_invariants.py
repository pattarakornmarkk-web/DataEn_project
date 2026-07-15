"""run-invariants entry point: standing checks; violations fail the job task."""

from __future__ import annotations

import sys

from retail_lakehouse.config import params


def main() -> None:
    from pyspark.sql import SparkSession

    p = params.from_task_args(sys.argv[1:])
    spark = SparkSession.builder.getOrCreate()
    from retail_lakehouse.audit import invariants

    violations = invariants.run_all(spark, p)
    if violations:
        for violation in violations:
            print(f"INVARIANT VIOLATION: {violation}")
        sys.exit(1)
    print("all invariants healthy")
