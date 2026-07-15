"""Shared fixtures: local SparkSession, scenario-catalog loader, assertion helpers."""

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local PySpark session — no cluster, no Databricks.

    Timezone is pinned at EVERY layer (R3): session TZ governs SQL semantics, but
    collect() converts timestamps via the JVM/system zone — the differential harness
    caught a +7h Asia/Bangkok shift when only session TZ was set.
    """
    import os
    import pathlib
    import time

    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    # executor Python workers need the package too (applyInPandas ships oracle code);
    # pytest's pythonpath only patches the driver's sys.path
    src = str(pathlib.Path(__file__).parents[1] / "src")
    os.environ["PYTHONPATH"] = src + os.pathsep + os.environ.get("PYTHONPATH", "")
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("retail-lakehouse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def load_scenario(spark):
    """Scenario -> per-source DataFrames, via the SINGLE shared interpreter
    (retail_lakehouse.scenarios) — never parse scenario YAML anywhere else."""

    def _load(name: str):
        from retail_lakehouse.scenarios import generator, parser

        generator.generate_batches(parser.load_scenario(name))  # rows -> DataFrames below
        raise NotImplementedError  # TODO: rows -> DataFrames via spark.createDataFrame

    return _load


# TODO shared assertion helpers:
#   assert_schema_equal, assert_conservation, assert_interval_integrity,
#   assert_exactly_one_current
