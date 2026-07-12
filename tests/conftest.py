"""Shared fixtures: local SparkSession, scenario-catalog loader, assertion helpers."""

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local PySpark session — no cluster, no Databricks. Pin TZ/decimal semantics (R3)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("retail-lakehouse-tests")
        .config("spark.sql.session.timeZone", "UTC")
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
