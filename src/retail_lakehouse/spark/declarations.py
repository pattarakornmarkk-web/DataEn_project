"""SDP object declarations — the runtime DAG, built from registries + compilers.

transforms/ files call register_bronze/register_silver/register_gold with the dp
module; everything here is closure-safe (factory-bound, the agg_sales lesson) and
resolves names through PipelineParams (dev-prefixed schemas). The R1 catalog guard
runs inside params resolution at graph build.

Approved slice-7 architecture: bronze = Auto Loader streaming tables; silver/gold =
materialized views calling the differential-proven compilers; dims via
scd2_compiler (auto_cdc_flow deferred); revenue/funnel via oracle-in-executor.

Runtime tiebreak (approved adjustment): silver composes
_ingestion_order := concat(_source_file, '#', lpad(<in-file row idx>, 9, '0'))
so ordering is (sequence, _source_file, in-file row order), deterministic across
files and within a file.
"""

from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from retail_lakehouse.config import loader
from retail_lakehouse.config import params as params_mod
from retail_lakehouse.dq.rules import bind_ruleset
from retail_lakehouse.ingest.batches import CdcSpec, bind_cdc_spec
from retail_lakehouse.spark import cdc_compiler, dq_compiler, gold_compiler, scd2_compiler
from retail_lakehouse.transform.fx import FxRates

CUSTOMER_ATTRS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "birth_date",
    "loyalty_tier",
    "home_store_id",
]


@lru_cache(maxsize=1)
def _ctx():
    spark = SparkSession.getActiveSession()
    p = params_mod.from_spark_conf(spark)  # R1 guard fires here, at graph build
    return {
        "spark": spark,
        "params": p,
        "sources": loader.load_sources(),
        "contracts": loader.load_contracts(),
        "rules": loader.load_dq_rules(),
        "as_of": datetime.now(timezone.utc),
        "rates": FxRates.from_records(loader.load_fx_rates()),
    }


def _bronze_name(source):
    return _ctx()["params"].qualified("bronze", f"{source}_raw")


def _silver_name(table):
    return _ctx()["params"].qualified("silver", table)


def _gold_name(table):
    return _ctx()["params"].qualified("gold", table)


# ------------------------------------------------------------------- BRONZE
def _bronze_reader(rel_path: str, columns: tuple, fmt: str):
    ctx = _ctx()
    p = ctx["params"]
    schema = T.StructType(
        [T.StructField(c, T.StringType(), True) for c in (*columns, "_ingestion_order")]
    )
    reader = (
        ctx["spark"]
        .readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(schema)
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    df = reader.load(f"/Volumes/{p.catalog}/{p.landing_schema}/files/{rel_path}")
    return df.select(
        "*",
        F.current_timestamp().alias("_ingest_ts"),
        F.col("_metadata.file_name").alias("_source_file"),
        F.col("_metadata.file_path").alias("_file_path"),
        F.col("_metadata.file_size").alias("_file_size"),
        F.col("_metadata.file_modification_time").alias("_file_mod_ts"),
        F.to_date(
            F.regexp_extract(F.col("_metadata.file_name"), r"_(\d{8})_", 1), "yyyyMMdd"
        ).alias("_batch_date"),
    )


def register_bronze(dp) -> None:
    ctx = _ctx()
    for source, cfg in ctx["sources"].items():
        target = _bronze_name(source)
        dp.create_streaming_table(name=target)
        emitters = cfg.get("emitters") or {"main": cfg["path"]}
        columns = tuple(ctx["contracts"]["source_contracts"][source]["columns"])
        for emitter, rel_path in emitters.items():

            def make_flow(path=rel_path, cols=columns, fmt=cfg["format"]):
                def flow():
                    return _bronze_reader(path, cols, fmt)

                return flow

            flow_fn = make_flow()
            flow_fn.__name__ = f"{source}_from_{emitter}"
            dp.append_flow(target=target, name=flow_fn.__name__)(flow_fn)


# ------------------------------------------------------------------- SILVER
_LINEAGE_KEEP = ["_source_file", "_ingestion_order", "_batch_date"]


def _validated(source: str):
    """Bronze read -> composite tiebreak -> compiled validation (batch semantics)."""
    ctx = _ctx()
    ruleset = bind_ruleset(
        source, ctx["contracts"]["source_contracts"][source], ctx["rules"][source]
    )
    df = (
        ctx["spark"]
        .read.table(_bronze_name(source))
        .withColumn(
            "_ingestion_order",
            F.concat(  # (sequence, _source_file, in-file row order) — approved composite
                F.col("_source_file"),
                F.lit("#"),
                F.lpad(F.coalesce(F.col("_ingestion_order"), F.lit("0")), 9, "0"),
            ),
        )
    )
    return dq_compiler.validated_df(df, ruleset, ctx["as_of"]), ruleset


def register_silver(dp) -> None:
    ctx = _ctx()
    for source in ctx["sources"]:

        def make_valid(src=source):
            def valid():
                validated, ruleset = _validated(src)
                return dq_compiler.split(validated, ruleset, passthrough=_LINEAGE_KEEP)[0]

            return valid

        def make_quarantine(src=source):
            def quarantine():
                validated, ruleset = _validated(src)
                return dq_compiler.split(validated, ruleset, passthrough=_LINEAGE_KEEP)[1]

            return quarantine

        dp.materialized_view(name=_silver_name(f"{source}_valid"))(make_valid())
        dp.materialized_view(name=_silver_name(f"{source}_quarantine"))(make_quarantine())

    _register_dims(dp)


def _customer_event_stream():
    """ADR-0004 unification: snapshots -> synthetic upserts, unioned with CDC events."""
    ctx = _ctx()
    spark = ctx["spark"]
    updates = spark.read.table(_silver_name("customer_updates_valid")).select(
        *CUSTOMER_ATTRS, "op", "source_system", "change_ts", "change_seq"
    )
    snapshots = spark.read.table(_silver_name("customers_valid")).select(
        *CUSTOMER_ATTRS,
        F.lit(None).cast("string").alias("op"),  # op-less -> upsert (snapshot branch)
        F.lit("SNAPSHOT").alias("source_system"),
        F.col("updated_at").alias("change_ts"),
        F.lit(None).cast("bigint").alias("change_seq"),  # nulls-first: loses ties to CDC
    )
    spec = bind_cdc_spec(
        _ctx()["contracts"]["entity_contracts"]["dim_customer"],
        _ctx()["contracts"]["source_contracts"]["customer_updates"],
    )
    return cdc_compiler.normalized_stream(updates.unionByName(snapshots), spec, eager_counts=False)


def _snapshot_spec(entity: str, source: str) -> CdcSpec:
    ctx = _ctx()
    return bind_cdc_spec(
        ctx["contracts"]["entity_contracts"][entity], ctx["contracts"]["source_contracts"][source]
    )


def _register_dims(dp) -> None:
    def dim_customer_hist():
        stream = _customer_event_stream()
        return scd2_compiler.rebuild_history(
            None,
            stream.applied,
            keys=["customer_id"],
            seq_cols=["change_ts", "change_seq"],
            attr_cols=[*CUSTOMER_ATTRS, "source_system"],
        )

    def dim_customer_held_orphans():
        return _customer_event_stream().held_orphans  # ADR-0006: inspectable, never applied

    def dim_product_hist():
        events = _ctx()["spark"].read.table(_silver_name("products_valid"))
        stream = cdc_compiler.normalized_stream(
            events, _snapshot_spec("dim_product", "products"), eager_counts=False
        )
        return scd2_compiler.rebuild_history(
            None,
            stream.applied,
            keys=["product_id"],
            seq_cols=["effective_date", "_ingestion_order"],
            attr_cols=[
                "product_id",
                "sku",
                "product_name",
                "category",
                "subcategory",
                "unit_price",
                "currency",
                "is_active",
            ],
        )

    def dim_store_hist():
        events = _ctx()["spark"].read.table(_silver_name("stores_valid"))
        stream = cdc_compiler.normalized_stream(
            events, _snapshot_spec("dim_store", "stores"), eager_counts=False
        )
        return scd2_compiler.rebuild_history(
            None,
            stream.applied,
            keys=["store_id"],
            seq_cols=["update_date", "_ingestion_order"],
            attr_cols=[
                "store_id",
                "store_name",
                "region",
                "store_format",
                "manager_name",
                "opened_date",
                "closed_date",
            ],
        )

    for fn in (dim_customer_hist, dim_customer_held_orphans, dim_product_hist, dim_store_hist):
        dp.materialized_view(name=_silver_name(fn.__name__))(fn)


# --------------------------------------------------------------------- GOLD
def register_gold(dp) -> None:
    ctx = _ctx()
    spark = ctx["spark"]

    for entity in ("customer", "product", "store"):

        def make_current(name=entity):
            def current():
                return scd2_compiler.current_rows(
                    spark.read.table(_silver_name(f"dim_{name}_hist"))
                )

            return current

        current_fn = make_current()
        current_fn.__name__ = f"dim_{entity}"
        dp.materialized_view(name=_gold_name(f"dim_{entity}"))(current_fn)

    def fct_sales():
        lines = gold_compiler.revenue_lines(
            spark.read.table(_silver_name("order_events_valid")), ctx["rates"]
        )
        return gold_compiler.with_product_asof(
            lines, spark.read.table(_silver_name("dim_product_hist"))
        )

    def sales_per_region_daily():
        stores = spark.read.table(_gold_name("dim_store")).select("store_id", "region")
        joined = spark.read.table(_gold_name("fct_sales")).join(stores, "store_id", "left")
        return gold_compiler.daily_aggregate(joined, ["sale_date", "region"])

    def sales_per_store_daily():
        return gold_compiler.daily_aggregate(
            spark.read.table(_gold_name("fct_sales")), ["sale_date", "store_id"]
        )

    def funnel_conversion_daily():
        sessions = gold_compiler.funnel_sessions(
            spark.read.table(_silver_name("customer_activity_valid")),
            spark.read.table(_silver_name("order_events_valid")).filter(
                F.col("event_type") == "PAID"
            ),
            window_hours=24,
        )
        return sessions.groupBy("session_date").agg(
            F.count(F.lit(1)).alias("sessions"),
            F.sum(F.when(F.col("stage") >= 2, 1).otherwise(0)).alias("carted"),
            F.sum(F.when(F.col("converted"), 1).otherwise(0)).alias("converted"),
        )

    def dq_reconciliation():
        # fully lazy — actions are illegal at SDP graph-build time
        parts = []
        for source in ctx["sources"]:
            bronze = spark.read.table(_bronze_name(source)).agg(
                F.count(F.lit(1)).alias("bronze_rows")
            )
            valid = spark.read.table(_silver_name(f"{source}_valid")).agg(
                F.count(F.lit(1)).alias("valid_rows")
            )
            quarantine = spark.read.table(_silver_name(f"{source}_quarantine")).agg(
                F.count(F.lit(1)).alias("quarantine_rows")
            )
            parts.append(
                bronze.crossJoin(valid)
                .crossJoin(quarantine)
                .select(
                    F.lit(source).alias("source"),
                    "bronze_rows",
                    "valid_rows",
                    "quarantine_rows",
                    (F.col("bronze_rows") == F.col("valid_rows") + F.col("quarantine_rows")).alias(
                        "conserved"
                    ),
                )
            )
        result = parts[0]
        for part in parts[1:]:
            result = result.unionByName(part)
        return result

    for fn in (
        fct_sales,
        sales_per_region_daily,
        sales_per_store_daily,
        funnel_conversion_daily,
        dq_reconciliation,
    ):
        dp.materialized_view(name=_gold_name(fn.__name__))(fn)
