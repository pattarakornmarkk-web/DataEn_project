"""CDC compiler: contract-coerced records -> normalized event stream (Spark).

Verdicts must equal transform.cdc.normalize (oracle):
  - op normalization: 'D' stays delete, everything else (incl. missing op column,
    the snapshot branch of ADR-0004) is an upsert
  - replay drop: identical (key, op, sequence, payload) collapses to one, counted
  - ordering-tie conflicts: winner by (op desc, canonical payload desc); losers kept
  - orphan deletes: D with no EARLIER-sequenced upsert for an unknown key ->
    tagged and held, never applied, never dropped (ADR-0006)
  - output ordered by (key, sequence) with EXPLICIT null placement (nulls first,
    matching the oracle's None-lowest rule)

Position in the pipeline: input is post-DQ, contract-coerced data (typed columns).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from retail_lakehouse.ingest.batches import CdcSpec
from retail_lakehouse.spark.types import ROW_ID

OP_COL = "_op"
ORPHAN_COL = "is_orphan_delete"


@dataclass(frozen=True)
class CompiledCdcStream:
    applied: DataFrame  # (key, _op, sequence, payload) — apply these, in order
    held_orphans: DataFrame  # ADR-0006: tagged, held
    conflicts: DataFrame  # ordering-tie losers (flagged)
    replays_dropped: int


def _seq_cols(spec: CdcSpec) -> list[str]:
    return [*spec.sequence_by, spec.tiebreak]


def _payload_cols(df: DataFrame, spec: CdcSpec) -> list[str]:
    excluded = {spec.op_column, *_seq_cols(spec), OP_COL, ROW_ID}
    return [c for c in df.columns if c not in excluded]


def _canonical(payload_cols: list[str]):
    """Deterministic content ordering within a slot (winner selection only —
    never compared against the oracle's canonical form, verdicts are)."""
    return F.to_json(F.struct(*[F.col(c) for c in sorted(payload_cols)]))


def normalized_stream(
    df: DataFrame, spec: CdcSpec, known_keys_df: DataFrame | None = None
) -> CompiledCdcStream:
    """Replay-drop, conflict-resolve, orphan-hold, and order the stream."""
    if ROW_ID in df.columns:
        df = df.drop(ROW_ID)  # event identity is content, not row identity

    if spec.op_column and spec.op_column in df.columns:
        op = F.when(F.trim(F.coalesce(F.col(spec.op_column), F.lit(""))) == "D", "D").otherwise("U")
        df = df.withColumn(OP_COL, op).drop(spec.op_column)
    else:
        df = df.withColumn(OP_COL, F.lit("U"))  # snapshot branch: synthetic upserts

    seq_cols = _seq_cols(spec)
    payload_cols = _payload_cols(df, spec)
    keys = list(spec.keys)

    # 1) exact replays collapse to one
    input_count = df.count()
    deduped = df.dropDuplicates([*keys, OP_COL, *seq_cols, *payload_cols])
    replays = input_count - deduped.count()

    # 2) ordering-tie conflicts: one winner per (key, sequence) slot
    slot_window = Window.partitionBy(*keys, *seq_cols).orderBy(
        F.desc_nulls_last(OP_COL), F.desc_nulls_last(_canonical(payload_cols))
    )
    ranked = deduped.withColumn("_slot_rank", F.row_number().over(slot_window))
    conflicts = ranked.filter(F.col("_slot_rank") > 1).drop("_slot_rank")
    resolved = ranked.filter(F.col("_slot_rank") == 1).drop("_slot_rank")

    # 3) orphan deletes: no upsert with an EARLIER sequence, key unknown to the target
    key_window = (
        Window.partitionBy(*keys)
        .orderBy(*[F.asc_nulls_first(c) for c in seq_cols])
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    prior_upserts = F.coalesce(
        F.sum(F.when(F.col(OP_COL) == "U", 1).otherwise(0)).over(key_window), F.lit(0)
    )
    resolved = resolved.withColumn("_prior_upserts", prior_upserts)
    if known_keys_df is not None:
        known = known_keys_df.select(*keys).distinct().withColumn("_known", F.lit(True))
        resolved = resolved.join(known, on=keys, how="left")
    else:
        resolved = resolved.withColumn("_known", F.lit(None).cast("boolean"))

    is_orphan = (F.col(OP_COL) == "D") & (F.col("_prior_upserts") == 0) & F.col("_known").isNull()
    resolved = resolved.withColumn(ORPHAN_COL, F.coalesce(is_orphan, F.lit(False)))

    ordering = [F.asc_nulls_first(c) for c in (*keys, *seq_cols)]
    cleanup = ("_prior_upserts", "_known")
    applied = resolved.filter(~F.col(ORPHAN_COL)).drop(*cleanup, ORPHAN_COL).orderBy(*ordering)
    held = resolved.filter(F.col(ORPHAN_COL)).drop(*cleanup).orderBy(*ordering)
    return CompiledCdcStream(
        applied=applied,
        held_orphans=held,
        conflicts=conflicts.orderBy(*ordering),
        replays_dropped=replays,
    )
