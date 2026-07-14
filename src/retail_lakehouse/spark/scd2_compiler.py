"""SCD2 batch compiler: event-merge + rebuild, verdict-equal to transform.scd2.

Same algorithm as the oracle: existing versions re-expressed as states, merged with
incoming events, per-slot conflicts resolved, no-change states collapsed by content,
then re-materialized with lead()/lag() windows. Late corrections and replays are
handled by construction.

Role: differential truth for the pipeline AND the comparator shape for auto_cdc_flow
(integration tier projects __START_AT/__END_AT onto this canonical relation).
Production streaming SCD2 remains auto_cdc_flow (ADR-0002).

Canonical relation: keys..., <seq cols> (valid_from), _to_<seq cols> (valid_to),
attrs..., is_current, is_deleted, _is_terminal (True = open-ended chain tail).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from retail_lakehouse.spark.cdc_compiler import OP_COL

_TOMBSTONE = "__tombstone__"
DELETED_COL = "is_deleted"
CURRENT_COL = "is_current"
TERMINAL_COL = "_is_terminal"


def _content_key(attr_cols: list[str]):
    return F.when(F.col(DELETED_COL), F.lit(_TOMBSTONE)).otherwise(
        F.to_json(F.struct(*[F.col(c) for c in sorted(attr_cols)]))
    )


def _history_states(history: DataFrame, keys, seq_cols, store_cols) -> DataFrame:
    return history.select(*keys, *seq_cols, *store_cols, DELETED_COL)


def _event_states(events: DataFrame, keys, seq_cols, store_cols) -> DataFrame:
    deleted = F.col(OP_COL) == "D"
    # tombstones carry no attributes (oracle: attributes=None); key columns stay
    nulled = [F.when(deleted, F.lit(None)).otherwise(F.col(c)).alias(c) for c in store_cols]
    return events.select(*keys, *seq_cols, *nulled, deleted.alias(DELETED_COL))


def rebuild_history(
    history: DataFrame | None,
    events: DataFrame,
    keys: list[str],
    seq_cols: list[str],
    attr_cols: list[str],
) -> DataFrame:
    """Merge normalized CDC events into existing history; return the NEW full history."""
    # attrs that overlap the key are stored once (as the key column) — content
    # verdicts are unaffected since keys are constant within a chain partition
    store_cols = [c for c in attr_cols if c not in keys]
    states = _event_states(events, keys, seq_cols, store_cols)
    if history is not None:
        states = _history_states(history, keys, seq_cols, store_cols).unionByName(states)

    # per-slot resolution: identical content dedupes; differing content -> keep max
    # canonical (oracle keep-max rule); ordering keys are explicit about nulls
    states = states.withColumn("_content", _content_key(store_cols))
    slot_window = Window.partitionBy(*keys, *seq_cols).orderBy(F.desc_nulls_last("_content"))
    states = (
        states.withColumn("_rank", F.row_number().over(slot_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    # no-change collapse: drop states whose content equals the immediate predecessor
    chain = Window.partitionBy(*keys).orderBy(*[F.asc_nulls_first(c) for c in seq_cols])
    states = (
        states.withColumn("_prev_content", F.lag("_content").over(chain))
        .filter(F.col("_prev_content").isNull() | (F.col("_prev_content") != F.col("_content")))
        .drop("_prev_content")
    )

    # materialize intervals: valid_to = next valid_from; terminal = no successor
    to_cols = [F.lead(c).over(chain).alias(f"_to_{c}") for c in seq_cols]
    terminal = F.lead(F.lit(1)).over(chain).isNull()
    return (
        states.select(
            *keys, *seq_cols, *store_cols, DELETED_COL, *to_cols, terminal.alias(TERMINAL_COL)
        )
        .withColumn(CURRENT_COL, F.col(TERMINAL_COL) & ~F.col(DELETED_COL))
        .drop("_content")
        .orderBy(*[F.asc_nulls_first(c) for c in (*keys, *seq_cols)])
    )


def current_rows(history: DataFrame) -> DataFrame:
    """The BI-facing view: current, non-deleted versions only."""
    return history.filter(F.col(CURRENT_COL))
