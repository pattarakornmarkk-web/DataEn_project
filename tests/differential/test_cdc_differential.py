"""Phase 6C — CDC compiler vs transform.cdc oracle on the SAME fixtures.

Comparison unit: the event tuple (key, op, sequence, payload) — replay counts,
applied/held/conflict membership, and output ordering must all match.
"""

import pytest

from retail_lakehouse.spark import cdc_compiler
from retail_lakehouse.transform import cdc
from tests.differential import adapter
from tests.differential.comparators import diff_report
from tests.unit.test_cdc import SPEC, _snap, _upd

pytestmark = [pytest.mark.unit, pytest.mark.spark]

COLUMN_TYPES = {"op": "string", "cid": "bigint", "email": "string", "ts": "string", "seq": "bigint"}
SEQ_COLS = ("ts", "seq")


def _oracle_tuple(event: cdc.CdcEvent):
    return (event.key, event.op, event.sequence, tuple(sorted(event.payload.items())))


def _spark_tuple(row: dict):
    # oracle payload keeps key columns (state), excludes op/sequence/tiebreak (ordering)
    payload = {
        k: v
        for k, v in row.items()
        if k not in (cdc_compiler.OP_COL, *SEQ_COLS, cdc_compiler.ORPHAN_COL)
    }
    return (
        tuple(row[k] for k in SPEC.keys),
        row[cdc_compiler.OP_COL],
        tuple(row[c] for c in SEQ_COLS),
        tuple(sorted(payload.items())),
    )


def run_differential(spark, records, known_keys=frozenset(), case=""):
    # oracle path
    oracle = cdc.normalize(cdc.to_events(list(records), SPEC), known_keys=frozenset(known_keys))

    # spark path (typed input — post-DQ position)
    df = adapter.to_typed_dataframe(spark, adapter.with_row_ids(records), COLUMN_TYPES)
    known_df = (
        adapter.to_typed_dataframe(
            spark, adapter.with_row_ids([{"cid": k[0]} for k in known_keys]), {"cid": "bigint"}
        )
        if known_keys
        else None
    )
    compiled = cdc_compiler.normalized_stream(df, SPEC, known_keys_df=known_df)

    checks = [
        (
            "applied",
            [_oracle_tuple(e) for e in oracle.events],
            [
                _spark_tuple(r)
                for r in adapter.collect_normalized(compiled.applied, sort_by_row_id=False)
            ],
        ),
        (
            "held_orphans",
            sorted(_oracle_tuple(e) for e in oracle.held_orphans),
            sorted(
                _spark_tuple(r)
                for r in adapter.collect_normalized(compiled.held_orphans, sort_by_row_id=False)
            ),
        ),
        (
            "conflicts",
            sorted(_oracle_tuple(e) for e in oracle.conflicts),
            sorted(
                _spark_tuple(r)
                for r in adapter.collect_normalized(compiled.conflicts, sort_by_row_id=False)
            ),
        ),
    ]
    for what, oracle_side, spark_side in checks:
        assert oracle_side == spark_side, diff_report(
            case, [f"{what}: oracle={oracle_side!r}", f"{what}: spark ={spark_side!r}"]
        )
    assert oracle.replays_dropped == compiled.replays_dropped, (
        f"{case}: replay counts differ oracle={oracle.replays_dropped} "
        f"spark={compiled.replays_dropped}"
    )
    return oracle, compiled


class TestReplayAndConflicts:
    def test_duplicate_delivery_dropped_and_counted(self, spark):
        records = [_upd(1, "t1", 1), _upd(1, "t1", 1), _upd(1, "t1", 1)]
        oracle, _ = run_differential(spark, records, case="replay x3")
        assert oracle.replays_dropped == 2

    def test_conflict_same_slot_different_payload(self, spark):
        records = [_upd(1, "t1", 1, email="aaa@x.co"), _upd(1, "t1", 1, email="zzz@x.co")]
        run_differential(spark, records, case="slot conflict")

    def test_same_timestamp_tiebreak_orders_not_conflicts(self, spark):
        records = [_upd(1, "t1", 2, email="new@x.co"), _upd(1, "t1", 1, email="old@x.co")]
        oracle, compiled = run_differential(spark, records, case="tiebreak")
        assert oracle.conflicts == () and compiled.conflicts.count() == 0


class TestOrphansAndOrdering:
    def test_orphan_delete_tagged_and_held(self, spark):
        records = [_upd(9, "t1", 1, op="D", email=None)]
        oracle, compiled = run_differential(spark, records, case="orphan hold")
        assert len(oracle.held_orphans) == 1 and compiled.held_orphans.count() == 1

    def test_delete_after_in_stream_upsert_applied(self, spark):
        records = [_upd(1, "t2", 1, op="D", email=None), _upd(1, "t1", 1)]
        run_differential(spark, records, case="delete after upsert (arrived first)")

    def test_delete_for_known_target_key_not_orphan(self, spark):
        records = [_upd(1, "t5", 1, op="D", email=None)]
        run_differential(spark, records, known_keys={(1,)}, case="known-key delete")

    def test_delete_sequenced_before_only_upsert_is_orphan(self, spark):
        records = [_upd(1, "t1", 1, op="D", email=None), _upd(1, "t2", 1)]
        oracle, _ = run_differential(spark, records, case="delete before upsert")
        assert len(oracle.held_orphans) == 1  # upsert is LATER: still an orphan

    def test_out_of_order_batch_resequenced(self, spark):
        records = [
            _upd(2, "t9", 1),
            _upd(1, "t2", 1, email="second@x.co"),
            _upd(1, "t1", 1, email="first@x.co"),
            _upd(1, "t1", 0, email="zeroth@x.co"),
        ]
        run_differential(spark, records, case="out-of-order")

    def test_null_sequence_orders_lowest(self, spark):
        records = [_upd(1, None, None, email="null-seq@x.co"), _upd(1, "t1", 1)]
        run_differential(spark, records, case="null ordering")


class TestSnapshotUnification:
    def test_snapshot_rows_become_upserts_and_union_matches(self, spark):
        # ADR-0004: snapshot branch (no meaningful op) unified with CDC branch
        snapshot_rows = [{**_snap(7, "t0"), "op": None}]
        cdc_rows = [_upd(7, "t1", 1, email="later@x.co")]
        run_differential(spark, snapshot_rows + cdc_rows, case="snapshot+cdc union")
