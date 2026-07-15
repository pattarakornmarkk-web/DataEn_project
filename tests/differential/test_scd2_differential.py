"""Phase 6D — SCD2 compiler vs transform.scd2 oracle, CHAINED through the CDC compiler.

The strongest differential in the suite: raw records -> (oracle: cdc.normalize +
apply_scd2) vs (spark: cdc_compiler + rebuild_history) -> canonical relation compare.
Incremental application across batches is compared at every step.
"""

import pytest

from retail_lakehouse.spark import cdc_compiler, scd2_compiler
from retail_lakehouse.transform import cdc, scd2
from tests.differential import adapter
from tests.differential.comparators import diff_report
from tests.unit.test_scd2 import SPEC, _upd

pytestmark = [pytest.mark.unit, pytest.mark.spark]

COLUMN_TYPES = {"op": "string", "cid": "bigint", "tier": "string", "ts": "string", "seq": "bigint"}
KEYS, SEQ_COLS, ATTR_COLS = ["cid"], ["ts", "seq"], ["cid", "tier"]


def _oracle_canonical(history):
    rows = []
    for v in history:
        attrs = tuple(sorted(v.attributes.items())) if v.attributes is not None else None
        rows.append((v.key, v.valid_from, v.valid_to, v.is_current, v.is_deleted, attrs))
    return sorted(rows, key=lambda r: (str(r[0]), str(r[1])))


def _spark_canonical(history_df):
    rows = []
    for r in adapter.collect_normalized(history_df, sort_by_row_id=False):
        valid_to = None if r[scd2_compiler.TERMINAL_COL] else tuple(r[f"_to_{c}"] for c in SEQ_COLS)
        attrs = (
            None if r[scd2_compiler.DELETED_COL] else tuple(sorted((c, r[c]) for c in ATTR_COLS))
        )
        rows.append(
            (
                tuple(r[k] for k in KEYS),
                tuple(r[c] for c in SEQ_COLS),
                valid_to,
                r[scd2_compiler.CURRENT_COL],
                r[scd2_compiler.DELETED_COL],
                attrs,
            )
        )
    return sorted(rows, key=lambda r: (str(r[0]), str(r[1])))


class Chains:
    """Applies the same batches through both worlds, comparing after every step."""

    def __init__(self, spark):
        self.spark = spark
        self.oracle_history: tuple = ()
        self.spark_history = None

    def apply(self, records, known_keys=frozenset(), case=""):
        stream = cdc.normalize(cdc.to_events(list(records), SPEC), known_keys=frozenset(known_keys))
        self.oracle_history = scd2.apply_scd2(list(self.oracle_history), list(stream.events))

        df = adapter.to_typed_dataframe(self.spark, adapter.with_row_ids(records), COLUMN_TYPES)
        known_df = (
            adapter.to_typed_dataframe(
                self.spark,
                adapter.with_row_ids([{"cid": k[0]} for k in known_keys]),
                {"cid": "bigint"},
            )
            if known_keys
            else None
        )
        compiled = cdc_compiler.normalized_stream(df, SPEC, known_keys_df=known_df)
        self.spark_history = scd2_compiler.rebuild_history(
            self.spark_history, compiled.applied, KEYS, SEQ_COLS, ATTR_COLS
        )
        oracle_side, spark_side = (
            _oracle_canonical(self.oracle_history),
            _spark_canonical(self.spark_history),
        )
        assert oracle_side == spark_side, diff_report(
            case, [f"oracle={oracle_side!r}", f"spark ={spark_side!r}"]
        )
        return self


class TestHistoryStories:
    def test_change_closes_old_opens_new(self, spark):
        Chains(spark).apply([_upd(1, "t1", 1, "BRONZE")], case="base").apply(
            [_upd(1, "t2", 1, "SILVER")], case="change"
        )

    def test_replay_adds_zero_rows(self, spark):
        chain = Chains(spark).apply([_upd(1, "t1", 1), _upd(1, "t2", 1, "SILVER")], case="base")
        chain.apply([_upd(1, "t2", 1, "SILVER"), _upd(1, "t1", 1)], case="replay")

    def test_no_change_update_collapses(self, spark):
        Chains(spark).apply([_upd(1, "t1", 1, "BRONZE")], case="base").apply(
            [_upd(1, "t2", 1, "BRONZE")], case="no-change"
        )

    def test_late_event_slots_mid_history(self, spark):
        Chains(spark).apply([_upd(1, "t1", 1, "BRONZE")], case="t1").apply(
            [_upd(1, "t3", 1, "GOLD")], case="t3"
        ).apply([_upd(1, "t2", 1, "SILVER")], case="late t2")

    def test_delete_tombstone_and_reinstatement(self, spark):
        chain = Chains(spark).apply([_upd(1, "t1", 1)], case="base")
        chain.apply([_upd(1, "t2", 1, op="D", tier=None)], known_keys={(1,)}, case="delete")
        chain.apply([_upd(1, "t2", 1, op="D", tier=None)], known_keys={(1,)}, case="delete replay")
        chain.apply([_upd(1, "t3", 1, "SILVER")], case="reinstatement")

    def test_full_reconstruction_story(self, spark):
        chain = Chains(spark).apply(
            [_upd(1, "t1", 1, "BRONZE"), _upd(2, "t1", 1, "GOLD")], case="seed"
        )
        chain.apply([_upd(1, "t4", 1, "PLATINUM")], case="change")
        chain.apply([_upd(1, "t2", 1, "SILVER")], case="late correction")
        chain.apply([_upd(2, "t5", 1, op="D", tier=None)], known_keys={(2,)}, case="delete")

    def test_composite_key_isolation_and_out_of_order_batch(self, spark):
        records = [
            _upd(1, "t3", 1, "GOLD"),
            _upd(2, "t1", 1, "BRONZE"),
            _upd(1, "t1", 1, "BRONZE"),
            _upd(1, "t2", 1, "SILVER"),
        ]
        Chains(spark).apply(records, case="out-of-order single batch")

    def test_batch_split_invariance_across_worlds(self, spark):
        records = [_upd(1, "t1", 1), _upd(1, "t2", 1, "SILVER"), _upd(1, "t3", 1, "GOLD")]
        one_shot = Chains(spark).apply(records, case="one-shot")
        incremental = Chains(spark)
        for record in records:
            incremental.apply([record], case=f"incremental {record['ts']}")
        assert _spark_canonical(one_shot.spark_history) == _spark_canonical(
            incremental.spark_history
        )

    def test_current_rows_view_matches(self, spark):
        chain = Chains(spark).apply(
            [_upd(1, "t1", 1), _upd(1, "t2", 1, "SILVER"), _upd(2, "t1", 1, "GOLD")],
            case="currents",
        )
        oracle_currents = {v.key for v in scd2.current_rows(chain.oracle_history)}
        spark_currents = {
            (r["cid"],)
            for r in adapter.collect_normalized(
                scd2_compiler.current_rows(chain.spark_history), sort_by_row_id=False
            )
        }
        assert oracle_currents == spark_currents == {(1,), (2,)}
