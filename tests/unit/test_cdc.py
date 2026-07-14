"""Unit spec §1.3 — CDC normalization: synthesis, unify, replay, orphans, ordering."""

import pytest

from retail_lakehouse.config.contracts import ConfigError
from retail_lakehouse.ingest.batches import CdcSpec
from retail_lakehouse.transform import cdc

pytestmark = pytest.mark.unit

SPEC = CdcSpec(
    keys=("cid",),
    sequence_by=("ts",),
    tiebreak="seq",
    op_column="op",
    delete_payload="key_only",
)


def _upd(cid, ts, seq, op="U", email="a@b.co"):
    return {"op": op, "cid": cid, "email": email, "ts": ts, "seq": seq}


def _snap(cid, ts, email="a@b.co"):
    return {"cid": cid, "email": email, "ts": ts, "seq": None}


class TestSynthesisAndUnify:
    def test_snapshot_rows_become_synthetic_upserts(self):
        events = cdc.snapshot_to_events([_snap(1, "t1")], SPEC)
        assert events[0].op == cdc.OP_UPSERT
        assert events[0].sequence == ("t1", None)
        assert events[0].key == (1,)

    def test_insert_op_normalizes_to_upsert(self):
        events = cdc.to_events([_upd(1, "t1", 1, op="I")], SPEC)
        assert events[0].op == cdc.OP_UPSERT

    def test_both_branches_produce_identical_schema(self):
        cdc_events = cdc.to_events([_upd(1, "t1", 1)], SPEC)
        snap_events = cdc.snapshot_to_events([_snap(2, "t0")], SPEC)
        unified = cdc.unify_streams(cdc_events, snap_events)
        assert len({tuple(sorted(e.payload)) for e in unified}) == 1
        assert all("op" not in e.payload and "_op" not in e.payload for e in unified)

    def test_unify_rejects_schema_mismatch(self):
        good = cdc.to_events([_upd(1, "t1", 1)], SPEC)
        bad = [cdc.CdcEvent(key=(9,), op="U", sequence=("t", 1), payload={"other": 1})]
        with pytest.raises(ConfigError, match="payload schema"):
            cdc.unify_streams(good, bad)


class TestReplayAndConflicts:
    def test_duplicate_delivery_dropped_and_counted(self):
        events = cdc.to_events([_upd(1, "t1", 1), _upd(1, "t1", 1), _upd(1, "t1", 1)], SPEC)
        stream = cdc.normalize(events)
        assert len(stream.events) == 1
        assert stream.replays_dropped == 2

    def test_same_slot_different_payload_flagged_conflict_with_deterministic_winner(self):
        a, b = _upd(1, "t1", 1, email="aaa@x.co"), _upd(1, "t1", 1, email="zzz@x.co")
        stream = cdc.normalize(cdc.to_events([a, b], SPEC))
        assert stream.events[0].payload["email"] == "zzz@x.co"
        assert len(stream.conflicts) == 1

    def test_same_timestamp_updates_ordered_by_tiebreak_not_conflict(self):
        first, second = _upd(1, "t1", 1, email="old@x.co"), _upd(1, "t1", 2, email="new@x.co")
        stream = cdc.normalize(cdc.to_events([second, first], SPEC))
        assert [e.payload["email"] for e in stream.events] == ["old@x.co", "new@x.co"]
        assert stream.conflicts == ()


class TestOrphanDeletes:
    def test_orphan_delete_tagged_and_held_not_dropped(self):
        stream = cdc.normalize(cdc.to_events([_upd(9, "t1", 1, op="D", email=None)], SPEC))
        assert stream.events == ()  # not applied
        assert len(stream.held_orphans) == 1  # not dropped (ADR-0006)
        assert stream.held_orphans[0].is_orphan_delete

    def test_delete_after_in_stream_upsert_is_applied(self):
        events = cdc.to_events([_upd(1, "t1", 1), _upd(1, "t2", 1, op="D", email=None)], SPEC)
        stream = cdc.normalize(events)
        assert [e.op for e in stream.events] == [cdc.OP_UPSERT, cdc.OP_DELETE]
        assert stream.held_orphans == ()

    def test_delete_for_known_target_key_is_not_orphan(self):
        events = cdc.to_events([_upd(1, "t5", 1, op="D", email=None)], SPEC)
        stream = cdc.normalize(events, known_keys=frozenset({(1,)}))
        assert stream.held_orphans == () and len(stream.events) == 1

    def test_delete_arriving_before_its_own_insert_in_one_batch(self):
        # sequence decides: the delete is LATER than the insert, so after sorting
        # it is not an orphan even though it arrived first in the file
        events = cdc.to_events([_upd(1, "t2", 1, op="D", email=None), _upd(1, "t1", 1)], SPEC)
        stream = cdc.normalize(events)
        assert stream.held_orphans == ()
        assert [e.op for e in stream.events] == [cdc.OP_UPSERT, cdc.OP_DELETE]


class TestOrderingSemantics:
    def test_out_of_order_within_batch_resequenced(self):
        t2, t1 = _upd(1, "t2", 1, email="second@x.co"), _upd(1, "t1", 1, email="first@x.co")
        stream = cdc.normalize(cdc.to_events([t2, t1], SPEC))
        assert [e.payload["email"] for e in stream.events] == ["first@x.co", "second@x.co"]

    def test_normalize_is_input_order_independent(self):
        records = [_upd(1, "t1", 1), _upd(1, "t2", 1, email="x@y.co"), _upd(2, "t1", 1)]
        forward = cdc.normalize(cdc.to_events(records, SPEC))
        backward = cdc.normalize(cdc.to_events(list(reversed(records)), SPEC))
        assert forward == backward
