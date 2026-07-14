"""Unit spec §1.4 — the SCD2 oracle. These tests DEFINE correct SCD2 semantics;
the integration suite holds auto_cdc_flow to them (ADR-0002)."""

import pytest

from retail_lakehouse.ingest.batches import CdcSpec
from retail_lakehouse.transform import cdc, scd2

pytestmark = pytest.mark.unit

SPEC = CdcSpec(
    keys=("cid",), sequence_by=("ts",), tiebreak="seq", op_column="op", delete_payload="key_only"
)


def _events(*records):
    return cdc.normalize(cdc.to_events(list(records), SPEC)).events


def _upd(cid, ts, seq, tier="BRONZE", op="U"):
    return {"op": op, "cid": cid, "tier": tier, "ts": ts, "seq": seq}


def _apply(history, *records, known_keys=frozenset()):
    stream = cdc.normalize(cdc.to_events(list(records), SPEC), known_keys=known_keys)
    return scd2.apply_scd2(list(history), list(stream.events))


class TestCoreVersioning:
    def test_change_closes_old_version_opens_new(self):
        history = _apply((), _upd(1, "t1", 1, "BRONZE"))
        history = _apply(history, _upd(1, "t2", 1, "SILVER"))
        v1, v2 = history
        assert v1.valid_to == v2.valid_from == ("t2", 1)
        assert (v1.is_current, v2.is_current) == (False, True)
        assert v2.attributes["tier"] == "SILVER"
        assert scd2.validate_history(history) == []

    def test_identical_resend_adds_zero_rows(self):
        history = _apply((), _upd(1, "t1", 1), _upd(1, "t2", 1, "SILVER"))
        replayed = _apply(history, _upd(1, "t2", 1, "SILVER"), _upd(1, "t1", 1))
        assert replayed == history

    def test_no_change_update_with_new_sequence_collapses(self):
        history = _apply((), _upd(1, "t1", 1, "BRONZE"))
        history = _apply(history, _upd(1, "t2", 1, "BRONZE"))  # same content, later seq
        assert len(history) == 1  # no-change short-circuit
        assert history[0].valid_from == ("t1", 1)

    def test_exactly_one_current_per_key(self):
        history = _apply((), _upd(1, "t1", 1), _upd(1, "t2", 1, "SILVER"), _upd(2, "t1", 1, "GOLD"))
        currents = scd2.current_rows(history)
        assert {v.key for v in currents} == {(1,), (2,)}
        assert scd2.validate_history(history) == []

    def test_intervals_contiguous_and_non_overlapping(self):
        history = _apply((), _upd(1, "t1", 1), _upd(1, "t3", 1, "SILVER"), _upd(1, "t5", 1, "GOLD"))
        chain = [v for v in history if v.key == (1,)]
        for earlier, later in zip(chain, chain[1:], strict=False):
            assert earlier.valid_to == later.valid_from
        assert chain[-1].valid_to is None

    def test_effective_dates_from_sequence_never_processing_time(self):
        history = _apply((), _upd(1, "2026-03-06T00:00:00Z", 1))
        assert history[0].valid_from == ("2026-03-06T00:00:00Z", 1)  # business time only

    def test_composite_keys_never_cross_match(self):
        spec = CdcSpec(
            keys=("region", "sid"),
            sequence_by=("ts",),
            tiebreak="seq",
            op_column=None,
            delete_payload=None,
        )
        records = [
            {"region": "N", "sid": 1, "name": "JOHN", "ts": "t1", "seq": 1},
            {"region": "S", "sid": 1, "name": "PITI", "ts": "t1", "seq": 1},
        ]
        stream = cdc.normalize(cdc.to_events(records, spec))
        history = scd2.apply_scd2([], list(stream.events))
        assert len(scd2.current_rows(history)) == 2  # same sid, different region


class TestLateArrivingCorrections:
    def test_late_event_resequences_history_correctly(self):
        # apply t1 then t3; a LATE t2 must slot BETWEEN them
        history = _apply((), _upd(1, "t1", 1, "BRONZE"))
        history = _apply(history, _upd(1, "t3", 1, "GOLD"))
        history = _apply(history, _upd(1, "t2", 1, "SILVER"))  # late arrival
        chain = [(v.valid_from, v.attributes["tier"], v.is_current) for v in history]
        assert chain == [
            (("t1", 1), "BRONZE", False),
            (("t2", 1), "SILVER", False),
            (("t3", 1), "GOLD", True),
        ]
        assert scd2.validate_history(history) == []

    def test_history_reconstruction_full_story(self):
        # scenario-day story: base -> change -> replay -> late correction -> delete
        history = _apply((), _upd(1, "t1", 1, "BRONZE"), _upd(2, "t1", 1, "GOLD"))
        history = _apply(history, _upd(1, "t4", 1, "PLATINUM"))
        history = _apply(history, _upd(1, "t4", 1, "PLATINUM"))  # replay: no delta
        history = _apply(history, _upd(1, "t2", 1, "SILVER"))  # late correction
        # delete without an in-stream upsert needs known_keys from the target (ADR-0006)
        history = _apply(history, _upd(2, "t5", 1, op="D", tier=None), known_keys={(2,)})
        assert scd2.validate_history(history) == []
        key1 = [v.attributes["tier"] for v in history if v.key == (1,) and not v.is_deleted]
        assert key1 == ["BRONZE", "SILVER", "PLATINUM"]
        assert {v.key for v in scd2.current_rows(history)} == {(1,)}  # 2 deleted


class TestDeletes:
    def test_delete_closes_chain_no_current_row(self):
        history = _apply((), _upd(1, "t1", 1))
        history = _apply(history, _upd(1, "t2", 1, op="D", tier=None), known_keys={(1,)})
        assert scd2.current_rows(history) == ()
        closed = next(v for v in history if not v.is_deleted)
        assert closed.valid_to == ("t2", 1)
        assert scd2.validate_history(history) == []

    def test_delete_replay_is_noop(self):
        history = _apply((), _upd(1, "t1", 1))
        history = _apply(history, _upd(1, "t2", 1, op="D", tier=None), known_keys={(1,)})
        replayed = _apply(history, _upd(1, "t2", 1, op="D", tier=None), known_keys={(1,)})
        assert replayed == history  # tombstone makes delete idempotent

    def test_reinstatement_after_delete(self):
        history = _apply((), _upd(1, "t1", 1))
        history = _apply(history, _upd(1, "t2", 1, op="D", tier=None), known_keys={(1,)})
        history = _apply(history, _upd(1, "t3", 1, "SILVER"))
        assert scd2.current_rows(history)[0].attributes["tier"] == "SILVER"
        assert scd2.validate_history(history) == []


class TestDeterminism:
    def test_batch_split_invariance(self):
        # applying events in one batch == applying them across three batches
        records = [_upd(1, "t1", 1), _upd(1, "t2", 1, "SILVER"), _upd(1, "t3", 1, "GOLD")]
        one_shot = scd2.apply_scd2([], list(_events(*records)))
        incremental = ()
        for record in records:
            incremental = _apply(incremental, record)
        assert one_shot == incremental

    def test_validate_history_catches_corruption(self):
        history = _apply((), _upd(1, "t1", 1), _upd(1, "t2", 1, "SILVER"))
        corrupted = list(history)
        from dataclasses import replace

        corrupted[0] = replace(corrupted[0], is_current=True)  # two currents
        assert any("current" in v for v in scd2.validate_history(tuple(corrupted)))

    def test_record_hash_excludes_requested_columns(self):
        a = {"tier": "GOLD", "_noise": 1}
        b = {"tier": "GOLD", "_noise": 2}
        assert scd2.record_hash(a, exclude=("_noise",)) == scd2.record_hash(b, exclude=("_noise",))
