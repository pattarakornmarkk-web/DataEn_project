"""Unit spec §1.2 — deterministic keyed deduplication (transform oracle)."""

import random

import pytest

from retail_lakehouse.transform.dedup import dedup_by_key

pytestmark = pytest.mark.unit

KEYS, SEQ, TIE = ["id"], ["ts"], "seq"


def _r(id, ts, seq, v):
    return {"id": id, "ts": ts, "seq": seq, "v": v}


def test_exact_duplicates_collapsed():
    a = _r(1, "t1", 1, "x")
    result = dedup_by_key([a, dict(a), dict(a)], KEYS, SEQ, TIE)
    assert len(result.survivors) == 1
    assert len(result.exact_duplicates) == 2
    assert result.superseded == () and result.conflicts == ()


def test_same_key_keeps_latest_by_sequence_payload_checked():
    older, newer = _r(1, "t1", 1, "OLD"), _r(1, "t2", 1, "NEW")
    result = dedup_by_key([older, newer], KEYS, SEQ, TIE)
    assert result.survivors[0]["v"] == "NEW"  # payload asserted, not just count
    assert result.superseded == (older,)


def test_equal_sequence_uses_documented_tiebreaker():
    low, high = _r(1, "t1", 1, "low"), _r(1, "t1", 9, "high")
    result = dedup_by_key([low, high], KEYS, SEQ, TIE)
    assert result.survivors[0]["v"] == "high"  # ADR-0009: highest tiebreak wins
    assert result.superseded == (low,)


def test_full_ordering_tie_with_different_payload_is_flagged_conflict():
    a, b = _r(1, "t1", 1, "aaa"), _r(1, "t1", 1, "zzz")
    result = dedup_by_key([a, b], KEYS, SEQ, TIE)
    assert result.survivors[0]["v"] == "zzz"  # deterministic: max canonical payload
    assert result.conflicts == (a,)


def test_stable_under_input_shuffle():
    records = [_r(i % 3, f"t{i % 4}", i % 2, f"v{i}") for i in range(12)]
    baseline = dedup_by_key(records, KEYS, SEQ, TIE)
    for seed in range(5):
        shuffled = records[:]
        random.Random(seed).shuffle(shuffled)
        assert dedup_by_key(shuffled, KEYS, SEQ, TIE) == baseline


def test_empty_and_single_row_pass_through():
    assert dedup_by_key([], KEYS, SEQ, TIE).survivors == ()
    only = _r(1, "t1", 1, "x")
    result = dedup_by_key([only], KEYS, SEQ, TIE)
    assert result.survivors == (only,) and result.input_count == 1


def test_null_sequence_sorts_below_any_real_value():
    with_null, real = _r(1, None, None, "null-seq"), _r(1, "t1", 0, "real")
    result = dedup_by_key([with_null, real], KEYS, SEQ, TIE)
    assert result.survivors[0]["v"] == "real"


def test_conservation_and_stable_survivor_ordering():
    records = [_r(3, "t1", 1, "c"), _r(1, "t1", 1, "a"), _r(2, "t1", 1, "b")]
    result = dedup_by_key(records, KEYS, SEQ, TIE)
    assert result.input_count == 3
    assert [r["id"] for r in result.survivors] == [1, 2, 3]  # key-ordered output


def test_composite_keys_never_cross_match():
    r1, r2 = _r(1, "t1", 1, "x"), _r(1, "t1", 1, "x")
    r2 = {**r2, "region": "S"}
    r1 = {**r1, "region": "N"}
    result = dedup_by_key([r1, r2], ["id", "region"], SEQ, TIE)
    assert len(result.survivors) == 2  # (1,N) and (1,S) are distinct entities


def test_input_not_mutated():
    records = [_r(1, "t1", 1, "x"), _r(1, "t2", 2, "y")]
    import copy

    snapshot = copy.deepcopy(records)
    dedup_by_key(records, KEYS, SEQ, TIE)
    assert records == snapshot
