"""Property-style differential: seeded random batches, oracle == compiler, every seed.

Seeds are fixed (deterministic CI); a failure prints the seed and shrinks the batch
to a minimal failing subset before reporting.
"""

import random

import pytest

from tests.differential.comparators import shrink
from tests.differential.test_cdc_differential import (
    run_differential as run_cdc,
)
from tests.differential.test_dq_differential import (
    CONTRACT,
    RULES,
)
from tests.differential.test_dq_differential import (
    run_differential as run_dq,
)

pytestmark = [pytest.mark.unit, pytest.mark.spark]

SEEDS = [7, 23, 99]


def _random_dq_records(rng, n=25):
    ids = [None, "", "1", "2", "3", "  ", "x"]
    tiers = ["A", "B", "DIAMOND", None]
    qtys = ["0", "1", "5", "10", "11", "junk", None]
    ts = ["2026-06-06T07:00:00Z", "2026-06-06T08:05:01Z", "not-a-ts", None]
    return [
        {
            "id": rng.choice(ids),
            "tier": rng.choice(tiers),
            "email": "a@b.co",
            "qty": rng.choice(qtys),
            "price": rng.choice(["9.99", "N/A", None]),
            "born": rng.choice(["1990-05-01", "1899-13-45", None]),
            "seen_ts": rng.choice(ts),
            "active": rng.choice(["true", "N", "maybe", None]),
        }
        for _ in range(n)
    ]


def _random_cdc_records(rng, n=25):
    return [
        {
            "op": rng.choice(["I", "U", "U", "D"]),
            "cid": rng.choice([1, 2, 3]),
            "email": rng.choice(["a@x.co", "b@x.co", None]),
            "ts": rng.choice(["t1", "t2", "t3", None]),
            "seq": rng.choice([1, 2, None]),
        }
        for _ in range(n)
    ]


@pytest.mark.parametrize("seed", SEEDS)
def test_dq_property_equivalence(spark, seed):
    records = _random_dq_records(random.Random(seed))
    try:
        run_dq(spark, records, CONTRACT, RULES, case=f"dq-property seed={seed}")
    except AssertionError:
        minimal = shrink(
            records,
            lambda subset: _fails(lambda: run_dq(spark, subset, CONTRACT, RULES)),
        )
        raise AssertionError(f"seed={seed} minimal failing subset: {minimal!r}") from None


@pytest.mark.parametrize("seed", SEEDS)
def test_cdc_property_equivalence(spark, seed):
    records = _random_cdc_records(random.Random(seed))
    try:
        run_cdc(spark, records, case=f"cdc-property seed={seed}")
    except AssertionError:
        minimal = shrink(records, lambda subset: _fails(lambda: run_cdc(spark, subset)))
        raise AssertionError(f"seed={seed} minimal failing subset: {minimal!r}") from None


def _fails(fn) -> bool:
    try:
        fn()
        return False
    except AssertionError:
        return True
