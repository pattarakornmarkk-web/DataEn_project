"""Unit spec §1.7 — DQ rule semantics (the pure oracle) + reason composition."""

from datetime import datetime, timezone

import pytest

from retail_lakehouse.config.contracts import ConfigError
from retail_lakehouse.dq import engine, reason
from retail_lakehouse.dq.rules import bind_ruleset

pytestmark = pytest.mark.unit

AS_OF = datetime(2026, 6, 6, 8, 0, 0, tzinfo=timezone.utc)

CONTRACT = {
    "identity": ["id"],
    "columns": {
        "id": "bigint",
        "tier": "string",
        "email": "string",
        "qty": "int",
        "price": "decimal(10,2)",
        "born": "date",
        "seen_ts": "timestamp",
        "active": "boolean",
    },
}
RULES = [
    {"rule": "null_key", "column": "id", "severity": "quarantine"},
    {"rule": "duplicate", "severity": "quarantine"},
    {"rule": "domain", "column": "tier", "values": ["A", "B"], "severity": "quarantine"},
    {
        "rule": "format",
        "column": "email",
        "pattern": r"[^@\s]+@[^@\s]+\.[^@\s]+",
        "severity": "quarantine",
    },
    {"rule": "range", "column": "qty", "min": 1, "max": 10, "severity": "quarantine"},
    {
        "rule": "plausibility",
        "column": "seen_ts",
        "max_future_skew_minutes": 5,
        "severity": "quarantine",
    },
]


@pytest.fixture(scope="module")
def ruleset():
    return bind_ruleset("test_source", CONTRACT, RULES)


def _record(**overrides):
    base = {
        "id": "7",
        "tier": "A",
        "email": "a@b.co",
        "qty": "5",
        "price": "9.99",
        "born": "1990-05-01",
        "seen_ts": "2026-06-06T07:00:00Z",
        "active": "true",
    }
    return {**base, **overrides}


def _reasons_for(ruleset, record):
    result = engine.validate_batch([record], ruleset, AS_OF)
    return list(result.quarantine[0].reasons) if result.quarantine else []


# clean / violating / boundary / null per rule type (unit spec §1.7)
CASES = [
    ("null_key clean", {}, []),
    ("null_key none", {"id": None}, ["id_null_key"]),
    ("null_key blank string counts as null", {"id": "  "}, ["id_null_key"]),
    ("domain clean", {"tier": "B"}, []),
    ("domain violating", {"tier": "DIAMOND"}, ["tier_domain"]),
    ("domain null ignored", {"tier": None}, []),
    ("format violating", {"email": "not-an-email"}, ["email_format"]),
    ("format null ignored", {"email": ""}, []),
    ("range low boundary ok", {"qty": "1"}, []),
    ("range high boundary ok", {"qty": "10"}, []),
    ("range below", {"qty": "0"}, ["qty_range"]),
    ("range above", {"qty": "11"}, ["qty_range"]),
    ("range unparseable is try_cast not range", {"qty": "many"}, ["qty_try_cast"]),
    ("plausibility exactly now+skew ok", {"seen_ts": "2026-06-06T08:05:00Z"}, []),
    ("plausibility past ok", {"seen_ts": "2020-01-01T00:00:00Z"}, []),
    ("try_cast impossible date", {"born": "1899-13-45"}, ["born_try_cast"]),
    ("try_cast bad decimal", {"price": "N/A"}, ["price_try_cast"]),
    ("try_cast bad boolean", {"active": "maybe"}, ["active_try_cast"]),
    ("try_cast null passes (null_key's job)", {"born": None}, []),
]


@pytest.mark.parametrize("label,overrides,expected", CASES, ids=[c[0] for c in CASES])
def test_each_rule_type_clean_violating_boundary_null(ruleset, label, overrides, expected):
    assert _reasons_for(ruleset, _record(**overrides)) == expected


def test_future_ts_boundary_now_plus_skew(ruleset):
    exactly = _record(seen_ts="2026-06-06T08:05:00Z")
    one_second_past = _record(seen_ts="2026-06-06T08:05:01Z")
    assert _reasons_for(ruleset, exactly) == []
    assert _reasons_for(ruleset, one_second_past) == ["seen_ts_plausibility"]


def test_composed_reason_array_exact_set(ruleset):
    record = _record(id="", tier="C", qty="0")
    assert _reasons_for(ruleset, record) == ["id_null_key", "qty_range", "tier_domain"]


def test_valid_quarantine_partition_total_and_disjoint(ruleset):
    records = [_record(id=str(i)) for i in range(4)] + [_record(id=None), _record(tier="X", id="9")]
    result = engine.validate_batch(records, ruleset, AS_OF)
    assert result.summary.conserved
    assert len(result.valid) + len(result.quarantine) == len(records)
    valid_ids = {r["id"] for r in result.valid}
    quarantined_ids = {q.record["id"] for q in result.quarantine}
    assert valid_ids.isdisjoint(quarantined_ids - {None})


def test_validation_is_side_effect_free(ruleset):
    records = [_record(id=None), _record(qty="99")]
    import copy

    snapshot = copy.deepcopy(records)
    engine.validate_batch(records, ruleset, AS_OF)
    assert records == snapshot


def test_binding_rejects_rule_on_unknown_column():
    bad = [{"rule": "null_key", "column": "ghost", "severity": "quarantine"}]
    with pytest.raises(ConfigError, match="not in the contract"):
        bind_ruleset("test_source", CONTRACT, bad)


def test_reason_helpers_are_exact_set_and_melt_compatible():
    assert reason.compose(["b", "a", "b"]) == ["a", "b"]
    flags = {"_is_id_null_key": True, "_is_tier_domain": False, "_is_qty_range": True}
    assert reason.reasons_from_flags(flags) == ["id_null_key", "qty_range"]
