"""DQ engine — batch semantics: duplicates, op-awareness, severity routing, metrics."""

from datetime import datetime, timezone

import pytest

from retail_lakehouse.dq import engine
from retail_lakehouse.dq.engine import KEY_DUPLICATE, ROW_DUPLICATE
from retail_lakehouse.dq.rules import bind_ruleset

pytestmark = pytest.mark.unit

AS_OF = datetime(2026, 6, 6, 8, 0, 0, tzinfo=timezone.utc)

SIMPLE_CONTRACT = {
    "identity": ["id"],
    "columns": {"id": "bigint", "name": "string", "qty": "int"},
}
SIMPLE_RULES = [
    {"rule": "null_key", "column": "id", "severity": "quarantine"},
    {"rule": "duplicate", "severity": "quarantine"},
]

CDC_CONTRACT = {
    "identity": ["cid", "ts", "seq"],
    "op_column": "op",
    "delete_payload": "key_only",
    "columns": {
        "op": "string",
        "cid": "bigint",
        "email": "string",
        "ts": "timestamp",
        "seq": "bigint",
    },
}
CDC_RULES = [
    {"rule": "null_key", "column": "cid", "severity": "quarantine"},
    {"rule": "null_key", "column": "ts", "severity": "quarantine"},
    {"rule": "null_key", "column": "email", "severity": "quarantine"},
    {"rule": "domain", "column": "op", "values": ["I", "U", "D"], "severity": "quarantine"},
]


@pytest.fixture(scope="module")
def simple_ruleset():
    return bind_ruleset("simple", SIMPLE_CONTRACT, SIMPLE_RULES)


@pytest.fixture(scope="module")
def cdc_ruleset():
    return bind_ruleset("cdc", CDC_CONTRACT, CDC_RULES)


class TestDuplicates:
    def test_row_and_key_duplicate_semantics(self, simple_ruleset):
        r1 = {"id": "1", "name": "a", "qty": "5"}
        r1_copy = dict(r1)
        r2_same_key = {"id": "1", "name": "DIFFERENT", "qty": "5"}
        r3 = {"id": "3", "name": "c", "qty": "7"}
        result = engine.validate_batch([r1, r1_copy, r2_same_key, r3], simple_ruleset, AS_OF)

        reasons_by_name = {q.record["name"]: q.reasons for q in result.quarantine}
        assert reasons_by_name["a"] == (KEY_DUPLICATE,) or ROW_DUPLICATE in reasons_by_name["a"]
        assert any(ROW_DUPLICATE in r for r in reasons_by_name.values())
        assert reasons_by_name["DIFFERENT"] == (KEY_DUPLICATE,)
        assert [r["id"] for r in result.valid] == [3]

    def test_null_identity_excluded_from_key_grouping(self, simple_ruleset):
        records = [{"id": None, "name": "x", "qty": "1"}, {"id": None, "name": "y", "qty": "2"}]
        result = engine.validate_batch(records, simple_ruleset, AS_OF)
        for q in result.quarantine:
            assert q.reasons == ("id_null_key",)  # null_key owns them, not key_duplicate

    def test_shuffle_stable_outcomes(self, simple_ruleset):
        records = [
            {"id": "1", "name": "a", "qty": "5"},
            {"id": "1", "name": "b", "qty": "5"},
            {"id": "2", "name": "c", "qty": "1"},
            {"id": "3", "name": "d", "qty": "1"},
        ]
        forward = engine.validate_batch(records, simple_ruleset, AS_OF)
        backward = engine.validate_batch(list(reversed(records)), simple_ruleset, AS_OF)
        as_set = lambda res: {(q.record["name"], q.reasons) for q in res.quarantine}  # noqa: E731
        assert as_set(forward) == as_set(backward)
        assert {r["id"] for r in forward.valid} == {r["id"] for r in backward.valid}


class TestOpAwareness:
    def test_delete_image_exempt_from_non_identity_null_checks(self, cdc_ruleset):
        delete = {"op": "D", "cid": "5", "ts": "2026-06-01T00:00:00Z", "seq": "1", "email": None}
        result = engine.validate_batch([delete], cdc_ruleset, AS_OF)
        assert not result.quarantine

    def test_delete_with_null_sequence_still_quarantined(self, cdc_ruleset):
        delete = {"op": "D", "cid": "5", "ts": None, "seq": "1", "email": None}
        result = engine.validate_batch([delete], cdc_ruleset, AS_OF)
        assert result.quarantine[0].reasons == ("ts_null_key",)

    def test_upsert_images_get_full_validation(self, cdc_ruleset):
        upsert = {"op": "U", "cid": "5", "ts": "2026-06-01T00:00:00Z", "seq": "1", "email": None}
        result = engine.validate_batch([upsert], cdc_ruleset, AS_OF)
        assert result.quarantine[0].reasons == ("email_null_key",)


class TestSeverityRouting:
    def test_observe_violations_stay_valid_but_are_measured(self):
        rules = [{"rule": "domain", "column": "name", "values": ["ok"], "severity": "observe"}]
        ruleset = bind_ruleset("obs", SIMPLE_CONTRACT, rules)
        result = engine.validate_batch([{"id": "1", "name": "weird", "qty": "2"}], ruleset, AS_OF)
        assert len(result.valid) == 1 and not result.quarantine
        metric = next(m for m in result.metrics if m.rule == "domain")
        assert (metric.evaluated, metric.violations) == (1, 1)

    def test_try_cast_wildcard_override_downgrades_coercion(self):
        rules = [{"rule": "try_cast", "column": "*", "severity": "observe"}]
        ruleset = bind_ruleset("obs", SIMPLE_CONTRACT, rules)
        result = engine.validate_batch([{"id": "1", "name": "x", "qty": "junk"}], ruleset, AS_OF)
        assert len(result.valid) == 1 and not result.quarantine
        metric = next(m for m in result.metrics if m.rule == "try_cast" and m.column == "qty")
        assert metric.violations == 1

    def test_quarantine_preserves_original_values(self, simple_ruleset):
        record = {"id": None, "name": "keepme", "qty": "007"}
        result = engine.validate_batch([record], simple_ruleset, AS_OF)
        assert result.quarantine[0].record == record  # uncoerced, byte-for-byte


class TestCoercionAndOutputs:
    def test_valid_records_are_contract_coerced(self, simple_ruleset):
        result = engine.validate_batch(
            [{"id": "42", "name": "n", "qty": "7"}], simple_ruleset, AS_OF
        )
        assert result.valid[0] == {"id": 42, "name": "n", "qty": 7}

    @pytest.mark.parametrize(
        "value,ctype,expected",
        [
            ("true", "boolean", True),
            ("N", "boolean", False),
            ("1", "boolean", True),
            ("12", "int", 12),
            (12, "int", 12),
            ("+5", "bigint", 5),
            ("", "int", None),
            (None, "string", None),
            ("2026-06-06T08:00:00Z", "timestamp", datetime(2026, 6, 6, 8, tzinfo=timezone.utc)),
        ],
    )
    def test_coerce_value_semantics(self, value, ctype, expected):
        assert engine.coerce_value(value, ctype) == expected

    def test_naive_timestamps_assumed_utc(self):
        coerced = engine.coerce_value("2026-06-06T08:00:00", "timestamp")
        assert coerced.tzinfo is not None

    def test_metrics_and_summary_shapes(self, simple_ruleset):
        records = [{"id": "1", "name": "a", "qty": "1"}, {"id": None, "name": "b", "qty": "x"}]
        result = engine.validate_batch(records, simple_ruleset, AS_OF)
        null_metric = next(m for m in result.metrics if m.rule == "null_key")
        assert (null_metric.source, null_metric.evaluated, null_metric.violations) == (
            "simple",
            2,
            1,
        )
        assert {m.column for m in result.metrics if m.rule == "duplicate"} == {"row", "key"}
        assert result.summary.conserved
        assert result.summary.input_count == 2

    def test_deterministic_repeat_runs(self, simple_ruleset):
        records = [{"id": "1", "name": "a", "qty": "1"}, {"id": "1", "name": "a", "qty": "1"}]
        first = engine.validate_batch(records, simple_ruleset, AS_OF)
        second = engine.validate_batch(records, simple_ruleset, AS_OF)
        assert first == second
