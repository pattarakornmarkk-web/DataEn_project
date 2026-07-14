"""Phase 6B — DQ compiler vs oracle engine on the SAME fixtures.

Fixture reuse: cases are imported from the unit suites (single source of truth).
Comparison: per _row_id — quarantine membership, exact reason arrays, and coerced
valid values. Oracle is the source of truth; mismatch == compiler defect.
"""

from datetime import datetime, timezone

import pytest

from retail_lakehouse.config import loader
from retail_lakehouse.dq import engine
from retail_lakehouse.dq.rules import bind_ruleset
from retail_lakehouse.spark import dq_compiler
from retail_lakehouse.spark.types import ROW_ID
from tests.differential import adapter, comparators
from tests.unit.test_engine import CDC_CONTRACT, CDC_RULES, SIMPLE_CONTRACT, SIMPLE_RULES
from tests.unit.test_rules import CASES, CONTRACT, RULES, _record

pytestmark = [pytest.mark.unit, pytest.mark.spark]

AS_OF = datetime(2026, 6, 6, 8, 0, 0, tzinfo=timezone.utc)


def run_differential(spark, records, contract, rules, case=""):
    """One dataset, two paths, full verdict comparison."""
    ruleset = bind_ruleset("diff", contract, rules)
    records = adapter.with_row_ids(records)

    # oracle path
    oracle = engine.validate_batch(records, ruleset, AS_OF)
    oracle_quarantined = {q.record[ROW_ID]: list(q.reasons) for q in oracle.quarantine}
    oracle_valid_ids = [r[ROW_ID] for r in records if r[ROW_ID] not in oracle_quarantined]
    oracle_valid = [
        {ROW_ID: row_id, **typed}
        for row_id, typed in zip(oracle_valid_ids, oracle.valid, strict=True)
    ]

    # spark path
    columns = list(dict.fromkeys(c for r in records for c in r if c != ROW_ID))
    df = adapter.to_raw_dataframe(spark, records, columns)
    validated = dq_compiler.validated_df(df, ruleset, AS_OF)
    valid_df, quarantine_df = dq_compiler.split(validated, ruleset, passthrough=[ROW_ID])
    spark_quarantined = {r[ROW_ID]: r["reason"] for r in adapter.collect_normalized(quarantine_df)}
    spark_valid = adapter.collect_normalized(valid_df)

    # verdicts: membership + exact reasons
    comparators.assert_sets_equivalent(
        set(oracle_quarantined), set(spark_quarantined), "quarantined rows", case
    )
    for row_id in oracle_quarantined:
        assert oracle_quarantined[row_id] == spark_quarantined[row_id], comparators.diff_report(
            case,
            [
                f"row {row_id} reasons: oracle={oracle_quarantined[row_id]} "
                f"spark={spark_quarantined[row_id]}"
            ],
        )
    # values: coerced valid records must match exactly (Decimal/ts/date/bool)
    comparators.assert_rows_equivalent(oracle_valid, spark_valid, case=f"{case} valid values")
    return oracle, spark_quarantined


class TestRuleCases:
    @pytest.mark.parametrize("label,overrides,expected", CASES, ids=[c[0] for c in CASES])
    def test_every_oracle_rule_case(self, spark, label, overrides, expected):
        run_differential(spark, [_record(**overrides)], CONTRACT, RULES, case=label)

    def test_composed_reasons_and_boundaries_in_one_batch(self, spark):
        records = [
            _record(id="", tier="C", qty="0"),  # three violations at once
            _record(seen_ts="2026-06-06T08:05:00Z", id="1"),  # exactly now+skew: valid
            _record(seen_ts="2026-06-06T08:05:01Z", id="2"),  # one second past: violation
            _record(id="3"),  # clean
        ]
        run_differential(spark, records, CONTRACT, RULES, case="composed+boundary")


class TestBatchSemantics:
    def test_row_and_key_duplicates(self, spark):
        records = [
            {"id": "1", "name": "a", "qty": "5"},
            {"id": "1", "name": "a", "qty": "5"},  # exact dup
            {"id": "1", "name": "DIFFERENT", "qty": "5"},  # key dup
            {"id": "3", "name": "c", "qty": "7"},  # clean
            {"id": None, "name": "x", "qty": "1"},  # null identity: excluded from key grouping
            {"id": None, "name": "y", "qty": "2"},
        ]
        run_differential(spark, records, SIMPLE_CONTRACT, SIMPLE_RULES, case="duplicates")

    def test_op_aware_cdc_suppression(self, spark):
        records = [
            {"op": "D", "cid": "5", "ts": "2026-06-01T00:00:00Z", "seq": "1", "email": None},
            {"op": "D", "cid": "5", "ts": None, "seq": "2", "email": None},  # null sequence
            {"op": "U", "cid": "6", "ts": "2026-06-01T00:00:00Z", "seq": "1", "email": None},
            {"op": "X", "cid": "7", "ts": "2026-06-01T00:00:00Z", "seq": "1", "email": "a@b.co"},
        ]
        run_differential(spark, records, CDC_CONTRACT, CDC_RULES, case="op-aware")

    def test_observe_severity_and_wildcard_try_cast(self, spark):
        rules = [
            {"rule": "domain", "column": "name", "values": ["ok"], "severity": "observe"},
            {"rule": "try_cast", "column": "*", "severity": "observe"},
        ]
        records = [{"id": "1", "name": "weird", "qty": "junk"}]  # all observe: stays valid
        oracle, spark_quarantined = run_differential(
            spark, records, SIMPLE_CONTRACT, rules, case="observe-tier"
        )
        assert spark_quarantined == {} and len(oracle.valid) == 1


class TestRealRegistry:
    def test_order_events_ruleset_on_poison_batch(self, spark):
        contracts = loader.load_contracts()["source_contracts"]["order_events"]
        rules = loader.load_dq_rules()["order_events"]
        good = {
            "event_id": "e1",
            "order_id": "1",
            "event_type": "PAID",
            "customer_id": "10",
            "store_id": "1",
            "product_id": "7",
            "quantity": "2",
            "unit_price": "10.00",
            "currency": "USD",
            "event_ts": "2026-06-06T07:00:00Z",
            "region_source": "north",
        }
        records = [
            good,
            {**good, "event_id": "e2", "quantity": "-1"},  # range
            {**good, "event_id": "e3", "event_type": "REFUNDED"},  # domain
            {**good, "event_id": "e4", "customer_id": None},  # null_key (guest policy)
            {**good, "event_id": "e5", "event_ts": "2030-01-01T00:00:00Z"},  # plausibility
            {**good},  # exact duplicate of row 0
            {**good, "event_id": "e6", "unit_price": "N/A"},  # try_cast decimal
        ]
        run_differential(spark, records, contracts, rules, case="order_events poison")
