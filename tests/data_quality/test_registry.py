"""DQ registry meta-tests — run in CI (local, no workspace).

Coverage cannot silently regress: these fail if conf/dq_rules.yml weakens, if a rule
references a phantom column, or if a scenario fault loses its catching rule.
"""

import pytest

from retail_lakehouse.config import loader
from retail_lakehouse.dq.rules import bind_ruleset
from retail_lakehouse.scenarios.parser import KNOWN_FAULTS

pytestmark = pytest.mark.dq

# Quarantine-tier faults from the scenario vocabulary -> the registry rule that
# catches them (source, rule, column; None = columnless). Faults resolved by design
# rather than row rules (replay, sequencing, orphans, drift) are listed as exempt.
FAULT_RULE_MAP = {
    "null_key": ("customers", "null_key", "customer_id"),
    "row_duplicate": ("customers", "duplicate", None),
    "key_duplicate": ("customers", "duplicate", None),
    "invalid_birth_date": ("customers", "plausibility", "birth_date"),
    "invalid_loyalty_tier": ("customers", "domain", "loyalty_tier"),
    "duplicate_event_id": ("order_events", "duplicate", None),
    "negative_quantity": ("order_events", "range", "quantity"),
    "future_event_ts": ("order_events", "plausibility", "event_ts"),
    "null_customer_id": ("order_events", "null_key", "customer_id"),
    "null_change_ts": ("customer_updates", "null_key", "change_ts"),
    "invalid_op": ("customer_updates", "domain", "op"),
}
DESIGN_TIER_FAULTS = {
    # engine-external by design: CDC sequencing, lifecycle, drift, reconciliation
    "broken_store_ref",
    "orphan_update",
    "orphan_delete",
    "delete_events",
    "late_arriving_events",
    "out_of_order_within_batch",
    "equal_sequence_tiebreak",
    "cancelled_before_placed",
    "stale_redelivery",
    "extra_column_south",
    "extra_column_south_all_rows",
    "epoch_millis_timestamps",
}


@pytest.fixture(scope="module")
def registry():
    return loader.load_dq_rules()


@pytest.fixture(scope="module")
def contracts():
    return loader.load_contracts()


def test_every_rule_references_real_source_and_column(registry, contracts):
    sources = contracts["source_contracts"]
    for source, rules in registry.items():
        assert source in sources, source
        for rule in rules:
            column = rule.get("column")
            if column not in (None, "*"):
                assert column in sources[source]["columns"], f"{source}: {rule}"


def test_every_source_binds_cleanly(registry, contracts):
    for source, rules in registry.items():
        bind_ruleset(source, contracts["source_contracts"][source], rules)


def test_every_source_has_minimum_key_null_and_dup_rules(registry):
    for source, rules in registry.items():
        types = {r["rule"] for r in rules}
        assert "null_key" in types, source
        assert "duplicate" in types, source


def test_severity_values_valid_and_thresholds_in_range(registry):
    for rules in registry.values():
        for rule in rules:
            assert rule["severity"] in loader.KNOWN_SEVERITIES
            if "threshold_pct" in rule:
                assert 0 < rule["threshold_pct"] <= 100


def test_no_gate_rules_on_observe_tier_activity(registry):
    assert all(r["severity"] != "gate" for r in registry["customer_activity"])


def test_no_observe_rules_on_order_event_keys(registry):
    for rule in registry["order_events"]:
        if rule["rule"] in ("null_key", "duplicate") and rule.get("column") in ("event_id", None):
            assert rule["severity"] == "quarantine", rule


def test_every_scenario_fault_is_mapped_or_design_exempt():
    assert set(FAULT_RULE_MAP) | DESIGN_TIER_FAULTS == set(KNOWN_FAULTS)
    assert not (set(FAULT_RULE_MAP) & DESIGN_TIER_FAULTS)


def test_fault_rule_map_entries_exist_in_registry(registry):
    for fault, (source, rule_type, column) in FAULT_RULE_MAP.items():
        matches = [
            r
            for r in registry[source]
            if r["rule"] == rule_type and (column is None or r.get("column") == column)
        ]
        assert matches, f"fault {fault!r} has no catching rule {rule_type}:{column} on {source}"
