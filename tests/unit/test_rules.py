"""Unit spec §1.7 — DQ rule engine + get_reason composition."""

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.skip(reason="skeleton — not implemented")]


def test_each_rule_type_clean_violating_boundary_null(): ...  # TODO: parametrize per rule type
def test_composed_reason_array_exact_set(): ...
def test_valid_quarantine_partition_total_and_disjoint(): ...
def test_validation_is_side_effect_free(): ...
def test_future_ts_boundary_now_plus_skew(): ...


# registry load-time validation + R1 catalog guard: implemented in test_config_loader.py
