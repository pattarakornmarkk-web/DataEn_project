"""Deployed-pipeline scenario assertions (blueprint testing strategy §2).

Each function asserts one scenario's expected outcome against catalog tables and
returns a structured result (scenario, check, pass/fail, detail) — never raises on
data mismatch, so the runner can report all failures in one pass.
"""


def assert_cold_start(spark, catalog: str) -> list[dict]:
    raise NotImplementedError


def assert_replay_zero_deltas(spark, catalog: str) -> list[dict]:
    raise NotImplementedError


def assert_scd2_matches_oracle(spark, catalog: str) -> list[dict]:
    """Cross-check auto_cdc_flow output against transform.scd2 (the oracle, ADR-0002)."""
    raise NotImplementedError


def assert_quarantine_routing(spark, catalog: str) -> list[dict]:
    raise NotImplementedError


def assert_schema_drift_handling(spark, catalog: str) -> list[dict]:
    raise NotImplementedError


def assert_late_fact_aggregate_correction(spark, catalog: str) -> list[dict]:
    raise NotImplementedError


def assert_idempotent_rerun(spark, catalog: str) -> list[dict]:
    raise NotImplementedError
