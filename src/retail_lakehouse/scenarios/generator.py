"""Scenario spec -> generated rows (per source, faults injected, logical clock applied).

Shared by the emulator (rows -> landing files) and test fixtures (rows -> DataFrames).
Determinism contract: same scenario name => byte-identical rows (seeded, logical clock,
no wall-clock sampling — review item R6).
"""


def generate_batches(scenario: dict) -> dict:
    """Return {source_name: rows} for every batch in the scenario spec."""
    raise NotImplementedError
