# Testing Traceability

## Coverage plan

The gate lives in `pyproject.toml` (`[tool.coverage.report] fail_under`) — nowhere else.
It rises with implementation; **raising it is part of the definition-of-done for each phase**:

| Phase | Scope implemented | Gate |
|---|---|---|
| 0 | skeleton only (all tests skip-marked) | 0 |
| 1 | `dq/`, `config/`, `ingest/`, `scenarios/` | 60 |
| 2 | `transform/`, `audit/` | 80 |
| 3 | everything (blueprint target) | 90 |

A PR that implements a module without un-skipping its tests and raising the gate is
incomplete. Never lower the gate; exceptions require an ADR.

<!-- TODO: the traceability table — fault type -> scenario (mock_data/scenarios/) ->
test IDs (unit / integration / dq). Every fault in the injection matrix must map to
at least one test ID. -->

| Fault | Scenario | Unit test | Integration test |
|---|---|---|---|
| _TODO_ | | | |
