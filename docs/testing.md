# Testing Traceability

## Coverage plan

The gate lives in `pyproject.toml` (`[tool.coverage.report] fail_under`) — nowhere else.
It rises with implementation; **raising it is part of the definition-of-done for each phase**:

| Phase | Scope implemented | Gate | Status |
|---|---|---|---|
| 0 | skeleton only (all tests skip-marked) | 0 | ✅ done |
| 1 | `dq/`, `config/`, `ingest/`, `scenarios/` parser | 60 | ✅ done (82% actual; `scenarios/generator` moved to phase 2 with the emulator) |
| 2 | `transform/` (full oracle layer), `config/params` | 80 | ✅ done (92% actual) |
| 3 | Spark compiler, `audit/`, emulator/generator, entrypoints (blueprint target) | 90 | pending |

A PR that implements a module without un-skipping its tests and raising the gate is
incomplete. Never lower the gate; exceptions require an ADR.

<!-- TODO: the traceability table — fault type -> scenario (mock_data/scenarios/) ->
test IDs (unit / integration / dq). Every fault in the injection matrix must map to
at least one test ID. -->

| Fault | Scenario | Unit test | Integration test |
|---|---|---|---|
| _TODO_ | | | |
