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



## Fault → control → test traceability

| Fault | Caught by | Oracle/unit test | Differential | Workspace proof |
|---|---|---|---|---|
| null_key / null_customer_id / null_change_ts | null_key rule → quarantine | test_rules, test_engine (op-aware) | test_dq_differential | poison_day (ops-run, post-release) |
| row/key duplicate, duplicate_event_id | duplicate rule (window pair) | test_engine duplicates | test_dq_differential batch | day1 run (0 false positives after generator fix) |
| invalid_loyalty_tier / invalid_op | domain rule | test_rules CASES | test_dq_differential | poison_day |
| invalid_birth_date (impossible date) | try_cast | test_rules CASES | test_dq_differential | poison_day |
| negative_quantity | range rule | test_rules CASES | order_events poison batch | poison_day |
| future_event_ts | plausibility (+skew boundary) | test_rules boundary pair | test_dq_differential | poison_day |
| late_arriving_events | SCD2 re-sequencing | test_scd2 late slotting | test_scd2_differential | day2 (ops-run) |
| out_of_order_within_batch / equal_sequence_tiebreak | CDC ordering + ADR-0009 tiebreak | test_cdc, test_dedup | test_cdc_differential | day3 (ops-run) |
| delete_events / orphan_delete | tombstones / hold (ADR-0006) | test_scd2 deletes, test_cdc orphans | test_scd2_differential, test_cdc_differential | day4 (ops-run) |
| replay (files / events) | Auto Loader checkpoint / CDC drop | test_generator byte-identity, test_cdc replay | replay differentials | proven live: overwritten files not re-ingested (slice 7) |
| cancelled_before_placed | sticky-terminal (ADR-0005) | test_revenue | gold revenue parity | poison_day |
| extra_column_south / epoch_millis | rescue column / coercion | test_generator drift | — (rescue is engine behavior) | day5 (ops-run) |
| stale_redelivery | sequence ignores older | test_cdc / test_scd2 | test_cdc_differential | day3 (ops-run) |
| broken_store_ref / orphan_update | reconciliation tier (observe) | registry exempt-list meta-test | — | v1.1 |
| day6 silence | freshness alerting | — | — | v1.1 (alerts resource) |

## v1.0 status

- 292 tests: oracle unit (~200), differential (60), registry/meta, harness.
- Coverage gate 80% (pyproject) — declarations/entrypoints are only executable
  in-workspace and are validated by the deployed orchestrator run instead.
- "Workspace proof: ops-run" rows are the documented day-1 post-release task.
