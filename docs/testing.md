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
| null_key / null_customer_id / null_change_ts | null_key rule → quarantine | test_rules, test_engine (op-aware) | test_dq_differential | ✅ poison_day 2026-07-17 (run 29560772153): customer_id_null_key ×2, change_ts_null_key, customer_id_null_key |
| row/key duplicate, duplicate_event_id | duplicate rule (window pair) | test_engine duplicates | test_dq_differential batch | ✅ day1 clean + poison_day: row_duplicate ×2, key_duplicate (cross-emitter THB variant) ×2 |
| invalid_loyalty_tier / invalid_op | domain rule | test_rules CASES | test_dq_differential | ✅ poison_day: loyalty_tier_domain ×2, op_domain ×1 |
| invalid_birth_date (impossible date) | try_cast | test_rules CASES | test_dq_differential | ✅ poison_day: birth_date_try_cast ×2 |
| negative_quantity | range rule | test_rules CASES | order_events poison batch | ✅ poison_day: quantity_range ×1 |
| future_event_ts | plausibility (+skew boundary) | test_rules boundary pair | test_dq_differential | ✅ poison_day: event_ts_plausibility ×1 |
| late_arriving_events | SCD2 re-sequencing | test_scd2 late slotting | test_scd2_differential | ✅ day2 (run 29488084632): +80 late events, invariants healthy |
| out_of_order_within_batch / equal_sequence_tiebreak | CDC ordering + ADR-0009 tiebreak | test_cdc, test_dedup | test_cdc_differential | ✅ day3 (run 29498965291) |
| delete_events / orphan_delete | tombstones / hold (ADR-0006) | test_scd2 deletes, test_cdc orphans | test_scd2_differential, test_cdc_differential | ✅ day4 (run 29500297656): deletes applied, orphan held, one-current healthy |
| replay (files / events) | Auto Loader checkpoint / CDC drop | test_generator byte-identity, test_cdc replay | replay differentials | proven live: overwritten files not re-ingested (slice 7) |
| cancelled_before_placed | sticky-terminal (ADR-0005) | test_revenue | gold revenue parity | ✅ day3 batch (event applied; fct_sales excludes order) |
| extra_column_south / epoch_millis | rescue column / coercion | test_generator drift | — (rescue is engine behavior) | ✅ day5 (run 29561762052): +1,100 orders w/ _rescued_data, +25k epoch-millis activity — zero new quarantine (tolerate tier) |
| stale_redelivery | sequence ignores older | test_cdc / test_scd2 | test_cdc_differential | ✅ day3: older update_date ignored |
| broken_store_ref / orphan_update | reconciliation tier (observe) | registry exempt-list meta-test | — | v1.1 |
| day6 silence | freshness alerting | — | — | v1.1 (alerts resource) |

## v1.0 status

- 292 tests: oracle unit (~200), differential (60), registry/meta, harness.
- Coverage gate 80% (pyproject) — declarations/entrypoints are only executable
  in-workspace and are validated by the deployed orchestrator run instead.
- Workspace proof completed 2026-07-17: all 8 scenarios executed on retail_dev via
  ops-run — every run conserved, every invariant healthy. See "Day-1 findings" below.

## Day-1 findings (2026-07-17)

Running the full catalog on the workspace surfaced two real behaviors:

1. **Logical-clock collision (fixed).** poison_day shared day4's clock, producing
   identical file names that Auto Loader's path checkpoint silently skipped — the
   orders/updates poison batches never landed on the first attempt. poison_day moved
   to a unique date and a meta-test now enforces clock uniqueness across batch
   scenarios (replays exempt).
2. **Cross-batch snapshot duplicates (known behavior, v1.1 refinement).** Re-sent
   customer snapshot rows (same key, new payload) are flagged `key_duplicate`
   against prior batches because silver validates over the FULL bronze history
   (MV semantics). Weekly snapshots therefore accumulate quarantine rows
   (customers: 599 on poison day) while the dimension itself stays correct — the
   CDC-unification path sequences snapshots by `updated_at` regardless. v1.1:
   scope the duplicate rule per `_batch_date` for snapshot sources.

Final day-6 state: orders 6,302 · activity 88,000 · quarantine exactly the injected
faults · all sources conserved · all invariants healthy across every run.
