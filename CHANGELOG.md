# Changelog

## v1.0.1 — 2026-07-17

Day-1 validation release: all 8 scenario days executed on retail_dev with
evidence recorded (run ids + observed quarantine reasons in docs/testing.md).

### Fixed
- poison_day shared day4's logical clock; identical file names were silently
  skipped by Auto Loader's path checkpoint. Clock moved to a unique date and a
  meta-test now enforces clock uniqueness across batch scenarios.

### Added
- post-run audit prints the quarantine reason breakdown (per source.reason
  counts) — the DQ evidence line for scenario runs.
- Workspace-proof column completed in the testing traceability table.

### Known behavior (v1.1 candidate)
- Re-sent snapshot rows (same key, new payload) accumulate cross-batch
  key_duplicate quarantine because silver validates the full bronze history;
  dimensions remain correct via CDC unification. Planned refinement: scope the
  duplicate rule per _batch_date for snapshot sources.

## v1.0.0 — 2026-07-14

First production release. End-to-end verified on Databricks Free Edition
(serverless, Spark 4.0/ANSI): 26,375 rows across 6 sources, all conserved,
all invariants healthy.

### Platform
- Medallion pipeline (Lakeflow SDP): 6 Auto Loader bronze streaming tables,
  validated/quarantine silver MVs, 3 SCD2 history dimensions + held-orphans
  table, gold star schema (exact-decimal revenue, FX, as-of joins, funnel,
  daily aggregates), ops audit + reconciliation.
- Deterministic scenario emulator: 8 scenario days, 23 fault handlers,
  byte-identical replay, logical-clock time.
- Deployment: Databricks Asset Bundles (dev/prod catalog targets), GitHub
  Actions (CI → dev bake → gated prod release with smoke + auto-tagging),
  weekly drift check, manual ops-run.

### Engineering approach
- Oracle-first pure-Python semantics (DQ, dedup, CDC, SCD2, revenue, FX,
  aggregates, funnel) — 292 tests.
- Differential testing: Spark compilers held verdict-equal to the oracle
  (60 cases + seeded property tests with shrinking).
- Conservation by construction; completeness meta-tests; R1 runtime catalog
  guard; config-as-data packaged in the wheel.

### Known v1.0 limitations (see README threat model)
- In-workspace integration assertion suite stubbed (day1 orchestrator +
  invariants are the promotion gate).
- Scenario days 2–6 proven at oracle+differential tier; workspace runs are the
  first post-release task.
- Dashboards/alerts resources, auto_cdc_flow cross-check, event-log audit,
  PII governance: v1.1 roadmap.
