# Changelog

## v1.0.5 — 2026-07-17

Documentation release.

### Added
- "How this was built" — an explicit statement of AI pair-programming, placed in the
  executive summary so a reader meets it before the commit history. States what is
  defensible and points at the documented failures as evidence.

## v1.0.4 — 2026-07-17

Cleanup release. No new features — this removes code that was declared but never
wired, and completes one feature that was plumbed but never connected.

### Removed
- Integration assertion scaffolding (`integration/` package, its entry point, the
  `job.integration_tests` resource, and the disabled deploy-dev step). It never had
  an implementation; the real promotion gate is CI + a full dev orchestrator run +
  the post-run invariants task, and scenario evidence is recorded in docs/testing.md.
- `audit/event_log.py` — a stub; audit is computed directly from tables.
- `ingest/lineage.py` — the Spark bronze reader owns the lineage column contract;
  a second unenforced copy was dead weight.
- `transform.dedup.dedup_by_key` / `DedupResult` — orphaned oracle with no runtime
  caller (CDC normalization does its own dedup). The module kept only its live
  primitives and is now honestly named `transform/ordering.py`.

### Fixed
- `lateness_window_days` was a required pipeline parameter passed to every job task
  and consumed nowhere. Silver fact tables now carry a `_lateness` flag derived from
  it (`in_window` / `beyond_window`, boundary inclusive, flagged never dropped),
  held verdict-equal to `ingest.batches.classify_lateness` by a new differential test.

### Changed
- Docs now state the real promotion gate instead of a planned one.

## v1.0.3 — 2026-07-17

Documentation release.

### Changed
- Repository renamed `DataEn_project` -> `Retail-Lakehouse` to match the bundle,
  package, and README naming (GitHub redirects the old URL). All badges and
  links updated.

## v1.0.2 — 2026-07-17

Documentation release.

### Changed
- README rewritten as a professional portfolio document: 18 sections, four
  Mermaid diagrams (architecture, medallion, CI/CD, differential testing),
  verified pipeline outputs, lessons learned from real defects, and roadmap.

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
