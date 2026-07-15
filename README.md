# Retail Lakehouse Platform

> Production-grade Databricks data engineering platform: Lakeflow Spark Declarative
> Pipelines, Databricks Asset Bundles, Unity Catalog, pytest, GitHub Actions —
> running end-to-end on **Databricks Free Edition**.

**Version 1.0.0** · **Last validated:** 2026-07-14 on Databricks serverless (Spark 4.0, ANSI) · Python 3.11

## What this is

A retail lakehouse that ingests three source patterns — batch snapshots (customers,
products, stores), a CDC feed with inserts/updates/deletes (customer_updates), and
streaming events from multiple emitters (order_events north/south, clickstream) —
through a medallion architecture with quarantine-based data quality, SCD Type 2
history, exact-decimal revenue with FX normalization, and run-level audit that traces
every number back to a git commit.

Every deliberate data fault (nulls, duplicates, invalid values, late arrivals,
out-of-order events, orphan deletes, replays, schema drift) is injected by a
deterministic scenario emulator and caught by a named control — the fault-injection
matrix is the traceability spine from design to test to demo.

## The engineering ideas worth stealing

1. **Oracle-first semantics.** Every business rule (DQ verdicts, dedup, CDC
   normalization, SCD2, revenue lifecycle, FX, funnel) is defined FIRST as pure
   Python (`src/retail_lakehouse/{dq,transform}`) — deterministic, testable in
   milliseconds, zero Spark. The Spark layer is a *translation*, never a decision.
2. **Differential testing.** The Spark compilers (`src/retail_lakehouse/spark/`)
   are held verdict-equal to the oracle on shared fixtures — 60 differential cases
   plus seeded property tests with shrink-to-minimal-subset diagnosis. The harness
   caught real defects before production did: a +7h JVM-timezone shift, a
   blank-string-vs-null semantic divergence, and Spark 4 ANSI cast traps.
3. **Conservation by construction.** `bronze == valid + quarantine` is asserted
   inside the engine, recomputed by an in-DAG reconciliation view, and re-checked by
   a post-run invariants task that fails the job. Three independent definitions must
   agree.
4. **Config as data, shipped in the wheel.** Source contracts, DQ rules, FX rates,
   and scenarios are versioned YAML packaged into the artifact (ADR-0008) — identical
   access in CI, wheel tasks, and serverless; config changes ride the same CI gate
   as code.
5. **Completeness meta-tests.** A new DQ rule type cannot exist without a compiler
   entry; a scenario fault cannot exist without a generator handler; a registry
   cannot silently lose its minimum rules. CI fails on the *absence* of things.

## Verified end-to-end run (retail_dev, day1_clean)

```
customers          bronze= 5,000  valid= 5,000  quarantine= 0  conserved=True
products           bronze=   300  valid=   300  quarantine= 0  conserved=True
stores             bronze=    25  valid=    25  quarantine= 0  conserved=True
customer_updates   bronze=    50  valid=    50  quarantine= 0  conserved=True
order_events       bronze= 1,000  valid= 1,000  quarantine= 0  conserved=True
customer_activity  bronze=20,000  valid=20,000  quarantine= 0  conserved=True
all invariants healthy
```

Pipeline: 6 Auto Loader streaming tables → 12 validated/quarantine MVs → 3 SCD2
history dims (+ held-orphans table) → gold star schema (as-of joins, exact-decimal
revenue, funnel, daily aggregates) → `ops.pipeline_runs` / `ops.dq_results`.

## Architecture

```
GitHub (feature/* → develop → main)          Databricks Free Edition (one workspace)
  ci: ruff · pytest(292) · differential        retail_dev ←── DAB target dev
      wheel build · bundle validate ×2         retail_prod ←─ DAB target prod (gated)
  deploy-dev: deploy + orchestrator run      ┌──────────────────────────────────┐
  deploy-prod: approval gate + smoke + tag   │ landing volume → BRONZE (stream) │
                                             │  → SILVER (MV: DQ split, SCD2)   │
Scenario emulator (deterministic, seeded)    │  → GOLD (MV: revenue/FX/funnel)  │
  writes landing files per scenario day      │  → OPS (audit, reconciliation)   │
                                             └──────────────────────────────────┘
```

Environment isolation is **catalog-level** (Free Edition has one workspace); the R1
runtime guard makes cross-environment writes fail at graph build. Full design:
[docs/architecture.md](docs/architecture.md) · decisions: [docs/adr/](docs/adr/README.md).

## Quickstart

```bash
git clone https://github.com/pattarakornmarkk-web/DataEn_project && cd DataEn_project
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"                       # needs Java 17+ for the spark tier
pytest -m "not spark"                          # oracle tier: ~1s
pytest -m "(unit or dq) and not integration"   # full local suite incl. differential

databricks auth login                          # then, against your workspace:
databricks bundle deploy --target dev
databricks bundle run source_emulator --target dev --params scenario=day1_clean
databricks bundle run orchestrator --target dev
```

Scenario demos (each day proves a named capability): [mock_data/README.md](mock_data/README.md).

## Repository map

| Path | Purpose |
|---|---|
| `src/retail_lakehouse/` | The wheel. `config/ dq/ ingest/ transform/ scenarios/` are the **pure oracle** (an import-wall meta-test keeps pyspark out forever); `spark/` is the compiler layer; `audit/ entrypoints/` are the ops runtime |
| `transforms/` | Three registration-only SDP files — all logic lives in the wheel |
| `conf/` | Contracts, DQ rules, FX rates — config as data, packaged (ADR-0008) |
| `resources/` | DAB: pipeline, orchestrator/emulator/integration jobs, schemas |
| `mock_data/` | Scenario catalog (single parser in `scenarios/`) — shared by emulator, tests, demos |
| `tests/` | `unit/` (oracle) · `differential/` (oracle vs Spark) · `data_quality/` (registry meta-tests) · `integration/` (in-workspace) |
| `docs/` | Architecture, 11 ADRs, runbook, testing traceability |

## Testing & release

- **292 tests** (oracle, differential, meta) run on every PR; coverage gate 80%
  (modules measurable only in-workspace are excluded from the local number).
- `develop` merge ⇒ deploy to `retail_dev` + a full orchestrator run (the staging bake).
- `main` merge (release PR only) ⇒ **required-reviewer approval** on the `prod`
  environment ⇒ deploy to `retail_prod` ⇒ smoke run ⇒ **release tag** (the rollback
  anchor — see [docs/runbook.md](docs/runbook.md)).

## Threat model & known limitations (stated, not hidden)

- **Single CI identity** (Free Edition): one PAT in GitHub Secrets drives both
  environments; a leak compromises both. The prod approval gate is GitHub-side.
  Paid-tier path: per-env service principals + workspace permissions.
- **Solo-admin bypass:** branch protections exclude admins (`enforce_admins=false`)
  so the integration flow can push to `develop`; the hard release stop is the prod
  environment reviewer, which applies to everyone.
- **Local Spark ≠ serverless DBR** (pinned pyspark 4.0): the differential tier is
  local truth; the deployed orchestrator run is the arbiter.
- **v1.0 scope cuts:** in-workspace integration assertion suite is stubbed (day1
  orchestrator + invariants are the current gate); scenario days 2–6 are proven at
  oracle+differential tier, with workspace runs via `ops-run.yml` as the first
  post-release task; dashboards/alerts resources are v1.1; `auto_cdc_flow`
  cross-check deferred (ADR-0010).
- No load/performance testing — Free Edition scale (~26k rows/day) proves
  correctness, not throughput.

## Roadmap (v1.1+)

Integration assertion suite → scenario days 2–6 on workspace → ops dashboard +
SQL alerts → auto_cdc_flow cross-check → event-log-based audit → PII tagging &
masking → streaming silver (watermarked) at scale.

## License

MIT — see [LICENSE](LICENSE).
