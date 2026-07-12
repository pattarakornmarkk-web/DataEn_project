# Retail Lakehouse Platform

> Production-grade Databricks data engineering platform — SDP (Lakeflow Declarative
> Pipelines), Databricks Asset Bundles, Unity Catalog, pytest, GitHub Actions.
> Runs on Databricks Free Edition.

<!-- TODO: CI badge, coverage badge -->

**Last validated on:** <!-- TODO: date + Databricks platform state -->

## Problem Statement

<!-- TODO: 3-4 sentences — retail multi-source ingestion (batch snapshots, CDC feed,
streaming events) into a governed medallion lakehouse with quarantine-based DQ,
SCD2 history, full audit lineage (row -> commit), and gated CI/CD promotion. -->

## Architecture

<!-- TODO: diagram image (docs/architecture.md has the full design) -->

- One workspace, two Unity Catalog environments: `retail_dev` / `retail_prod`
- Landing volumes -> Bronze (Auto Loader) -> Silver (validate/split + SCD2) -> Gold (star schema MVs) -> Ops (audit, DQ, reconciliation)
- One SDP pipeline (28 objects), one orchestrating Lakeflow Job, deployed via DAB targets

## Quickstart

<!-- TODO:
1. clone, install (uv/pip), run unit tests locally
2. databricks auth login
3. databricks bundle deploy --target dev
4. bundle run orchestrator; open ops dashboard
5. run demo days (see mock_data/README.md)
-->

## Repository Map

| Path | Purpose |
|---|---|
| `src/retail_lakehouse/` | Installable library — all business logic (pytest-covered) |
| `pipelines/` | SDP bootstrap glue (parameters, config registry) |
| `transforms/` | Thin SDP declarations, one folder per medallion layer |
| `conf/` | Config-as-data: sources, contracts, DQ rules — ships inside the wheel as package data (ADR-0008) |
| `resources/` | DAB resources (pipeline, jobs, schemas, dashboard, alerts) |
| `mock_data/` | Scenario catalog — single parser in `retail_lakehouse.scenarios`; ships in the wheel (ADR-0008) |
| `tests/` | unit / integration / data_quality |
| `docs/` | Architecture, ADRs, testing traceability, runbook |

## Testing

<!-- TODO: pyramid summary + how to run each tier; link docs/testing.md -->

## Deployment

<!-- TODO: git strategy (feature/* -> develop -> main), environments, rollback; link runbook -->

## Threat Model & Known Limitations (Free Edition)

<!-- TODO: single CI identity (R2), catalog-level isolation + runtime guard (R1),
local-vs-serverless runtime drift (R3), no load testing. Be explicit. -->

## Demo Script

See [mock_data/README.md](mock_data/README.md) — each emulator "day" proves a named capability.

## Architecture Decision Records

See [docs/adr/](docs/adr/) — index in [docs/adr/README.md](docs/adr/README.md).
