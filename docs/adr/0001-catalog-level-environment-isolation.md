# ADR-0001: Catalog-level environment isolation

**Status:** accepted
**Date:** 2026-07-12

## Context

Databricks Free Edition provides exactly one workspace, so the standard
dev-workspace / prod-workspace separation is unavailable. The platform still needs
two fully isolated environments with identical shape.

## Decision

Environments are Unity Catalog catalogs — `retail_dev` and `retail_prod` — inside
one workspace, selected exclusively by the DAB target's `catalog` variable. Targets
may differ only in variable values and bundle mode ("parity by construction").
A runtime guard (`assert_catalog_matches_environment`, the "R1 guard") runs at SDP
graph build and at the start of every wheel task: a `retail_prod` catalog paired
with a `dev` tag (or any unknown pairing) refuses to run before any table is touched.

## Consequences

- Promotion dev→prod is a variable change, not a topology change; paid-tier
  multi-workspace migration is a secrets/host change only.
- Cross-environment writes are blocked by code, not platform permissions — the
  guard is unit-tested and fired for real (slice 7's first run was stopped by it
  when task parameters were incomplete).
- Revisit trigger: paid tier → separate workspaces + per-env service principals;
  the guard stays as defense-in-depth.
