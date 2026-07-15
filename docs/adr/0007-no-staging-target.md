# ADR-0007: No staging target; the dev integration run is the bake

**Status:** accepted
**Date:** 2026-07-12

## Context

Three environments (dev/staging/prod) is the enterprise default, but this
platform has one developer, one workspace (Free Edition), and a deterministic
scenario emulator that makes dev runs reproducible.

## Decision

Two DAB targets only. Every merge to develop deploys retail_dev AND executes a
full orchestrator run (emulator → pipeline → audit → invariants) — that run is the
staging bake. main receives release PRs only, gated by the prod environment
reviewer.

## Consequences

- Less infrastructure, faster promotion, one fewer catalog to drift.
- Revisit trigger: >1 concurrent feature stream needing isolated integration runs,
  or shared-team ownership — then add a staging target (a variables-only change).
