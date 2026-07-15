# ADR-0003: Config as data in git

**Status:** accepted
**Date:** 2026-07-12

## Context

The predecessor stored pipeline configuration in a hand-inserted Delta table:
unreviewable, unversioned, and divergent between environments by accident.

## Decision

All configuration is versioned YAML in `conf/` — source registries, schema
contracts (source vs entity split), DQ rules, FX rates — validated at load time
(unknown fields/types are errors, never no-ops) and consumed through one loader.
Registry weakening is prevented by meta-tests (minimum rule coverage, fault→rule
mapping, severity policy).

## Consequences

- Config changes are pull requests: diffable, reviewable, promoted with the code
  that interprets them.
- The runtime never mutates config; anything ops-adjustable at runtime must be a
  pipeline/job parameter instead (see ADR-0008 revisit trigger).
