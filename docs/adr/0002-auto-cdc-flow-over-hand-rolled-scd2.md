# ADR-0002: Managed SCD2 as production target; hand-rolled SCD2 kept as the oracle

**Status:** accepted (production mechanism amended by ADR-0010)
**Date:** 2026-07-12

## Context

The predecessor project implemented SCD2 by hand with a non-atomic two-phase
MERGE (close, then append) and processing-time effective dates — both correctness
hazards. Databricks offers managed SCD2 (auto_cdc_flow) with sequence ordering and
atomic application.

## Decision

Production SCD2 uses a managed/compiled mechanism, never the legacy two-phase
merge. The hand-rolled implementation survives as `transform/scd2.py` — the pure
ORACLE that defines correct semantics: sequence-derived effective dates, contiguous
non-overlapping intervals, exactly-one-current per key, replay zero-delta,
tombstone deletes. Any runtime implementation must reproduce the oracle's canonical
relation on shared fixtures (differential tests).

## Consequences

- The oracle's scope is FROZEN to the documented scenario set (risk R7): a
  differential mismatch is a runtime defect by default; changing the oracle
  requires an ADR update.
- ADR-0010 selected spark/scd2_compiler.py (differential-proven event-merge
  rebuild) as the v1.0 runtime; auto_cdc_flow cross-checked against the same
  canonical relation is the v1.1 path back to this ADR's original target.
