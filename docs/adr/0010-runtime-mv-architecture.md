# ADR-0010: Runtime architecture — streaming bronze, materialized-view silver/gold, scd2_compiler as SCD2 runtime

**Status:** accepted
**Date:** 2026-07-14

## Context

Slice 7 wired the differential-proven compilers into SDP. The DQ duplicate rule
and SCD2 rebuild are batch-scoped window computations that cannot run on streaming
DataFrames without semantic drift from the oracle. Portfolio scale is ~26k
rows/day.

## Decision

Bronze = Auto Loader streaming tables (exactly-once file ingestion, string
schema + rescue, lineage columns). Silver and gold = MATERIALIZED VIEWS calling the
compilers directly — full-recompute semantics identical to the oracle, replay-safe
by construction. SCD2 dims are rebuilt each update via spark/scd2_compiler
(event-merge rebuild), which also eliminates the known-keys dependency: every
upsert is always in-stream. auto_cdc_flow is deferred to v1.1, to be cross-checked
against the same canonical relation. Composite runtime tiebreak (approved slice-7
adjustment): _ingestion_order := _source_file + '#' + zero-padded in-file row
position, giving (sequence, file, row) total order.

## Consequences

- Correctness-first: runtime verdicts equal oracle verdicts (60 differential
  cases). Recompute cost is trivial at this scale.
- Revisit triggers: MV recompute latency/cost at higher volume → watermarked
  streaming silver; auto_cdc_flow adoption per ADR-0002.
- SDP constraint honored: all MV builders are fully lazy (eager actions are
  illegal at graph build; cdc_compiler exposes eager_counts=False).
