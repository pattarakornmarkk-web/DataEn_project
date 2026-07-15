# ADR-0006: Orphan deletes are tagged and held, never dropped, never applied

**Status:** accepted
**Date:** 2026-07-12

## Context

CDC feeds occasionally deliver a delete for a key with no earlier-sequenced
upsert in the stream and unknown to the target (out-of-order capture, partial
backfills). Silently dropping loses information; applying creates history for an
entity that never existed.

## Decision

Such deletes are tagged `is_orphan_delete` and HELD in a dedicated, inspectable
table (silver.dim_customer_held_orphans). They are excluded from SCD2 application.
"Earlier-sequenced" is decided after normalization ordering, so a delete arriving
before its own insert in one batch is NOT an orphan if the insert's sequence
precedes it.

## Consequences

- Zero data loss; operators can inspect and, with a corrected feed, replay.
- Held orphans are a monitorable signal (v1.1 alert candidate).
- A delete for a key known only to the TARGET requires known-keys context — under
  the v1.0 full-rebuild runtime (ADR-0010) every applied upsert is in-stream, so
  the distinction is moot at runtime while remaining explicit in the library API.
