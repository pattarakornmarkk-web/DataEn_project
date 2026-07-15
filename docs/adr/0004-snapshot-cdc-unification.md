# ADR-0004: Snapshot + CDC unification into one customer event stream

**Status:** accepted
**Date:** 2026-07-12

## Context

The customer entity has two sources with different shapes: weekly full snapshots
(customers) and an intraday CDC feed (customer_updates with I/U/D ops and a
change_ts/change_seq sequence). Two application paths into one dimension invite
divergence.

## Decision

Snapshot rows are normalized into SYNTHETIC UPSERTS (op=U, sequenced by
updated_at mapped onto the change_ts axis, tiebreak null so a CDC event wins any
exact tie) and unioned with the CDC stream. One normalized stream, one SCD2
application path. The union asserts both branches carry identical payload schemas.

## Consequences

- Snapshot-vs-CDC reconciliation is solved by construction — there is only one
  stream, so there is nothing to reconcile.
- Op-aware DQ (delete images exempt from non-key null checks) lives in the shared
  validation path, keyed off the source contract's op_column/delete_payload.
