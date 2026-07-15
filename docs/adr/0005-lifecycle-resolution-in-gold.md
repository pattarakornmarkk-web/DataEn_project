# ADR-0005: Order lifecycle resolution deferred to gold; CANCELLED is sticky terminal

**Status:** accepted
**Date:** 2026-07-12

## Context

Order events (PLACED/PAID/SHIPPED/CANCELLED) arrive out of order across two
emitters. Resolving lifecycle state in silver would make the fact table
non-replayable; resolving per-event would double-count revenue.

## Decision

Silver stores immutable events exactly as validated (replayable forever). Gold
resolves lifecycle per order over the SEQUENCE ordering: any CANCELLED event makes
the order terminally cancelled — even events sequenced later never reinstate it
(reinstatement requires a new order_id). Revenue eligibility = has PAID and not
cancelled; the max-sequence PAID event carries the revenue facts. Excluded orders
are ABSENT with a reason, never zero-valued rows (per-order conservation).

## Consequences

- fct_sales recomputes correctly when late events arrive (MV semantics).
- The "cancelled_before_placed" fault is deterministic in both worlds; the policy
  is pinned by oracle tests and enforced at runtime via oracle-in-executor
  (ADR-0011).
