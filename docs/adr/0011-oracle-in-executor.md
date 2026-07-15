# ADR-0011: Oracle-in-executor for stateful gold logic

**Status:** accepted
**Date:** 2026-07-14

## Context

Revenue lifecycle resolution and funnel attribution are per-group state
machines. Translating them into Spark expressions would be a large, subtle
compiler surface for logic that is embarrassingly parallel per order/customer.

## Decision

Gold runs the ACTUAL oracle code inside executors via applyInPandas /
cogroup-applyInPandas, grouped by order_id (revenue) and customer_id (funnel).
Money crosses the arrow boundary as strings and is cast to decimal exactly once.
Relational shapes (daily aggregates, FX identity, as-of SCD2 join) stay native
Spark with differential tests. Tests for oracle-in-executor paths are PARITY
checks (same code — wiring proof), not differentials.

## Consequences

- Zero translation risk for the most intricate business logic; one
  implementation to maintain.
- Costs: pandas/pyarrow dependency, per-group serialization overhead (fine at this
  scale), executors need the wheel (guaranteed by the pipeline environment).
- Revisit trigger: group sizes or throughput where arrow overhead dominates —
  then compile hot paths natively WITH differentials, per the established pattern.
