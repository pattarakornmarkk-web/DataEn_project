# ADR-0009: Deterministic sequence tie-break policy

**Status:** accepted
**Date:** 2026-07-12

## Context

Dedup and SCD2 sequencing order records by a business sequence column
(`change_ts`, `effective_date`, `update_date`). Real feeds produce ties: two updates
with identical timestamps, two product versions with the same `effective_date`.
Non-deterministic tie handling makes runs unreproducible and the SCD2 oracle
cross-check meaningless.

## Decision

Every SCD2 entity contract in `conf/contracts.yml` MUST declare a `tiebreak` column
(e.g. `change_seq` for customer_updates). Contract loading fails if an SCD2 entity
lacks one. Where the source has no natural tiebreaker (products), the contract
designates ingestion order metadata (`_source_file`, then row position within file) —
documented per entity, never implicit. `transform.dedup` and `transform.scd2` take the
tiebreak column explicitly; ties broken descending on it (highest wins), consistently
in both implementations and in the `sequence_by` fed to auto_cdc_flow.

## Consequences

- Same input -> byte-identical output everywhere (unit-test stability contract).
- The unit spec "equal sequence uses documented tiebreaker" pins this behavior.
- Cost: contracts carry one more required field; the loader enforces it.
