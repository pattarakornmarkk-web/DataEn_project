# Scenario Catalog — the Demo Script

One YAML per named day. **Single source shared by three consumers:** the source-emulator
job (writes landing files), pytest fixtures (in-memory DataFrames), and integration
assertions (referenced by scenario name).

All scenarios use a **fixed logical clock** (review item R6) — no wall-clock sampling.

## Demo order (each day proves a named capability)

| Scenario | Proves |
|---|---|
| `day1_clean` | cold start: 28 objects, conservation green, quarantine empty |
| `replay_day1` | exactly-once: re-dropped files add zero rows |
| `day2_late_cdc` | late-arriving CDC slots into SCD2 history correctly |
| `day3_out_of_order` | sequence/tiebreak handling, out-of-order lifecycle |
| `day4_deletes` | apply_as_deletes + orphan-delete policy |
| `poison_day` | nulls/dups/invalids/future-ts -> quarantine with exact reasons |
| `day5_schema_drift` | rescue column + drift gate |
| `day6_silence` | missing-data alerting (north sends nothing) |

<!-- TODO: full fault-injection matrix link -> docs/testing.md traceability table -->
