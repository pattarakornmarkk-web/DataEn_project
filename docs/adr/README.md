# Architecture Decision Records

| # | Title | Status |
|---|---|---|
| [0001](0001-catalog-level-environment-isolation.md) | Catalog-level environment isolation (Free Edition) | accepted |
| [0002](0002-auto-cdc-flow-over-hand-rolled-scd2.md) | Managed SCD2 target; hand-rolled SCD2 kept as the oracle | accepted (amended by 0010) |
| [0003](0003-config-as-data-in-git.md) | Config as data in git, projected at deploy | accepted |
| [0004](0004-snapshot-cdc-unification.md) | Snapshot + CDC unification into one customer stream | accepted |
| [0005](0005-lifecycle-resolution-in-gold.md) | Lifecycle resolution in gold; CANCELLED sticky terminal | accepted |
| [0006](0006-orphan-delete-policy.md) | Orphan deletes: tagged and held, never dropped | accepted |
| [0007](0007-no-staging-target.md) | No staging target; dev integration run is the bake | accepted |
| [0008](0008-runtime-packaging-package-data-in-wheel.md) | Runtime packaging: conf/ + scenarios ship in the wheel | accepted |
| [0009](0009-deterministic-tiebreak-policy.md) | Deterministic sequence tie-break policy | accepted |
| [0010](0010-runtime-mv-architecture.md) | Runtime: streaming bronze, MV silver/gold, scd2_compiler runtime | accepted |
| [0011](0011-oracle-in-executor.md) | Oracle-in-executor for stateful gold logic | accepted |

Template: [template.md](template.md)
