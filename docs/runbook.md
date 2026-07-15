# Runbook

## Failed prod deploy — decision tree

1. `deploy-prod` red BEFORE "Deploy bundle (prod)": nothing changed in prod — fix
   forward on `develop`, new release PR.
2. Red AT deploy: bundles are idempotent — re-run the workflow. Repeated failure:
   inspect the step log; prod resources are partially updated but jobs/pipeline
   only switch atomically per resource.
3. Red AT smoke (deployed but not working): **default = roll back** (below), then
   diagnose on dev. Fix-forward only for trivially-obvious one-line causes.

## Rollback

**Code rollback (default):** Actions → `deploy-prod` → *Run workflow* → set `ref`
to the previous release tag (e.g. `v1.0.0`). This redeploys that exact bundle;
the approval gate still applies; no new tag is created. Release tags are created
automatically on every non-rollback prod deploy — they are the rollback anchors.

**Data rollback (rare):** silver/gold are materialized views — a code rollback
plus one orchestrator run rebuilds them from bronze. Bronze streaming tables and
`ops.*` history: use Delta `RESTORE TABLE <t> TO VERSION AS OF <n>`.
**Never full-refresh in prod without a decision record:** bronze re-ingestion
depends on files still present in the landing volume.

## Alert / failure response

| Signal | Meaning | First action |
|---|---|---|
| orchestrator task `invariant_checks` failed | conservation or SCD2 current-flag violation | read task output — it names the source/dim; check `ops.pipeline_runs` last row |
| task `post_run_audit` failed | audit could not compute (missing table/params) | pipeline likely failed earlier; check pipeline update log |
| `source_emulator` failed in dev | scenario/params issue | task output shows the ConfigError; guard failures mean target/vars mismatch |
| quarantine spike in `ops.dq_results` | upstream data faults | group by `reason`; scenario docs map reasons to fault types |

## Partial-outage play (risk R8)

The single pipeline means one gate can halt all domains. If a non-critical source
blocks the DAG: (1) confirm via dq_reconciliation which source; (2) an operator MAY
temporarily downgrade that source's gate severity in `conf/dq_rules.yml` via an
expedited PR (registry meta-tests still apply); (3) record the downgrade + restore
plan in the PR description. Never edit prod tables or workspace objects by hand.

## Token rotation (risk R2)

Rotate `DATABRICKS_TOKEN` quarterly or on any suspicion of exposure:
workspace → Settings → Developer → Access tokens → revoke + create; update the
GitHub Actions secret. Verify with a manual `drift-check` run (validates prod via
the new secret, changes nothing).

## Dev reset / scenario demos

`ops-run.yml` (workflow_dispatch): choose a scenario, optionally `full_refresh` to
re-read the entire landing volume (Auto Loader path checkpoints ignore overwritten
files — proven live in slice 7).
