# Runbook

## Failed prod deploy — decision tree
<!-- TODO: default = redeploy previous tag (workflow_dispatch with ref); fix-forward criteria -->

## Rollback
<!-- TODO: code rollback (tag redeploy) vs data rollback (Delta RESTORE);
never-full-refresh protection list: SCD2 hist tables, quarantine tables -->

## Alert response
<!-- TODO: one section per alert in resources/alerts.yml -->

## Partial-outage play (R8)
<!-- TODO: which gates may be temporarily downgraded, who decides, how it's recorded -->

## Token rotation (R2)
<!-- TODO: rotate DATABRICKS_TOKEN in GitHub Environments; cadence -->
