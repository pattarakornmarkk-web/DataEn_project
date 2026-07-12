# GOLD/OPS | dq_reconciliation — the in-DAG audit anchor (waits on everything upstream).
# Per _batch_date: bronze = silver + quarantine conservation, orphan-FK rates,
# SCD2 version-churn counts. Consumed by dashboard, alerts, post-run audit.
#
# TODO: @dp.materialized_view declaration
