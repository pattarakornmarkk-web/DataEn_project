# SILVER | dim_customer_hist — SCD2 via auto_cdc_flow.
# Upstream: _v_customer_cdc_unified (the snapshot+CDC unification view — ADR-0004):
#   customers snapshots -> synthetic op=U events, unioned with customer_updates.
# keys=[customer_id], sequence_by=(change_ts, change_seq), apply_as_deletes on op=D,
# stored_as_scd_type=2. NEVER full-refreshed in prod (protection list).
#
# TODO: @dp.view (unification via transform.cdc) + dp.create_streaming_table
#       + dp.create_auto_cdc_flow
