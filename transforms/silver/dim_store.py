# SILVER | dim_store_hist — SCD2 via auto_cdc_flow.
# keys=[store_id], sequence_by=update_date. Closure = attribute change, NOT delete (ADR).
#
# TODO: streaming table + auto_cdc_flow declaration
