# GOLD | fct_sales — the convergence point (wave 5). One row per completed order line.
# Lifecycle resolution (transform.revenue) + AS-OF SCD2 join to dim_product_hist
# (event_ts between __START_AT/__END_AT — never "current row"). Temporal orphans
# kept with null product attrs + flag.
#
# TODO: @dp.materialized_view declaration
