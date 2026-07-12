# GOLD | dim_customer, dim_product, dim_store — BI-friendly MVs over the hist tables.
# current = __END_AT IS NULL; deleted customers excluded (count exposed to reconciliation);
# active store = current version AND closed_date IS NULL.
#
# TODO: three @dp.materialized_view declarations
