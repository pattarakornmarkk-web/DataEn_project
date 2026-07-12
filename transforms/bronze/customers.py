# BRONZE | customers_raw — weekly full snapshots, Auto Loader, string-typed, append-only.
# Declaration only (<=5 lines of body): read via registry source config + lineage columns.
# Downstream: silver validation view. Warn-only expectations. Blueprint: Bronze Design §4.
#
# TODO: @dp.table declaration -> spark.readStream cloudFiles + with_lineage_columns()
