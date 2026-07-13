# SILVER | <source>_quarantine x6 — streaming tables, flagged rows in original string
# form + reason array + lineage. Exactly-once (fixes the legacy append duplication).
#
# TODO: @dp.table declarations filtering the validated view on non-empty `reason`
#       (semantics defined by dq.engine.validate_batch; Spark compiler in next slice)
