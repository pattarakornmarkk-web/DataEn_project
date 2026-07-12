# SILVER | fct_order_events — typed, deduplicated (watermarked, event_id), FX-normalized.
# Lifecycle ordering NOT resolved here (gold's job — keeps silver replayable, ADR-0005).
# expect_or_fail: rescue-rate threshold (schema-drift tripwire).
#
# TODO: @dp.table calling transform.dedup + transform.fx on the valid split
