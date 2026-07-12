# BRONZE | order_events_raw — JSONL lifecycle events, TWO regions (north/, south/).
# Structure: dp.create_streaming_table + two @dp.append_flow (from_north, from_south).
# NO transformation here — the south FX conversion happens in silver.
#
# TODO: streaming table + two append_flow declarations
