"""Unit spec §1.5 — filename parsing, lateness windows, replay identity, CDC prep, file hooks."""

from datetime import datetime, timedelta, timezone

import pytest

from retail_lakehouse.config import loader
from retail_lakehouse.ingest import batches, filenames

pytestmark = pytest.mark.unit

AS_OF = datetime(2026, 6, 13, 8, 0, 0, tzinfo=timezone.utc)


class TestFilenameParsing:
    def test_valid_filename_parses_batch_metadata(self):
        meta = filenames.parse_batch_metadata("customer_updates_20260706_081500_001.csv")
        assert not meta.is_malformed
        assert meta.source == "customer_updates"
        assert meta.batch_ts == datetime(2026, 7, 6, 8, 15, 0, tzinfo=timezone.utc)
        assert meta.batch_seq == 1
        assert meta.extension == "csv"

    @pytest.mark.parametrize(
        "bad",
        [
            "orders_final_v2.csv",
            "",
            "no_extension_20260101_000000_001",
            "x" * 300,
            "customers_20261301_000000_001.csv",
        ],  # month 13: pattern-valid, calendar-impossible
    )
    def test_malformed_filename_yields_sentinel_never_raises(self, bad):
        meta = filenames.parse_batch_metadata(bad)
        assert meta.is_malformed and meta.batch_ts is None and meta.source is None


class TestLateness:
    def test_late_event_inside_window_marked_in_window(self):
        three_days_late = AS_OF - timedelta(days=3)
        assert batches.classify_lateness(three_days_late, AS_OF, 7) == batches.IN_WINDOW

    def test_late_event_beyond_window_flagged_not_dropped(self):
        ten_days_late = AS_OF - timedelta(days=10)
        assert batches.classify_lateness(ten_days_late, AS_OF, 7) == batches.BEYOND_WINDOW

    def test_exact_window_boundary_is_inclusive(self):
        exactly_seven = AS_OF - timedelta(days=7)
        just_past = exactly_seven - timedelta(seconds=1)
        assert batches.classify_lateness(exactly_seven, AS_OF, 7) == batches.IN_WINDOW
        assert batches.classify_lateness(just_past, AS_OF, 7) == batches.BEYOND_WINDOW


class TestReplayIdentity:
    def test_file_identity_normalizes_paths(self):
        a = batches.file_identity("/Volumes/x/landing//customers/f.csv ")
        b = batches.file_identity("/Volumes/x/landing/customers/f.csv")
        assert a == b

    def test_batch_fingerprint_is_order_insensitive_and_content_sensitive(self):
        r1, r2 = {"id": 1, "v": "a"}, {"id": 2, "v": "b"}
        assert batches.batch_fingerprint([r1, r2]) == batches.batch_fingerprint([r2, r1])
        assert batches.batch_fingerprint([r1]) != batches.batch_fingerprint([r2])


class TestCdcPreparation:
    def test_prepare_cdc_metadata_attaches_op_and_full_ordering_tuple(self):
        contracts = loader.load_contracts()
        spec = batches.bind_cdc_spec(
            contracts["entity_contracts"]["dim_customer"],
            contracts["source_contracts"]["customer_updates"],
        )
        record = {"op": "D", "customer_id": 9, "change_ts": "t1", "change_seq": 3}
        prepared = batches.prepare_cdc_metadata(record, spec)
        assert prepared["_op"] == "D"
        assert prepared["_sequence"] == ("t1", 3)  # sequence_by + tiebreak (ADR-0009)
        assert "_op" not in record  # input not mutated

    def test_attach_ingestion_order_is_monotonic_and_pure(self):
        records = [{"a": 1}, {"a": 2}]
        ordered = batches.attach_ingestion_order(records, batch_seq=3)
        assert [r["_ingestion_order"] for r in ordered] == [(3, 0), (3, 1)]
        assert "_ingestion_order" not in records[0]  # input not mutated

    def test_snapshot_source_defaults_to_upsert(self):
        contracts = loader.load_contracts()
        spec = batches.bind_cdc_spec(
            contracts["entity_contracts"]["dim_product"],
            contracts["source_contracts"]["products"],
        )
        prepared = batches.prepare_cdc_metadata({"product_id": 1, "effective_date": "d"}, spec)
        assert prepared["_op"] == "U"


class TestFileHooks:
    @pytest.fixture(scope="class")
    def sources(self):
        return loader.load_sources()

    def test_clean_file_accepted(self, sources):
        issues = batches.validate_file(
            "customers", sources["customers"], "customers_20260706_081500_001.csv"
        )
        assert issues == []

    def test_wrong_source_name_rejected(self, sources):
        issues = batches.validate_file(
            "customers", sources["customers"], "products_20260706_081500_001.csv"
        )
        assert any("does not match" in i for i in issues)

    def test_wrong_extension_for_format_rejected(self, sources):
        issues = batches.validate_file(
            "order_events",
            sources["order_events"],
            "order_events_20260706_081500_001.csv",
            emitter="north",
        )
        assert any("not valid for format" in i for i in issues)  # order_events is json

    def test_emitter_source_requires_declared_emitter(self, sources):
        cfg = sources["order_events"]
        name = "order_events_20260706_081500_001.json"
        assert any(
            "requires an emitter" in i for i in batches.validate_file("order_events", cfg, name)
        )
        assert any(
            "undeclared emitter" in i
            for i in batches.validate_file("order_events", cfg, name, emitter="west")
        )
        assert batches.validate_file("order_events", cfg, name, emitter="south") == []
