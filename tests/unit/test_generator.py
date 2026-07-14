"""Scenario generator + writer — determinism, faults, replay, drift (slice 7)."""

import pytest

from retail_lakehouse.scenarios import generator, parser, writer

pytestmark = pytest.mark.unit


def _files(name):
    return generator.generate_batches(parser.load_scenario(name))


def _rendered(name):
    """scenario -> {relative path: bytes} via the real writer (tmp-free, in-memory)."""
    from retail_lakehouse.config import loader

    sources, contracts = loader.load_sources(), loader.load_contracts()["source_contracts"]
    out = {}
    for f in _files(name):
        cfg = sources[f.source]
        rel = cfg["emitters"][f.emitter] if f.emitter else cfg["path"]
        out[f"{rel}/{f.file_name}"] = writer.render_file(
            f, cfg["format"], list(contracts[f.source]["columns"])
        )
    return out


class TestDeterminismAndReplay:
    def test_same_scenario_generates_identical_bytes(self):
        assert _rendered("day1_clean") == _rendered("day1_clean")

    def test_replay_scenario_is_byte_identical_to_target(self):
        assert _rendered("replay_day1") == _rendered("day1_clean")

    def test_every_packaged_scenario_generates(self):
        for name in parser.list_scenarios():
            files = _files(name)
            assert files, name
            for f in files:
                assert f.records, f.file_name
                orders = [r["_ingestion_order"] for r in f.records]
                assert orders == list(range(len(orders)))  # emulator tiebreak component


class TestFaults:
    def test_fault_handler_completeness_for_packaged_scenarios(self):
        used = set()
        for name in parser.list_scenarios():
            spec = parser.load_scenario(name)
            for batch in (spec.get("batches") or {}).values():
                used |= set(batch.get("faults") or [])
            for variant in (spec.get("variants") or {}).values():
                for batch in variant.values():
                    used |= set(batch.get("faults") or [])
        missing = used - set(generator.FAULT_HANDLERS)
        assert not missing, f"faults without generator handlers: {sorted(missing)}"

    def test_poison_day_injects_expected_faults(self):
        by_source = {}
        for f in _files("poison_day"):
            by_source.setdefault(f.source, []).extend(f.records)
        customers = by_source["customers"]
        assert any(r["customer_id"] is None for r in customers)  # null_key
        assert any(r.get("loyalty_tier") == "DIAMOND" for r in customers)
        assert any(r.get("birth_date") == "1899-13-45" for r in customers)
        orders = by_source["order_events"]
        assert any(r.get("quantity") == -1 for r in orders)
        assert any(str(r.get("event_ts", "")).startswith("2030") for r in orders)
        updates = by_source["customer_updates"]
        assert any(r.get("change_ts") is None for r in updates)
        assert any(r.get("op") == "X" for r in updates)

    def test_schema_drift_day_adds_extra_column(self):
        orders = [
            r for f in _files("day5_schema_drift") if f.source == "order_events" for r in f.records
        ]
        assert any("discount_pct" in r for r in orders)

    def test_late_arriving_day_has_pre_clock_timestamps(self):
        spec = parser.load_scenario("day2_late_cdc")
        clock = spec["logical_clock"]
        updates = [
            r for f in _files("day2_late_cdc") if f.source == "customer_updates" for r in f.records
        ]
        assert any(
            r["change_ts"] and r["change_ts"] < clock.strftime("%Y-%m-%dT%H") for r in updates
        )


class TestEmittersAndAbsence:
    def test_multi_emitter_sources_split_across_paths(self):
        emitters = {f.emitter for f in _files("day1_clean") if f.source == "order_events"}
        assert emitters == {"north", "south"}

    def test_south_quotes_thb(self):
        south = [
            r
            for f in _files("day1_clean")
            if f.source == "order_events" and f.emitter == "south"
            for r in f.records
        ]
        assert south and all(r["currency"] == "THB" for r in south)

    def test_expected_absent_emitter_writes_nothing(self):
        north = [
            f for f in _files("day6_silence") if f.source == "order_events" and f.emitter == "north"
        ]
        assert north == []


class TestWriter:
    def test_csv_render_headers_and_nulls(self):
        f = next(f for f in _files("poison_day") if f.source == "customers")
        content = writer.render_file(f, "csv", ["customer_id", "email"]).decode()
        header = content.splitlines()[0]
        assert header.startswith("customer_id,email") and header.endswith("_ingestion_order")

    def test_jsonl_values_all_strings_or_null(self):
        import json

        f = next(f for f in _files("day1_clean") if f.source == "order_events")
        for line in writer.render_file(f, "json", ["event_id"]).decode().splitlines():
            assert all(v is None or isinstance(v, str) for v in json.loads(line).values())

    def test_write_files_to_disk(self, tmp_path):
        written = writer.write_files(_files("day1_clean"), str(tmp_path))
        assert written and all(p.startswith(str(tmp_path)) for p in written)
        again = writer.write_files(_files("day1_clean"), str(tmp_path))
        assert written == again  # deterministic paths + overwrite-safe
