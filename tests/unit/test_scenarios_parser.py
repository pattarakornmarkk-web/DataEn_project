"""Scenario parser — the single interpreter of mock_data/scenarios (coverage phase 1)."""

import pytest

from retail_lakehouse.config import loader
from retail_lakehouse.config.contracts import ConfigError
from retail_lakehouse.scenarios import parser

pytestmark = pytest.mark.unit

EXPECTED_CATALOG = {
    "day1_clean",
    "day2_late_cdc",
    "day3_out_of_order",
    "day4_deletes",
    "day5_schema_drift",
    "day6_silence",
    "poison_day",
    "replay_day1",
}


class TestPackagedCatalog:
    def test_list_scenarios_returns_the_full_catalog(self):
        assert set(parser.list_scenarios()) == EXPECTED_CATALOG

    @pytest.mark.parametrize("name", sorted(EXPECTED_CATALOG))
    def test_every_packaged_scenario_loads_and_validates(self, name):
        spec = parser.load_scenario(name)
        assert spec["logical_clock"].tzinfo is not None  # R6: fixed, tz-aware clock
        assert spec["proves"]


@pytest.fixture(scope="module")
def sources():
    return loader.load_sources()


def _minimal(name="day1_clean"):
    return {
        "scenario": name,
        "logical_clock": "2026-06-06T08:00:00Z",
        "description": "test",
        "proves": ["cold_start"],
        "batches": {"customers": {"rows": 10, "faults": []}},
    }


class TestValidationRejections:
    def test_name_must_match_file_name(self, sources):
        with pytest.raises(ConfigError, match="does not match"):
            parser.parse_scenario(
                _minimal("day1_clean"), name="other", sources=sources, known_scenarios=set()
            )

    def test_missing_logical_clock_raises(self, sources):
        doc = _minimal()
        del doc["logical_clock"]
        with pytest.raises(ConfigError, match="logical_clock"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())

    def test_naive_logical_clock_raises(self, sources):
        doc = _minimal()
        doc["logical_clock"] = "2026-06-06T08:00:00"
        with pytest.raises(ConfigError, match="timezone-aware"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())

    def test_unknown_fault_raises(self, sources):
        doc = _minimal()
        doc["batches"]["customers"]["faults"] = ["gremlins"]
        with pytest.raises(ConfigError, match="unknown fault"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())

    def test_unknown_batch_source_raises(self, sources):
        doc = _minimal()
        doc["batches"]["warehouse_robots"] = {"rows": 5, "faults": []}
        with pytest.raises(ConfigError, match="unknown source"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())

    def test_nonpositive_rows_raises(self, sources):
        doc = _minimal()
        doc["batches"]["customers"]["rows"] = 0
        with pytest.raises(ConfigError, match="positive integer"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())

    def test_batches_and_replay_of_are_mutually_exclusive(self, sources):
        doc = _minimal()
        doc["replay_of"] = "day1_clean"
        with pytest.raises(ConfigError, match="exactly one"):
            parser.parse_scenario(
                doc, name="day1_clean", sources=sources, known_scenarios={"day1_clean"}
            )

    def test_replay_of_must_reference_existing_scenario(self, sources):
        doc = {k: v for k, v in _minimal("replay_x").items() if k != "batches"}
        doc["replay_of"] = "day_zero"
        with pytest.raises(ConfigError, match="unknown scenario"):
            parser.parse_scenario(
                doc, name="replay_x", sources=sources, known_scenarios={"day1_clean"}
            )

    def test_emitter_subset_must_be_declared(self, sources):
        doc = _minimal()
        doc["batches"]["order_events"] = {"rows": 5, "faults": [], "emitters": ["west"]}
        with pytest.raises(ConfigError, match="undeclared emitter"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())

    def test_expected_absent_requires_valid_source_and_emitter(self, sources):
        doc = _minimal()
        doc["expected_absent"] = ["order_events.west"]
        with pytest.raises(ConfigError, match="no emitter"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())

    def test_variants_are_validated_like_batches(self, sources):
        doc = _minimal()
        doc["variants"] = {"bad": {"customers": {"rows": 5, "faults": ["gremlins"]}}}
        with pytest.raises(ConfigError, match="variant 'bad'"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())

    def test_unknown_top_level_field_raises(self, sources):
        doc = _minimal()
        doc["surprises"] = True
        with pytest.raises(ConfigError, match="unknown field"):
            parser.parse_scenario(doc, name="day1_clean", sources=sources, known_scenarios=set())
