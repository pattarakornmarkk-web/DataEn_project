"""Config loader + R1 catalog guard — first implemented slice (coverage phase 1).

Two halves: packaged-registry tests (the real conf/ files must validate) and
synthetic-document tests (each validation rule rejects its violation).
"""

import pytest

from retail_lakehouse.config import loader
from retail_lakehouse.config.contracts import (
    ConfigError,
    EnvironmentMismatchError,
    assert_catalog_matches_environment,
)

pytestmark = pytest.mark.unit

ALL_SOURCES = {
    "customers",
    "products",
    "stores",
    "customer_updates",
    "order_events",
    "customer_activity",
}


class TestPackagedRegistries:
    def test_load_sources_returns_all_six_sources(self):
        assert set(loader.load_sources()) == ALL_SOURCES

    def test_multi_emitter_sources_declare_their_emitters(self):
        sources = loader.load_sources()
        assert set(sources["order_events"]["emitters"]) == {"north", "south"}
        assert set(sources["customer_updates"]["emitters"]) == {"crm", "mobile_app"}

    def test_contracts_split_into_source_and_entity(self):
        contracts = loader.load_contracts()
        assert set(contracts) == {"source_contracts", "entity_contracts"}
        assert set(contracts["source_contracts"]) == ALL_SOURCES

    def test_sequence_by_is_always_a_list(self):
        for name, entity in loader.load_contracts()["entity_contracts"].items():
            assert isinstance(entity["sequence_by"], list), name

    def test_scd2_entities_declare_tiebreak(self):
        for name, entity in loader.load_contracts()["entity_contracts"].items():
            if entity.get("scd_type") == 2:
                assert entity.get("tiebreak"), name

    def test_load_dq_rules_parses_packaged_registry(self):
        rules = loader.load_dq_rules()
        assert "fct_order_events" in rules
        assert all(r["severity"] in loader.KNOWN_SEVERITIES for r in rules["fct_order_events"])


def _minimal_sources():
    return {"sources": {"customers": {"path": "customers/", "format": "csv"}}}


def _minimal_contracts():
    return {
        "source_contracts": {"customers": {"identity": ["customer_id"], "columns": {}}},
        "entity_contracts": {
            "dim_customer": {
                "sources": ["customers"],
                "keys": ["customer_id"],
                "sequence_by": ["updated_at"],
                "scd_type": 2,
                "tiebreak": "change_seq",
            }
        },
    }


class TestValidationRejections:
    def test_unknown_source_field_raises(self):
        doc = _minimal_sources()
        doc["sources"]["customers"]["frmat"] = "csv"
        with pytest.raises(ConfigError, match="unknown field"):
            loader.parse_sources(doc)

    def test_source_requires_exactly_one_of_path_or_emitters(self):
        doc = _minimal_sources()
        doc["sources"]["customers"]["emitters"] = {"crm": "x/"}
        with pytest.raises(ConfigError, match="exactly one"):
            loader.parse_sources(doc)

    def test_unknown_format_raises(self):
        doc = _minimal_sources()
        doc["sources"]["customers"]["format"] = "xml"
        with pytest.raises(ConfigError, match="format"):
            loader.parse_sources(doc)

    def test_scalar_sequence_by_raises(self):
        doc = _minimal_contracts()
        doc["entity_contracts"]["dim_customer"]["sequence_by"] = "updated_at"
        with pytest.raises(ConfigError, match="sequence_by"):
            loader.parse_contracts(doc)

    def test_scd2_without_tiebreak_raises(self):
        doc = _minimal_contracts()
        del doc["entity_contracts"]["dim_customer"]["tiebreak"]
        with pytest.raises(ConfigError, match="ADR-0009"):
            loader.parse_contracts(doc)

    def test_entity_referencing_undeclared_source_raises(self):
        doc = _minimal_contracts()
        doc["entity_contracts"]["dim_customer"]["sources"] = ["ghosts"]
        with pytest.raises(ConfigError, match="undeclared source"):
            loader.parse_contracts(doc)

    def test_unknown_entity_field_raises(self):
        doc = _minimal_contracts()
        doc["entity_contracts"]["dim_customer"]["scd"] = 2
        with pytest.raises(ConfigError, match="unknown field"):
            loader.parse_contracts(doc)

    def test_unknown_rule_type_raises_at_load(self):
        doc = {"rules": {"t": [{"rule": "vibes_check", "severity": "gate"}]}}
        with pytest.raises(ConfigError, match="rule type"):
            loader.parse_dq_rules(doc)

    def test_unknown_severity_raises_at_load(self):
        doc = {"rules": {"t": [{"rule": "null_key", "severity": "meh"}]}}
        with pytest.raises(ConfigError, match="severity"):
            loader.parse_dq_rules(doc)


class TestCatalogGuard:
    @pytest.mark.parametrize("catalog,tag", [("retail_dev", "dev"), ("retail_prod", "prod")])
    def test_matching_pairs_pass(self, catalog, tag):
        assert_catalog_matches_environment(catalog, tag)

    @pytest.mark.parametrize("catalog,tag", [("retail_prod", "dev"), ("retail_dev", "prod")])
    def test_cross_environment_pairs_rejected(self, catalog, tag):
        with pytest.raises(EnvironmentMismatchError, match="does not belong"):
            assert_catalog_matches_environment(catalog, tag)

    def test_unknown_environment_tag_rejected(self):
        with pytest.raises(EnvironmentMismatchError, match="Unknown environment_tag"):
            assert_catalog_matches_environment("retail_dev", "staging")
