"""Single parser for scenario YAML — no other module may interpret scenario files.

Validates on load (ConfigError, never a silent no-op): unknown fault names, unknown
source names, missing/naive logical clocks, dangling replay_of references, undeclared
emitters. The fault vocabulary below is the registry the injection matrix maps onto.
"""

from datetime import datetime

import yaml

from retail_lakehouse.config import loader
from retail_lakehouse.config.contracts import ConfigError

KNOWN_FAULTS = frozenset(
    {
        # poison_day
        "null_key",
        "row_duplicate",
        "key_duplicate",
        "invalid_birth_date",
        "invalid_loyalty_tier",
        "broken_store_ref",
        "duplicate_event_id",
        "negative_quantity",
        "future_event_ts",
        "null_customer_id",
        "null_change_ts",
        "invalid_op",
        "orphan_update",
        # day2 / day3
        "late_arriving_events",
        "out_of_order_within_batch",
        "equal_sequence_tiebreak",
        "cancelled_before_placed",
        "stale_redelivery",
        # day4 / day5
        "delete_events",
        "orphan_delete",
        "extra_column_south",
        "extra_column_south_all_rows",
        "epoch_millis_timestamps",
    }
)

_SCENARIO_FIELDS = frozenset(
    {
        "scenario",
        "logical_clock",
        "description",
        "proves",
        "batches",
        "replay_of",
        "variants",
        "expected_absent",
    }
)
_BATCH_FIELDS = frozenset({"rows", "faults", "emitters"})


def list_scenarios() -> list[str]:
    """Names of all packaged scenarios (drives the README-vs-catalog drift meta-test)."""
    return sorted(
        name.removesuffix(".yml") for name in loader.list_data("scenarios") if name.endswith(".yml")
    )


def load_scenario(name: str) -> dict:
    """Parse + validate one packaged scenario into a spec with a parsed logical clock."""
    raw = yaml.safe_load(loader.read_data_text("scenarios", f"{name}.yml"))
    return parse_scenario(
        raw, name=name, sources=loader.load_sources(), known_scenarios=set(list_scenarios())
    )


def parse_scenario(raw: dict, *, name: str, sources: dict, known_scenarios: set) -> dict:
    """Pure validation of a scenario document (testable with synthetic inputs)."""
    if not isinstance(raw, dict):
        raise ConfigError(f"scenario {name!r} must be a mapping")
    unknown = set(raw) - _SCENARIO_FIELDS
    if unknown:
        raise ConfigError(f"scenario {name!r}: unknown field(s) {sorted(unknown)}")

    for field in ("scenario", "logical_clock", "description", "proves"):
        if field not in raw:
            raise ConfigError(f"scenario {name!r}: missing required field {field!r}")
    if raw["scenario"] != name:
        raise ConfigError(f"scenario name {raw['scenario']!r} does not match file name {name!r}")
    if not isinstance(raw["proves"], list) or not raw["proves"]:
        raise ConfigError(f"scenario {name!r}: proves must be a non-empty list")

    clock = _parse_logical_clock(raw["logical_clock"], name)

    has_batches, has_replay = "batches" in raw, "replay_of" in raw
    if has_batches == has_replay:
        raise ConfigError(f"scenario {name!r}: exactly one of 'batches' or 'replay_of' is required")
    if has_replay:
        target = raw["replay_of"]
        if target == name or target not in known_scenarios:
            raise ConfigError(
                f"scenario {name!r}: replay_of references unknown scenario {target!r}"
            )
        if "variants" in raw or "expected_absent" in raw:
            raise ConfigError(
                f"scenario {name!r}: replay scenarios take no batches/variants/absences"
            )
    else:
        _validate_batches(raw["batches"], sources, f"scenario {name!r}")
        for variant_name, variant_batches in (raw.get("variants") or {}).items():
            _validate_batches(
                variant_batches, sources, f"scenario {name!r} variant {variant_name!r}"
            )
        for entry in raw.get("expected_absent") or []:
            _validate_absent_entry(entry, sources, name)

    return {**raw, "logical_clock": clock}


def _parse_logical_clock(value, name: str) -> datetime:
    try:
        clock = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConfigError(f"scenario {name!r}: logical_clock is not ISO-8601: {value!r}") from exc
    if clock.tzinfo is None:
        raise ConfigError(f"scenario {name!r}: logical_clock must be timezone-aware (R6)")
    return clock


def _validate_batches(batches, sources: dict, where: str) -> None:
    if not isinstance(batches, dict) or not batches:
        raise ConfigError(f"{where}: batches must be a non-empty mapping")
    for source_name, spec in batches.items():
        bwhere = f"{where} batch {source_name!r}"
        if source_name not in sources:
            raise ConfigError(f"{bwhere}: unknown source; declared sources {sorted(sources)}")
        if not isinstance(spec, dict):
            raise ConfigError(f"{bwhere}: must be a mapping")
        unknown = set(spec) - _BATCH_FIELDS
        if unknown:
            raise ConfigError(f"{bwhere}: unknown field(s) {sorted(unknown)}")
        rows = spec.get("rows")
        if not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0:
            raise ConfigError(f"{bwhere}: rows must be a positive integer")
        unknown_faults = set(spec.get("faults") or []) - KNOWN_FAULTS
        if unknown_faults:
            raise ConfigError(f"{bwhere}: unknown fault(s) {sorted(unknown_faults)}")
        if "emitters" in spec:
            declared = set(sources[source_name].get("emitters") or {})
            if not declared:
                raise ConfigError(f"{bwhere}: source declares no emitters to subset")
            undeclared = set(spec["emitters"]) - declared
            if undeclared:
                raise ConfigError(f"{bwhere}: undeclared emitter(s) {sorted(undeclared)}")


def _validate_absent_entry(entry, sources: dict, name: str) -> None:
    where = f"scenario {name!r} expected_absent {entry!r}"
    if not isinstance(entry, str) or entry.count(".") != 1:
        raise ConfigError(f"{where}: must be a 'source.emitter' string")
    source_name, emitter = entry.split(".")
    if source_name not in sources:
        raise ConfigError(f"{where}: unknown source {source_name!r}")
    if emitter not in (sources[source_name].get("emitters") or {}):
        raise ConfigError(f"{where}: source {source_name!r} declares no emitter {emitter!r}")
