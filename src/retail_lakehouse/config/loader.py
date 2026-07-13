"""Load conf/ registries (sources, contracts, dq_rules) into validated config objects.

ADR-0008: registries are read from PACKAGED data (retail_lakehouse/_data via
importlib.resources) — identical behavior in CI, wheel tasks, and SDP serverless.
Source-tree/editable runs have no _data; the same accessor falls back to the repo's
conf/ and mock_data/ directories, so the API never changes with the install mode.

Validation philosophy: unknown fields, unknown rule types, and shape violations are
load-time ConfigErrors — never silent no-ops (blueprint testing spec §1.7).
"""

import re
from importlib import resources
from pathlib import Path

import yaml

from retail_lakehouse.config.contracts import ConfigError

KNOWN_RULE_TYPES = frozenset(
    {
        "null_key",
        "duplicate",
        "domain",
        "format",
        "plausibility",
        "range",
        "try_cast",
        "rescue_rate",
    }
)
KNOWN_SEVERITIES = frozenset({"gate", "quarantine", "observe"})
KNOWN_FORMATS = frozenset({"csv", "json"})
KNOWN_SCALAR_TYPES = frozenset(
    {"string", "int", "bigint", "double", "date", "timestamp", "boolean"}
)
_DECIMAL_TYPE_RE = re.compile(r"^decimal\(\d+,\d+\)$")

# Rules that operate on the whole batch rather than a single column.
_COLUMNLESS_RULES = frozenset({"duplicate", "rescue_rate"})
# Required params per rule type (range additionally needs min and/or max).
_RULE_REQUIRED_PARAMS = {
    "domain": ("values",),
    "format": ("pattern",),
    "rescue_rate": ("threshold_pct",),
}


def is_known_column_type(ctype) -> bool:
    """True for the declared contract type vocabulary (scalar or decimal(p,s))."""
    return isinstance(ctype, str) and (
        ctype in KNOWN_SCALAR_TYPES or bool(_DECIMAL_TYPE_RE.match(ctype))
    )


# Source-tree fallback layout: category -> repo-relative directory.
_DEV_LAYOUT = {"conf": "conf", "scenarios": "mock_data/scenarios", "seeds": "mock_data/seeds"}


def data_root():
    """Packaged _data anchor (importlib.resources Traversable), or None in a source tree."""
    root = resources.files("retail_lakehouse") / "_data"
    return root if root.is_dir() else None


def _category_dir(category: str):
    if category not in _DEV_LAYOUT:
        raise ConfigError(f"Unknown data category {category!r}; expected {sorted(_DEV_LAYOUT)}")
    root = data_root()
    if root is not None:
        return root / category
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / _DEV_LAYOUT[category]


def list_data(category: str) -> list[str]:
    """Sorted file names available in a data category."""
    return sorted(
        entry.name for entry in _category_dir(category).iterdir() if not entry.name.startswith(".")
    )


def read_data_text(category: str, name: str) -> str:
    """Read one packaged data file as text. Missing file is a ConfigError."""
    path = _category_dir(category) / name
    if not path.is_file():
        raise ConfigError(f"Data file not found: {category}/{name}")
    return path.read_text()


def _require_mapping(value, where: str) -> dict:
    if not isinstance(value, dict):
        raise ConfigError(f"{where} must be a mapping, got {type(value).__name__}")
    return value


def _reject_unknown_fields(spec: dict, allowed: frozenset, where: str) -> None:
    unknown = set(spec) - allowed
    if unknown:
        raise ConfigError(f"{where}: unknown field(s) {sorted(unknown)}; allowed {sorted(allowed)}")


_SOURCE_FIELDS = frozenset({"path", "format", "filename_pattern", "emitters"})


def parse_sources(raw: dict) -> dict:
    """Validate a sources registry document; return the per-source mapping."""
    raw = _require_mapping(raw, "sources registry")
    if set(raw) != {"sources"}:
        raise ConfigError(
            f"sources registry must have exactly one top-level key 'sources', got {sorted(raw)}"
        )
    sources = _require_mapping(raw["sources"], "sources")
    for name, spec in sources.items():
        where = f"source {name!r}"
        spec = _require_mapping(spec, where)
        _reject_unknown_fields(spec, _SOURCE_FIELDS, where)
        if spec.get("format") not in KNOWN_FORMATS:
            raise ConfigError(f"{where}: format must be one of {sorted(KNOWN_FORMATS)}")
        has_path, has_emitters = "path" in spec, "emitters" in spec
        if has_path == has_emitters:
            raise ConfigError(f"{where}: exactly one of 'path' or 'emitters' is required")
        if has_emitters and not _require_mapping(spec["emitters"], f"{where} emitters"):
            raise ConfigError(f"{where}: emitters must be a non-empty mapping")
    return sources


_SOURCE_CONTRACT_FIELDS = frozenset({"identity", "columns", "op_column", "delete_payload"})
_ENTITY_CONTRACT_FIELDS = frozenset(
    {"sources", "keys", "scd_type", "sequence_by", "tiebreak", "apply_as_deletes", "dedup_key"}
)


def parse_contracts(raw: dict) -> dict:
    """Validate a contracts document; return {'source_contracts': ..., 'entity_contracts': ...}.

    Enforced (ADR-0009 among others): sequence_by always a list; scd_type-2 entities
    declare tiebreak; entity sources reference declared source contracts.
    """
    raw = _require_mapping(raw, "contracts registry")
    if set(raw) != {"source_contracts", "entity_contracts"}:
        raise ConfigError(
            "contracts registry must have exactly the top-level keys "
            f"'source_contracts' and 'entity_contracts', got {sorted(raw)}"
        )
    source_contracts = _require_mapping(raw["source_contracts"], "source_contracts")
    for name, spec in source_contracts.items():
        where = f"source contract {name!r}"
        spec = _require_mapping(spec, where)
        _reject_unknown_fields(spec, _SOURCE_CONTRACT_FIELDS, where)
        if not isinstance(spec.get("identity"), list) or not spec["identity"]:
            raise ConfigError(f"{where}: identity must be a non-empty list")
        columns = spec.get("columns")
        if not isinstance(columns, dict) or not columns:
            raise ConfigError(f"{where}: columns must be a non-empty mapping")
        for col, ctype in columns.items():
            if not is_known_column_type(ctype):
                raise ConfigError(
                    f"{where}: column {col!r} has unknown type {ctype!r}; "
                    f"expected one of {sorted(KNOWN_SCALAR_TYPES)} or decimal(p,s)"
                )
        missing_identity = set(spec["identity"]) - set(columns)
        if missing_identity:
            raise ConfigError(
                f"{where}: identity column(s) {sorted(missing_identity)} not in columns"
            )

    entity_contracts = _require_mapping(raw["entity_contracts"], "entity_contracts")
    for name, spec in entity_contracts.items():
        where = f"entity contract {name!r}"
        spec = _require_mapping(spec, where)
        _reject_unknown_fields(spec, _ENTITY_CONTRACT_FIELDS, where)
        for field in ("sources", "keys", "sequence_by"):
            if not isinstance(spec.get(field), list) or not spec[field]:
                raise ConfigError(f"{where}: {field} must be a non-empty list")
        undeclared = set(spec["sources"]) - set(source_contracts)
        if undeclared:
            raise ConfigError(
                f"{where}: references undeclared source contract(s) {sorted(undeclared)}"
            )
        if "scd_type" in spec:
            if spec["scd_type"] != 2:
                raise ConfigError(
                    f"{where}: only scd_type 2 is supported, got {spec['scd_type']!r}"
                )
            if not isinstance(spec.get("tiebreak"), str) or not spec["tiebreak"]:
                raise ConfigError(f"{where}: scd_type-2 entities must declare tiebreak (ADR-0009)")
    return {"source_contracts": source_contracts, "entity_contracts": entity_contracts}


_RULE_FIELDS = frozenset(
    {
        "rule",
        "severity",
        "column",
        "values",
        "pattern",
        "min",
        "max",
        "threshold_pct",
        "max_future_skew_minutes",
    }
)


def parse_dq_rules(raw: dict) -> dict:
    """Validate a DQ rule registry document; return {table: [rule, ...]}."""
    raw = _require_mapping(raw, "dq_rules registry")
    if set(raw) != {"rules"}:
        raise ConfigError(
            f"dq_rules registry must have exactly one top-level key 'rules', got {sorted(raw)}"
        )
    rules = _require_mapping(raw["rules"], "rules")
    for table, table_rules in rules.items():
        if not isinstance(table_rules, list) or not table_rules:
            raise ConfigError(f"rules for {table!r} must be a non-empty list")
        for rule in table_rules:
            where = f"rule {rule!r} on {table!r}"
            rule = _require_mapping(rule, where)
            _reject_unknown_fields(rule, _RULE_FIELDS, where)
            rule_type = rule.get("rule")
            if rule_type not in KNOWN_RULE_TYPES:
                raise ConfigError(f"{where}: rule type must be one of {sorted(KNOWN_RULE_TYPES)}")
            if rule.get("severity") not in KNOWN_SEVERITIES:
                raise ConfigError(f"{where}: severity must be one of {sorted(KNOWN_SEVERITIES)}")
            column = rule.get("column")
            if rule_type in _COLUMNLESS_RULES:
                if column is not None:
                    raise ConfigError(f"{where}: {rule_type} takes no column")
            elif column == "*":
                if rule_type != "try_cast":
                    raise ConfigError(f"{where}: column '*' is only valid for try_cast")
            elif not isinstance(column, str) or not column:
                raise ConfigError(f"{where}: column is required")
            for param in _RULE_REQUIRED_PARAMS.get(rule_type, ()):
                if param not in rule:
                    raise ConfigError(f"{where}: {rule_type} requires {param!r}")
            if rule_type == "range" and "min" not in rule and "max" not in rule:
                raise ConfigError(f"{where}: range requires min and/or max")
    return rules


def load_sources() -> dict:
    """Parse packaged conf/sources.yml."""
    return parse_sources(yaml.safe_load(read_data_text("conf", "sources.yml")))


def load_contracts() -> dict:
    """Parse packaged conf/contracts.yml (source_contracts + entity_contracts)."""
    return parse_contracts(yaml.safe_load(read_data_text("conf", "contracts.yml")))


def load_dq_rules() -> dict:
    """Parse packaged conf/dq_rules.yml."""
    return parse_dq_rules(yaml.safe_load(read_data_text("conf", "dq_rules.yml")))
