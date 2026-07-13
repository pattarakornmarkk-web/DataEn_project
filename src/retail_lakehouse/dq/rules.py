"""DQ rule model — the pure-Python SEMANTIC ORACLE for data quality.

Two-layer design (approved design review, slice 3): these evaluators DEFINE what each
rule means; the future Spark compiler must produce identical verdicts on the same
fixtures (differential tests, same philosophy as the SCD2 oracle in ADR-0002).

Rules are contract-agnostic by construction: a BoundRule carries only its column,
severity, and params. All contract knowledge (types, identity, op semantics) is
resolved ONCE in bind_ruleset and frozen into the RuleSet.

Null semantics: None and empty/whitespace-only strings count as null. null_key flags
nulls; every other record-local rule ignores null values (nullness is null_key's job).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from retail_lakehouse.config.contracts import ConfigError

# Record-local rules run per record; batch rules (duplicate) are engine-owned;
# metric rules (rescue_rate) are recorded now, evaluated in the Spark layer.
RECORD_LOCAL_TYPES = frozenset(
    {"null_key", "domain", "format", "plausibility", "range", "try_cast"}
)
BATCH_METRIC_TYPES = frozenset({"rescue_rate"})


def is_null(value) -> bool:
    """Null semantics for landed string data: None or blank string."""
    return value is None or (isinstance(value, str) and value.strip() == "")


@dataclass(frozen=True)
class BoundRule:
    """One executable rule: type + column + severity + frozen params. No contract access."""

    rule: str
    column: str
    severity: str
    params: tuple[tuple[str, object], ...] = ()

    def param(self, name: str, default=None):
        return dict(self.params).get(name, default)

    @property
    def flag_name(self) -> str:
        """Spark-parity flag column name (_is_* convention from the original framework)."""
        return f"_is_{self.column}_{self.rule}"

    @property
    def reason_code(self) -> str:
        """Reason token = flag name minus the _is_ prefix (melt-compatible)."""
        return f"{self.column}_{self.rule}"

    def evaluate(self, raw: dict, typed: dict, as_of: datetime) -> bool:
        """True = violation. Deterministic, side-effect-free.

        raw holds landed values; typed holds contract-coerced values (None on failure).
        """
        value = raw.get(self.column)
        if self.rule == "null_key":
            return is_null(value)
        if is_null(value):
            return False  # non-null rules ignore nulls
        if self.rule == "try_cast":
            return typed.get(self.column) is None
        if self.rule == "domain":
            return str(value).strip() not in self.param("values")
        if self.rule == "format":
            return re.fullmatch(self.param("pattern"), str(value).strip()) is None
        if self.rule == "range":
            coerced = typed.get(self.column)
            if coerced is None:
                return False  # coercion failure is try_cast's violation, not range's
            lo, hi = self.param("min"), self.param("max")
            return (lo is not None and coerced < lo) or (hi is not None and coerced > hi)
        if self.rule == "plausibility":
            coerced = typed.get(self.column)
            if coerced is None:
                return False
            limit = as_of + timedelta(minutes=self.param("max_future_skew_minutes", 0))
            if isinstance(coerced, date) and not isinstance(coerced, datetime):
                return coerced > limit.date()
            return coerced > limit
        raise ConfigError(f"rule {self.rule!r} is not record-local")


@dataclass(frozen=True)
class RuleSet:
    """Everything the engine needs for one source, resolved from contract + registry."""

    source: str
    columns: tuple[tuple[str, str], ...]  # (name, declared type), contract order
    identity: tuple[str, ...]
    record_rules: tuple[BoundRule, ...]
    duplicate_severity: str | None
    metric_rules: tuple[BoundRule, ...]
    op_column: str | None = None
    delete_payload: str | None = None

    def column_types(self) -> dict[str, str]:
        return dict(self.columns)


def bind_ruleset(source_name: str, source_contract: dict, rules_config: list[dict]) -> RuleSet:
    """Resolve registry entries + contract into a frozen, executable RuleSet.

    Also synthesizes a try_cast rule for every non-string contract column (default
    severity quarantine; a registry entry {rule: try_cast, column: "*"} overrides it).
    Registry rules referencing columns absent from the contract fail here, at bind time.
    """
    columns = source_contract["columns"]
    duplicate_severity: str | None = None
    coercion_severity = "quarantine"
    record_rules: list[BoundRule] = []
    metric_rules: list[BoundRule] = []
    explicit_try_cast: set[str] = set()

    for entry in rules_config:
        rule_type, severity = entry["rule"], entry["severity"]
        column = entry.get("column")
        params = tuple(
            sorted(
                (k, tuple(v) if isinstance(v, list) else v)
                for k, v in entry.items()
                if k not in ("rule", "severity", "column")
            )
        )
        if rule_type == "duplicate":
            duplicate_severity = severity
            continue
        if rule_type in BATCH_METRIC_TYPES:
            metric_rules.append(BoundRule(rule_type, "", severity, params))
            continue
        if rule_type == "try_cast" and column == "*":
            coercion_severity = severity
            continue
        if column not in columns:
            raise ConfigError(
                f"source {source_name!r}: rule {rule_type!r} references column {column!r} "
                f"not in the contract"
            )
        if rule_type == "try_cast":
            explicit_try_cast.add(column)
        record_rules.append(BoundRule(rule_type, column, severity, params))

    for column, ctype in columns.items():
        if ctype != "string" and column not in explicit_try_cast:
            record_rules.append(BoundRule("try_cast", column, coercion_severity))

    return RuleSet(
        source=source_name,
        columns=tuple(columns.items()),
        identity=tuple(source_contract["identity"]),
        record_rules=tuple(record_rules),
        duplicate_severity=duplicate_severity,
        metric_rules=tuple(metric_rules),
        op_column=source_contract.get("op_column"),
        delete_payload=source_contract.get("delete_payload"),
    )
