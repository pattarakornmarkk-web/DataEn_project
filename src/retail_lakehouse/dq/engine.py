"""DQ execution engine — orchestrates rules over a batch and partitions the result.

Owns everything a single rule must not know about:
  - type coercion (try_cast semantics: failure flags, never raises)
  - batch-scoped duplicate detection (row_duplicate / key_duplicate)
  - op-aware suppression for CDC delete images (key-only payloads)
  - severity routing: quarantine splits, observe records metrics only, gate rules
    are batch metrics deferred to the Spark layer (rescue_rate)
  - structured outputs: typed dataclasses shaped for ops.dq_results (RuleMetric),
    conservation checks (BatchSummary), and silver quarantine tables

Conservation by construction: every input record lands in exactly one of
(valid, quarantine) — asserted internally, not just tested.

Determinism: same records + same as_of => identical result. Input order only decides
which exact duplicate survives (survivors are identical, so outcomes are
shuffle-stable as sets).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from retail_lakehouse.dq import reason
from retail_lakehouse.dq.rules import BoundRule, RuleSet, is_null

ROW_DUPLICATE = "row_duplicate"
KEY_DUPLICATE = "key_duplicate"

# Rules suppressed on non-identity columns of CDC delete images (spec §1.3):
# a key-only delete payload is not a data-quality failure.
_OP_SUPPRESSED_RULES = frozenset({"null_key", "format", "try_cast"})

_TRUE_WORDS = frozenset({"true", "yes", "y", "1"})
_FALSE_WORDS = frozenset({"false", "no", "n", "0"})


@dataclass(frozen=True)
class RuleMetric:
    """One row per source x rule x column — the ops.dq_results shape."""

    source: str
    rule: str
    column: str
    severity: str
    evaluated: int
    violations: int


@dataclass(frozen=True)
class BatchSummary:
    """Conservation numbers — consumed by audit.invariants.check_conservation."""

    source: str
    input_count: int
    valid_count: int
    quarantine_count: int

    @property
    def conserved(self) -> bool:
        return self.input_count == self.valid_count + self.quarantine_count


@dataclass(frozen=True)
class QuarantinedRecord:
    """Original (uncoerced) record + sorted quarantine-tier reasons — silver *_quarantine shape."""

    record: dict
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ValidationResult:
    source: str
    valid: tuple[dict, ...]  # contract-coerced records
    quarantine: tuple[QuarantinedRecord, ...]
    metrics: tuple[RuleMetric, ...]
    summary: BatchSummary


def coerce_value(value, ctype: str):
    """try_cast semantics for one value: coerced value, or None on null OR failure."""
    if is_null(value):
        return None
    if ctype == "string":
        return value if isinstance(value, str) else str(value)
    try:
        if ctype in ("int", "bigint"):
            if isinstance(value, bool):
                return None
            if isinstance(value, int):
                return value
            text = str(value).strip()
            return int(text) if text.lstrip("+-").isdigit() else None
        if ctype == "double":
            return float(str(value).strip())
        if ctype.startswith("decimal"):
            return Decimal(str(value).strip())
        if ctype == "date":
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
        if ctype == "timestamp":
            if isinstance(value, datetime):
                parsed = value
            else:
                parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        if ctype == "boolean":
            if isinstance(value, bool):
                return value
            word = str(value).strip().lower()
            return True if word in _TRUE_WORDS else False if word in _FALSE_WORDS else None
    except (ValueError, InvalidOperation, OverflowError):
        return None
    return None  # unknown type — loader prevents this; stay total anyway


def _coerce_record(raw: dict, column_types: dict[str, str]) -> dict:
    return {col: coerce_value(raw.get(col), ctype) for col, ctype in column_types.items()}


def _is_delete_image(raw: dict, ruleset: RuleSet) -> bool:
    return (
        ruleset.op_column is not None
        and ruleset.delete_payload == "key_only"
        and str(raw.get(ruleset.op_column) or "").strip() == "D"
    )


def _suppressed(rule: BoundRule, delete_image: bool, ruleset: RuleSet) -> bool:
    return (
        delete_image
        and rule.rule in _OP_SUPPRESSED_RULES
        and rule.column not in ruleset.identity
        and rule.column != ruleset.op_column
    )


def _projection(raw: dict, columns: tuple[str, ...]) -> tuple:
    return tuple(str(raw.get(col)) if raw.get(col) is not None else None for col in columns)


def _find_duplicates(records: list[dict], ruleset: RuleSet) -> tuple[set[int], set[int]]:
    """(row_dup indexes, key_dup indexes). Mirrors the original framework's semantics:
    exact duplicates beyond the first are row_duplicate; among the survivors, EVERY
    record in an identity group with >1 distinct payloads is key_duplicate. Records
    with any null identity value are excluded from key grouping (null_key owns them).
    """
    data_columns = tuple(name for name, _ in ruleset.columns)
    row_dups: set[int] = set()
    seen_payloads: set[tuple] = set()
    survivors: list[int] = []
    for idx, raw in enumerate(records):
        payload = _projection(raw, data_columns)
        if payload in seen_payloads:
            row_dups.add(idx)
        else:
            seen_payloads.add(payload)
            survivors.append(idx)

    key_groups: dict[tuple, list[int]] = {}
    for idx in survivors:
        if any(is_null(records[idx].get(col)) for col in ruleset.identity):
            continue
        key_groups.setdefault(_projection(records[idx], ruleset.identity), []).append(idx)
    key_dups = {idx for group in key_groups.values() if len(group) > 1 for idx in group}
    return row_dups, key_dups


def validate_batch(records: list[dict], ruleset: RuleSet, as_of: datetime) -> ValidationResult:
    """Run the full DQ pass over one batch. Pure: input records are never mutated."""
    column_types = ruleset.column_types()
    evaluated: dict[BoundRule, int] = dict.fromkeys(ruleset.record_rules, 0)
    violated: dict[BoundRule, int] = dict.fromkeys(ruleset.record_rules, 0)

    per_record_codes: list[dict[str, str]] = []  # reason_code -> severity
    typed_records: list[dict] = []
    for raw in records:
        typed = _coerce_record(raw, column_types)
        typed_records.append(typed)
        delete_image = _is_delete_image(raw, ruleset)
        codes: dict[str, str] = {}
        for rule in ruleset.record_rules:
            if _suppressed(rule, delete_image, ruleset):
                continue
            evaluated[rule] += 1
            if rule.evaluate(raw, typed, as_of):
                violated[rule] += 1
                codes[rule.reason_code] = rule.severity
        per_record_codes.append(codes)

    row_dups: set[int] = set()
    key_dups: set[int] = set()
    if ruleset.duplicate_severity is not None:
        row_dups, key_dups = _find_duplicates(records, ruleset)
        for idx in row_dups:
            per_record_codes[idx][ROW_DUPLICATE] = ruleset.duplicate_severity
        for idx in key_dups:
            per_record_codes[idx][KEY_DUPLICATE] = ruleset.duplicate_severity

    valid: list[dict] = []
    quarantine: list[QuarantinedRecord] = []
    for raw, typed, codes in zip(records, typed_records, per_record_codes, strict=True):
        blocking = reason.compose(c for c, sev in codes.items() if sev in ("quarantine", "gate"))
        if blocking:
            quarantine.append(QuarantinedRecord(record=dict(raw), reasons=tuple(blocking)))
        else:
            valid.append(typed)

    metrics = [
        RuleMetric(ruleset.source, r.rule, r.column, r.severity, evaluated[r], violated[r])
        for r in ruleset.record_rules
    ]
    if ruleset.duplicate_severity is not None:
        metrics.append(
            RuleMetric(
                ruleset.source,
                "duplicate",
                "row",
                ruleset.duplicate_severity,
                len(records),
                len(row_dups),
            )
        )
        metrics.append(
            RuleMetric(
                ruleset.source,
                "duplicate",
                "key",
                ruleset.duplicate_severity,
                len(records),
                len(key_dups),
            )
        )
    for rule in ruleset.metric_rules:  # batch metrics (rescue_rate): Spark-layer, recorded now
        metrics.append(RuleMetric(ruleset.source, rule.rule, rule.column, rule.severity, 0, 0))

    summary = BatchSummary(ruleset.source, len(records), len(valid), len(quarantine))
    if not summary.conserved:  # conservation by construction — never silently broken
        raise AssertionError(f"conservation violated for {ruleset.source}: {summary}")
    return ValidationResult(
        source=ruleset.source,
        valid=tuple(valid),
        quarantine=tuple(quarantine),
        metrics=tuple(metrics),
        summary=summary,
    )
