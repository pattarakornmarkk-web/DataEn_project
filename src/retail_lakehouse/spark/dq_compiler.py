"""DQ compiler: BoundRule -> Spark expressions. Verdicts must equal dq.engine (oracle).

Completeness invariant (CI meta-test): SUPPORTED_COMPILER_RULES keys ==
config.loader.KNOWN_RULE_TYPES. Every compilation goes through compile_rule.

Spark 4 runs ANSI mode: only try_* functions are used for casts.
Ordering rule: null placement is ALWAYS explicit (asc_nulls_first / desc_nulls_last).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from retail_lakehouse.config.contracts import ConfigError
from retail_lakehouse.dq.engine import KEY_DUPLICATE, ROW_DUPLICATE
from retail_lakehouse.dq.rules import BoundRule, RuleSet

_BLOCKING = ("quarantine", "gate")
_ORD = "_ord"  # deterministic duplicate-survivor ordering column


@dataclass(frozen=True)
class CompileContext:
    ruleset: RuleSet
    as_of: datetime


def _nullish(col: str) -> Column:
    """Oracle null semantics: None or blank/whitespace-only string."""
    return F.col(col).isNull() | (F.trim(F.col(col)) == "")


def _typed(col: str) -> Column:
    return F.col(f"_typed_{col}")


def _delete_image(ruleset: RuleSet) -> Column:
    if ruleset.op_column is None or ruleset.delete_payload != "key_only":
        return F.lit(False)
    return F.trim(F.coalesce(F.col(ruleset.op_column), F.lit(""))) == "D"


def _compile_null_key(rule: BoundRule, ctx: CompileContext) -> Column:
    return _nullish(rule.column)


def _compile_try_cast(rule: BoundRule, ctx: CompileContext) -> Column:
    return ~_nullish(rule.column) & _typed(rule.column).isNull()


def _compile_domain(rule: BoundRule, ctx: CompileContext) -> Column:
    values = [str(v) for v in rule.param("values")]
    return ~_nullish(rule.column) & ~F.trim(F.col(rule.column)).isin(values)


def _compile_format(rule: BoundRule, ctx: CompileContext) -> Column:
    anchored = f"^(?:{rule.param('pattern')})$"  # oracle uses re.fullmatch
    return ~_nullish(rule.column) & ~F.trim(F.col(rule.column)).rlike(anchored)


def _compile_range(rule: BoundRule, ctx: CompileContext) -> Column:
    typed = _typed(rule.column)
    lo, hi = rule.param("min"), rule.param("max")
    checks = F.lit(False)
    if lo is not None:
        checks = checks | (typed < F.lit(lo))
    if hi is not None:
        checks = checks | (typed > F.lit(hi))
    return typed.isNotNull() & checks


def _compile_plausibility(rule: BoundRule, ctx: CompileContext) -> Column:
    limit = ctx.as_of + timedelta(minutes=rule.param("max_future_skew_minutes", 0))
    typed = _typed(rule.column)
    ctype = ctx.ruleset.column_types()[rule.column]
    bound = F.lit(limit.date()) if ctype == "date" else F.lit(limit)
    return typed.isNotNull() & (typed > bound)


def _compile_duplicate(rule, ctx: CompileContext):
    """Batch-scoped: returns a df->df transform adding both duplicate flags.

    Mirrors dq.engine._find_duplicates: exact payload duplicates beyond the first
    (by deterministic input order) are row_duplicate; among survivors, every member
    of an identity group with >1 records is key_duplicate; null-identity rows are
    excluded from key grouping (null_key owns them).
    """
    ruleset = ctx.ruleset
    data_cols = [name for name, _ in ruleset.columns]

    def add_flags(df: DataFrame) -> DataFrame:
        payload_window = Window.partitionBy(*data_cols).orderBy(F.asc_nulls_first(_ORD))
        row_dup = F.row_number().over(payload_window) > 1
        df = df.withColumn(f"_is_{ROW_DUPLICATE}", row_dup)
        identity_window = Window.partitionBy(*[F.col(c) for c in ruleset.identity])
        survivor_count = F.sum(F.when(~F.col(f"_is_{ROW_DUPLICATE}"), 1).otherwise(0)).over(
            identity_window
        )
        null_identity = None
        for col in ruleset.identity:
            null_identity = (
                _nullish(col) if null_identity is None else null_identity | _nullish(col)
            )
        return df.withColumn(
            f"_is_{KEY_DUPLICATE}",
            ~F.col(f"_is_{ROW_DUPLICATE}") & ~null_identity & (survivor_count > 1),
        )

    return add_flags


def _compile_rescue_rate(rule: BoundRule, ctx: CompileContext):
    """Gate tier is a BATCH metric: expressed as a row-level SDP expectation on
    _rescued_data (rate computed downstream from the expectation counters; the
    hard gate lives in the post-run reconciliation task). Returns (name, sql).
    """
    return (f"rescue_rate_max_{rule.param('threshold_pct')}", "_rescued_data IS NULL")


SUPPORTED_COMPILER_RULES = {
    "null_key": _compile_null_key,
    "try_cast": _compile_try_cast,
    "domain": _compile_domain,
    "format": _compile_format,
    "range": _compile_range,
    "plausibility": _compile_plausibility,
    "duplicate": _compile_duplicate,
    "rescue_rate": _compile_rescue_rate,
}


def compile_rule(rule_type: str, rule, ctx):
    """The single dispatch point — every rule compilation goes through here."""
    compiler = SUPPORTED_COMPILER_RULES.get(rule_type)
    if compiler is None:
        raise ConfigError(
            f"no compiler for rule type {rule_type!r}; supported: "
            f"{sorted(SUPPORTED_COMPILER_RULES)}"
        )
    return compiler(rule, ctx)


def validated_df(df: DataFrame, ruleset: RuleSet, as_of: datetime) -> DataFrame:
    """Raw (string) df -> df with _typed_* columns, _is_* flags, and blocking `reason`."""
    ctx = CompileContext(ruleset=ruleset, as_of=as_of)
    df = df.withColumn(_ORD, F.monotonically_increasing_id()) if _ORD not in df.columns else df

    for col, ctype in ruleset.columns:
        if ctype == "string":
            # oracle null semantics apply to strings too: blank/whitespace-only -> null
            typed = F.when(_nullish(col), F.lit(None)).otherwise(F.col(col))
        else:
            typed = F.col(col).try_cast(ctype)
        df = df.withColumn(f"_typed_{col}", typed)

    delete_image = _delete_image(ruleset)
    suppressible = {"null_key", "format", "try_cast"}
    for rule in ruleset.record_rules:
        violation = compile_rule(rule.rule, rule, ctx)
        if (
            rule.rule in suppressible
            and rule.column not in ruleset.identity
            and rule.column != ruleset.op_column
        ):
            violation = violation & ~delete_image
        df = df.withColumn(rule.flag_name, F.coalesce(violation, F.lit(False)))

    if ruleset.duplicate_severity is not None:
        df = compile_rule("duplicate", None, ctx)(df)

    blocking_codes = []
    for rule in ruleset.record_rules:
        if rule.severity in _BLOCKING:
            blocking_codes.append(F.when(F.col(rule.flag_name), F.lit(rule.reason_code)))
    if ruleset.duplicate_severity in _BLOCKING:
        for code in (ROW_DUPLICATE, KEY_DUPLICATE):
            blocking_codes.append(F.when(F.col(f"_is_{code}"), F.lit(code)))
    reason = (
        F.array_sort(F.filter(F.array(*blocking_codes), lambda c: c.isNotNull()))
        if blocking_codes
        else F.array().cast("array<string>")
    )
    return df.withColumn("reason", reason)


def split(validated: DataFrame, ruleset: RuleSet, passthrough: list[str] = ()) -> tuple:
    """(valid_df: typed columns, quarantine_df: original columns + reason)."""
    raw_cols = [name for name, _ in ruleset.columns]
    keep = [c for c in passthrough if c in validated.columns]
    valid = validated.filter(F.size("reason") == 0).select(
        *keep, *[F.col(f"_typed_{c}").alias(c) for c in raw_cols]
    )
    quarantine = validated.filter(F.size("reason") > 0).select(*keep, *raw_cols, "reason")
    return valid, quarantine
