"""DQ compiler: BoundRule -> Spark expressions (Phase 6B bodies; 6A = dispatcher).

Completeness invariant (CI meta-test): SUPPORTED_COMPILER_RULES keys ==
config.loader.KNOWN_RULE_TYPES — a new rule type cannot exist without a compiler
entry, and every entry is reached exclusively through compile_rule.

Spark 4 runs ANSI mode: only try_* functions are permitted for casts/arithmetic.
"""

from __future__ import annotations

from retail_lakehouse.config.contracts import ConfigError


def _compile_null_key(rule, ctx):
    raise NotImplementedError("Phase 6B")


def _compile_try_cast(rule, ctx):
    raise NotImplementedError("Phase 6B")


def _compile_domain(rule, ctx):
    raise NotImplementedError("Phase 6B")


def _compile_format(rule, ctx):
    raise NotImplementedError("Phase 6B")


def _compile_range(rule, ctx):
    raise NotImplementedError("Phase 6B")


def _compile_plausibility(rule, ctx):
    raise NotImplementedError("Phase 6B")


def _compile_duplicate(rule, ctx):
    raise NotImplementedError("Phase 6B")  # batch-scoped: window pair over payload/identity


def _compile_rescue_rate(rule, ctx):
    raise NotImplementedError("Phase 6B")  # gate tier: SDP expectation projection


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
