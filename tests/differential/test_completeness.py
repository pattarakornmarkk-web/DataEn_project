"""Compiler completeness + import-wall meta-tests (approved slice-6 requirement).

These fail CI immediately if: a rule type lacks a compiler entry, the dispatcher
bypasses the mapping, or pyspark leaks into a pure oracle package.
"""

import pathlib

import pytest

from retail_lakehouse.config.contracts import ConfigError
from retail_lakehouse.config.loader import KNOWN_RULE_TYPES
from retail_lakehouse.spark import dq_compiler

pytestmark = pytest.mark.unit  # pure — no Spark session needed

PURE_PACKAGES = ("config", "dq", "ingest", "transform", "scenarios")
SRC = pathlib.Path(__file__).parents[2] / "src" / "retail_lakehouse"


class TestCompilerCompleteness:
    def test_every_rule_type_has_a_compiler(self):
        assert set(dq_compiler.SUPPORTED_COMPILER_RULES) == set(KNOWN_RULE_TYPES), (
            "KNOWN_RULE_TYPES and SUPPORTED_COMPILER_RULES diverged — a new rule "
            "type requires a compiler entry (and differential coverage)."
        )

    def test_every_compiler_entry_is_callable_and_distinct(self):
        entries = dq_compiler.SUPPORTED_COMPILER_RULES
        assert all(callable(fn) for fn in entries.values())
        assert len({id(fn) for fn in entries.values()}) == len(entries)

    def test_dispatcher_routes_through_the_mapping(self, monkeypatch):
        sentinel = object()
        monkeypatch.setitem(
            dq_compiler.SUPPORTED_COMPILER_RULES, "null_key", lambda rule, ctx: sentinel
        )
        assert dq_compiler.compile_rule("null_key", None, None) is sentinel

    def test_unknown_rule_type_fails_loudly(self):
        with pytest.raises(ConfigError, match="no compiler"):
            dq_compiler.compile_rule("vibes_check", None, None)


class TestImportWall:
    def test_pure_packages_never_import_pyspark(self):
        offenders = []
        for package in PURE_PACKAGES:
            for path in sorted((SRC / package).rglob("*.py")):
                text = path.read_text()
                if "import pyspark" in text or "from pyspark" in text:
                    offenders.append(str(path.relative_to(SRC)))
        assert not offenders, f"pyspark leaked into pure oracle packages: {offenders}"
