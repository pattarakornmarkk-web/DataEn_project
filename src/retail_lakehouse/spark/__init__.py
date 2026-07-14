"""Spark compiler layer — translates oracle semantics into Spark expressions.

IMPORT WALL: pyspark may be imported ONLY inside this package (enforced by
tests/differential/test_completeness.py). The pure oracle packages (config, dq,
ingest, transform, scenarios) must stay Spark-free forever.

Governance (approved slice-6 design): the oracle is the source of truth; any
differential mismatch is a compiler defect; oracle changes require an ADR update.
"""
