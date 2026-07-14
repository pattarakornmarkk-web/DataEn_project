"""Differential assertions — structured diffs, never bare asserts.

Governance: on mismatch the oracle is presumed correct; a compiler fix is the
default response. Changing the oracle instead requires an ADR update.
"""

from __future__ import annotations

from retail_lakehouse.spark.types import ROW_ID


def _fmt(value):
    return f"{value!r} ({type(value).__name__})"


def diff_report(case: str, mismatches: list[str]) -> str:
    lines = [f"DIFFERENTIAL MISMATCH — case {case!r} (oracle is the source of truth):"]
    lines += [f"  {m}" for m in mismatches[:25]]
    if len(mismatches) > 25:
        lines.append(f"  ... and {len(mismatches) - 25} more")
    return "\n".join(lines)


def assert_rows_equivalent(oracle_rows: list[dict], spark_rows: list[dict], case: str = "") -> None:
    """Row-by-row (by _row_id) and column-by-column equivalence with a readable diff."""
    mismatches: list[str] = []
    oracle_by_id = {r[ROW_ID]: r for r in oracle_rows}
    spark_by_id = {r[ROW_ID]: r for r in spark_rows}
    if set(oracle_by_id) != set(spark_by_id):
        mismatches.append(
            f"row membership differs: oracle-only={sorted(set(oracle_by_id) - set(spark_by_id))} "
            f"spark-only={sorted(set(spark_by_id) - set(oracle_by_id))}"
        )
    for row_id in sorted(set(oracle_by_id) & set(spark_by_id)):
        oracle_row, spark_row = oracle_by_id[row_id], spark_by_id[row_id]
        for column in sorted(set(oracle_row) | set(spark_row)):
            o, s = oracle_row.get(column), spark_row.get(column)
            if o != s:
                mismatches.append(f"row {row_id} col {column}: oracle={_fmt(o)} spark={_fmt(s)}")
    assert not mismatches, diff_report(case, mismatches)


def assert_sets_equivalent(oracle_items: set, spark_items: set, what: str, case: str = "") -> None:
    if oracle_items != spark_items:
        mismatches = [
            f"{what} oracle-only: {sorted(oracle_items - spark_items)!r}",
            f"{what} spark-only:  {sorted(spark_items - oracle_items)!r}",
        ]
        raise AssertionError(diff_report(case, mismatches))


def shrink(records: list[dict], still_fails) -> list[dict]:
    """Greedy bisection to a minimal failing subset (property-test diagnosis)."""
    current = list(records)
    changed = True
    while changed and len(current) > 1:
        changed = False
        half = len(current) // 2
        for candidate in (current[:half], current[half:]):
            if candidate and still_fails(candidate):
                current, changed = list(candidate), True
                break
        else:
            for i in range(len(current)):
                candidate = current[:i] + current[i + 1 :]
                if candidate and still_fails(candidate):
                    current, changed = candidate, True
                    break
    return current
