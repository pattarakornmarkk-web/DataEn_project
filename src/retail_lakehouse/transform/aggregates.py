"""Aggregations — pure oracle (unit spec §1.6).

Semantic contract:
  - conservation: sum of group totals == sum of input amounts, EXACTLY (Decimal),
    asserted inside the function, not just in tests
  - null grouping values land in an explicit UNKNOWN bucket — never dropped
  - deterministic grouping: output rows sorted by group key
  - replay-safe: aggregation is pure recompute — same input, identical output
  - closure-safe segment factories: the segment value is bound via default
    argument at creation time (the structural fix for the original loop-closure bug)
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from retail_lakehouse.transform.ordering import sequence_sort_key

UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class AggregateRow:
    group: tuple[tuple[str, object], ...]  # (dimension, value) pairs, declared order
    total: Decimal
    row_count: int


@dataclass(frozen=True)
class AggregateResult:
    rows: tuple[AggregateRow, ...]
    input_total: Decimal
    input_count: int

    @property
    def conserved(self) -> bool:
        return sum((row.total for row in self.rows), Decimal("0")) == self.input_total


def _amount(record: dict, amount_col: str) -> Decimal:
    value = record.get(amount_col)
    return Decimal(str(value)) if value is not None else Decimal("0")


def aggregate_sum(records: list[dict], group_by: list[str], amount_col: str) -> AggregateResult:
    """Group + sum with UNKNOWN bucketing and built-in conservation."""
    groups: dict[tuple, tuple[Decimal, int]] = {}
    input_total = Decimal("0")
    for record in records:
        key = tuple(
            (dim, record.get(dim) if record.get(dim) is not None else UNKNOWN) for dim in group_by
        )
        amount = _amount(record, amount_col)
        input_total += amount
        total, count = groups.get(key, (Decimal("0"), 0))
        groups[key] = (total + amount, count + 1)

    rows = tuple(
        AggregateRow(group=key, total=total, row_count=count)
        for key, (total, count) in sorted(
            groups.items(), key=lambda item: sequence_sort_key(tuple(v for _, v in item[0]))
        )
    )
    result = AggregateResult(rows=rows, input_total=input_total, input_count=len(records))
    if not result.conserved:  # conservation by construction
        raise AssertionError(f"aggregate conservation violated: {result.input_total}")
    return result


def sales_by_dimension_daily(
    records: list[dict],
    dims: list[str],
    date_col: str = "sale_date",
    amount_col: str = "amount_base",
) -> AggregateResult:
    """The gold daily-aggregate shape: (date, *dims) -> total."""
    return aggregate_sum(records, group_by=[date_col, *dims], amount_col=amount_col)


def make_segment_aggregate(
    group_by: list[str], amount_col: str, segment_col: str, segment_value: object
):
    """Factory for per-segment aggregates. LATE-BINDING-SAFE: the segment value is
    frozen as a default argument at creation time, so factories built in a loop
    each keep their own segment (the original agg_sales.py bug is unrepresentable).
    """

    def segment_aggregate(
        records: list[dict],
        *,
        _segment_col: str = segment_col,
        _segment_value: object = segment_value,
    ) -> AggregateResult:
        filtered = [r for r in records if r.get(_segment_col) == _segment_value]
        return aggregate_sum(filtered, group_by=group_by, amount_col=amount_col)

    segment_aggregate.segment = (segment_col, segment_value)
    return segment_aggregate
