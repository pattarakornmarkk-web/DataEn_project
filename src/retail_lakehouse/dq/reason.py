"""Reason composition — deterministic, exact-set semantics.

Reason tokens are flag names minus the `_is_` prefix (e.g. `customer_id_null_key`,
`row_duplicate`), so the future Spark melt over `_is_*` flag columns produces the
SAME vocabulary — the differential tests compare these lists directly.
"""


def compose(codes) -> list[str]:
    """Reason codes -> sorted, de-duplicated reason list (quarantine `reason` column)."""
    return sorted(set(codes))


def reasons_from_flags(flags: dict[str, bool]) -> list[str]:
    """Spark-parity helper: `_is_*` flag map -> reason list (melt equivalent)."""
    return sorted({name.removeprefix("_is_") for name, violated in flags.items() if violated})
