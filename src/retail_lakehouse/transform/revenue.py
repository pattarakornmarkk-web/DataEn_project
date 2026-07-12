"""Order lifecycle resolution + line revenue (unit spec §1.1, gold fct_sales).

ADR: terminal-state precedence — CANCELLED wins regardless of arrival order;
revenue = PAID and not CANCELLED; line revenue = quantity * normalized unit_price.
"""

from pyspark.sql import DataFrame


def resolve_order_lifecycle(events: DataFrame) -> DataFrame:
    """One row per order with resolved terminal state."""
    raise NotImplementedError


def compute_line_revenue(resolved_orders: DataFrame) -> DataFrame:
    """Revenue rows for completed orders (decimal semantics, both currencies kept)."""
    raise NotImplementedError
