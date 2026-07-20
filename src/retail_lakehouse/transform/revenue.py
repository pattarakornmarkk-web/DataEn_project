"""Order lifecycle resolution + revenue — pure oracle (unit spec §1.1, ADR-0005).

Semantic contract:
  - lifecycle is resolved per order_id over the SEQUENCE ordering (ADR-0009),
    never arrival order
  - CANCELLED is STICKY TERMINAL: once an order has any CANCELLED event, no event —
    even one sequenced later — reinstates it. Reinstatement requires a new order_id
    (documented policy; tested).
  - revenue eligibility: has a PAID event AND not cancelled
  - the PAID event (max-sequence if several survive upstream dedup) carries the
    revenue facts: quantity, unit_price, currency, event_ts
  - conservation per order: every input order_id lands in exactly one of
    (lines, excluded-with-reason); excluded orders are ABSENT from revenue,
    never zero-valued rows
  - money is Decimal end-to-end; FX applied exactly once via transform.fx with the
    original amount and currency retained on every line
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from retail_lakehouse.transform.fx import BASE_CURRENCY, FxRates, MissingRateError
from retail_lakehouse.transform.ordering import sequence_sort_key

CANCELLED, PAID = "CANCELLED", "PAID"

REASON_CANCELLED = "cancelled"
REASON_NOT_PAID = "not_paid"
REASON_MISSING_AMOUNTS = "missing_amounts"
REASON_UNPRICED = "unpriced_missing_fx"


@dataclass(frozen=True)
class OrderResolution:
    order_id: object
    terminal_state: str
    is_cancelled: bool
    revenue_eligible: bool
    paid_event: dict | None  # the revenue-bearing event


@dataclass(frozen=True)
class RevenueLine:
    order_id: object
    quantity: int
    unit_price: Decimal
    currency: str  # original, retained
    amount_local: Decimal  # original, retained
    amount_base: Decimal
    base_currency: str = BASE_CURRENCY


@dataclass(frozen=True)
class RevenueResult:
    lines: tuple[RevenueLine, ...]
    excluded: tuple[tuple[object, str], ...]  # (order_id, reason)
    total_base: Decimal

    @property
    def order_count(self) -> int:
        return len(self.lines) + len(self.excluded)


def _ordering(event: dict, sequence_by: tuple[str, ...], tiebreak: str) -> tuple:
    return sequence_sort_key(tuple(event.get(col) for col in (*sequence_by, tiebreak)))


def resolve_order_lifecycle(
    events: list[dict],
    sequence_by: tuple[str, ...] = ("event_ts",),
    tiebreak: str = "_ingestion_order",
) -> tuple[OrderResolution, ...]:
    """One resolution per order_id; deterministic, sorted by order key."""
    by_order: dict[object, list[dict]] = {}
    for event in events:
        by_order.setdefault(event.get("order_id"), []).append(event)

    resolutions = []
    for order_id in sorted(by_order, key=lambda k: sequence_sort_key((k,))):
        ordered = sorted(by_order[order_id], key=lambda e: _ordering(e, sequence_by, tiebreak))
        cancelled = any(e.get("event_type") == CANCELLED for e in ordered)  # sticky terminal
        paid_events = [e for e in ordered if e.get("event_type") == PAID]
        resolutions.append(
            OrderResolution(
                order_id=order_id,
                terminal_state=CANCELLED if cancelled else ordered[-1].get("event_type"),
                is_cancelled=cancelled,
                revenue_eligible=bool(paid_events) and not cancelled,
                paid_event=dict(paid_events[-1]) if paid_events else None,
            )
        )
    return tuple(resolutions)


def compute_line_revenue(resolutions: tuple[OrderResolution, ...], rates: FxRates) -> RevenueResult:
    """Revenue lines for eligible orders; everything else excluded WITH a reason."""
    lines: list[RevenueLine] = []
    excluded: list[tuple[object, str]] = []
    for resolution in resolutions:
        if resolution.is_cancelled:
            excluded.append((resolution.order_id, REASON_CANCELLED))
            continue
        if not resolution.revenue_eligible:
            excluded.append((resolution.order_id, REASON_NOT_PAID))
            continue
        paid = resolution.paid_event
        quantity, unit_price = paid.get("quantity"), paid.get("unit_price")
        event_ts: datetime | None = paid.get("event_ts")
        if quantity is None or unit_price is None or event_ts is None:
            excluded.append((resolution.order_id, REASON_MISSING_AMOUNTS))
            continue
        currency = paid.get("currency") or BASE_CURRENCY
        amount_local = Decimal(str(unit_price)) * quantity
        try:
            amount_base = rates.convert(amount_local, currency, event_ts.date())
        except MissingRateError:
            excluded.append((resolution.order_id, REASON_UNPRICED))
            continue
        lines.append(
            RevenueLine(
                order_id=resolution.order_id,
                quantity=quantity,
                unit_price=Decimal(str(unit_price)),
                currency=currency,
                amount_local=amount_local,
                amount_base=amount_base,
            )
        )

    result = RevenueResult(
        lines=tuple(lines),
        excluded=tuple(excluded),
        total_base=sum((line.amount_base for line in lines), Decimal("0.00")),
    )
    if result.order_count != len(resolutions):  # conservation by construction
        raise AssertionError("revenue conservation violated")
    return result
