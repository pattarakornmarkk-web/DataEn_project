"""Session funnel — pure oracle (unit spec §1.6, observe-tier semantics).

Semantic contract:
  - stage ladder: PAGE_VIEW/SEARCH (browse) -> ADD_TO_CART (cart) -> conversion.
    Progression is monotone: reaching a stage is permanent for the session.
  - duplicate stage events never advance progression twice; they are counted
    (duplicate_stage_events) — at-least-once delivery is expected upstream
  - regression: REMOVE_FROM_CART after a cart-add, with NO LATER cart-add, marks
    the session cart_regressed (abandonment signal); a later re-add clears it
  - attribution: a session converts iff a PAID revenue-eligible order of the SAME
    customer has order_ts within [last_cart_add_ts, last_cart_add_ts + window],
    boundary INCLUSIVE at the window edge; anonymous sessions (customer_id None)
    never convert
  - deterministic: events ordered by (activity_ts, tiebreak); output sorted by session
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from retail_lakehouse.transform.ordering import sequence_sort_key

BROWSE_EVENTS = frozenset({"PAGE_VIEW", "SEARCH", "WISHLIST"})
CART_ADD = "ADD_TO_CART"
CART_REMOVE = "REMOVE_FROM_CART"

STAGE_NONE, STAGE_BROWSE, STAGE_CART, STAGE_CONVERTED = 0, 1, 2, 3


@dataclass(frozen=True)
class SessionFunnel:
    session_id: object
    customer_id: object
    stage: int  # highest stage reached (STAGE_*)
    duplicate_stage_events: int
    cart_regressed: bool
    converted: bool
    converting_order_id: object | None
    last_cart_add_ts: datetime | None


@dataclass(frozen=True)
class FunnelMetrics:
    sessions: int
    browsed: int
    carted: int
    converted: int

    @property
    def conversion_rate(self) -> Decimal:
        if self.carted == 0:
            return Decimal("0")
        return (Decimal(self.converted) / Decimal(self.carted)).quantize(Decimal("0.0001"))


@dataclass(frozen=True)
class AttributionResult:
    sessions: tuple[SessionFunnel, ...]
    metrics: FunnelMetrics


def attribute_conversions(
    activity: list[dict],
    paid_orders: list[dict],
    window_hours: int,
    tiebreak: str = "_ingestion_order",
) -> AttributionResult:
    """Session summaries + conversion metrics. Pure; inputs never mutated.

    activity: coerced clickstream records (session_id, customer_id, activity_type,
    activity_ts). paid_orders: revenue-eligible orders (customer_id, order_id, event_ts).
    """
    by_session: dict[object, list[dict]] = {}
    for event in activity:
        by_session.setdefault(event.get("session_id"), []).append(event)

    orders_by_customer: dict[object, list[dict]] = {}
    for order in paid_orders:
        if order.get("customer_id") is not None:
            orders_by_customer.setdefault(order["customer_id"], []).append(order)
    for orders in orders_by_customer.values():
        orders.sort(key=lambda o: sequence_sort_key((o.get("event_ts"),)))

    sessions: list[SessionFunnel] = []
    for session_id in sorted(by_session, key=lambda s: sequence_sort_key((s,))):
        events = sorted(
            by_session[session_id],
            key=lambda e: sequence_sort_key((e.get("activity_ts"), e.get(tiebreak))),
        )
        customer_id = next(
            (e["customer_id"] for e in events if e.get("customer_id") is not None), None
        )
        stage, duplicates, regressed = STAGE_NONE, 0, False
        last_cart_add: datetime | None = None
        seen_types: set[str] = set()
        for event in events:
            etype = event.get("activity_type")
            if etype in seen_types:
                duplicates += 1
            seen_types.add(etype)
            if etype in BROWSE_EVENTS:
                stage = max(stage, STAGE_BROWSE)
            elif etype == CART_ADD:
                stage = max(stage, STAGE_CART)
                last_cart_add = event.get("activity_ts")
                regressed = False  # a re-add clears an earlier regression
            elif etype == CART_REMOVE and stage >= STAGE_CART:
                regressed = True

        converted, converting_order = False, None
        if customer_id is not None and last_cart_add is not None:
            deadline = last_cart_add + timedelta(hours=window_hours)
            for order in orders_by_customer.get(customer_id, ()):
                order_ts = order.get("event_ts")
                if order_ts is not None and last_cart_add <= order_ts <= deadline:  # inclusive
                    converted, converting_order = True, order.get("order_id")
                    break

        sessions.append(
            SessionFunnel(
                session_id=session_id,
                customer_id=customer_id,
                stage=STAGE_CONVERTED if converted else stage,
                duplicate_stage_events=duplicates,
                cart_regressed=regressed,
                converted=converted,
                converting_order_id=converting_order,
                last_cart_add_ts=last_cart_add,
            )
        )

    metrics = FunnelMetrics(
        sessions=len(sessions),
        browsed=sum(1 for s in sessions if s.stage >= STAGE_BROWSE),
        carted=sum(1 for s in sessions if s.stage >= STAGE_CART),
        converted=sum(1 for s in sessions if s.converted),
    )
    return AttributionResult(sessions=tuple(sessions), metrics=metrics)
