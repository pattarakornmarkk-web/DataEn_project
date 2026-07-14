"""Unit spec §1.1 — revenue calculation (lifecycle + FX + line revenue)."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from retail_lakehouse.transform import revenue
from retail_lakehouse.transform.fx import FxRates

pytestmark = pytest.mark.unit

TS = datetime(2026, 6, 6, 12, 0, tzinfo=timezone.utc)
RATES = FxRates.from_records(
    [{"currency": "THB", "effective_date": date(2026, 1, 1), "per_base": "35"}]
)


def _e(order_id, etype, ts=TS, qty=1, price="10.00", ccy="USD", order_pos=0):
    return {
        "order_id": order_id,
        "event_type": etype,
        "event_ts": ts,
        "quantity": qty,
        "unit_price": price,
        "currency": ccy,
        "_ingestion_order": order_pos,
    }


def _revenue(events):
    return revenue.compute_line_revenue(revenue.resolve_order_lifecycle(events), RATES)


# the spec §1.1 fixture: 101 paid, 102 paid-then-cancelled, 103 placed-only, 104 THB paid
SPEC_EVENTS = [
    _e(101, "PLACED"),
    _e(101, "PAID", qty=2, price="10.00"),
    _e(102, "PLACED"),
    _e(102, "PAID"),
    _e(102, "CANCELLED"),
    _e(103, "PLACED"),
    _e(104, "PAID", qty=1, price="700.00", ccy="THB"),
]


def test_paid_order_included_in_revenue():
    result = _revenue(SPEC_EVENTS)
    line = next(ln for ln in result.lines if ln.order_id == 101)
    assert line.amount_base == Decimal("20.00")


def test_cancelled_order_excluded():
    result = _revenue(SPEC_EVENTS)
    assert 102 not in {ln.order_id for ln in result.lines}  # absent, not zero-valued
    assert (102, revenue.REASON_CANCELLED) in result.excluded


def test_placed_only_order_excluded():
    result = _revenue(SPEC_EVENTS)
    assert (103, revenue.REASON_NOT_PAID) in result.excluded


def test_fx_normalization_applied_once_original_retained():
    result = _revenue(SPEC_EVENTS)
    thb = next(ln for ln in result.lines if ln.order_id == 104)
    assert (thb.amount_local, thb.currency) == (Decimal("700.00"), "THB")  # original kept
    assert (thb.amount_base, thb.base_currency) == (Decimal("20.00"), "USD")


def test_cancelled_before_placed_resolves_terminal():
    # CANCELLED at t1, PLACED+PAID sequenced later: cancellation is sticky (ADR-0005)
    t1, t2 = TS, TS.replace(hour=13)
    events = [_e(105, "CANCELLED", ts=t1), _e(105, "PLACED", ts=t2), _e(105, "PAID", ts=t2)]
    result = _revenue(events)
    assert result.lines == ()
    assert result.excluded == ((105, revenue.REASON_CANCELLED),)


def test_reinstatement_requires_new_order_id():
    # documented policy: a later PAID never reinstates a cancelled order_id
    resolution = revenue.resolve_order_lifecycle(
        [_e(1, "PAID", ts=TS.replace(hour=15)), _e(1, "CANCELLED", ts=TS)]
    )[0]
    assert resolution.is_cancelled and not resolution.revenue_eligible
    assert resolution.terminal_state == "CANCELLED"


def test_total_revenue_exact_decimal():
    result = _revenue(SPEC_EVENTS)
    assert result.total_base == Decimal("40.00")  # 20 + 20, exact, not float


def test_idempotent_on_same_input():
    assert _revenue(SPEC_EVENTS) == _revenue(SPEC_EVENTS)


def test_conservation_every_order_lands_exactly_once():
    result = _revenue(SPEC_EVENTS)
    all_orders = {ln.order_id for ln in result.lines} | {oid for oid, _ in result.excluded}
    assert all_orders == {101, 102, 103, 104}
    assert result.order_count == 4


def test_missing_fx_rate_classified_unpriced_never_zero():
    events = [_e(9, "PAID", ccy="EUR")]  # no EUR rate loaded
    result = _revenue(events)
    assert result.lines == ()
    assert result.excluded == ((9, revenue.REASON_UNPRICED),)


def test_missing_amounts_classified_not_crashed():
    events = [_e(9, "PAID", qty=None)]
    assert _revenue(events).excluded == ((9, revenue.REASON_MISSING_AMOUNTS),)


def test_lifecycle_uses_sequence_not_arrival_order():
    shipped_then_paid_arrival = [_e(1, "SHIPPED", ts=TS.replace(hour=14)), _e(1, "PAID", ts=TS)]
    resolution = revenue.resolve_order_lifecycle(shipped_then_paid_arrival)[0]
    assert resolution.terminal_state == "SHIPPED"  # max sequence, not last arrival
    assert resolution.revenue_eligible
