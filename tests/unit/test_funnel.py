"""Funnel oracle — stage progression, duplicates, regression, conversion metrics."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from retail_lakehouse.transform import funnel

pytestmark = pytest.mark.unit

T0 = datetime(2026, 6, 6, 10, 0, tzinfo=timezone.utc)


def _a(session, etype, minutes=0, customer=1, pos=0):
    return {
        "session_id": session,
        "customer_id": customer,
        "activity_type": etype,
        "activity_ts": T0 + timedelta(minutes=minutes),
        "_ingestion_order": pos,
    }


def _order(customer, order_id, minutes):
    return {
        "customer_id": customer,
        "order_id": order_id,
        "event_ts": T0 + timedelta(minutes=minutes),
    }


def _run(activity, orders=(), window=24):
    return funnel.attribute_conversions(list(activity), list(orders), window)


class TestStageProgression:
    def test_view_cart_convert_ladder(self):
        activity = [_a("s", "PAGE_VIEW", 0), _a("s", "ADD_TO_CART", 5)]
        result = _run(activity, [_order(1, 99, 30)])
        session = result.sessions[0]
        assert session.stage == funnel.STAGE_CONVERTED
        assert session.converting_order_id == 99

    def test_browse_only_session(self):
        session = _run([_a("s", "PAGE_VIEW", 0), _a("s", "SEARCH", 1)]).sessions[0]
        assert session.stage == funnel.STAGE_BROWSE and not session.converted

    def test_progression_is_monotone(self):
        # a browse event AFTER a cart-add does not lower the stage
        activity = [_a("s", "ADD_TO_CART", 0), _a("s", "PAGE_VIEW", 5)]
        assert _run(activity).sessions[0].stage == funnel.STAGE_CART


class TestDuplicatesAndRegression:
    def test_duplicate_stage_counted_once_for_progression(self):
        activity = [_a("s", "ADD_TO_CART", 0), _a("s", "ADD_TO_CART", 1), _a("s", "ADD_TO_CART", 2)]
        session = _run(activity).sessions[0]
        assert session.stage == funnel.STAGE_CART  # not advanced thrice
        assert session.duplicate_stage_events == 2

    def test_remove_after_add_marks_regression(self):
        activity = [_a("s", "ADD_TO_CART", 0), _a("s", "REMOVE_FROM_CART", 5)]
        session = _run(activity).sessions[0]
        assert session.cart_regressed and session.stage == funnel.STAGE_CART

    def test_readd_after_remove_clears_regression(self):
        activity = [
            _a("s", "ADD_TO_CART", 0),
            _a("s", "REMOVE_FROM_CART", 5),
            _a("s", "ADD_TO_CART", 9),
        ]
        assert not _run(activity).sessions[0].cart_regressed

    def test_remove_without_cart_is_not_regression(self):
        assert not _run([_a("s", "REMOVE_FROM_CART", 0)]).sessions[0].cart_regressed


class TestAttribution:
    def test_anonymous_sessions_never_convert(self):
        activity = [_a("s", "ADD_TO_CART", 0, customer=None)]
        session = _run(activity, [_order(1, 99, 5)]).sessions[0]
        assert not session.converted

    def test_order_before_cart_add_does_not_convert(self):
        activity = [_a("s", "ADD_TO_CART", 10)]
        assert not _run(activity, [_order(1, 99, 5)]).sessions[0].converted

    def test_window_measured_from_last_cart_add(self):
        activity = [_a("s", "ADD_TO_CART", 0), _a("s", "ADD_TO_CART", 60)]
        late_order = [_order(1, 99, 60 + 23 * 60)]  # inside window of the SECOND add
        assert _run(activity, late_order).sessions[0].converted

    def test_events_ordered_by_sequence_not_arrival(self):
        # remove arrives first in the list but is sequenced BEFORE the add
        activity = [_a("s", "REMOVE_FROM_CART", 0, pos=1), _a("s", "ADD_TO_CART", 5, pos=0)]
        assert not _run(activity).sessions[0].cart_regressed


class TestMetrics:
    def test_conversion_metrics_counts_and_rate(self):
        activity = [
            _a("s1", "PAGE_VIEW", 0, customer=1),
            _a("s2", "ADD_TO_CART", 0, customer=2),
            _a("s3", "ADD_TO_CART", 0, customer=3),
            _a("s4", "ADD_TO_CART", 0, customer=4),
        ]
        result = _run(activity, [_order(2, 90, 5), _order(3, 91, 5)])
        m = result.metrics
        assert (m.sessions, m.browsed, m.carted, m.converted) == (4, 4, 3, 2)
        assert m.conversion_rate == Decimal("0.6667")

    def test_zero_carted_rate_is_zero_not_error(self):
        metrics = _run([_a("s", "PAGE_VIEW", 0)]).metrics
        assert metrics.conversion_rate == Decimal("0")

    def test_deterministic_session_ordering(self):
        activity = [_a("s2", "PAGE_VIEW", 0), _a("s1", "PAGE_VIEW", 0)]
        result = _run(activity)
        assert [s.session_id for s in result.sessions] == ["s1", "s2"]
