"""FX conversion oracle — effective dating, determinism, missing-rate handling."""

from datetime import date
from decimal import Decimal

import pytest

from retail_lakehouse.config.contracts import ConfigError
from retail_lakehouse.transform.fx import FxRates, MissingRateError

pytestmark = pytest.mark.unit

RATES = FxRates.from_records(
    [
        {"currency": "THB", "effective_date": date(2026, 1, 1), "per_base": "35"},
        {"currency": "THB", "effective_date": date(2026, 6, 1), "per_base": "36"},
    ]
)


def test_effective_date_selection_latest_on_or_before():
    assert RATES.lookup("THB", date(2026, 3, 1)).per_base == Decimal("35")
    assert RATES.lookup("THB", date(2026, 6, 1)).per_base == Decimal("36")  # boundary: inclusive
    assert RATES.lookup("THB", date(2026, 7, 1)).per_base == Decimal("36")


def test_date_before_first_rate_has_no_rate():
    assert RATES.lookup("THB", date(2025, 12, 31)) is None
    with pytest.raises(MissingRateError):
        RATES.convert(Decimal("100"), "THB", date(2025, 12, 31))


def test_unknown_currency_raises_never_zero_fills():
    with pytest.raises(MissingRateError):
        RATES.convert(Decimal("100"), "EUR", date(2026, 6, 6))


def test_base_currency_is_identity_with_quantization():
    assert RATES.convert(Decimal("10"), "USD", date(2026, 6, 6)) == Decimal("10.00")


def test_deterministic_rounding_half_up_once():
    # 100 THB / 36 = 2.777... -> 2.78 (HALF_UP at the end, exactly once)
    assert RATES.convert(Decimal("100"), "THB", date(2026, 6, 6)) == Decimal("2.78")


def test_exact_division_stays_exact():
    assert RATES.convert(Decimal("700"), "THB", date(2026, 3, 1)) == Decimal("20.00")


def test_duplicate_currency_date_rejected_at_load():
    with pytest.raises(ConfigError, match="duplicate"):
        FxRates.from_records(
            [
                {"currency": "THB", "effective_date": date(2026, 1, 1), "per_base": "35"},
                {"currency": "THB", "effective_date": date(2026, 1, 1), "per_base": "36"},
            ]
        )


def test_nonpositive_rate_rejected_at_load():
    with pytest.raises(ConfigError, match="positive"):
        FxRates.from_records(
            [{"currency": "THB", "effective_date": date(2026, 1, 1), "per_base": "0"}]
        )
