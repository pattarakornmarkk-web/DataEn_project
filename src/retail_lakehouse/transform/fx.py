"""Currency conversion — pure oracle.

Semantic contract:
  - rate convention: LOCAL UNITS PER 1 BASE unit (THB 35.00 per USD) — conversion
    divides, so representable rates stay exact (700 THB / 35 = 20.00 USD)
  - effective-date selection: the latest rate with effective_date <= as_of
  - deterministic rounding: quantize to 0.01, ROUND_HALF_UP, applied exactly once
  - duplicate (currency, effective_date) is a load-time ConfigError
  - missing rate raises MissingRateError — callers classify (revenue: 'unpriced'),
    nothing converts silently to zero
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from retail_lakehouse.config.contracts import ConfigError

BASE_CURRENCY = "USD"
_CENT = Decimal("0.01")


class MissingRateError(LookupError):
    """No applicable rate for (currency, as_of) — caller must classify, never zero-fill."""


@dataclass(frozen=True)
class FxRate:
    currency: str
    effective_date: date
    per_base: Decimal  # local units per 1 base unit


@dataclass(frozen=True)
class FxRates:
    rates: tuple[FxRate, ...]  # sorted (currency, effective_date)

    @classmethod
    def from_records(cls, records: list[dict]) -> FxRates:
        seen: set[tuple] = set()
        rates = []
        for record in records:
            key = (record["currency"], record["effective_date"])
            if key in seen:
                raise ConfigError(f"duplicate fx rate for {key}")
            seen.add(key)
            per_base = Decimal(str(record["per_base"]))
            if per_base <= 0:
                raise ConfigError(f"fx rate for {key} must be positive")
            rates.append(FxRate(record["currency"], record["effective_date"], per_base))
        rates.sort(key=lambda r: (r.currency, r.effective_date))
        return cls(rates=tuple(rates))

    def lookup(self, currency: str, as_of: date) -> FxRate | None:
        """Latest rate with effective_date <= as_of, or None."""
        applicable = [r for r in self.rates if r.currency == currency and r.effective_date <= as_of]
        return applicable[-1] if applicable else None

    def convert(self, amount: Decimal, currency: str, as_of: date) -> Decimal:
        """Local amount -> base amount, quantized once (0.01, HALF_UP)."""
        if currency == BASE_CURRENCY:
            return amount.quantize(_CENT, rounding=ROUND_HALF_UP)
        rate = self.lookup(currency, as_of)
        if rate is None:
            raise MissingRateError(f"no {currency} rate effective on or before {as_of}")
        return (amount / rate.per_base).quantize(_CENT, rounding=ROUND_HALF_UP)
