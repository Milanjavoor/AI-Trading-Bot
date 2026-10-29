
from collections import deque
from statistics import fmean


class TradingStrategy:
    """Simple moving-average crossover strategy for practice."""

    def __init__(self, fast_window=5, slow_window=20):
        if fast_window < 2 or slow_window <= fast_window:
            raise ValueError("Require 2 <= fast_window < slow_window")

        self.fast_window = fast_window
        self.slow_window = slow_window
        self.prices = deque(maxlen=slow_window)
        self.previous_difference = None

    def update(self, price, position="FLAT"):
        if position not in {"FLAT", "LONG", "SHORT"}:
            raise ValueError("Invalid position")

        price = float(price)
        if price <= 0:
            raise ValueError("Price must be positive")

        self.prices.append(price)

        if len(self.prices) < self.slow_window:
            return {
                "signal": "HOLD",
                "reason": "Waiting for price history",
                "price": price,
            }

        values = list(self.prices)
        fast_avg = fmean(values[-self.fast_window:])
        slow_avg = fmean(values)
        difference = fast_avg - slow_avg

        previous = self.previous_difference
        self.previous_difference = difference

        crossed_up = (
            previous is not None and previous <= 0 < difference
        )
        crossed_down = (
            previous is not None and previous >= 0 > difference
        )

        signal = "HOLD"
        reason = "No confirmed crossover"

        if position == "FLAT" and crossed_up:
            signal = "BUY"
            reason = "Bullish moving-average crossover"
        elif position == "LONG" and crossed_down:
            signal = "SELL"
            reason = "Exit long position"
        elif position == "FLAT" and crossed_down:
            signal = "SHORT"
            reason = "Bearish moving-average crossover"
        elif position == "SHORT" and crossed_up:
            signal = "COVER"
            reason = "Exit short position"

        return {
            "signal": signal,
            "reason": reason,
            "price": price,
            "fast_average": round(fast_avg, 4),
            "slow_average": round(slow_avg, 4),
        }
