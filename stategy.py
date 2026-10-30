from collections import deque
from math import isfinite
from statistics import fmean, pstdev


class TradingStrategy:
    def __init__(
        self,
        fast_window=5,
        slow_window=20,
        rsi_window=14,
        slope_window=5,
        trend_slope_threshold=0.003,
        trend_spread_threshold=0.002,
        mean_reversion_z=1.5,
        rsi_buy_min=50.0,
        rsi_buy_max=70.0,
        rsi_short_min=30.0,
        rsi_short_max=50.0,
        mean_reversion_rsi_low=35.0,
        mean_reversion_rsi_high=65.0,
    ):
        if fast_window < 2 or slow_window <= fast_window:
            raise ValueError("Require 2 <= fast_window < slow_window")
        if rsi_window < 2 or slope_window < 1:
            raise ValueError("rsi_window must be >= 2 and slope_window >= 1")
        if trend_slope_threshold < 0 or trend_spread_threshold < 0:
            raise ValueError("Trend thresholds cannot be negative")
        if mean_reversion_z <= 0:
            raise ValueError("mean_reversion_z must be positive")

        self.fast_window = int(fast_window)
        self.slow_window = int(slow_window)
        self.rsi_window = int(rsi_window)
        self.slope_window = int(slope_window)
        self.trend_slope_threshold = float(trend_slope_threshold)
        self.trend_spread_threshold = float(trend_spread_threshold)
        self.mean_reversion_z = float(mean_reversion_z)
        self.rsi_buy_min = float(rsi_buy_min)
        self.rsi_buy_max = float(rsi_buy_max)
        self.rsi_short_min = float(rsi_short_min)
        self.rsi_short_max = float(rsi_short_max)
        self.mean_reversion_rsi_low = float(mean_reversion_rsi_low)
        self.mean_reversion_rsi_high = float(mean_reversion_rsi_high)

        # Current slow average plus an earlier slow average for slope.
        self.required_history = max(
            self.slow_window + self.slope_window,
            self.rsi_window + 1,
        )
        self.prices = deque(maxlen=self.required_history)

    @staticmethod
    def _calculate_rsi(prices, window): 
        changes = [
            prices[i] - prices[i - 1]
            for i in range(len(prices) - window, len(prices))
        ]
        gains = [max(change, 0.0) for change in changes]
        losses = [max(-change, 0.0) for change in changes]
        avg_gain = fmean(gains)
        avg_loss = fmean(losses)

        if avg_gain == 0 and avg_loss == 0:
            return 50.0
        if avg_loss == 0:
            return 100.0
        if avg_gain == 0:
            return 0.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def update(self, price, position="FLAT"):
        if position not in {"FLAT", "LONG", "SHORT"}:
            raise ValueError("position must be FLAT, LONG, or SHORT")

        price = float(price)
        if not isfinite(price) or price <= 0:
            raise ValueError("price must be finite and positive")

        self.prices.append(price)
        if len(self.prices) < self.required_history:
            return {
                "signal": "HOLD",
                "reason": "Waiting for price history",
                "price": price,
                "history": len(self.prices),
                "required_history": self.required_history,
            }

        values = list(self.prices)
        fast_avg = fmean(values[-self.fast_window:])
        slow_avg = fmean(values[-self.slow_window:])

        old_window = values[
            -(self.slow_window + self.slope_window):-self.slope_window
        ]
        old_slow_avg = fmean(old_window)
        slope_pct = (
            (slow_avg - old_slow_avg) / old_slow_avg
            if old_slow_avg > 0 else 0.0
        )
        spread_pct = (fast_avg - slow_avg) / slow_avg
        rsi = self._calculate_rsi(values, self.rsi_window)

        recent = values[-self.slow_window:]
        price_std = pstdev(recent)
        z_score = (price - slow_avg) / price_std if price_std > 0 else 0.0

        # Approximate regime classifier using close prices only.
        trending = (
            abs(slope_pct) >= self.trend_slope_threshold
            and abs(spread_pct) >= self.trend_spread_threshold
        )
        regime = "TREND" if trending else "RANGE"

        signal = "HOLD"
        reason = "No qualifying setup"

        if position == "FLAT":
            if trending:
                # Strategy B: follow directional momentum with MA + RSI confirmation.
                if (
                    slope_pct > 0
                    and fast_avg > slow_avg
                    and self.rsi_buy_min <= rsi <= self.rsi_buy_max
                ):
                    signal = "BUY"
                    reason = "Trend entry: bullish MA direction confirmed by RSI"
                elif (
                    slope_pct < 0
                    and fast_avg < slow_avg
                    and self.rsi_short_min <= rsi < self.rsi_short_max
                ):
                    signal = "SHORT"
                    reason = "Trend entry: bearish MA direction confirmed by RSI"
                else:
                    reason = "Trend detected, but momentum filters did not agree"
            else:
                # Strategy C: fade an extreme only in a range-like regime.
                if (
                    z_score <= -self.mean_reversion_z
                    and rsi <= self.mean_reversion_rsi_low
                ):
                    signal = "BUY"
                    reason = "Mean reversion: price stretched low and RSI is weak"
                elif (
                    z_score >= self.mean_reversion_z
                    and rsi >= self.mean_reversion_rsi_high
                ):
                    signal = "SHORT"
                    reason = "Mean reversion: price stretched high and RSI is strong"
                else:
                    reason = "Range regime, but no mean-reversion extreme"

        elif position == "LONG":
            if trending and (fast_avg < slow_avg or slope_pct < 0):
                signal = "SELL"
                reason = "Exit long: trend weakened or reversed"
            elif not trending and price >= slow_avg:
                signal = "SELL"
                reason = "Exit long: price returned to recent mean"

        elif position == "SHORT":
            if trending and (fast_avg > slow_avg or slope_pct > 0):
                signal = "COVER"
                reason = "Exit short: trend weakened or reversed"
            elif not trending and price <= slow_avg:
                signal = "COVER"
                reason = "Exit short: price returned to recent mean"

        return {
            "signal": signal,
            "reason": reason,
            "price": price,
            "fast_average": round(fast_avg, 4),
            "slow_average": round(slow_avg, 4),
            "rsi": round(rsi, 2),
            "slope_pct": round(slope_pct * 100.0, 4),
            "spread_pct": round(spread_pct * 100.0, 4),
            "z_score": round(z_score, 3),
            "regime": regime,
            "history": len(self.prices),
            "required_history": self.required_history,
            }