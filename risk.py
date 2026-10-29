
class RiskManager:
    def __init__(
        self,
        max_position_fraction=0.10,
        max_daily_loss_fraction=0.02,
    ):
        self.max_position_fraction = max_position_fraction
        self.max_daily_loss_fraction = max_daily_loss_fraction
        self.starting_equity = None

    def set_starting_equity(self, equity):
        if equity <= 0:
            raise ValueError("Equity must be positive")
        self.starting_equity = float(equity)

    def check_order(self, price, quantity, equity):
        price = float(price)
        quantity = int(quantity)
        equity = float(equity)

        if price <= 0 or quantity <= 0 or equity <= 0:
            return False, "Invalid order values"

        if price * quantity > equity * self.max_position_fraction:
            return False, "Position limit exceeded"

        if self.starting_equity is not None:
            loss = self.starting_equity - equity
            limit = self.starting_equity * self.max_daily_loss_fraction

            if loss >= limit:
                return False, "Daily loss limit reached"

        return True, "Risk checks passed"
