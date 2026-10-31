# MANTHAN – Algorithmic Trading Bot
### Real-Time Market Data Processing | Hybrid Trading Strategies | Automated Execution | Risk Management

---

## 1. Project Overview

MANTHAN is an automated algorithmic trading bot developed for a hackathon trading simulation.

The system consumes real-time market data and financial news through Apache Kafka, processes incoming stock price updates, evaluates trading opportunities using a hybrid technical analysis strategy, and submits trading orders through the MANTHAN API.

The primary objective is to maximize ending portfolio wealth while maintaining appropriate risk controls.

Unlike manual trading, where decisions are made by an individual observing market movements, this system automates the process of:

1. Receiving market data.
2. Processing stock price updates.
3. Maintaining price history for individual stocks.
4. Calculating technical indicators.
5. Identifying potential trading opportunities.
6. Evaluating risk before entering new positions.
7. Executing BUY, SELL, SHORT and COVER orders.
8. Tracking portfolio positions and performance.

The bot is designed to operate continuously, responding to incoming market events rather than relying on manually entered trading instructions.

**Competition Objective:** Maximize final portfolio equity.

---

## 2. Key Features

### 2.1 Real-Time Market Data Processing

- Connects to an Apache Kafka broker.
- Consumes market tick data from the `market.ticks` topic.
- Processes stock prices and trading volumes.
- Maintains independent price histories for individual stock tickers.
- Evaluates trading opportunities whenever new price data arrives.

### 2.2 Financial News Processing

- Consumes financial news events from the `market.news` topic.
- Extracts relevant information such as stock ticker symbols and headlines.
- Logs incoming news events for monitoring and analysis.

Note: The current strategy should not be described as news-driven unless news sentiment is explicitly connected to its trading decisions.

### 2.3 Hybrid Trading Strategy

The upgraded strategy combines multiple technical analysis approaches:

- Moving Average Crossover.
- Relative Strength Index (RSI).
- Trend-Following Momentum.
- Mean Reversion.

The strategy attempts to identify different market conditions and select an appropriate trading approach.

### 2.4 Automated Order Execution

The bot communicates with the MANTHAN trading gateway to submit orders.

Supported trading actions:

| Action | Description |
|---|---|
| BUY | Opens a long position |
| SELL | Closes a long position |
| SHORT | Opens a short position |
| COVER | Closes a short position |
| HOLD | Does not submit an order |

The bot processes the API response and updates its local position state according to the execution result.

### 2.5 Portfolio Monitoring

The bot uses the MANTHAN portfolio API:

`GET /api/v1/portfolio`

The endpoint provides information including:

- Initial capital.
- Remaining capital.
- Portfolio market value.
- Total wealth.
- Total P&L.
- Realized P&L.
- Unrealized P&L.
- Open holdings.
- Long and short positions.

The `total_wealth` field is used as the authoritative portfolio equity value for risk calculations.

### 2.6 Risk Management

The project includes a separate `risk.py` module containing the `RiskManager` class.

The intended risk controls include:

- Maximum position fraction: 10%.
- Maximum daily loss fraction: 2%.
- Order-size validation.
- Daily equity-loss monitoring.

The intended execution workflow checks risk before opening new BUY or SHORT positions.

SELL and COVER orders must remain available to close existing positions.

**Implementation note:** These controls should only be described as fully operational after the risk checks have been integrated, tested and verified in the main trading loop. Aggregate portfolio exposure and restart-safe daily loss tracking require additional implementation.

---

## 3. System Architecture

The project follows an event-driven architecture.

### High-Level Architecture

```text
                  MANTHAN PLATFORM
                         |
             +-----------+-----------+
             |                       |
        MARKET DATA              NEWS DATA
             |                       |
             v                       v
       Kafka Broker             Kafka Broker
             |                       |
             v                       v
       market.ticks              market.news
             |                       |
             +-----------+-----------+
                         |
                         v
                    MAIN.PY
                (Central Controller)
                         |
             +-----------+-----------+
             |                       |
             v                       v
       Price Processing          News Logging
             |
             v
      Trading Strategy
             |
             v
       Technical Analysis
             |
             +-----------------------+
             |                       |
             v                       v
       Moving Averages             RSI
             |                       |
             +-----------+-----------+
                         |
                         v
                Market Regime Analysis
                         |
              +----------+----------+
              |                     |
              v                     v
         Trend Following       Mean Reversion
              |                     |
              +----------+----------+
                         |
                         v
                  Trading Signal
                         |
                         v
                  Risk Manager
                         |
                         v
                 MANTHAN Portfolio
                         |
                         v
                  Order Execution
                         |
                         v
                 Portfolio Updates
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| `main.py` | Main controller and event-processing pipeline |
| `strategy.py` | Technical analysis and trading decisions |
| `risk.py` | Risk validation and trading limits |
| `config.py` | Centralized configuration, where applicable |
| Kafka | Real-time event transportation |
| MANTHAN API | Market information, portfolio data and order execution |

---

## 4. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| Apache Kafka | Real-time event streaming |
| kafka-python | Kafka consumer integration |
| Requests | HTTP communication with MANTHAN |
| REST API | Portfolio retrieval and order submission |
| Python Collections | Price-history management |
| Statistics | Moving averages and price dispersion |
| Logging | Monitoring and debugging |
| Virtual Environment | Dependency isolation |

---

## 5. Trading Strategy Explained

The upgraded strategy combines three approaches.

### 5.1 Moving Average Crossover

Moving averages help identify the direction of a stock's recent price movement.

The implementation uses:

- Fast Moving Average: 5 observations.
- Slow Moving Average: 20 observations.

**Bullish crossover:**

When the fast moving average crosses above the slow moving average, it indicates a possible upward trend.

**Bearish crossover:**

When the fast moving average crosses below the slow moving average, it indicates a possible downward trend.

Mathematically:

Fast MA:

\[
MA_f = \frac{1}{5}\sum_{i=1}^{5}P_i
\]

Slow MA:

\[
MA_s = \frac{1}{20}\sum_{i=1}^{20}P_i
\]

Where \(P_i\) represents an observed stock price.

### 5.2 Relative Strength Index (RSI)

RSI measures the magnitude of recent upward and downward price movements.

Its value ranges between 0 and 100.

The implementation uses a 14-period RSI.

General interpretation:

| RSI | Interpretation |
|---|---|
| Above 70 | Potentially overbought |
| Below 30 | Potentially oversold |
| Around 50 | Relatively balanced momentum |

The strategy uses RSI to confirm potential entries rather than relying entirely on moving-average signals.

The current implementation uses a simple-average RSI calculation rather than the conventional Wilder-smoothed calculation.

### 5.3 Trend-Following Strategy

Trend-following attempts to identify sustained directional price movements.

The implementation uses:

- Moving-average direction.
- Slow moving-average slope.
- Fast-versus-slow average spread.
- RSI confirmation.

A bullish trend may generate BUY signals when the required conditions agree.

A bearish trend may generate SHORT signals when the required conditions agree.

The objective is to participate in an established trend instead of reacting to every small price fluctuation.

### 5.4 Mean Reversion Strategy

Mean reversion assumes that unusually large deviations from a recent average may eventually move back toward that average.

The implementation calculates a price deviation score:

\[
Z = \frac{P-MA_s}{\sigma}
\]

Where:

- \(P\) = Current price.
- \(MA_s\) = Recent slow moving average.
- \(\sigma\) = Standard deviation of recent prices.

The strategy looks for unusually low or high prices in range-like market conditions.

Potential signals:

- BUY when the price is sufficiently below its recent mean and RSI confirms weakness.
- SHORT when the price is sufficiently above its recent mean and RSI confirms strength.

Mean reversion is disabled as an entry approach when the regime classifier identifies a sufficiently strong trend.

### 5.5 Market Regime Classification

The strategy attempts to classify the market into:

- TREND
- RANGE

This classification uses slow-average slope and the percentage spread between fast and slow averages.

In a trending market, the bot follows directional momentum.

In a range-like market, the bot looks for mean-reversion opportunities.

This is an approximate regime classifier based on price ticks, not a complete statistical market-regime model.

---

## 6. Trading Decision Workflow

The strategy evaluates each incoming price observation.

```text
Incoming Stock Price
         |
         v
Validate Price
         |
         v
Update Price History
         |
         v
Enough Historical Data?
       /     \
     NO       YES
     |         |
     v         v
   HOLD    Calculate Indicators
               |
               v
         Calculate RSI
               |
               v
       Calculate Moving Averages
               |
               v
        Classify Market Regime
               |
          +----+----+
          |         |
        TREND      RANGE
          |         |
          v         v
       Momentum   Mean Reversion
       Analysis    Analysis
          |         |
          +----+----+
               |
               v
        Generate Signal
               |
               v
         Check Position
               |
               v
         Return Decision
```

Each ticker maintains its own price history to prevent price information from different stocks from being mixed.

The default hybrid strategy requires 25 price observations per ticker before performing its complete calculations.

---

## 7. Risk Management Architecture

Risk management is an important component of automated trading.

The objective is to prevent the bot from opening positions that violate predefined limits.

### 7.1 Maximum Position Fraction

Default setting:

\[
Maximum\ Position\ Fraction = 10\%
\]

For an account with ₹10,00,000 equity:

\[
Maximum\ Order\ Value = ₹1,00,000
\]

An order exceeding the configured individual order-value limit should be rejected.

### 7.2 Daily Loss Threshold

Default setting:

\[
Maximum\ Daily\ Loss = 2\%
\]

For ₹10,00,000 starting daily equity:

\[
Maximum\ Loss = ₹20,000
\]

Once the daily loss threshold is reached, new positions should be blocked.

### 7.3 Portfolio-Based Risk Assessment

The MANTHAN portfolio endpoint provides current equity through:

`body.total_wealth`

This value is used to calculate order exposure.

Short positions require special handling because the MANTHAN API represents their market value as a negative liability.

### 7.4 Fail-Closed Design

If the bot cannot retrieve or validate the portfolio information, it should reject new BUY and SHORT orders rather than proceeding without risk verification.

However, SELL and COVER orders must remain available for closing positions.

### 7.5 Additional Risk Controls to Implement

- Aggregate exposure limits across all holdings.
- Restart-safe daily loss tracking.
- Duplicate-order prevention.
- Position reconciliation with the actual portfolio API.
- Order execution verification.
- Handling of API timeouts and rejected orders.
- Protection against excessive short exposure.

---

## 8. Kafka Integration

Kafka is used to transport market events to the bot.

### Topics

| Topic | Purpose |
|---|---|
| `market.ticks` | Stock price and volume updates |
| `market.news` | Financial news events |
| Pre-flight heartbeat events | Bot readiness and sequence acknowledgements |

### Consumer Group

The bot joins its configured Kafka consumer group.

Kafka assigns partitions to the consumer, allowing it to process messages from the configured topics.

### Consumer Workflow

1. Connect to the Kafka broker.
2. Join the consumer group.
3. Receive partition assignments.
4. Consume incoming messages.
5. Decode and process the message.
6. Update the appropriate strategy.
7. Generate a trading decision.

### Heartbeat and Pre-flight

The bot also participates in the MANTHAN pre-flight process.

The platform expects heartbeat sequence acknowledgements through:

`POST /api/v1/bot/heartbeat`

The bot must acknowledge valid pre-flight sequence numbers within the platform's required time window.

Kafka connectivity alone does not establish that the bot is fully ready. Both market-data connectivity and heartbeat acknowledgement are required.

---

## 9. MANTHAN API Integration

The bot communicates with the MANTHAN gateway using HTTP requests.

Important endpoints:

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/v1/stocks` | Retrieve simulation stock universe |
| GET | `/api/v1/portfolio` | Retrieve portfolio and account information |
| POST | `/api/v1/bot/heartbeat` | Acknowledge heartbeat sequence |
| Order submission endpoint | Configured in the controller | Submit trading orders |

The exact order-submission route is determined by the existing project implementation.

### Portfolio API Response

Important fields include:

- `initial_capital`
- `capital_remaining`
- `portfolio_market_value`
- `total_wealth`
- `total_pnl`
- `realized_pnl`
- `unrealized_pnl`
- `positions_count`
- `orders_count`
- `holdings`

The `total_wealth` field is used as the portfolio equity input for risk calculations.

---

## 10. Security Features

Security is important because the bot communicates with external APIs and can submit trading orders.

### 10.1 Environment-Based Authentication

The bot retrieves its access token from an environment variable:

`BOT_ACCESS_TOKEN`

Example PowerShell configuration:

```powershell
$env:BOT_ACCESS_TOKEN = "YOUR_ACTUAL_TOKEN"
```

The token should never be hard-coded into source files.

### 10.2 Virtual Environment Isolation

Project dependencies are installed inside `.venv`.

This prevents the project from relying on unrelated global Python packages.

### 10.3 HTTP Request Timeouts

API calls should use request timeouts to prevent the bot from waiting indefinitely for an unavailable server.

### 10.4 HTTP Response Validation

The implementation uses:

```python
response.raise_for_status()
```

This detects unsuccessful HTTP responses and allows the bot to handle API failures.

### 10.5 Input Validation

Market prices should be validated before calculations.

The upgraded strategy rejects:

- Non-numeric values.
- Non-finite prices.
- Zero or negative prices.
- Invalid position states.

### 10.6 Risk-Based Order Rejection

The bot is designed to reject new entries when portfolio equity cannot be verified or configured risk limits are violated.

### 10.7 Logging and Monitoring

The system records events such as:

- Kafka connection status.
- Market ticks.
- News events.
- Trading decisions.
- Risk approvals and rejections.
- Order submissions.
- API responses.
- Position updates.

### 10.8 Additional Security Recommendations

For a production-quality implementation:

- Prefer HTTPS where supported.
- Never commit `.env` files or access tokens.
- Avoid logging credentials or sensitive API responses.
- Validate all API data before using it.
- Use idempotency mechanisms where supported.
- Reconcile actual positions after ambiguous order outcomes.

---

## 11. Project Structure

```text
Haackathon2/
│
├── main.py
│   Main trading controller.
│   Handles Kafka consumption,
│   strategy execution and API orders.
│
├── strategy.py
│   Hybrid trading strategy.
│   Implements moving averages,
│   RSI, trend following and mean reversion.
│
├── risk.py
│   Risk management module.
│   Contains position-size and
│   daily-loss validation logic.
│
├── config.py
│   Project configuration.
│
├── .venv/
│   Isolated Python environment.
│
├── requirements.txt
│   Python dependency list.
│
└── README.md
    Project documentation.
```

---

## 12. Installation and Setup

### Prerequisites

- Python 3.10 or newer, subject to dependency compatibility.
- Access to the MANTHAN hackathon platform.
- Valid MANTHAN Bearer token.
- Kafka broker access.
- Internet or network access to the configured gateway.

### Step 1: Clone the repository

```bash
git clone YOUR_REPOSITORY_URL
cd Haackathon2
```

### Step 2: Create a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
```

### Step 3: Activate the environment

```powershell
.\.venv\Scripts\Activate.ps1
```

### Step 4: Install dependencies

```powershell
python -m pip install requests kafka-python
```

Alternatively, if a `requirements.txt` is provided:

```powershell
python -m pip install -r requirements.txt
```

### Step 5: Configure the access token

```powershell
$env:BOT_ACCESS_TOKEN = "YOUR_ACTUAL_TOKEN"
```

### Step 6: Run the trading bot

```powershell
python main.py
```

---

## 13. Performance Evaluation

The primary competition metric is ending portfolio wealth.

The strategy should therefore be evaluated using actual account equity rather than simply counting successful orders.

### Important Performance Metrics

**1. Ending Portfolio Wealth**

The final account equity at the competition deadline.

**2. Net Profit and Loss**

The difference between final and initial equity, adjusted for the platform's accounting rules.

**3. Return on Investment**

\[
ROI = \frac{Final\ Equity - Initial\ Equity}
{Initial\ Equity}\times100
\]

**4. Win Rate**

\[
Win\ Rate =
\frac{Profitable\ Closed\ Trades}
{Total\ Closed\ Trades}\times100
\]

**5. Profit Factor**

\[
Profit\ Factor =
\frac{Gross\ Profits}{|Gross\ Losses|}
\]

**6. Maximum Drawdown**

Measures the largest decline from a previous portfolio-equity peak.

### Strategy Comparison

The original and hybrid strategies should be tested on the same historical or replayed market data.

Comparison should include:

- Ending equity.
- Net P&L after trading costs.
- Win rate.
- Profit factor.
- Maximum drawdown.
- Transaction costs.
- Portfolio exposure.

A separate validation period should be used to avoid overfitting the strategy to historical data.

---

## 14. Testing and Validation

Before allowing the bot to run continuously, verify the following:

| Test | Expected result |
|---|---|
| Kafka connection | Successful connection |
| Market tick processing | Valid ticks are processed |
| News consumption | News messages are received |
| Strategy warm-up | HOLD until sufficient history exists |
| RSI calculation | Valid values between 0 and 100 |
| Invalid price | Rejected |
| BUY signal | Correct entry validation |
| SHORT signal | Correct entry validation |
| SELL signal | Existing long position can close |
| COVER signal | Existing short position can close |
| Portfolio API failure | New entries blocked |
| Risk limit exceeded | New entry rejected |
| Order API rejection | Local position not falsely updated |
| Bot restart | Portfolio state reconciled |

---

## 15. Current Limitations and Future Improvements

The project can be extended in several ways:

1. **Advanced Risk Management:** Add aggregate portfolio exposure, persistent daily loss tracking and position reconciliation.
2. **Improved Indicators:** Explore Wilder RSI, ATR, ADX and volatility-adjusted thresholds.
3. **News Sentiment Analysis:** Extract sentiment from financial news and incorporate it into trading decisions.
4. **Backtesting Engine:** Evaluate strategies against historical market data before deployment.
5. **Dynamic Position Sizing:** Adjust order quantities based on volatility, equity and risk limits.
6. **Execution Optimization:** Improve handling of slippage, transaction costs, partial fills and API timeouts.
7. **Performance Dashboard:** Visualize equity curves, trade history, drawdown and strategy performance.
8. **Parameter Optimization:** Evaluate strategy parameters using out-of-sample testing rather than relying on arbitrary thresholds.

---

## 16. Conclusion

MANTHAN is an event-driven algorithmic trading system that combines real-time market data processing, technical analysis, automated execution and portfolio-based risk management.

Its hybrid strategy attempts to adapt to different market conditions by combining trend-following momentum with mean reversion, supported by moving averages and RSI confirmation.

The system is designed around modular components, allowing the trading strategy, risk management and data-processing layers to be improved independently.

The ultimate objective is not simply to execute more trades, but to make better-informed trading decisions and maximize ending portfolio wealth under the hackathon's simulation rules.

---

## Disclaimer

This project is developed for an algorithmic trading hackathon and simulated trading environment. Its strategies are experimental and do not guarantee positive returns. Historical or simulated performance does not guarantee future performance. Risk controls must be tested and verified before automated order execution.
