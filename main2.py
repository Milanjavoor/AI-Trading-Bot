import json
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from config import settings
from execution.api_client import TradingAPI
from execution.lifecycle import is_trading_allowed
from monitoring.scorecard import Scorecard
from strategy.news_sentiment import score_news
from strategy.signal_engine import build_signal
from strategy.risk_manager import RiskManager
from quantum.optimizer import choose_portfolio

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("manthan")


def run_demo():
    """Run a small, deterministic simulation without connecting to the venue."""
    log.info("Starting DEMO mode. No real API orders will be submitted.")
    prices = {
        "ALPHA": deque([100, 101, 100.5, 102, 103], maxlen=30),
        "BETA": deque([80, 79.5, 80, 80.2, 81], maxlen=30),
        "GAMMA": deque([150, 149, 148, 149, 147], maxlen=30),
        "DELTA": deque([50, 50.5, 51, 51.5, 52], maxlen=30),
    }
    sentiment = defaultdict(float)
    scorecard = Scorecard(starting_cash=settings.starting_cash)
    risk = RiskManager(settings)
    cash = settings.starting_cash
    holdings = defaultdict(int)

    # Sample-only events let you verify the whole pipeline before venue access.
    demo_news = {
        "ALPHA": "Company reports strong profit growth and raises its outlook",
        "BETA": "Company announces steady revenue and stable demand",
        "GAMMA": "Company warns of falling profit and weaker demand",
        "DELTA": "Company wins a major contract and expands operations",
    }
    for ticker, headline in demo_news.items():
        sentiment[ticker] = score_news(headline)

    signals = {}
    for ticker, history in prices.items():
        signals[ticker] = build_signal(list(history), sentiment[ticker])

    chosen = choose_portfolio(
        tickers=list(signals.keys()),
        expected_returns={t: signals[t]["score"] for t in signals},
        covariance=None,
        max_assets=2,
        method=settings.quantum_method,
    )
    log.info("Allocator method=%s, selected=%s", chosen["method"], chosen["selected"])

    for ticker, signal in signals.items():
        decision = risk.decide(
            ticker=ticker,
            price=prices[ticker][-1],
            signal=signal,
            cash=cash,
            current_quantity=holdings[ticker],
            portfolio_value=settings.starting_cash,
        )
        log.info(
            "%-6s score=%+.3f sentiment=%+.2f action=%s reason=%s",
            ticker, signal["score"], sentiment[ticker], decision.action, decision.reason
        )
        scorecard.record_decision(ticker, decision.action, accepted=True, latency_ms=0.1)

    log.info("Demo scorecard: %s", json.dumps(scorecard.summary(), indent=2))
    log.info("Demo complete. To connect to venue, configure .env and set ENABLE_LIVE_TRADING=true.")


def run_live():
    """Venue integration. Fails closed if configuration is incomplete."""
    if not settings.server_ip or not settings.access_token or not settings.kafka_bootstrap:
        raise RuntimeError(
            "Live mode needs SERVER_IP, BOT_ACCESS_TOKEN and KAFKA_BOOTSTRAP in .env. "
            "Do not paste your token into source code."
        )

    from data.kafka_client import create_consumer
    api = TradingAPI(settings)
    consumer = create_consumer(settings)
    risk = RiskManager(settings)
    scorecard = Scorecard(starting_cash=settings.starting_cash)
    prices = defaultdict(lambda: deque(maxlen=200))
    sentiments = defaultdict(float)
    latest_news_time = {}
    last_stage_check = 0.0
    stage = {}
    last_sim_date = None

    log.warning("LIVE MODE enabled. Orders can be submitted to the MANTHAN server.")
    log.info("Connecting to Kafka topics market.ticks and market.news...")
    consumer.subscribe(["market.ticks", "market.news"])

    try:
        stocks_response = api.get("/stocks")
        stocks = stocks_response.get("stocks", [])
        tickers = [s.get("ticker") or s.get("symbol") for s in stocks]
        tickers = [t for t in tickers if t]
        log.info("Discovered %d active tickers.", len(tickers))
    except Exception:
        log.exception("Could not load stock universe; staying fail-closed.")
        tickers = []

    try:
        while True:
            now = time.monotonic()
            if now - last_stage_check >= settings.stage_poll_seconds:
                try:
                    stage = api.get_public("/stage")
                    last_stage_check = now
                    log.info("Lifecycle stage=%s status=%s",
                             stage.get("stage"), stage.get("status"))
                except Exception:
                    log.exception("Stage poll failed; orders remain disabled.")
                    stage = {}

            message = consumer.poll(timeout=0.2)
            if message is None:
                continue
            if message.error():
                log.error("Kafka error: %s", message.error())
                continue

            try:
                event = json.loads(message.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                log.warning("Skipping malformed Kafka message.")
                continue

            topic = message.topic()
            event_type = event.get("event_type") or event.get("event")

            # Mandatory pre-flight ACK: acknowledge each sequence received.
            if topic == "market.ticks" and event_type == "preflight_tick":
                seq = event.get("seq", (event.get("data") or {}).get("seq"))
                if seq is not None:
                    try:
                        api.post("/bot/heartbeat", {"seq": int(seq)})
                        log.info("Heartbeat ACK sent for seq=%s", seq)
                    except Exception:
                        log.exception("Heartbeat ACK failed for seq=%s", seq)
                continue

            if topic == "market.news" and event_type == "news_flash":
                data = event.get("data") or {}
                ticker = data.get("ticker")
                if ticker:
                    text = f"{data.get('headline', '')} {data.get('description', '')}"
                    sentiments[ticker] = score_news(text)
                    latest_news_time[ticker] = time.monotonic()
                continue

            if topic != "market.ticks" or event_type != "price_tick":
                continue

            data = event.get("data") or {}
            ticker = data.get("ticker")
            close = data.get("close")
            sim_date = event.get("simulated_date")
            if not ticker or close is None:
                log.warning("Skipping tick with missing ticker/close.")
                continue

            # Do not act unless the official simulation is running.
            if not is_trading_allowed(stage):
                continue

            prices[ticker].append(float(close))
            if sim_date:
                last_sim_date = sim_date

            # Wait for enough observations; no look-ahead data is used.
            if len(prices[ticker]) < settings.minimum_history:
                continue

            signal = build_signal(list(prices[ticker]), sentiments[ticker])
            # Keep first release conservative: only long BUY/SELL actions.
            # SHORT/COVER support can be added after validating venue semantics.
            try:
                portfolio = api.get("/portfolio")
                cash_value = float(portfolio.get("cash", settings.starting_cash))
                holdings_data = portfolio.get("holdings", portfolio.get("positions", []))
                current_quantity = 0
                for item in holdings_data if isinstance(holdings_data, list) else []:
                    if item.get("ticker") == ticker:
                        current_quantity = int(item.get("quantity", item.get("shares", 0)))
                        break
                portfolio_value = float(
                    portfolio.get("total_wealth", portfolio.get("total_value", settings.starting_cash))
                )
            except Exception:
                log.exception("Portfolio fetch failed; skip this tick.")
                continue

            decision = risk.decide(
                ticker=ticker,
                price=float(close),
                signal=signal,
                cash=cash_value,
                current_quantity=current_quantity,
                portfolio_value=portfolio_value,
            )
            if decision.action in ("BUY", "SELL") and decision.quantity > 0:
                order = {
                    "ticker": ticker,
                    "action": decision.action,
                    "quantity": decision.quantity,
                    "date": sim_date or last_sim_date or datetime.now(timezone.utc).date().isoformat(),
                }
                try:
                    api.post("/orders", order)
                    scorecard.record_decision(ticker, decision.action, accepted=True,
                                              latency_ms=(time.monotonic() - now) * 1000)
                    log.info("Order sent: %s", order)
                except Exception as exc:
                    scorecard.record_decision(ticker, decision.action, accepted=False,
                                              latency_ms=(time.monotonic() - now) * 1000)
                    log.error("Order rejected/failed for %s: %s", ticker, exc)
            else:
                scorecard.record_decision(ticker, decision.action, accepted=True,
                                          latency_ms=(time.monotonic() - now) * 1000)

    except KeyboardInterrupt:
        log.info("Shutdown requested.")
    finally:
        consumer.close()
        log.info("Final bot scorecard: %s", json.dumps(scorecard.summary(), indent=2))


if __name__ == "__main__":
    if settings.enable_live_trading:
        run_live()
    else:
        run_demo()




{
  "status": 200,
  "body": {
    "team_id": "team_8f21bc9e",
    "team_name": "Alpha Traders",
    "initial_capital": 1000000,
    "capital_remaining": 900000,
    "portfolio_market_value": 102500,
    "total_wealth": 1006250,
    "total_pnl": 21250,
    "total_pnl_pct": 2.13,
    "realized_pnl": 15000,
    "unrealized_pnl": 6250,
    "positions_count": 2,
    "orders_count": 12,
    "holdings": [
      {
        "ticker": "AETX",
        "quantity": 100,
        "average_buy_price": 1000,
        "current_price": 1025,
        "market_value": 102500,
        "unrealized_pnl": 2500,
        "unrealized_pnl_pct": 2.5,
        "is_short": false
      },
      {
        "ticker": "BETATECH",
        "quantity": -50,
        "average_buy_price": 1100,
        "current_price": 1025,
        "market_value": -51250,
        "unrealized_pnl": 3750,
        "unrealized_pnl_pct": 6.82,
        "is_short": true
      }
    ]
  }
}
