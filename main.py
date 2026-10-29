
import json
import logging
import os

import requests
from kafka import KafkaConsumer

# --------------------------------------------------
# MANTHAN Trading Bot Configuration
# --------------------------------------------------

GATEWAY = "http://192.168.65.157:3002"
BROKER = "192.168.65.157:9094"
USERNAME = "mn2611"
CONSUMER_GROUP = f"group-{USERNAME}"

TOKEN = os.environ.get("BOT_ACCESS_TOKEN", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("manthan-bot")

if not TOKEN:
    raise SystemExit(
        "BOT_ACCESS_TOKEN is missing. Set it in your terminal "
        "before starting the bot."
    )

# Reuse one HTTP session for API requests.
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
})


# --------------------------------------------------
# Fetch the active simulation's stock universe
# --------------------------------------------------

def fetch_stocks():
    response = session.get(
        f"{GATEWAY}/api/v1/stocks",
        timeout=5,
    )
    response.raise_for_status()

    payload = response.json()
    body = payload.get("body", payload)
    stocks = body.get("stocks", [])

    if not stocks:
        raise RuntimeError(
            f"No stocks returned by API: {payload}"
        )

    tickers = {
        item.get("ticker") or item.get("symbol")
        for item in stocks
        if item.get("ticker") or item.get("symbol")
    }

    if not tickers:
        raise RuntimeError("No valid tickers in stock response.")

    return tickers


# --------------------------------------------------
# Acknowledge pre-flight heartbeat events
# --------------------------------------------------

def acknowledge_heartbeat(seq):
    response = session.post(
        f"{GATEWAY}/api/v1/bot/heartbeat",
        json={"seq": int(seq)},
        timeout=3,
    )
    response.raise_for_status()

    log.info(
        "Heartbeat acknowledged: seq=%s response=%s",
        seq,
        response.text,
    )


# --------------------------------------------------
# Main Kafka consumer
# --------------------------------------------------

def main():
    consumer = None

    try:
        log.info("Connecting to Kafka at %s", BROKER)

        consumer = KafkaConsumer(
            "market.ticks",
            "market.news",
            bootstrap_servers=[BROKER],
            group_id=CONSUMER_GROUP,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            value_deserializer=lambda msg: json.loads(
                msg.decode("utf-8")
            ),
            request_timeout_ms=10000,
        )

        log.info(
            "Kafka consumer created. Group=%s",
            CONSUMER_GROUP,
        )

        tickers = None

        for message in consumer:
            event = message.value

            if not isinstance(event, dict):
                log.warning("Ignoring unexpected event: %r", event)
                continue

            event_type = (
                event.get("event_type")
                or event.get("event")
            )
            if "preflight" in str(event).lower():
                log.info(
                     "PREFLIGHT DEBUG: topic=%s event=%s",
                     message.topic,
                     event,
                )

            # ------------------------------------------
            # Pre-flight: discover stocks and heartbeat
            # ------------------------------------------

            if event_type == "preflight_tick":
                log.info(
                    "PREFLIGHT EVENT RECEIVED: %s",
                    event,
                )
                log.info(
                    "PRE-FLIGHT EVENT RECEIVED: seq=%s",
                    event.get("seq"),
                )

                if tickers is None:
                    try:
                        tickers = fetch_stocks()
                        log.info(
                            "Discovered %d simulation tickers: %s",
                            len(tickers),
                            sorted(tickers),
                        )
                    except requests.RequestException:
                        log.exception("Failed to fetch stock universe")
                        continue
                    except RuntimeError:
                        log.exception("Invalid stock universe response")
                        continue

                seq = event.get("seq")
                if seq is None:
                    seq = (event.get("data") or {}).get("seq")

                if seq is None:
                    log.error(
                        "Pre-flight event has no sequence number: %s",
                        event,
                    )
                    continue

                try:
                    acknowledge_heartbeat(seq)
                except requests.RequestException:
                    log.exception(
                        "Heartbeat request failed for seq=%s",
                        seq,
                    )

                continue

            # ------------------------------------------
            # Market price ticks
            # ------------------------------------------

            if message.topic == "market.ticks":
                data = event.get("data") or {}

                ticker = data.get("ticker")
                price = data.get("close")

                if tickers is not None and ticker not in tickers:
                    log.warning("Ignoring unknown ticker: %s", ticker)
                    continue

                log.info(
                    "TICK date=%s ticker=%s close=%s volume=%s",
                    event.get("simulated_date"),
                    ticker,
                    price,
                    data.get("volume"),
                )

                # Strategy and order execution are not implemented yet.
                # Do not submit trades until the strategy is tested.

            # ------------------------------------------
            # Market news events
            # ------------------------------------------

            elif message.topic == "market.news":
                data = event.get("data") or {}

                log.info(
                    "NEWS date=%s ticker=%s headline=%s description=%s",
                    event.get("simulated_date"),
                    data.get("ticker"),
                    data.get("headline"),
                    data.get("description"),
                )

    except KeyboardInterrupt:
        log.info("Stopping bot after keyboard interrupt.")

    except Exception:
        log.exception("Bot stopped because of an unexpected error.")

    finally:
        if consumer is not None:
            consumer.close()

        session.close()
        log.info("Bot shut down.")


if __name__ == "__main__":
    main()
