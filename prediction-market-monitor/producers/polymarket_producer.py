import json
import time
import re
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

POLYMARKET_API = "https://clob.polymarket.com/markets"
KAFKA_TOPIC = "polymarket-predictions-raw"
POLL_INTERVAL = 300  # 5 minutes

WEATHER_KEYWORDS = ["rain", "temperature", "snow", "precipitation", "wind", "storm"]
CITY_PATTERN = re.compile(r"\bin\s+([A-Z][a-zA-Z\s]+?)(?:\s+on|\s+during|\s+for|\?|$)")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
)


def fetch_with_backoff(url, params=None, max_retries=5):
    delay = 1
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[attempt {attempt + 1}/{max_retries}] error: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 16)
    return None


def is_weather_market(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in WEATHER_KEYWORDS)


def parse_city(question: str) -> str | None:
    match = CITY_PATTERN.search(question)
    if match:
        return match.group(1).strip()
    return None


def validate_tokens(tokens: list) -> bool:
    try:
        total = sum(float(t["price"]) for t in tokens)
        return abs(total - 1.0) <= 0.01
    except (KeyError, TypeError, ValueError):
        return False


def fetch_markets() -> list:
    all_markets = []
    next_cursor = None

    while True:
        params = {"tag": "Weather"}
        if next_cursor:
            params["next_cursor"] = next_cursor

        data = fetch_with_backoff(POLYMARKET_API, params=params)
        if not data:
            break

        markets = data.get("data", [])
        all_markets.extend(markets)

        next_cursor = data.get("next_cursor")
        if not next_cursor or next_cursor == "LTE=":
            break

    return all_markets


def process_markets(markets: list):
    sent = 0
    poll_ts = datetime.now(timezone.utc).isoformat()

    for market in markets:
        question = market.get("question", "")

        if not is_weather_market(question):
            continue

        tokens = market.get("tokens", [])
        if not tokens:
            continue

        prices_valid = validate_tokens(tokens)
        price_sum = sum(float(t.get("price", 0)) for t in tokens)
        arbitrage_flag = abs(price_sum - 1.0) > 0.01

        city = parse_city(question)

        record = {
            "condition_id": market.get("condition_id"),
            "question_id": market.get("question_id"),
            "question": question,
            "market_slug": market.get("market_slug"),
            "end_date_iso": market.get("end_date_iso"),
            "game_start_time": market.get("game_start_time"),
            "closed": market.get("closed", False),
            "active": market.get("active", False),
            "tokens": [
                {
                    "outcome": t.get("outcome"),
                    "price": float(t.get("price", 0)),
                    "winner": t.get("winner"),
                }
                for t in tokens
            ],
            "tags": market.get("tags", []),
            "LOCATION_NAME": city,
            "POLL_TIMESTAMP": poll_ts,
            "prices_valid": prices_valid,
            "price_sum": round(price_sum, 4),
            "arbitrage_flag": arbitrage_flag,
        }

        producer.send(KAFKA_TOPIC, value=record)
        sent += 1

        if arbitrage_flag:
            print(f"[ARBITRAGE] {question[:60]} | sum={price_sum:.4f}")

    producer.flush()
    print(f"[{poll_ts}] sent {sent} weather markets")


def main():
    print("Polymarket producer started")
    while True:
        markets = fetch_markets()
        if markets:
            process_markets(markets)
        else:
            print("No markets fetched, retrying next cycle")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
