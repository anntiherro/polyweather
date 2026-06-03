import json
import time
import re
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

GAMMA_API = "https://gamma-api.polymarket.com/events"
KAFKA_TOPIC = "polymarket-predictions-raw"
POLL_INTERVAL = 300  # 5 minutes

# daily-temperature tag targets city-level temperature markets (Warsaw, Tokyo, London etc.)
WEATHER_TAG_SLUG = "daily-temperature"

WEATHER_KEYWORDS = ["rain", "temperature", "snow", "precipitation", "wind", "storm", "weather", "hurricane", "flood"]
CITY_PATTERN = re.compile(r"\bin\s+([A-Z][a-zA-Z\s']+?)(?:\s+be\s|\s+on\s|\s+during|\s+for|\?|$)")

# condition_id -> tuple of prices, to detect changes
_price_cache: dict[str, tuple] = {}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
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


def is_weather_market(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in WEATHER_KEYWORDS)


def parse_city(question: str) -> str | None:
    # Try "in <City>" pattern
    match = CITY_PATTERN.search(question)
    if match:
        return match.group(1).strip()

    # Try "temperature in <City>" or "temperature <City>"
    match2 = re.search(r"temperature\s+(?:in\s+)?([A-Z][a-zA-Z\s]+?)(?:\s+on|\?|$)", question)
    if match2:
        return match2.group(1).strip()

    return None


def fetch_markets() -> list:
    all_events = []
    offset = 0
    limit = 100

    while True:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        params = {
            "tag_slug": WEATHER_TAG_SLUG,
            "active": "true",
            "closed": "false",
            "end_date_min": f"{today}T00:00:00Z",
            "end_date_max": f"{today}T23:59:59Z",
            "limit": limit,
            "offset": offset,
            "order": "startDate",
            "ascending": "false",
        }

        data = fetch_with_backoff(GAMMA_API, params=params)
        if not data:
            break

        all_events.extend(data)

        if len(data) < limit:
            break
        offset += limit

    return all_events


def process_events(events: list):
    sent = 0
    poll_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for event in events:
        title = event.get("title", "")
        markets = event.get("markets", [])

        if not markets:
            continue

        for market in markets:
            question = market.get("question", "")

            if not is_weather_market(question) and not is_weather_market(title):
                continue

            raw_prices = market.get("outcomePrices", "[]")
            if isinstance(raw_prices, str):
                raw_prices = json.loads(raw_prices)

            raw_outcomes = market.get("outcomes", "[]")
            if isinstance(raw_outcomes, str):
                raw_outcomes = json.loads(raw_outcomes)

            if not raw_prices:
                continue

            token_list = [
                {"outcome": raw_outcomes[i] if i < len(raw_outcomes) else str(i), "price": float(raw_prices[i]), "winner": None}
                for i in range(len(raw_prices))
            ]

            if not token_list:
                continue

            price_sum = sum(t["price"] for t in token_list)
            arbitrage_flag = abs(price_sum - 1.0) > 0.01

            city = parse_city(question) or parse_city(title)

            record = {
                "condition_id": market.get("conditionId"),
                "question_id": market.get("id"),
                "question": question,
                "market_slug": market.get("slug"),
                "end_date_iso": market.get("endDate"),
                "game_start_time": market.get("startDate"),
                "closed": market.get("closed", False),
                "active": market.get("active", True),
                "tokens": [
                    {"outcome": t["outcome"], "price": t["price"], "winner": t["winner"]}
                    for t in token_list
                ],
                "tags": [event.get("slug", "")],
                "LOCATION_NAME": city,
                "POLL_TIMESTAMP": poll_ts,
                "price_sum": round(price_sum, 4),
                "arbitrage_flag": arbitrage_flag,
            }

            prices_key = tuple(t["price"] for t in token_list)
            if _price_cache.get(market.get("conditionId")) == prices_key:
                continue
            _price_cache[market.get("conditionId")] = prices_key

            producer.send(KAFKA_TOPIC, value=record)
            sent += 1

            if arbitrage_flag:
                print(f"[ARBITRAGE] {question[:60]} | sum={price_sum:.4f}")

    producer.flush()
    print(f"[{poll_ts}] sent {sent} weather markets")


def main():
    print("Polymarket producer started")
    while True:
        events = fetch_markets()
        if events:
            process_events(events)
        else:
            print("No events fetched, retrying next cycle")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
