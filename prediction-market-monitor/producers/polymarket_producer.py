import json
import os
import time
import re
import requests
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from jsonschema import validate, ValidationError

GAMMA_API = "https://gamma-api.polymarket.com/events"
KAFKA_TOPIC = "polymarket-predictions-raw"
POLL_INTERVAL = 300  # 5 minutes
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

WEATHER_TAG_SLUG = "daily-temperature"

POLYMARKET_SCHEMA = {
    "type": "object",
    "required": ["condition_id", "question", "tokens", "LOCATION_NAME", "POLL_TIMESTAMP"],
    "properties": {
        "condition_id":    {"type": ["string", "null"]},
        "question":        {"type": "string", "minLength": 5},
        "market_slug":     {"type": ["string", "null"]},
        "end_date_iso":    {"type": ["string", "null"]},
        "game_start_time": {"type": ["string", "null"]},
        "closed":          {"type": "boolean"},
        "active":          {"type": "boolean"},
        "tokens": {
            "type": "array",
            "minItems": 2,
            "items": {
                "type": "object",
                "required": ["outcome", "price"],
                "properties": {
                    "outcome": {"type": "string"},
                    "price":   {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "winner":  {"type": ["boolean", "null"]},
                },
            },
        },
        "LOCATION_NAME":   {"type": ["string", "null"]},
        "POLL_TIMESTAMP":  {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"},
        "price_sum":       {"type": "number", "minimum": 0.0},
        "arbitrage_flag":  {"type": "boolean"},
    },
    "additionalProperties": True,
}


_schema_errors = 0

def validate_record(record: dict) -> bool:
    global _schema_errors
    try:
        validate(instance=record, schema=POLYMARKET_SCHEMA)
        return True
    except ValidationError as e:
        _schema_errors += 1
        print(f"[schema] INVALID record dropped ({e.message}) | cond={record.get('condition_id')}")
        return False

WEATHER_KEYWORDS = ["rain", "temperature", "snow", "precipitation", "wind", "storm", "weather", "hurricane", "flood"]
CITY_PATTERN = re.compile(r"\bin\s+([A-Z][a-zA-Z\s']+?)(?:\s+be\s|\s+on\s|\s+during|\s+for|\?|$)")

_price_cache: dict[str, tuple] = {}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
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
    match = CITY_PATTERN.search(question)
    if match:
        return match.group(1).strip()

    match2 = re.search(r"temperature\s+(?:in\s+)?([A-Z][a-zA-Z\s]+?)(?:\s+on|\?|$)", question)
    if match2:
        return match2.group(1).strip()

    return None


def fetch_markets() -> list:
    """Fetch today's open weather markets."""
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


def fetch_closed_markets() -> list:
    """Fetch yesterday's resolved weather markets for accuracy evaluation."""
    all_events = []
    offset = 0
    limit = 100
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    while True:
        params = {
            "tag_slug": WEATHER_TAG_SLUG,
            "closed": "true",
            "end_date_min": f"{yesterday}T00:00:00Z",
            "end_date_max": f"{yesterday}T23:59:59Z",
            "limit": limit,
            "offset": offset,
            "order": "endDate",
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

            # For resolved markets, the winner field contains the winning outcome name
            market_winner = market.get("winner")

            token_list = [
                {
                    "outcome": raw_outcomes[i] if i < len(raw_outcomes) else str(i),
                    "price": float(raw_prices[i]),
                    "winner": (raw_outcomes[i] == market_winner) if market_winner and i < len(raw_outcomes) else None,
                }
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
                "tokens": token_list,
                "tags": [event.get("slug", "")],
                "LOCATION_NAME": city,
                "POLL_TIMESTAMP": poll_ts,
                "price_sum": round(price_sum, 4),
                "arbitrage_flag": arbitrage_flag,
            }

            # Closed markets always resend (prices frozen at 0/1, cache would block them forever)
            if not market.get("closed", False):
                prices_key = tuple(t["price"] for t in token_list)
                if _price_cache.get(market.get("conditionId")) == prices_key:
                    continue
                _price_cache[market.get("conditionId")] = prices_key

            if not validate_record(record):
                continue

            producer.send(KAFKA_TOPIC, value=record)
            sent += 1

            if arbitrage_flag:
                print(f"[ARBITRAGE] {question[:60]} | sum={price_sum:.4f}")

    producer.flush()
    print(f"[{poll_ts}] sent {sent} weather markets | schema_errors_total={_schema_errors}")


def main():
    print("Polymarket producer started")
    while True:
        events = fetch_markets()
        if events:
            process_events(events)
        else:
            print("No open events fetched")

        closed_events = fetch_closed_markets()
        if closed_events:
            print(f"Processing {len(closed_events)} closed events from yesterday")
            process_events(closed_events)
        else:
            print("No closed events from yesterday")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
