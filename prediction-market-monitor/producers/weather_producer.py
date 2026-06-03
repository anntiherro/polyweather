import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer
from geopy.geocoders import Nominatim

OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast"
KAFKA_BOOTSTRAP = "localhost:9092"
IN_TOPIC = "polymarket-predictions-raw"
OUT_TOPIC = "weather-actuals-raw"
POLL_INTERVAL = 900  # 15 minutes

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    acks="all",
    retries=3,
)

geolocator = Nominatim(user_agent="weather-producer")
geo_cache: dict[str, tuple[float, float]] = {}


def get_active_locations() -> set[str]:
    """Read polymarket-predictions-raw and collect cities from open markets."""
    consumer = KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    cities: set[str] = set()
    for msg in consumer:
        record = msg.value
        if not record.get("closed", True) and record.get("LOCATION_NAME"):
            cities.add(record["LOCATION_NAME"])
    consumer.close()
    return cities


def get_coords(city: str):
    if city in geo_cache:
        return geo_cache[city]
    try:
        loc = geolocator.geocode(city, timeout=10)
        if loc is None:
            print(f"[geo] no result for {city}")
            return None
        coords = (loc.latitude, loc.longitude)
        geo_cache[city] = coords
        return coords
    except Exception as e:
        print(f"[geo] error for {city}: {e}")
        return None


def fetch_weather(lat: float, lon: float):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,precipitation,rain,weather_code,wind_speed_10m,relative_humidity_2m",
    }
    delay = 1
    for attempt in range(5):
        try:
            r = requests.get(OPEN_METEO_API, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"[weather] attempt {attempt+1} failed: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 16)
    return None


def poll_cycle():
    poll_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cities = get_active_locations()
    print(f"[{poll_ts}] active cities from Kafka: {sorted(cities)}")

    sent = 0
    for city in sorted(cities):
        coords = get_coords(city)
        if coords is None:
            continue
        lat, lon = coords

        data = fetch_weather(lat, lon)
        if not data or "current" not in data:
            print(f"[weather] skip {city}: no data")
            continue

        record = {
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "current_units": data.get("current_units"),
            "current": data.get("current"),
            "LOCATION_NAME": city,
            "POLL_TIMESTAMP": poll_ts,
        }
        producer.send(OUT_TOPIC, value=record)
        print(f"[weather] sent {city}: {data['current'].get('temperature_2m')}°C")
        sent += 1
        time.sleep(1)

    producer.flush()
    print(f"[{poll_ts}] sent {sent} weather records")


def main():
    print("Weather producer started")
    while True:
        try:
            poll_cycle()
        except Exception as e:
            print(f"[main] cycle error: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
