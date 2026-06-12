import json
import os
import subprocess
import time
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer
from geopy.geocoders import Nominatim

OPEN_METEO_FORECAST_API = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"


def ps_get(url: str, params: dict) -> dict | None:
    """HTTP GET via PowerShell Invoke-WebRequest (uses Windows Schannel, bypasses OpenSSL issues)."""
    import base64
    query = "&".join(f"{k}={v}" for k, v in params.items())
    full_url = f"{url}?{query}"
    # Use -EncodedCommand to avoid PowerShell misinterpreting & in URLs
    ps_script = f'(Invoke-WebRequest -Uri "{full_url}" -UseBasicParsing).Content'
    encoded = base64.b64encode(ps_script.encode("utf-16-le")).decode("ascii")
    result = subprocess.run(
        ["powershell", "-NoProfile", "-EncodedCommand", encoded],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode == 0 and result.stdout.strip():
        return json.loads(result.stdout)
    return None
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
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


def get_market_locations() -> tuple[set[str], dict[str, str]]:
    """
    Returns (open_cities, closed_locations) where:
    - open_cities: set of city names with active open markets
    - closed_locations: {city: end_date} for resolved markets (yesterday)
    """
    consumer = KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    open_cities: set[str] = set()
    closed_locations: dict[str, str] = {}

    for msg in consumer:
        record = msg.value
        city = record.get("LOCATION_NAME")
        if not city:
            continue

        if record.get("closed", False):
            end_date = (record.get("end_date_iso") or "")[:10]
            if end_date and city not in closed_locations:
                closed_locations[city] = end_date
        else:
            open_cities.add(city)

    consumer.close()
    return open_cities, closed_locations


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


def fetch_forecast(lat: float, lon: float):
    """Fetch current conditions + daily max/min forecast for today."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,precipitation,rain,weather_code,wind_speed_10m,relative_humidity_2m",
        "daily": "temperature_2m_max,temperature_2m_min",
        "timezone": "auto",
        "forecast_days": 1,
    }
    delay = 1
    for attempt in range(5):
        try:
            data = ps_get(OPEN_METEO_FORECAST_API, params)
            if data:
                return data
            raise ValueError("empty response")
        except Exception as e:
            print(f"[forecast] attempt {attempt+1} failed: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 16)
    return None


def fetch_historical(lat: float, lon: float, date: str):
    """Fetch actual daily max/min from archive API for a specific past date."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min",
        "start_date": date,
        "end_date": date,
        "timezone": "auto",
    }
    delay = 1
    for attempt in range(5):
        try:
            data = ps_get(OPEN_METEO_ARCHIVE_API, params)
            if data:
                return data
            raise ValueError("empty response")
        except Exception as e:
            print(f"[archive] attempt {attempt+1} failed: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 16)
    return None


def poll_cycle():
    poll_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    open_cities, closed_locations = get_market_locations()

    print(f"[{poll_ts}] open: {len(open_cities)} cities, closed: {len(closed_locations)} cities")

    sent = 0

    # Open markets: daily forecast (max/min) + current temperature
    for city in sorted(open_cities):
        coords = get_coords(city)
        if coords is None:
            continue
        lat, lon = coords

        data = fetch_forecast(lat, lon)
        if not data or "daily" not in data:
            print(f"[forecast] skip {city}: no data")
            continue

        raw_daily = data.get("daily", {})
        record = {
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "data_type": "forecast",
            "current_units": data.get("current_units"),
            "current": data.get("current"),
            "daily": {
                "temperature_2m_max": (raw_daily.get("temperature_2m_max") or [None])[0],
                "temperature_2m_min": (raw_daily.get("temperature_2m_min") or [None])[0],
            },
            "LOCATION_NAME": city,
            "POLL_TIMESTAMP": poll_ts,
        }
        producer.send(OUT_TOPIC, value=record)
        max_t = record["daily"]["temperature_2m_max"]
        min_t = record["daily"]["temperature_2m_min"]
        print(f"[forecast] sent {city}: max={max_t}°C min={min_t}°C")
        sent += 1
        time.sleep(1)

    # Closed markets: actual historical data for the resolved date
    for city, end_date in sorted(closed_locations.items()):
        coords = get_coords(city)
        if coords is None:
            continue
        lat, lon = coords

        data = fetch_historical(lat, lon, end_date)
        if not data or "daily" not in data:
            print(f"[archive] skip {city} ({end_date}): no data")
            continue

        raw_daily = data.get("daily", {})
        record = {
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "data_type": "historical",
            "date": end_date,
            "daily": {
                "temperature_2m_max": (raw_daily.get("temperature_2m_max") or [None])[0],
                "temperature_2m_min": (raw_daily.get("temperature_2m_min") or [None])[0],
            },
            "LOCATION_NAME": city,
            "POLL_TIMESTAMP": poll_ts,
        }
        producer.send(OUT_TOPIC, value=record)
        max_t = record["daily"]["temperature_2m_max"]
        min_t = record["daily"]["temperature_2m_min"]
        print(f"[archive] sent {city} ({end_date}): max={max_t}°C min={min_t}°C")
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
