import json
import time
import requests
from kafka import KafkaProducer
from geopy.geocoders import Nominatim

# ---------------- Kafka ----------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "weather-actuals-raw"

# ---------------- Geocoder ----------------
geolocator = Nominatim(user_agent="weather-producer")

cities = ["Seattle", "London", "Warsaw", "Tokyo"]

geo_cache = {}

def get_coords(city):
    if city in geo_cache:
        return geo_cache[city]

    try:
        location = geolocator.geocode(city, timeout=10)
        if location is None:
            print(f"Geocoding failed for {city}")
            return None

        coords = (location.latitude, location.longitude)
        geo_cache[city] = coords
        return coords

    except Exception as e:
        print(f"Geocoding error for {city}: {e}")
        return None


# ---------------- Weather API ----------------
def fetch_weather(lat, lon, retries=2):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,precipitation,rain,weather_code,wind_speed_10m,relative_humidity_2m"
    )

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()["current"]

        except Exception as e:
            print(f"attempt {attempt+1} failed:", e)
            time.sleep(2)

    return None


# ---------------- Main loop ----------------
LIMIT = 10

for i in range(LIMIT):

    for city in cities:

        coords = get_coords(city)
        if coords is None:
            continue

        lat, lon = coords
        current = fetch_weather(lat, lon)

        if current is None:
            print(f"Skipping {city} due to API failure")
            continue

        event = {
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "temperature_2m": current.get("temperature_2m"),
            "precipitation": current.get("precipitation"),
            "rain": current.get("rain"),
            "weather_code": current.get("weather_code"),
            "wind_speed_10m": current.get("wind_speed_10m"),
            "relative_humidity_2m": current.get("relative_humidity_2m"),
            "timestamp": current.get("time")
        }

        producer.send(topic, value=event)
        print("sent weather:", city, event["temperature_2m"])
        time.sleep(1)

    time.sleep(60)


producer.flush()
producer.close()