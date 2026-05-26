from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "polymarket-predictions-raw"

cities = ["Seattle", "London", "Warsaw", "Tokyo"]

# сколько сообщений отправим
LIMIT = 10

for i in range(LIMIT):
    event = {
        "condition_id": str(random.randint(1000, 9999)),
        "city": random.choice(cities),
        "prediction": "RAIN",
        "probability": round(random.random(), 2),
        "event_time": time.time()
    }

    producer.send(topic, value=event)
    print(f"{i+1}/{LIMIT} sent:", event)

    time.sleep(2)

producer.flush()
producer.close()