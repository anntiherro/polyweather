"""
Reads from 3 Kafka topics and writes to PostgreSQL.
  market-weather-correlations → correlations
  market-accuracy-aggregates  → accuracy_windows
  arbitrage-alerts            → alerts
"""
import json
import time
import psycopg2
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
PG_DSN = "host=localhost port=5432 dbname=polyweather user=polyweather password=polyweather"

TOPICS = [
    "market-weather-correlations",
    "market-accuracy-aggregates",
    "arbitrage-alerts",
]


def connect_pg():
    while True:
        try:
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            print("[pg] connected")
            return conn
        except Exception as e:
            print(f"[pg] connection failed: {e}, retrying in 5s")
            time.sleep(5)


def insert_correlation(cur, msg):
    cur.execute("""
        INSERT INTO correlations
          (condition_id, question, question_type, location_name,
           yes_price, target_temp, actual_temp, data_type, market_closed,
           actual_outcome, prediction_error, correlation_latency_sec,
           arbitrage_flag, poll_timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (
        msg.get("condition_id"),
        msg.get("question"),
        msg.get("question_type"),
        msg.get("LOCATION_NAME"),
        msg.get("yes_price"),
        msg.get("target_temp"),
        msg.get("actual_temp"),
        msg.get("data_type"),
        msg.get("market_closed"),
        msg.get("ACTUAL_OUTCOME"),
        msg.get("PREDICTION_ERROR"),
        msg.get("CORRELATION_LATENCY_SEC"),
        msg.get("arbitrage_flag"),
        msg.get("POLL_TIMESTAMP"),
    ))


def insert_accuracy_window(cur, msg):
    cur.execute("""
        INSERT INTO accuracy_windows
          (location_name, window_start, window_end,
           total_count, correct_count, accuracy_rate,
           avg_prediction_error, max_prediction_error, bias_score,
           closed_count, closed_correct_count, closed_accuracy_rate)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        msg.get("LOCATION_NAME"),
        msg.get("window_start"),
        msg.get("window_end"),
        msg.get("total_count"),
        msg.get("correct_count"),
        msg.get("accuracy_rate"),
        msg.get("avg_prediction_error"),
        msg.get("max_prediction_error"),
        msg.get("bias_score"),
        msg.get("closed_count"),
        msg.get("closed_correct_count"),
        msg.get("closed_accuracy_rate"),
    ))


def insert_alert(cur, msg):
    cur.execute("""
        INSERT INTO alerts
          (alert_type, location_name, condition_id, detail,
           poll_timestamp, alert_timestamp)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        msg.get("alert_type"),
        msg.get("LOCATION_NAME"),
        msg.get("condition_id"),
        msg.get("detail"),
        msg.get("POLL_TIMESTAMP"),
        msg.get("ALERT_TIMESTAMP"),
    ))


HANDLERS = {
    "market-weather-correlations": insert_correlation,
    "market-accuracy-aggregates":  insert_accuracy_window,
    "arbitrage-alerts":            insert_alert,
}


def main():
    conn = connect_pg()

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    print(f"[kafka] subscribed to {TOPICS}")
    batch_size = 0

    while True:
        try:
            for msg in consumer:
                topic = msg.topic
                data = msg.value
                handler = HANDLERS.get(topic)
                if handler is None:
                    continue
                try:
                    with conn.cursor() as cur:
                        handler(cur, data)
                    batch_size += 1
                    if batch_size >= 100:
                        conn.commit()
                        consumer.commit()
                        print(f"[pg] committed batch of {batch_size}")
                        batch_size = 0
                except Exception as e:
                    print(f"[pg] insert error on {topic}: {e}")
                    conn.rollback()

            # Flush remainder after consumer_timeout
            if batch_size > 0:
                conn.commit()
                consumer.commit()
                print(f"[pg] committed {batch_size} records")
                batch_size = 0

        except Exception as e:
            print(f"[main] error: {e}")
            try:
                conn.close()
            except Exception:
                pass
            conn = connect_pg()
            time.sleep(3)


if __name__ == "__main__":
    main()
