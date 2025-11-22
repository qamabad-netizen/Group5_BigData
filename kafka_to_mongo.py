#!/usr/bin/env python3
"""
kafka_to_mongo.py
Reliable Kafka consumer that writes valid JSON messages into MongoDB.
Skips empty or invalid messages and stores timestamp as a datetime.
"""

import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
from pymongo.errors import PyMongoError

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "streaming-data"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "streaming_db"
COLLECTION_NAME = "weather_history"


def parse_timestamp(ts):
    if not isinstance(ts, str):
        return None
    s = ts
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.strptime(ts.split('.')[0], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def main():
    mongo = MongoClient(MONGO_URI)
    coll = mongo[DB_NAME][COLLECTION_NAME]

    # ensure timestamp index
    try:
        coll.create_index([("timestamp", 1)])
    except PyMongoError as e:
        print("Warning: index create failed:", e)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',   # read from start for historical ingest
        enable_auto_commit=True,
        consumer_timeout_ms=10000
    )

    print(f"Listening on Kafka topic '{KAFKA_TOPIC}' -> inserting into MongoDB '{DB_NAME}.{COLLECTION_NAME}'")

    try:
        for msg in consumer:
            raw = msg.value
            if raw is None or raw == b"":
                continue

            try:
                s = raw.decode("utf-8").strip()
                if not s:
                    continue
                doc = json.loads(s)
            except Exception as e:
                print("Skipping invalid message:", e)
                continue

            parsed = parse_timestamp(doc.get("timestamp"))
            if parsed:
                doc["timestamp"] = parsed

            try:
                coll.insert_one(doc)
                print("Inserted:", doc.get("timestamp"))
            except PyMongoError as e:
                print("Insert error:", e, "| doc:", doc)

    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        consumer.close()
        mongo.close()


if __name__ == "__main__":
    main()
