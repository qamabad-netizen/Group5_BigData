#!/usr/bin/env python3
# kafka_to_mongo.py

import json
from kafka import KafkaConsumer
from pymongo import MongoClient

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "streaming-data"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "streaming_db"
COLLECTION = "weather_history"

def main():
    mongo = MongoClient(MONGO_URI)
    db = mongo[DB_NAME]
    coll = db[COLLECTION]

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000
    )

    print(f"Listening on {KAFKA_TOPIC} @ {KAFKA_BROKER} -> MongoDB {DB_NAME}.{COLLECTION}")

    try:
        for msg in consumer:
            try:
                doc = msg.value
                coll.insert_one(doc)
                print("Inserted:", doc.get("timestamp"))
            except Exception as e:
                print("Insert error:", e, "| Doc:", msg.value)

    except KeyboardInterrupt:
        print("Shutting down consumer...")

    finally:
        consumer.close()
        mongo.close()

if __name__ == "__main__":
    main()
