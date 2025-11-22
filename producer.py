#!/usr/bin/env python3
"""
producer.py
Simple, robust Kafka producer that emits realistic sensor data.
Uses realtime UTC timestamps (isoformat + 'Z') so downstream consumers
and Mongo store proper datetimes.
"""

import argparse
import json
import time
import random
import math
import signal
from datetime import datetime
from typing import Dict, Any, Optional

from kafka import KafkaProducer

RUNNING = True

def handle_sigint(signum, frame):
    global RUNNING
    print("\nReceived shutdown signal. Stopping producer gracefully...")
    RUNNING = False

signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


class StreamingDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str, client_id: str = "streaming-producer"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.client_id = client_id

        # sensors and ranges
        self.sensors = [
            {"id": "sensor_001", "location": "server_room_a", "metric_type": "temperature", "unit": "celsius"},
            {"id": "sensor_002", "location": "server_room_a", "metric_type": "humidity", "unit": "percent"},
            {"id": "sensor_003", "location": "server_room_b", "metric_type": "pressure", "unit": "pascal"},
            {"id": "sensor_004", "location": "lobby", "metric_type": "co2", "unit": "ppm"},
            {"id": "sensor_005", "location": "warehouse", "metric_type": "temperature", "unit": "celsius"},
        ]

        self.metric_ranges = {
            "temperature": {"min": -10, "max": 45, "daily_amplitude": 8, "trend_range": (-0.5, 0.5)},
            "humidity": {"min": 20, "max": 95, "daily_amplitude": 15, "trend_range": (-0.2, 0.2)},
            "pressure": {"min": 980, "max": 1040, "daily_amplitude": 5, "trend_range": (-0.1, 0.1)},
            "co2": {"min": 300, "max": 2000, "daily_amplitude": 200, "trend_range": (-5, 5)},
        }

        # initialize producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            acks="all",
            retries=5,
            linger_ms=5,
        )

    def generate_sample_data(self) -> Dict[str, Any]:
        sensor = random.choice(self.sensors)
        metric_type = sensor["metric_type"]
        cfg = self.metric_ranges[metric_type]

        # realistic base value + daily cycle + noise
        now = datetime.utcnow()
        hours = now.hour + now.minute / 60.0
        daily_cycle = math.sin(2 * math.pi * hours / 24 + random.uniform(0, 2 * math.pi))
        daily_variation = daily_cycle * cfg["daily_amplitude"]
        noise = random.uniform(-cfg["daily_amplitude"] * 0.1, cfg["daily_amplitude"] * 0.1)
        base = (cfg["min"] + cfg["max"]) / 2.0
        value = max(cfg["min"], min(cfg["max"], base + daily_variation + noise))

        payload = {
            "timestamp": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "value": round(value, 2),
            "metric_type": metric_type,
            "sensor_id": sensor["id"],
            "location": sensor["location"],
            "unit": sensor["unit"],
        }
        return payload

    def send_message(self, data: Dict[str, Any]) -> bool:
        try:
            payload = json.dumps(data).encode("utf-8")
            future = self.producer.send(self.topic, payload)
            meta = future.get(timeout=10)
            print(f"[SENT] {meta.topic} offset={meta.offset} -> {data}")
            return True
        except Exception as e:
            print("Send error:", e)
            return False

    def produce_stream(self, rate_hz: float = 0.5, duration: Optional[int] = None):
        interval = 1.0 / rate_hz if rate_hz > 0 else 1.0
        start = time.time()
        sent = 0
        try:
            while RUNNING:
                if duration and (time.time() - start) >= duration:
                    break
                data = self.generate_sample_data()
                if self.send_message(data):
                    sent += 1
                time.sleep(interval)
        finally:
            self.producer.flush()
            self.producer.close()
            print(f"[INFO] Producer stopped. Messages sent: {sent}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="streaming-data")
    p.add_argument("--rate", type=float, default=0.5, help="messages per second")
    p.add_argument("--duration", type=int, default=None, help="seconds to run")
    return p.parse_args()


def main():
    args = parse_args()
    producer = StreamingDataProducer(args.bootstrap_servers, args.topic)
    producer.produce_stream(rate_hz=args.rate, duration=args.duration)


if __name__ == "__main__":
    main()
