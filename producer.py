#!/usr/bin/env python3
"""
Kafka Producer Template for Streaming Data Dashboard
STUDENT PROJECT: Big Data Streaming Data Producer
"""

import argparse
import json
import time
import random
import math
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ---- Global run flag for graceful shutdown ----
RUNNING = True



def handle_sigint(signum, frame):
    global RUNNING
    print("\nReceived shutdown signal. Stopping producer gracefully...")
    RUNNING = False


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


class StreamingDataProducer:
    """
    Enhanced Kafka producer with stateful synthetic data generation
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        client_id: str = "streaming-producer",
        max_init_retries: int = 5,
        init_backoff: float = 2.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.client_id = client_id

        # Stateful generation
        self.sensor_states = {}
        self.time_counter = 0
        self.base_time = datetime.now()

        # Sensor pool (you may reduce with sensor_count)
        self.sensors = [
            {"id": "sensor_001", "location": "server_room_a", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_002", "location": "server_room_b", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_003", "location": "outdoor_north", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_004", "location": "lab_1", "type": "humidity", "unit": "percent"},
            {"id": "sensor_005", "location": "lab_2", "type": "humidity", "unit": "percent"},
            {"id": "sensor_006", "location": "control_room", "type": "pressure", "unit": "hPa"},
            {"id": "sensor_007", "location": "factory_floor", "type": "pressure", "unit": "hPa"},
            {"id": "sensor_008", "location": "warehouse", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_009", "location": "office_area", "type": "humidity", "unit": "percent"},
            {"id": "sensor_010", "location": "basement", "type": "pressure", "unit": "hPa"},
        ]

        self.metric_ranges = {
            "temperature": {"min": -10, "max": 45, "daily_amplitude": 8, "trend_range": (-0.5, 0.5)},
            "humidity": {"min": 20, "max": 95, "daily_amplitude": 15, "trend_range": (-0.2, 0.2)},
            "pressure": {"min": 980, "max": 1040, "daily_amplitude": 5, "trend_range": (-0.1, 0.1)},
        }

        # Kafka producer config (tunable)
        self.producer_config = {
            "bootstrap_servers": bootstrap_servers,
            "client_id": client_id,
            "acks": "all",  # wait for all replicas for durability
            "retries": 5,
            "linger_ms": 5,
            # No value_serializer: we serialize manually to bytes
        }

        # Initialize Kafka producer with retry/backoff
        self.producer: Optional[KafkaProducer] = None
        attempt = 0
        while attempt < max_init_retries:
            try:
                self.producer = KafkaProducer(**self.producer_config)
                print(f"[INFO] Kafka producer initialized for {bootstrap_servers} on topic '{topic}' (client_id={client_id})")
                break
            except NoBrokersAvailable as e:
                attempt += 1
                wait = init_backoff * attempt
                print(f"[WARN] No Kafka brokers available ({bootstrap_servers}). Retry {attempt}/{max_init_retries} in {wait:.1f}s...")
                time.sleep(wait)
            except Exception as e:
                attempt += 1
                wait = init_backoff * attempt
                print(f"[ERROR] Kafka init failed: {e}. Retry {attempt}/{max_init_retries} in {wait:.1f}s...")
                time.sleep(wait)

        if self.producer is None:
            print(f"[ERROR] Could not initialize Kafka producer after {max_init_retries} attempts. Exiting initialization with producer=None.")

    def generate_sample_data(self) -> Dict[str, Any]:
        """
        Generate realistic streaming data with stateful patterns.
        """
        sensor = random.choice(self.sensors)
        sensor_id = sensor["id"]
        metric_type = sensor["type"]

        if sensor_id not in self.sensor_states:
            config = self.metric_ranges[metric_type]
            base_value = random.uniform(config["min"], config["max"])
            trend = random.uniform(config["trend_range"][0], config["trend_range"][1])
            phase_offset = random.uniform(0, 2 * math.pi)
            self.sensor_states[sensor_id] = {
                "base_value": base_value,
                "trend": trend,
                "phase_offset": phase_offset,
                "last_value": base_value,
                "message_count": 0,
            }

        state = self.sensor_states[sensor_id]

        # progressive timestamp
        current_time = self.base_time + timedelta(seconds=self.time_counter)
        # variable increment for realism
        self.time_counter += random.uniform(0.5, 2.0)

        config = self.metric_ranges[metric_type]
        hours_in_day = 24
        current_hour = current_time.hour + current_time.minute / 60.0
        daily_cycle = math.sin(2 * math.pi * current_hour / hours_in_day + state["phase_offset"])
        trend_effect = state["trend"] * (state["message_count"] / 100.0)
        noise = random.uniform(-config["daily_amplitude"] * 0.1, config["daily_amplitude"] * 0.1)

        base_value = state["base_value"]
        daily_variation = daily_cycle * config["daily_amplitude"]
        raw_value = base_value + daily_variation + trend_effect + noise
        bounded_value = max(config["min"], min(config["max"], raw_value))

        state["last_value"] = bounded_value
        state["message_count"] += 1
        if random.random() < 0.01:
            state["trend"] = random.uniform(config["trend_range"][0], config["trend_range"][1])

        sample_data = {
            "timestamp": current_time.replace(microsecond=0).isoformat() + "Z",
            "value": round(bounded_value, 2),
            "metric_type": metric_type,
            "sensor_id": sensor_id,
            "location": sensor["location"],
            "unit": sensor["unit"],
        }
        return sample_data

    def serialize_data(self, data: Dict[str, Any]) -> Optional[bytes]:
        """
        JSON-serialize the data dict to UTF-8 bytes.
        Adds basic validation for required fields.
        """
        required_fields = {"timestamp", "value", "metric_type", "sensor_id"}
        try:
            missing = required_fields - set(data.keys())
            if missing:
                raise ValueError(f"Missing required fields: {missing}")

            # Ensure value is numeric
            if not isinstance(data["value"], (int, float)):
                raise ValueError("Field 'value' must be numeric")

            # Return JSON bytes
            return json.dumps(data, ensure_ascii=False).encode("utf-8")
        except Exception as e:
            print(f"[ERROR] Serialization failed: {e} - Data: {data}")
            return None

    def send_message(self, data: Dict[str, Any], max_retries: int = 3, retry_backoff: float = 1.0) -> bool:
        """
        Send a message to Kafka with retries and error handling.

        Returns True if successful, False otherwise.
        """
        if not self.producer:
            print("[ERROR] Kafka producer not initialized. Message not sent.")
            return False

        payload = self.serialize_data(data)
        if payload is None:
            return False

        attempt = 0
        while attempt <= max_retries:
            try:
                future = self.producer.send(self.topic, value=payload)
                # block until send result or timeout
                record_metadata = future.get(timeout=10)
                # optional: print metadata for debugging
                print(f"[DEBUG] Sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset} - {data}")
                return True
            except KafkaError as e:
                attempt += 1
                wait = retry_backoff * attempt
                print(f"[WARN] KafkaError on send (attempt {attempt}/{max_retries}): {e}. Backing off {wait:.1f}s...")
                time.sleep(wait)
            except Exception as e:
                attempt += 1
                wait = retry_backoff * attempt
                print(f"[ERROR] Unexpected error on send (attempt {attempt}/{max_retries}): {e}. Backing off {wait:.1f}s...")
                time.sleep(wait)

        print("[ERROR] Failed to send message after retries.")
        return False

    def produce_stream(self, messages_per_second: float = 0.1, duration: Optional[int] = None):
        """
        Main streaming loop that respects messages_per_second.
        messages_per_second: e.g., 0.1 -> one message every 10s; 2.0 -> 2 messages per second.
        duration: optional total runtime in seconds (None => infinite until SIGINT)
        """
        global RUNNING
        if messages_per_second <= 0:
            raise ValueError("messages_per_second must be > 0")

        interval = 1.0 / messages_per_second  # seconds between messages on average
        print(f"[INFO] Starting producer: rate={messages_per_second} msg/sec (interval ~{interval:.3f}s), duration={duration or 'infinite'}s")

        start_time = time.time()
        messages_sent = 0
        last_time = time.time()

        try:
            while RUNNING:
                # duration check
                if duration and (time.time() - start_time) >= duration:
                    print(f"[INFO] Reached requested duration of {duration} seconds.")
                    break

                now = time.time()
                elapsed = now - last_time

                # We can emit multiple messages if interval is very small and elapsed > interval
                if elapsed >= interval:
                    # Determine how many messages to emit to catch up (floor)
                    to_emit = max(1, int(elapsed // interval))
                    for _ in range(to_emit):
                        if not RUNNING:
                            break
                        data = self.generate_sample_data()
                        success = self.send_message(data)
                        if success:
                            messages_sent += 1
                        # small jitter to avoid synchronized bursts
                        time.sleep(random.uniform(0, min(0.01, interval)))
                    last_time = time.time()
                else:
                    # sleep remaining time (but wake up on signals)
                    time.sleep(min(interval - elapsed, 0.5))

        except Exception as e:
            print(f"[ERROR] Streaming error: {e}")
        finally:
            self.close()
            print(f"[INFO] Producer stopped. Total messages sent: {messages_sent}")

    def close(self):
        """Cleanly flush and close the Kafka producer."""
        if self.producer:
            try:
                print("[INFO] Flushing and closing Kafka producer...")
                self.producer.flush(timeout=10)
                self.producer.close(timeout=10)
                print("[INFO] Kafka producer closed successfully.")
            except Exception as e:
                print(f"[ERROR] Error closing Kafka producer: {e}")


# ---- CLI parsing ----
def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka Streaming Data Producer")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", type=str, default="streaming-data", help="Kafka topic to produce to (default: streaming-data)")
    parser.add_argument("--rate", type=float, default=0.1, help="Messages per second (default 0.1 => one message every 10s)")
    parser.add_argument("--duration", type=int, default=None, help="Total run duration in seconds (default: run until interrupted)")
    parser.add_argument("--sensor-count", type=int, default=0, help="Optional: limit number of sensors used (0 = use all sensors)")
    parser.add_argument("--client-id", type=str, default="streaming-producer", help="Kafka client id")
    parser.add_argument("--max-init-retries", type=int, default=5, help="Max init retries when connecting to Kafka")
    parser.add_argument("--init-backoff", type=float, default=2.0, help="Initial backoff multiplier for Kafka init retries")
    return parser.parse_args()


def main():
    args = parse_arguments()

    print("=" * 60)
    print("STREAMING DATA PRODUCER")
    print(f"Bootstrap servers: {args.bootstrap_servers}  Topic: {args.topic}")
    print(f"Rate: {args.rate} msg/sec  Duration: {args.duration or 'infinite'}  Sensor-count: {args.sensor_count or 'all'}")
    print("=" * 60)

    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        client_id=args.client_id,
        max_init_retries=args.max_init_retries,
        init_backoff=args.init_backoff,
    )

    # Optionally limit the number of sensors used to simulate
    if args.sensor_count and args.sensor_count > 0:
        if args.sensor_count < len(producer.sensors):
            producer.sensors = producer.sensors[: args.sensor_count]
            print(f"[INFO] Using first {args.sensor_count} sensors for simulation.")
        else:
            print(f"[WARN] sensor_count ({args.sensor_count}) >= available sensors ({len(producer.sensors)}). Using full pool.")

    try:
        producer.produce_stream(messages_per_second=args.rate, duration=args.duration)
    except Exception as e:
        print(f"[ERROR] Unhandled error in main: {e}")
    finally:
        print("[INFO] Producer execution completed.")


if __name__ == "__main__":
    main()
