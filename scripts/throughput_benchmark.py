"""Kafka throughput benchmark for the StreamForge review."""
from __future__ import annotations
import argparse
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="telemetry-events")
    args = parser.parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        acks="all",
        linger_ms=5,
        batch_size=64 * 1024,
        compression_type="lz4",
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    started = time.perf_counter()
    for i in range(args.events):
        producer.send(
            args.topic,
            key=f"TRUCK-{i % 50_000:05d}".encode(),
            value={
                "device_id": f"TRUCK-{i % 50_000:05d}",
                "temperature": 20.0 + (i % 20) / 10,
                "humidity": 50.0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
    producer.flush()
    elapsed = time.perf_counter() - started
    print(f"events={args.events}")
    print(f"elapsed_seconds={elapsed:.3f}")
    print(f"producer_events_per_second={args.events / elapsed:,.0f}")
    producer.close()

if __name__ == "__main__":
    main()
