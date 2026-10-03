"""Kafka throughput benchmark for the StreamForge review."""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument(
        "--brokers",
        default=os.getenv(
            "STREAMFORGE_KAFKA_BROKER",
            os.getenv("STREAMFORGE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        ),
    )
    parser.add_argument("--topic", default="telemetry-events")
    args = parser.parse_args()
    delivered = 0
    errors = []

    def on_delivery(error, _message):
        nonlocal delivered
        if error is not None:
            errors.append(str(error))
        else:
            delivered += 1

    producer = Producer(
        {
            "bootstrap.servers": args.brokers,
            "client.id": "streamforge-throughput-benchmark",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 100000,
        }
    )
    started = time.perf_counter()
    for i in range(args.events):
        truck_id = f"TRUCK-{i % 50_000 + 1:05d}"
        event = {
            "truck_id": truck_id,
            "temperature": 20.0 + (i % 20) / 10,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        while True:
            try:
                producer.produce(
                    args.topic,
                    key=truck_id.encode(),
                    value=json.dumps(event).encode(),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    remaining = producer.flush(60)
    elapsed = time.perf_counter() - started
    print(f"events={args.events}")
    print(f"delivered={delivered}")
    print(f"elapsed_seconds={elapsed:.3f}")
    print(f"producer_events_per_second={delivered / elapsed:,.0f}")
    if remaining or errors:
        raise RuntimeError(
            f"Kafka delivery incomplete: {remaining} message(s) queued, "
            f"{len(errors)} delivery error(s); first error: {errors[:1]}"
        )

if __name__ == "__main__":
    main()
