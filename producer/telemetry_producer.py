import json
import os
import random
import time
from concurrent.futures import Future
from datetime import datetime, timezone

from confluent_kafka import Producer


KAFKA_SERVER = os.getenv(
    "STREAMFORGE_KAFKA_BROKER",
    os.getenv("STREAMFORGE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
)
TOPIC = os.getenv(
    "STREAMFORGE_KAFKA_TOPIC",
    os.getenv("STREAMFORGE_KAFKA_TELEMETRY_TOPIC", "telemetry-events"),
)
TRUCK_COUNT = int(os.getenv("STREAMFORGE_TRUCK_COUNT", "50000"))
EVENTS_PER_SECOND = float(os.getenv("STREAMFORGE_PRODUCER_EVENTS_PER_SECOND", "5000"))


class _DeliveryFuture:
    """Small KafkaProducer-compatible future for integration-test callers."""

    def __init__(self, producer: "BufferedProducer") -> None:
        self._producer = producer
        self._future: Future = Future()

    def complete(self, error, message) -> None:
        if error is not None:
            self._future.set_exception(RuntimeError(str(error)))
        else:
            self._future.set_result(message)

    def get(self, timeout: float | None = None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self._future.done():
            wait = 0.1 if deadline is None else min(0.1, deadline - time.monotonic())
            if wait <= 0:
                raise TimeoutError("Kafka delivery timed out")
            self._producer._producer.poll(wait)
        return self._future.result()


class BufferedProducer:
    """Batching Confluent producer with the small API used by this project."""

    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    def send(self, topic: str, *, key: str, value: dict) -> _DeliveryFuture:
        future = _DeliveryFuture(self)
        while True:
            try:
                self._producer.produce(
                    topic,
                    key=key.encode("utf-8"),
                    value=json.dumps(value).encode("utf-8"),
                    on_delivery=future.complete,
                )
                break
            except BufferError:
                self._producer.poll(0.1)
        self._producer.poll(0)
        return future

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def close(self) -> None:
        self.flush()


def create_producer():
    return BufferedProducer(
        Producer(
            {
                "bootstrap.servers": KAFKA_SERVER,
                "client.id": os.getenv("STREAMFORGE_PRODUCER_ID", "streamforge-producer"),
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 5,
                "batch.num.messages": 10000,
                "queue.buffering.max.messages": 100000,
            }
        )
    )


def generate_event(truck_id: str | None = None):
    return {
        "truck_id": truck_id or f"TRUCK-{random.randint(1, TRUCK_COUNT):05d}",
        "temperature": round(random.uniform(20.0, 40.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = create_producer()

    print("StreamForge Telemetry Producer started.")
    print(f"Kafka: {KAFKA_SERVER}")
    print(f"Topic: {TOPIC}")
    print(
        f"Simulating {TRUCK_COUNT:,} trucks at "
        f"{EVENTS_PER_SECOND:,.0f} events/sec (one reading per truck every 10 sec)."
    )
    print("Press Ctrl+C to stop.\n")

    try:
        sequence = 0
        while True:
            started = time.perf_counter()
            truck_id = f"TRUCK-{sequence % TRUCK_COUNT + 1:05d}"
            event = generate_event(truck_id)
            producer.send(
                TOPIC,
                key=event["truck_id"],
                value=event,
            )
            sequence += 1
            if sequence % 1000 == 0:
                print(f"Published {sequence:,} events; latest truck={truck_id}")
            delay = (1.0 / EVENTS_PER_SECOND) - (time.perf_counter() - started)
            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main()
