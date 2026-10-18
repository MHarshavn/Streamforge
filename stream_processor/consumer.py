"""Kafka stream worker with manual commits, heartbeats, metrics and changelog snapshots."""
from __future__ import annotations

import json
import logging
import os
import socket
import time
import urllib.request
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata

from streamforge.metrics import (
    EVENTS_FAILED,
    EVENTS_FILTERED,
    EVENTS_PROCESSED,
    PROCESSING_LAG_SECONDS,
    PROCESSING_SECONDS,
    registry,
)

KAFKA_BROKER = os.getenv(
    "STREAMFORGE_KAFKA_BROKER",
    os.getenv("STREAMFORGE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
)
KAFKA_TOPIC = os.getenv(
    "STREAMFORGE_KAFKA_TOPIC",
    os.getenv("STREAMFORGE_KAFKA_TELEMETRY_TOPIC", "telemetry-events"),
)
CHANGELOG_TOPIC = os.getenv(
    "STREAMFORGE_KAFKA_CHANGELOG_TOPIC", "streamforge-state-changelog"
)
BACKEND_URL = os.getenv(
    "STREAMFORGE_BACKEND_URL", "http://127.0.0.1:8000/telemetry/ingest"
)
HEARTBEAT_URL = os.getenv(
    "STREAMFORGE_HEARTBEAT_URL", "http://127.0.0.1:8000/workers/heartbeat"
)
CONSUMER_GROUP = os.getenv(
    "STREAMFORGE_CONSUMER_GROUP",
    os.getenv("STREAMFORGE_KAFKA_CONSUMER_GROUP", "streamforge-workers"),
)
WORKER_ID = os.getenv("STREAMFORGE_WORKER_ID", socket.gethostname())

logger = logging.getLogger("streamforge.consumer")


def deserialize_json(value):
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        logger.error("Malformed JSON message received: %s", error)
        EVENTS_FAILED.inc()
        return None


def create_consumer(group_id=CONSUMER_GROUP):
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        value_deserializer=deserialize_json,
        max_poll_records=500,
    )


def create_changelog_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
    )


def _post_json(url: str, payload: dict) -> bool:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return 200 <= response.status < 300
    except Exception as exc:
        logger.warning("POST %s failed: %s", url, exc)
        return False


def heartbeat(consumer: KafkaConsumer) -> None:
    partitions = [tp.partition for tp in consumer.assignment()]
    registry.heartbeat(WORKER_ID, socket.gethostname(), CONSUMER_GROUP, partitions)
    _post_json(
        HEARTBEAT_URL,
        {
            "worker_id": WORKER_ID,
            "host": socket.gethostname(),
            "group_id": CONSUMER_GROUP,
            "partitions": partitions,
            "failed_events": int(EVENTS_FAILED._value.get()),
            "filtered_events": int(EVENTS_FILTERED._value.get()),
            "processing_lag_seconds": max(
                0.0, PROCESSING_LAG_SECONDS._value.get()
            ),
        },
    )


def process_event(event):
    if not isinstance(event, dict):
        message = "Invalid event: expected a JSON object"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False

    device_id = event.get("truck_id") or event.get("device_id")
    temperature = event.get("temperature")
    humidity = event.get("humidity")
    timestamp = event.get("timestamp")

    if not device_id:
        message = "Invalid telemetry event: missing device_id (truck_id is also accepted)"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False
    if temperature is None:
        message = "Invalid telemetry event: missing temperature"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False
    if not timestamp:
        message = "Invalid telemetry event: missing timestamp"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False

    try:
        parsed_event_time = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
            if isinstance(timestamp, str)
            else timestamp
        )
    except (TypeError, ValueError):
        message = "Invalid telemetry event: timestamp must be ISO-8601"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False
    if parsed_event_time.tzinfo is None or parsed_event_time.utcoffset() is None:
        message = "Invalid telemetry event: timestamp must include a timezone"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False
    timestamp = parsed_event_time.astimezone(timezone.utc).isoformat()

    try:
        temperature = float(temperature)
    except (TypeError, ValueError):
        message = "Invalid telemetry event: temperature must be numeric"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False

    if humidity is not None:
        try:
            humidity = float(humidity)
        except (TypeError, ValueError):
            message = "Invalid telemetry event: humidity must be numeric"
            logger.error(message)
            print(message)
            EVENTS_FAILED.inc()
            return False

    if temperature < -50 or temperature > 100:
        message = "Invalid telemetry event: temperature out of range"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False
    if humidity is not None and (humidity < 0 or humidity > 100):
        message = "Invalid telemetry event: humidity out of range"
        logger.error(message)
        print(message)
        EVENTS_FAILED.inc()
        return False

    if temperature <= 0:
        EVENTS_FILTERED.inc()
        print(f"Filtered | {device_id} | Temperature must be above 0C")
        return True

    backend_event = {
        "truck_id": device_id,
        "temperature": temperature,
        "timestamp": timestamp,
    }

    started = time.perf_counter()
    try:
        if not _post_json(BACKEND_URL, backend_event):
            raise RuntimeError("backend rejected telemetry")

        EVENTS_PROCESSED.inc()
        PROCESSING_SECONDS.observe(time.perf_counter() - started)
        print(
            f"Processed | {device_id} | Temp: {temperature}C | "
            f"Time: {timestamp} | Backend: OK"
        )

        PROCESSING_LAG_SECONDS.set(
            max(
                0.0,
                (datetime.now(timezone.utc) - parsed_event_time.astimezone(timezone.utc)).total_seconds(),
            )
        )

        return True
    except Exception as exc:
        EVENTS_FAILED.inc()
        logger.error("Backend error for %s: %s", device_id, exc)
        print(f"Backend error for {device_id}: {exc}")
        return False


def main():
    print("====================================")
    print(" StreamForge Distributed Worker")
    print("====================================")
    print(f"Worker ID     : {WORKER_ID}")
    print(f"Consumer Group: {CONSUMER_GROUP}")
    print(f"Backend URL   : {BACKEND_URL}")
    print(f"Kafka Broker  : {KAFKA_BROKER}")
    print(f"Kafka Topic   : {KAFKA_TOPIC}")

    consumer = create_consumer()
    changelog = create_changelog_producer()
    last_heartbeat = 0.0

    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=500)
            now = time.time()
            if now - last_heartbeat >= 5:
                heartbeat(consumer)
                last_heartbeat = now

            for topic_partition, messages in records.items():
                partition_succeeded = True
                last_offset = None

                for message in messages:
                    if message.value is None:
                        continue

                    if not process_event(message.value):
                        partition_succeeded = False
                        logger.warning(
                            "Event not committed; Kafka will redeliver "
                            "partition=%s offset=%s",
                            message.partition,
                            message.offset,
                        )
                        break

                    if float(message.value.get("temperature", 0)) > 0:
                        truck_id = message.value.get("truck_id") or message.value.get("device_id", "")
                        changelog.send(
                            CHANGELOG_TOPIC,
                            key=str(truck_id).encode(),
                            value={
                                "worker_id": WORKER_ID,
                                "topic": message.topic,
                                "partition": message.partition,
                                "offset": message.offset,
                                "event": message.value,
                            },
                        )
                    last_offset = message.offset

                if partition_succeeded and last_offset is not None:
                    changelog.flush()
                    consumer.commit(
                        offsets={
                            topic_partition: OffsetAndMetadata(last_offset + 1, None),
                        }
                    )
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    finally:
        changelog.close()
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    main()
