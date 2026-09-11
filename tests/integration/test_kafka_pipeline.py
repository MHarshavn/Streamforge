import json
import time
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = "localhost:9092"
TEST_TOPIC = "streamforge-test"


@pytest.fixture
def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
    )

    yield producer

    producer.close()


def test_producer_to_consumer_message_flow(kafka_producer):
    unique_group = f"streamforge-test-{uuid.uuid4()}"

    consumer = KafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=unique_group,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    try:
        # Wait for Kafka to assign partitions to the consumer.
        deadline = time.time() + 10

        while not consumer.assignment() and time.time() < deadline:
            consumer.poll(timeout_ms=500)

        assert consumer.assignment(), "Kafka consumer did not receive a partition assignment"

        event = {
            "device_id": f"integration_test_{uuid.uuid4()}",
            "temperature": 25.5,
            "humidity": 60.0,
            "timestamp": "2026-08-25T00:00:00+00:00",
        }

        kafka_producer.send(
            TEST_TOPIC,
            key=event["device_id"],
            value=event,
        )
        kafka_producer.flush()

        received_event = None
        deadline = time.time() + 10

        while time.time() < deadline:
            records = consumer.poll(timeout_ms=1000)

            for messages in records.values():
                for message in messages:
                    if message.value.get("device_id") == event["device_id"]:
                        received_event = message.value
                        break

                if received_event:
                    break

            if received_event:
                break

        assert received_event is not None
        assert received_event["device_id"] == event["device_id"]
        assert received_event["temperature"] == event["temperature"]
        assert received_event["humidity"] == event["humidity"]
        assert received_event["timestamp"] == event["timestamp"]

    finally:
        consumer.close()


def test_invalid_telemetry_is_detected():
    from stream_processor.consumer import process_event

    invalid_event = {
        "device_id": "invalid_device",
        "temperature": 200,
        "humidity": 50,
        "timestamp": "2026-08-25T00:00:00+00:00",
    }

    result = process_event(invalid_event)

    assert result is False