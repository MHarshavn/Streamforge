import time
import uuid

from producer.telemetry_producer import create_producer
from stream_processor.consumer import create_consumer


def test_producer_kafka_consumer_flow():
    producer = None
    consumer = None

    device_id = f"integration-{uuid.uuid4()}"

    event = {
        "device_id": device_id,
        "temperature": 25.5,
        "humidity": 60.0,
        "timestamp": "2026-08-25T00:00:00+00:00",
    }

    try:
        consumer = create_consumer()

        # Allow the consumer to connect and receive its Kafka partition.
        consumer.poll(timeout_ms=1000)

        producer = create_producer()

        future = producer.send(
            "telemetry-events",
            key=device_id,
            value=event,
        )

        future.get(timeout=10)
        producer.flush()

        received_event = None
        deadline = time.time() + 10

        while time.time() < deadline and received_event is None:
            records = consumer.poll(timeout_ms=1000)

            for messages in records.values():
                for message in messages:
                    if ( 
                        isinstance(message.value, dict)
                        and message.value.get("device_id") == device_id
                ):
                        received_event = message.value
                        break

        assert received_event is not None
        assert received_event["device_id"] == device_id
        assert received_event["temperature"] == 25.5
        assert received_event["humidity"] == 60.0



    finally:
        if producer is not None:
            producer.close()

        if consumer is not None:
            consumer.close()