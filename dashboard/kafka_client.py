from kafka import KafkaConsumer
import json


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetry-events"


def create_dashboard_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="streamforge-dashboard",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

def get_recent_events(max_events=30):
    consumer = create_dashboard_consumer()

    events = []

    try:
        for _ in range(max_events):
            message = next(consumer)
            events.append(message.value)
    finally:
        consumer.close()

    return events