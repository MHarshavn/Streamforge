from kafka import KafkaConsumer
import json
import streamlit as st


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetry-events"


@st.cache_resource
def create_dashboard_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="streamforge-dashboard",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )


def get_recent_events(max_events=30):
    consumer = create_dashboard_consumer()

    events = []

    records = consumer.poll(
        timeout_ms=1000,
        max_records=max_events
    )

    for messages in records.values():
        for message in messages:
            events.append(message.value)

    return events
