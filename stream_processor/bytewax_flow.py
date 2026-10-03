"""Bytewax dataflow for event-time temperature windows.

Run with:
    python -m bytewax.run stream_processor.bytewax_flow:flow -w 20

This is the high-throughput stream-processing path. The existing
kafka-python worker remains useful for local debugging and API integration.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.operators import windowing
from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import EventClock, SlidingWindower

BROKERS = os.getenv(
    "STREAMFORGE_KAFKA_BROKER",
    os.getenv("STREAMFORGE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
).split(",")
INPUT_TOPIC = os.getenv(
    "STREAMFORGE_KAFKA_TOPIC",
    os.getenv("STREAMFORGE_KAFKA_TELEMETRY_TOPIC", "telemetry-events"),
)
OUTPUT_TOPIC = os.getenv("STREAMFORGE_KAFKA_WINDOW_TOPIC", "streamforge-windowed")


def decode(message):
    return json.loads(message.value.decode("utf-8"))


def event_time(event):
    return datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))


def has_positive_temperature(event):
    try:
        return float(event["temperature"]) > 0
    except (KeyError, TypeError, ValueError):
        return False


def add(acc, event):
    acc["sum"] += float(event["temperature"])
    acc["count"] += 1
    acc["latest"] = event
    return acc


def merge(left, right):
    latest = max(left["latest"], right["latest"], key=lambda item: event_time(item))
    return {
        "sum": left["sum"] + right["sum"],
        "count": left["count"] + right["count"],
        "latest": latest,
    }


def to_sink(item):
    truck_id, (window_id, acc) = item
    payload = {
        "truck_id": truck_id,
        "window_id": window_id,
        "rolling_avg_temperature": round(acc["sum"] / acc["count"], 4),
        "reading_count": acc["count"],
        "latest": acc["latest"],
    }
    return KafkaSinkMessage(
        truck_id.encode("utf-8"),
        json.dumps(payload).encode("utf-8"),
    )


flow = Dataflow("streamforge-temperature")
incoming = kop.input(
    "kafka-in",
    flow,
    brokers=BROKERS,
    topics=[INPUT_TOPIC],
    batch_size=1000,
)
events = op.map("decode-json", incoming.oks, decode)
positive_temperature = op.filter(
    "filter-positive-temperature",
    events,
    has_positive_temperature,
)
keyed = op.key_on(
    "truck-key",
    positive_temperature,
    lambda event: event.get("truck_id") or event["device_id"],
)

windowed = windowing.fold_window(
    "five-minute-event-window",
    keyed,
    EventClock(event_time, wait_for_system_duration=timedelta(seconds=10)),
    SlidingWindower(
        length=timedelta(minutes=5),
        offset=timedelta(seconds=10),
        align_to=datetime(2026, 1, 1, tzinfo=timezone.utc),
    ),
    builder=lambda: {"sum": 0.0, "count": 0, "latest": None},
    folder=add,
    merger=merge,
)
output = op.map("format-window", windowed.down, to_sink)
kop.output("kafka-window-output", output, brokers=BROKERS, topic=OUTPUT_TOPIC)
