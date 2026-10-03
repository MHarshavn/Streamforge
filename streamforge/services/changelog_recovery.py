"""Rebuild RocksDB's rolling state from the Kafka worker changelog."""
from __future__ import annotations

import json
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import TopicPartition

from streamforge.schemas.telemetry import TelemetryEvent
from streamforge.services.rocksdb_state_store import RocksDBStateStore

logger = logging.getLogger("streamforge.changelog_recovery")


def _records_at_startup_snapshot(consumer: KafkaConsumer, topic: str):
    """Yield records through the offsets visible when recovery begins."""
    partition_ids = consumer.partitions_for_topic(topic) or set()
    partitions = [TopicPartition(topic, partition) for partition in partition_ids]
    if not partitions:
        return

    consumer.assign(partitions)
    beginnings = consumer.beginning_offsets(partitions)
    end_offsets = consumer.end_offsets(partitions)
    pending = set()
    for partition in partitions:
        consumer.seek(partition, beginnings[partition])
        if beginnings[partition] < end_offsets[partition]:
            pending.add(partition)

    while pending:
        records = consumer.poll(timeout_ms=1000)
        for partition, messages in records.items():
            if partition not in pending:
                continue
            for record in messages:
                if record.offset >= end_offsets[partition]:
                    break
                yield record
        for partition in tuple(pending):
            if consumer.position(partition) >= end_offsets[partition]:
                pending.remove(partition)


async def recover_from_changelog(
    store: RocksDBStateStore,
    bootstrap_servers: str,
    topic: str,
) -> int:
    """Replay retained accepted events, preserving event-time window semantics.

    The changelog consumer has no group ID and never commits offsets, so every
    API startup begins from the oldest retained record. RocksDB event IDs make
    replay safe when the local database already contains some of those events.
    If Kafka is unavailable, the durable local RocksDB state remains usable.
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[item.strip() for item in bootstrap_servers.split(",")],
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            bootstrap_timeout_ms=3000,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
    except KafkaError as error:
        logger.warning("Kafka changelog recovery skipped: %s", error)
        return 0

    recovered = 0
    try:
        for record in _records_at_startup_snapshot(consumer, topic):
            envelope = record.value
            event_data = envelope.get("event") if isinstance(envelope, dict) else None
            if not isinstance(event_data, dict):
                continue
            try:
                event = TelemetryEvent.model_validate(event_data)
            except Exception:
                logger.warning(
                    "Skipping invalid changelog record at %s[%s] offset %s",
                    record.topic,
                    record.partition,
                    record.offset,
                )
                continue
            if event.temperature <= 0:
                continue
            await store.store_telemetry(event)
            recovered += 1
    except KafkaError as error:
        logger.warning("Kafka changelog replay ended early: %s", error)
    finally:
        consumer.close()
    return recovered
