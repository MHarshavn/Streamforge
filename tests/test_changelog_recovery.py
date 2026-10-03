from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from streamforge.services import changelog_recovery
from streamforge.services.rocksdb_state_store import RocksDBStateStore
from kafka.structs import TopicPartition


class FakeConsumer:
    def __init__(self, records):
        self.records = records
        self.closed = False
        self.partition = TopicPartition("state-changelog", 0)
        self.next_offset = 0

    def partitions_for_topic(self, _topic):
        return {0}

    def assign(self, _partitions):
        pass

    def beginning_offsets(self, partitions):
        return {partition: 0 for partition in partitions}

    def end_offsets(self, partitions):
        return {partition: len(self.records) for partition in partitions}

    def seek(self, _partition, offset):
        self.next_offset = offset

    def poll(self, timeout_ms):
        messages = self.records[self.next_offset :]
        self.next_offset = len(self.records)
        return {self.partition: messages} if messages else {}

    def position(self, _partition):
        return self.next_offset

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_changelog_replay_rebuilds_only_accepted_positive_readings(
    tmp_path, monkeypatch
):
    accepted = {
        "truck_id": "TRUCK-1",
        "temperature": 22.0,
        "timestamp": "2026-08-25T12:00:00Z",
    }
    ignored_temperature = {
        "truck_id": "TRUCK-1",
        "temperature": 0,
        "timestamp": "2026-08-25T12:01:00Z",
    }
    records = [
        SimpleNamespace(
            value={"event": accepted}, topic="state-changelog", partition=0, offset=1
        ),
        SimpleNamespace(
            value={"event": ignored_temperature},
            topic="state-changelog",
            partition=0,
            offset=2,
        ),
        SimpleNamespace(
            value={"unexpected": "payload"},
            topic="state-changelog",
            partition=0,
            offset=3,
        ),
    ]
    consumer = FakeConsumer(records)
    monkeypatch.setattr(
        changelog_recovery, "KafkaConsumer", lambda *args, **kwargs: consumer
    )
    store = RocksDBStateStore(str(tmp_path / "state"))

    replayed = await changelog_recovery.recover_from_changelog(
        store, "localhost:9092", "state-changelog"
    )
    state = await store.get_truck_state("TRUCK-1")

    assert replayed == 1
    assert consumer.closed
    assert state is not None
    assert state.rolling_avg_temperature == 22.0
    await store.close()


def test_producer_emits_spec_fields_and_canonical_truck_id():
    from producer.telemetry_producer import generate_event

    event = generate_event("TRUCK-00042")
    assert set(event) == {"truck_id", "temperature", "timestamp"}
    assert event["truck_id"] == "TRUCK-00042"
    assert event["temperature"] > 0
    assert datetime.fromisoformat(event["timestamp"]).tzinfo == timezone.utc
