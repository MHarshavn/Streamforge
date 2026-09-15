from datetime import datetime, timezone, timedelta

import pytest

from streamforge.schemas.telemetry import TelemetryEvent
from streamforge.services.rocksdb_state_store import RocksDBStateStore


@pytest.mark.asyncio
async def test_rocksdb_state_survives_reopen(tmp_path):
    path = tmp_path / "state"
    event = TelemetryEvent(
        truck_id="TRUCK-1",
        temperature=20.0,
        humidity=50.0,
        timestamp=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
    )
    store = RocksDBStateStore(str(path))
    await store.store_telemetry(event)
    await store.close()
    recovered = RocksDBStateStore(str(path))
    state = await recovered.get_truck_state("TRUCK-1")
    assert state is not None
    assert state.rolling_avg_temperature == 20.0
    await recovered.close()


@pytest.mark.asyncio
async def test_rocksdb_event_time_window(tmp_path):
    store = RocksDBStateStore(str(tmp_path / "state"), window_size_seconds=300)
    base = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
    for minutes, temperature in [(0, 20), (2, 24), (6, 30)]:
        await store.store_telemetry(
            TelemetryEvent(
                truck_id="TRUCK-1",
                temperature=temperature,
                humidity=50,
                timestamp=base + timedelta(minutes=minutes),
            )
        )
    state = await store.get_truck_state("TRUCK-1")
    assert state is not None
    assert state.reading_count == 2
    assert state.rolling_avg_temperature == 27.0
    await store.close()
