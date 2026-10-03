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


@pytest.mark.asyncio
async def test_rocksdb_duplicate_delivery_is_idempotent(tmp_path):
    store = RocksDBStateStore(str(tmp_path / "state"))
    event = TelemetryEvent(
        truck_id="TRUCK-1",
        temperature=20.0,
        humidity=50.0,
        timestamp=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
    )
    await store.store_telemetry(event)
    await store.store_telemetry(event)
    state = await store.get_truck_state("TRUCK-1")
    assert state is not None
    assert state.reading_count == 1
    await store.close()


@pytest.mark.asyncio
async def test_rocksdb_orders_mixed_timezone_offsets_by_event_time(tmp_path):
    store = RocksDBStateStore(str(tmp_path / "state"), window_size_seconds=300)
    later = TelemetryEvent(
        truck_id="TRUCK-1", temperature=30.0, humidity=50.0,
        timestamp=datetime.fromisoformat("2026-08-25T12:00:00+00:00"),
    )
    earlier = TelemetryEvent(
        truck_id="TRUCK-1", temperature=20.0, humidity=50.0,
        timestamp=datetime.fromisoformat("2026-08-25T13:59:00+02:00"),
    )
    await store.store_telemetry(later)
    await store.store_telemetry(earlier)
    state = await store.get_truck_state("TRUCK-1")
    assert state is not None
    assert state.latest_temperature == 30.0
    assert state.reading_count == 2
    await store.close()


def test_api_defaults_to_rocksdb_and_recovers_after_restart(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from streamforge.config import Settings, StateBackend
    from streamforge.main import create_app

    monkeypatch.delenv("STREAMFORGE_STATE_BACKEND", raising=False)
    monkeypatch.setenv("STREAMFORGE_ROCKSDB_PATH", str(tmp_path / "api-state"))
    monkeypatch.setenv("STREAMFORGE_RECOVER_STATE_FROM_CHANGELOG", "false")
    assert Settings(_env_file=None).state_backend == StateBackend.ROCKSDB

    event = {
        "truck_id": "TRUCK-RECOVERY",
        "temperature": 28.5,
        "humidity": 55.0,
        "timestamp": "2026-08-25T12:00:00Z",
    }
    first_app = create_app()
    with TestClient(first_app) as client:
        assert client.get("/health").json()["state_backend"] == "rocksdb"
        assert client.post("/telemetry/ingest", json=event).status_code == 201

    restarted_app = create_app()
    with TestClient(restarted_app) as client:
        state = client.get("/telemetry/TRUCK-RECOVERY")
        assert state.status_code == 200
        assert state.json()["rolling_avg_temperature"] == 28.5
