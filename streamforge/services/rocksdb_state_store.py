"""Persistent RocksDB state store used for worker recovery."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import hashlib

from rocksdict import Rdict

from streamforge.schemas.telemetry import TelemetryEvent, TruckTelemetryState
from streamforge.services.state_store import BaseStateStore


class RocksDBStateStore(BaseStateStore):
    """Durable event-time state backed by RocksDB."""

    def __init__(self, path: str, window_size_seconds: int = 300) -> None:
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)
        self._window_size = window_size_seconds
        self._db = Rdict(str(self._path))
        self._lock = asyncio.Lock()
        self._healthy = True

    @property
    def backend_name(self) -> str:
        return "rocksdb"

    async def store_telemetry(self, event: TelemetryEvent) -> None:
        async with self._lock:
            readings = list(self._db.get(event.truck_id, []))
            event_id = hashlib.sha256(
                f"{event.truck_id}|{event.timestamp.isoformat()}|{event.temperature}|{event.humidity}".encode()
            ).hexdigest()
            if any(item.get("event_id") == event_id for item in readings):
                return
            readings.append({
                "temperature": event.temperature,
                "humidity": event.humidity,
                "event_time": event.timestamp.isoformat(),
                "event_id": event_id,
            })
            readings.sort(key=lambda item: item["event_time"])
            latest = datetime.fromisoformat(readings[-1]["event_time"])
            cutoff = latest - timedelta(seconds=self._window_size)
            readings = [
                item for item in readings
                if datetime.fromisoformat(item["event_time"]) >= cutoff
            ]
            self._db[event.truck_id] = readings
            self._db.flush()

    async def get_truck_state(self, truck_id: str) -> TruckTelemetryState | None:
        async with self._lock:
            readings = list(self._db.get(truck_id, []))
            return self._build_state(truck_id, readings) if readings else None

    async def list_trucks(
        self, *, limit: int = 50, offset: int = 0
    ) -> tuple[list[TruckTelemetryState], int]:
        async with self._lock:
            truck_ids = sorted(str(key) for key in self._db.keys())
            total = len(truck_ids)
            states = []
            for truck_id in truck_ids[offset:offset + limit]:
                readings = list(self._db.get(truck_id, []))
                if readings:
                    states.append(self._build_state(truck_id, readings))
            return states, total

    async def health_check(self) -> bool:
        return self._healthy

    async def close(self) -> None:
        async with self._lock:
            self._db.flush()
            self._db.close()
        self._healthy = False

    def set_healthy(self, healthy: bool) -> None:
        self._healthy = healthy

    @staticmethod
    def _build_state(truck_id: str, readings: list[dict]) -> TruckTelemetryState:
        latest = readings[-1]
        window_start = datetime.fromisoformat(readings[0]["event_time"])
        window_end = datetime.fromisoformat(latest["event_time"])
        average = sum(float(item["temperature"]) for item in readings) / len(readings)
        return TruckTelemetryState(
            truck_id=truck_id,
            latest_temperature=float(latest["temperature"]),
            latest_humidity=float(latest["humidity"]),
            rolling_avg_temperature=round(average, 4),
            window_start=window_start,
            window_end=window_end,
            reading_count=len(readings),
            last_updated=window_end,
        )
