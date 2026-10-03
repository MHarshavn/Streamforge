"""Abstract state-store contract and InMemoryStateStore implementation.

The :class:`BaseStateStore` defines the interface that all state backends must
implement.  The API layer depends **only** on this abstraction so that concrete
backends (RocksDB, Kafka changelog, etc.) can be swapped via configuration
without touching any route code.

The :class:`InMemoryStateStore` ships as the default development / testing
backend.  It is *not* intended for production use — it will be replaced by a
persistent backend once the Kafka + RocksDB layer is implemented.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from typing import NamedTuple

from streamforge.schemas.telemetry import TelemetryEvent, TruckTelemetryState

logger = logging.getLogger("streamforge.state_store")


# ── Abstract Contract ────────────────────────────────────────────────────────


class BaseStateStore(ABC):
    """Interface that every state-store backend must fulfil."""

    @abstractmethod
    async def store_telemetry(self, event: TelemetryEvent) -> None:
        """Persist a single telemetry event and update rolling aggregations."""

    @abstractmethod
    async def get_truck_state(self, truck_id: str) -> TruckTelemetryState | None:
        """Return the current processed state for *truck_id*, or ``None``."""

    @abstractmethod
    async def list_trucks(
        self, *, limit: int = 50, offset: int = 0
    ) -> tuple[list[TruckTelemetryState], int]:
        """Return a page of truck states and the total count of tracked trucks."""

    @abstractmethod
    async def health_check(self) -> bool:
        """Return ``True`` if the backend is operational."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources held by the backend."""

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """Human-readable name of this backend (e.g. ``'in_memory'``)."""


# ── In-Memory Implementation ────────────────────────────────────────────────


class _Reading(NamedTuple):
    """A single telemetry reading stored alongside its event timestamp."""

    temperature: float
    humidity: float | None
    event_time: datetime


class InMemoryStateStore(BaseStateStore):
    """Development / testing state store backed by plain Python data structures.

    **Rolling-window semantics**

    For every truck the store keeps an ordered list of ``(temperature,
    event_time)`` readings.  When a new reading arrives the window is
    *pruned* — all readings whose ``event_time`` is older than
    ``window_size_seconds`` relative to the **latest** event timestamp
    for that truck are discarded.  The rolling average is then
    calculated over the remaining readings in the window.

    This mirrors the event-time tumbling / hopping window model that
    Faust or Bytewax would use in production.
    """

    def __init__(self, window_size_seconds: int = 300) -> None:
        self._window_size = window_size_seconds
        # truck_id → list of readings, kept sorted by event_time
        self._readings: dict[str, list[_Reading]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._healthy = True

    # ── BaseStateStore interface ─────────────────────────────────────────

    @property
    def backend_name(self) -> str:
        return "in_memory"

    async def store_telemetry(self, event: TelemetryEvent) -> None:
        async with self._lock:
            readings = self._readings[event.truck_id]
            readings.append(
            _Reading(
                temperature=event.temperature,
                humidity=event.humidity,
                event_time=event.timestamp,
                )
            )
            # Keep sorted by event time so pruning is efficient
            readings.sort(key=lambda r: r.event_time)
            # Prune readings outside the window relative to latest event time
            self._prune(readings)

        logger.debug(
            "Stored telemetry: truck=%s temp=%.2f ts=%s",
            event.truck_id,
            event.temperature,
            event.timestamp.isoformat(),
        )

    async def get_truck_state(self, truck_id: str) -> TruckTelemetryState | None:
        async with self._lock:
            readings = self._readings.get(truck_id)
            if not readings:
                return None
            # Re-prune in case time has advanced since last write
            self._prune(readings)
            if not readings:
                # All readings have been pruned out of the window
                return None
            return self._build_state(truck_id, readings)

    async def list_trucks(
        self, *, limit: int = 50, offset: int = 0
    ) -> tuple[list[TruckTelemetryState], int]:
        async with self._lock:
            truck_ids = sorted(self._readings.keys())
            total = len(truck_ids)
            page_ids = truck_ids[offset : offset + limit]
            states: list[TruckTelemetryState] = []
            for tid in page_ids:
                readings = self._readings[tid]
                self._prune(readings)
                if readings:
                    states.append(self._build_state(tid, readings))
            return states, total

    async def health_check(self) -> bool:
        return self._healthy

    async def close(self) -> None:
        async with self._lock:
            self._readings.clear()
        logger.info("InMemoryStateStore closed and cleared.")

    # ── Internal helpers ─────────────────────────────────────────────────

    def _prune(self, readings: list[_Reading]) -> None:
        """Remove readings outside the window relative to the latest event time."""
        if not readings:
            return
        latest_time = readings[-1].event_time
        from datetime import timedelta

        cutoff = latest_time - timedelta(seconds=self._window_size)
        # Remove readings older than the cutoff (in-place)
        while readings and readings[0].event_time < cutoff:
            readings.pop(0)

    @staticmethod
    def _build_state(
        truck_id: str, readings: list[_Reading]
    ) -> TruckTelemetryState:
        """Build a :class:`TruckTelemetryState` from the current readings list."""
        latest = readings[-1]
        avg = sum(r.temperature for r in readings) / len(readings)
        return TruckTelemetryState(
            truck_id=truck_id,
            latest_temperature=latest.temperature,
            latest_humidity=latest.humidity,
            rolling_avg_temperature=round(avg, 4),
            window_start=readings[0].event_time,
            window_end=latest.event_time,
            reading_count=len(readings),
            last_updated=latest.event_time,
        )

    # ── Testing helpers (not part of the public contract) ────────────────

    def set_healthy(self, healthy: bool) -> None:
        """Allow tests to simulate backend failures."""
        self._healthy = healthy
