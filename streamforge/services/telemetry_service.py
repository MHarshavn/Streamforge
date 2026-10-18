"""Telemetry business-logic service.

Sits between the API routes and the state-store abstraction.  All
domain rules (validation beyond Pydantic, orchestration, future Kafka
publishing) live here so the route handlers stay thin.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from streamforge.metrics import (
    EVENTS_PROCESSED,
    PROCESSING_LAG_SECONDS,
    PROCESSING_SECONDS,
)
from streamforge.exceptions import StateStoreUnavailableError, TruckNotFoundError
from streamforge.schemas.telemetry import (
    TelemetryEvent,
    TelemetryIngestResponse,
    TruckListResponse,
    TruckTelemetryState,
)
from streamforge.services.state_store import BaseStateStore

logger = logging.getLogger("streamforge.telemetry_service")


class TelemetryService:
    """Orchestrates telemetry ingestion and state retrieval.

    Parameters
    ----------
    state_store:
        The injected state-store backend (never a concrete class directly).
    """

    def __init__(self, state_store: BaseStateStore) -> None:
        self._store = state_store

    # ── Ingestion ────────────────────────────────────────────────────────

    async def ingest(self, event: TelemetryEvent) -> TelemetryIngestResponse:
        """Ingest a single telemetry event.

        In the future this method will also publish to Kafka; for now it
        writes directly to the state store.
        """
        started = time.perf_counter()
        await self._ensure_store_healthy()
        await self._store.store_telemetry(event)
        EVENTS_PROCESSED.inc()
        PROCESSING_SECONDS.observe(time.perf_counter() - started)
        PROCESSING_LAG_SECONDS.set(
            max(
                0.0,
                (datetime.now(timezone.utc) - event.timestamp.astimezone(timezone.utc)).total_seconds(),
            )
        )
        logger.info(
            "Ingested telemetry: truck=%s temp=%.2f",
            event.truck_id,
            event.temperature,
        )
        return TelemetryIngestResponse(accepted=1, message="1 event(s) accepted")

    async def ingest_batch(
        self, events: list[TelemetryEvent]
    ) -> TelemetryIngestResponse:
        """Ingest a batch of telemetry events.

        Re-uses :meth:`store_telemetry` per event — no duplicated logic.
        """
        await self._ensure_store_healthy()
        for event in events:
            started = time.perf_counter()
            await self._store.store_telemetry(event)
            PROCESSING_SECONDS.observe(time.perf_counter() - started)
        EVENTS_PROCESSED.inc(len(events))
        latest_event_time = max(event.timestamp for event in events).astimezone(timezone.utc)
        PROCESSING_LAG_SECONDS.set(
            max(0.0, (datetime.now(timezone.utc) - latest_event_time).total_seconds())
        )
        count = len(events)
        logger.info("Ingested batch of %d telemetry event(s)", count)
        return TelemetryIngestResponse(
            accepted=count, message=f"{count} event(s) accepted"
        )

    # ── Retrieval ────────────────────────────────────────────────────────

    async def get_truck_state(self, truck_id: str) -> TruckTelemetryState:
        """Return the processed telemetry state for a single truck.

        Raises :class:`TruckNotFoundError` if no data exists.
        """
        await self._ensure_store_healthy()
        state = await self._store.get_truck_state(truck_id)
        if state is None:
            raise TruckNotFoundError(truck_id)
        return state

    async def list_trucks(
        self, *, limit: int = 50, offset: int = 0
    ) -> TruckListResponse:
        """Return a paginated list of all tracked trucks."""
        await self._ensure_store_healthy()
        trucks, total = await self._store.list_trucks(limit=limit, offset=offset)
        return TruckListResponse(
            trucks=trucks, total=total, limit=limit, offset=offset
        )

    # ── Internal ─────────────────────────────────────────────────────────

    async def _ensure_store_healthy(self) -> None:
        """Guard that raises :class:`StateStoreUnavailableError` when the
        backend reports itself as unhealthy."""
        if not await self._store.health_check():
            raise StateStoreUnavailableError(self._store.backend_name)
