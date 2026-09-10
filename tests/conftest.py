"""Shared pytest fixtures for the StreamForge API test suite.

All tests run against the :class:`InMemoryStateStore` — no Kafka,
RocksDB, or external infrastructure is required.
"""

from __future__ import annotations

from typing import Generator

import pytest
from httpx import ASGITransport, AsyncClient

from streamforge.api.dependencies import close_state_store, init_state_store
from streamforge.main import create_app
from streamforge.services.state_store import InMemoryStateStore


@pytest.fixture()
def state_store() -> InMemoryStateStore:
    """Provide a fresh InMemoryStateStore for each test."""
    return InMemoryStateStore(window_size_seconds=300)


@pytest.fixture()
def app(state_store: InMemoryStateStore):
    """Create a FastAPI app with the test state store wired in."""
    application = create_app()
    init_state_store(state_store)
    yield application
    close_state_store()


@pytest.fixture()
async def client(app) -> AsyncClient:
    """Provide an async HTTPX client for the test app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


# ── Reusable test data ───────────────────────────────────────────────────────

VALID_EVENT = {
    "truck_id": "TRUCK-00001",
    "temperature": 22.5,
    "humidity": 60.0,
    "timestamp": "2026-08-25T12:00:00Z",
}

VALID_EVENT_2 = {
    "truck_id": "TRUCK-00001",
    "temperature": 24.0,
    "humidity": 62.0,
    "timestamp": "2026-08-25T12:02:00Z",
}

VALID_EVENT_3 = {
    "truck_id": "TRUCK-00001",
    "temperature": 26.0,
    "humidity": 64.0,
    "timestamp": "2026-08-25T12:04:00Z",
}

VALID_EVENT_OLD = {
    "truck_id": "TRUCK-00001",
    "temperature": 20.0,
    "humidity": 55.0,
    "timestamp": "2026-08-25T11:58:00Z",
}

VALID_EVENT_OTHER_TRUCK = {
    "truck_id": "TRUCK-00002",
    "temperature": 18.0,
    "humidity": 50.0,
    "timestamp": "2026-08-25T12:00:00Z",
}