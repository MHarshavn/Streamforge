"""Tests for error handling — state-store failures and application resilience."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from streamforge.services.state_store import InMemoryStateStore
from tests.conftest import VALID_EVENT


@pytest.mark.asyncio
async def test_state_store_unavailable_ingest(
    client: AsyncClient, state_store: InMemoryStateStore
) -> None:
    """Ingestion returns 503 when the state store is unhealthy."""
    state_store.set_healthy(False)
    resp = await client.post("/telemetry/ingest", json=VALID_EVENT)
    assert resp.status_code == 503
    data = resp.json()
    assert "unavailable" in data["detail"].lower()


@pytest.mark.asyncio
async def test_state_store_unavailable_get_truck(
    client: AsyncClient, state_store: InMemoryStateStore
) -> None:
    """Truck retrieval returns 503 when the state store is unhealthy."""
    state_store.set_healthy(False)
    resp = await client.get("/telemetry/TRUCK-00001")
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_state_store_unavailable_list_trucks(
    client: AsyncClient, state_store: InMemoryStateStore
) -> None:
    """Truck listing returns 503 when the state store is unhealthy."""
    state_store.set_healthy(False)
    resp = await client.get("/telemetry/")
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_state_store_unavailable_batch(
    client: AsyncClient, state_store: InMemoryStateStore
) -> None:
    """Batch ingestion returns 503 when the state store is unhealthy."""
    state_store.set_healthy(False)
    resp = await client.post(
        "/telemetry/ingest/batch",
        json={"events": [VALID_EVENT]},
    )
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_404_response_includes_detail(client: AsyncClient) -> None:
    """404 responses include a human-readable detail message."""
    resp = await client.get("/telemetry/DOES-NOT-EXIST")
    assert resp.status_code == 404
    data = resp.json()
    assert "detail" in data
    assert len(data["detail"]) > 0


@pytest.mark.asyncio
async def test_422_response_on_malformed_json(client: AsyncClient) -> None:
    """Completely malformed body returns 422."""
    resp = await client.post(
        "/telemetry/ingest",
        content=b"not json at all",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_application_imports() -> None:
    """The main application module imports without error."""
    from streamforge.main import app  # noqa: F401

    assert app is not None


@pytest.mark.asyncio
async def test_application_creates_successfully() -> None:
    """create_app returns a valid FastAPI instance."""
    from fastapi import FastAPI
    from streamforge.main import create_app

    application = create_app()
    assert isinstance(application, FastAPI)
