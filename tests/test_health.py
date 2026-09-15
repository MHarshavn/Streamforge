"""Tests for the /health endpoint."""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_returns_200(client: AsyncClient) -> None:
    """Health endpoint returns 200 with healthy status."""
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["service"] == "streamforge-api"


@pytest.mark.asyncio
async def test_health_response_schema(client: AsyncClient) -> None:
    """Health response contains all required fields."""
    resp = await client.get("/health")
    data = resp.json()
    assert "status" in data
    assert "service" in data
    assert "version" in data
    assert "uptime_seconds" in data
    assert "state_backend" in data
    assert "components" in data
    assert isinstance(data["components"], list)
    assert data["uptime_seconds"] >= 0


@pytest.mark.asyncio
async def test_health_reports_state_backend(client: AsyncClient) -> None:
    """Health response correctly reports the in_memory backend."""
    resp = await client.get("/health")
    data = resp.json()
    assert data["state_backend"] == "in_memory"


@pytest.mark.asyncio
async def test_health_components_include_state_store(client: AsyncClient) -> None:
    """Components list includes the state_store with healthy status."""
    resp = await client.get("/health")
    components = resp.json()["components"]
    store_component = next(
        (c for c in components if c["name"] == "state_store"), None
    )
    assert store_component is not None
    assert store_component["status"] == "healthy"


@pytest.mark.asyncio
async def test_health_kafka_configured(client: AsyncClient) -> None:
    """Kafka component reports the configured distributed topology."""
    resp = await client.get("/health")
    components = resp.json()["components"]
    kafka = next((c for c in components if c["name"] == "kafka"), None)
    assert kafka is not None
    assert kafka["status"] == "configured"
