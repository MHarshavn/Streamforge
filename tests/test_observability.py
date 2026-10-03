import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_metrics_endpoint(client: AsyncClient):
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert "streamforge_events_processed_total" in response.text


@pytest.mark.asyncio
async def test_workers_endpoint(client: AsyncClient):
    response = await client.get("/workers")
    assert response.status_code == 200
    assert "workers" in response.json()


@pytest.mark.asyncio
async def test_topology_endpoint(client: AsyncClient):
    response = await client.get("/topology")
    assert response.status_code == 200
    data = response.json()
    assert "nodes" in data
    assert "edges" in data
    assert "events_per_second" in data
    assert "processing_lag_seconds" in data


@pytest.mark.asyncio
async def test_topology_ui_cors_preflight(client: AsyncClient):
    response = await client.options(
        "/topology",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"


@pytest.mark.asyncio
async def test_worker_heartbeat(client: AsyncClient):
    response = await client.post(
        "/workers/heartbeat",
        json={
            "worker_id": "test-worker",
            "host": "localhost",
            "group_id": "streamforge-workers",
            "partitions": [0, 1],
        },
    )
    assert response.status_code == 204

    workers = (await client.get("/workers")).json()
    assert any(worker["worker_id"] == "test-worker" for worker in workers["workers"])
