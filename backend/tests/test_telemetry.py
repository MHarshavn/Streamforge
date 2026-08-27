"""Tests for the /telemetry endpoints — ingestion, retrieval, and validation."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.conftest import (
    VALID_EVENT,
    VALID_EVENT_2,
    VALID_EVENT_3,
    VALID_EVENT_OLD,
    VALID_EVENT_OTHER_TRUCK,
)


# ═══════════════════════════════════════════════════════════════════════════
# Single-event ingestion
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_ingest_valid_event(client: AsyncClient) -> None:
    """A valid telemetry event is accepted with 201."""
    resp = await client.post("/telemetry/ingest", json=VALID_EVENT)
    assert resp.status_code == 201
    data = resp.json()
    assert data["accepted"] == 1
    assert "accepted" in data["message"]


@pytest.mark.asyncio
async def test_ingest_response_schema(client: AsyncClient) -> None:
    """Ingest response matches TelemetryIngestResponse schema."""
    resp = await client.post("/telemetry/ingest", json=VALID_EVENT)
    data = resp.json()
    assert isinstance(data["accepted"], int)
    assert isinstance(data["message"], str)


# ═══════════════════════════════════════════════════════════════════════════
# Batch ingestion
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_ingest_batch_valid(client: AsyncClient) -> None:
    """A valid batch of events is accepted with 201."""
    resp = await client.post(
        "/telemetry/ingest/batch",
        json={"events": [VALID_EVENT, VALID_EVENT_2]},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["accepted"] == 2


@pytest.mark.asyncio
async def test_ingest_batch_empty_fails(client: AsyncClient) -> None:
    """An empty batch fails validation with 422."""
    resp = await client.post(
        "/telemetry/ingest/batch",
        json={"events": []},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_batch_invalid_event_inside(client: AsyncClient) -> None:
    """A batch containing an invalid event fails with 422."""
    bad_event = {**VALID_EVENT, "temperature": 999.0}
    resp = await client.post(
        "/telemetry/ingest/batch",
        json={"events": [VALID_EVENT, bad_event]},
    )
    assert resp.status_code == 422


# ═══════════════════════════════════════════════════════════════════════════
# Validation — invalid requests
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_ingest_missing_truck_id(client: AsyncClient) -> None:
    """Missing truck_id field returns 422."""
    event = {"temperature": 22.5, "timestamp": "2026-08-25T12:00:00Z"}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_empty_truck_id(client: AsyncClient) -> None:
    """Empty truck_id returns 422."""
    event = {**VALID_EVENT, "truck_id": ""}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_whitespace_truck_id(client: AsyncClient) -> None:
    """Whitespace-only truck_id returns 422."""
    event = {**VALID_EVENT, "truck_id": "   "}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_truck_id_too_long(client: AsyncClient) -> None:
    """truck_id exceeding 50 characters returns 422."""
    event = {**VALID_EVENT, "truck_id": "T" * 51}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_temperature_too_low(client: AsyncClient) -> None:
    """Temperature below -80 returns 422."""
    event = {**VALID_EVENT, "temperature": -81.0}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_temperature_too_high(client: AsyncClient) -> None:
    """Temperature above 80 returns 422."""
    event = {**VALID_EVENT, "temperature": 81.0}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_invalid_temperature_type(client: AsyncClient) -> None:
    """Non-numeric temperature returns 422."""
    event = {**VALID_EVENT, "temperature": "hot"}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_invalid_timestamp(client: AsyncClient) -> None:
    """Malformed timestamp returns 422."""
    event = {**VALID_EVENT, "timestamp": "not-a-date"}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_missing_timestamp(client: AsyncClient) -> None:
    """Missing timestamp field returns 422."""
    event = {"truck_id": "TRUCK-00001", "temperature": 22.5}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_missing_temperature(client: AsyncClient) -> None:
    """Missing temperature field returns 422."""
    event = {"truck_id": "TRUCK-00001", "timestamp": "2026-08-25T12:00:00Z"}
    resp = await client.post("/telemetry/ingest", json=event)
    assert resp.status_code == 422


# ═══════════════════════════════════════════════════════════════════════════
# Retrieval — single truck
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_get_truck_state_after_ingest(client: AsyncClient) -> None:
    """After ingesting an event, the truck state is retrievable."""
    await client.post("/telemetry/ingest", json=VALID_EVENT)
    resp = await client.get(f"/telemetry/{VALID_EVENT['truck_id']}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["truck_id"] == VALID_EVENT["truck_id"]
    assert data["latest_temperature"] == VALID_EVENT["temperature"]
    assert data["reading_count"] == 1


@pytest.mark.asyncio
async def test_get_truck_state_response_schema(client: AsyncClient) -> None:
    """Truck state response has all required fields."""
    await client.post("/telemetry/ingest", json=VALID_EVENT)
    resp = await client.get(f"/telemetry/{VALID_EVENT['truck_id']}")
    data = resp.json()
    required_fields = {
        "truck_id",
        "latest_temperature",
        "rolling_avg_temperature",
        "window_start",
        "window_end",
        "reading_count",
        "last_updated",
    }
    assert required_fields.issubset(data.keys())


@pytest.mark.asyncio
async def test_get_unknown_truck_returns_404(client: AsyncClient) -> None:
    """Requesting an unknown truck returns 404."""
    resp = await client.get("/telemetry/NONEXISTENT")
    assert resp.status_code == 404
    data = resp.json()
    assert "detail" in data


# ═══════════════════════════════════════════════════════════════════════════
# Rolling average — 5-minute event-time window
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_rolling_avg_with_single_reading(client: AsyncClient) -> None:
    """Rolling average equals the single reading when only one exists."""
    await client.post("/telemetry/ingest", json=VALID_EVENT)
    resp = await client.get(f"/telemetry/{VALID_EVENT['truck_id']}")
    data = resp.json()
    assert data["rolling_avg_temperature"] == VALID_EVENT["temperature"]


@pytest.mark.asyncio
async def test_rolling_avg_multiple_readings_in_window(
    client: AsyncClient,
) -> None:
    """Rolling average is correct over multiple readings within the window."""
    for event in [VALID_EVENT, VALID_EVENT_2, VALID_EVENT_3]:
        await client.post("/telemetry/ingest", json=event)

    resp = await client.get(f"/telemetry/{VALID_EVENT['truck_id']}")
    data = resp.json()
    # (22.5 + 24.0 + 26.0) / 3 = 24.1667
    expected_avg = round((22.5 + 24.0 + 26.0) / 3, 4)
    assert data["rolling_avg_temperature"] == expected_avg
    assert data["reading_count"] == 3


@pytest.mark.asyncio
async def test_rolling_avg_excludes_old_readings(client: AsyncClient) -> None:
    """Readings outside the 5-minute window are excluded from the average.

    Timeline:
      11:58:00 → 20.0°C  (outside 5-min window of 12:04)
      12:00:00 → 22.5°C  (inside)
      12:02:00 → 24.0°C  (inside)
      12:04:00 → 26.0°C  (inside — latest)

    The 11:58 reading is > 5 minutes before 12:04 (cutoff = 11:59:00),
    BUT actually 12:04 - 11:58 = 6 minutes, so it IS outside. Let's
    verify the window is [11:59:00, 12:04:00].

    Expected avg = (22.5 + 24.0 + 26.0) / 3 = 24.1667
    """
    # Ingest in chronological order
    for event in [VALID_EVENT_OLD, VALID_EVENT, VALID_EVENT_2, VALID_EVENT_3]:
        await client.post("/telemetry/ingest", json=event)

    resp = await client.get(f"/telemetry/{VALID_EVENT['truck_id']}")
    data = resp.json()

    # The 11:58 reading should be pruned (6 min before latest 12:04)
    expected_avg = round((22.5 + 24.0 + 26.0) / 3, 4)
    assert data["rolling_avg_temperature"] == expected_avg
    assert data["reading_count"] == 3
    assert data["latest_temperature"] == 26.0


# ═══════════════════════════════════════════════════════════════════════════
# Retrieval — list trucks
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_list_trucks_empty(client: AsyncClient) -> None:
    """Listing trucks when none exist returns an empty list."""
    resp = await client.get("/telemetry/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["trucks"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_list_trucks_after_ingest(client: AsyncClient) -> None:
    """Listing trucks shows ingested trucks."""
    await client.post("/telemetry/ingest", json=VALID_EVENT)
    await client.post("/telemetry/ingest", json=VALID_EVENT_OTHER_TRUCK)
    resp = await client.get("/telemetry/")
    data = resp.json()
    assert data["total"] == 2
    assert len(data["trucks"]) == 2


@pytest.mark.asyncio
async def test_list_trucks_pagination(client: AsyncClient) -> None:
    """Pagination limit and offset work correctly."""
    await client.post("/telemetry/ingest", json=VALID_EVENT)
    await client.post("/telemetry/ingest", json=VALID_EVENT_OTHER_TRUCK)

    resp = await client.get("/telemetry/", params={"limit": 1, "offset": 0})
    data = resp.json()
    assert len(data["trucks"]) == 1
    assert data["total"] == 2
    assert data["limit"] == 1
    assert data["offset"] == 0
