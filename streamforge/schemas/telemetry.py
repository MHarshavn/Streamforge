"""Telemetry request / response schemas with Pydantic v2 validation."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


# ── Request Schemas ──────────────────────────────────────────────────────────


class TelemetryEvent(BaseModel):
    """A single telemetry reading from a truck sensor.

    * **truck_id** – Non-empty identifier, max 50 characters.
    * **temperature** – Celsius reading in the range [-80, 80].
    * **timestamp** – Timezone-aware ISO-8601 datetime (event time).
    """

    truck_id: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Unique truck identifier",
        examples=["TRUCK-00001"],
    )
    temperature: float = Field(
        ...,
        ge=-80.0,
        le=80.0,
        description="Temperature reading in Celsius",
        examples=[22.5],
    )
    timestamp: datetime = Field(
        ...,
        description="Event timestamp in ISO-8601 format (timezone-aware)",
        examples=["2026-08-25T12:00:00Z"],
    )

    @field_validator("truck_id")
    @classmethod
    def truck_id_not_blank(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("truck_id must not be blank or whitespace-only")
        return stripped

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError(
                "timestamp must be timezone-aware (e.g. 2026-08-25T12:00:00Z)"
            )
        return v


class TelemetryBatchRequest(BaseModel):
    """Batch of telemetry events for high-throughput ingestion.

    Between 1 and 1 000 events per request.
    """

    events: list[TelemetryEvent] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="List of telemetry events (1–1000)",
    )


# ── Response Schemas ─────────────────────────────────────────────────────────


class TelemetryIngestResponse(BaseModel):
    """Acknowledgement returned after successful telemetry ingestion."""

    accepted: int = Field(..., description="Number of events accepted", ge=0)
    message: str = Field(
        ...,
        description="Human-readable status message",
        examples=["1 event(s) accepted"],
    )


class TruckTelemetryState(BaseModel):
    """Processed telemetry state for a single truck.

    Contains the latest reading and the 5-minute rolling-average
    temperature calculated over event-time windows.
    """

    truck_id: str = Field(..., description="Truck identifier")
    latest_temperature: float = Field(
        ..., description="Most recent temperature reading (°C)"
    )
    rolling_avg_temperature: float | None = Field(
        None,
        description="5-minute rolling average temperature (°C), null if insufficient data",
    )
    window_start: datetime | None = Field(
        None, description="Start of the current rolling window (event time)"
    )
    window_end: datetime | None = Field(
        None, description="End of the current rolling window (event time)"
    )
    reading_count: int = Field(
        ..., description="Number of readings in the current window", ge=0
    )
    last_updated: datetime = Field(
        ..., description="Timestamp of the most recent update"
    )


class TruckListResponse(BaseModel):
    """Paginated list of truck telemetry states."""

    trucks: list[TruckTelemetryState] = Field(
        ..., description="List of truck telemetry states"
    )
    total: int = Field(..., description="Total number of tracked trucks", ge=0)
    limit: int = Field(..., description="Page size used for this response", ge=1)
    offset: int = Field(..., description="Offset used for this response", ge=0)
