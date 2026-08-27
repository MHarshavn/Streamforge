"""Health-check response schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ComponentHealth(BaseModel):
    """Health status of an individual backend component."""

    name: str = Field(..., description="Component identifier")
    status: str = Field(
        ..., description="Component status: healthy | degraded | unavailable | not_configured"
    )
    detail: str | None = Field(
        None, description="Optional human-readable detail about the component"
    )


class HealthResponse(BaseModel):
    """Top-level health-check response."""

    status: str = Field(
        ...,
        description="Overall system status: healthy | degraded | unavailable",
        examples=["healthy"],
    )
    service: str = Field(
        ...,
        description="Service identifier",
        examples=["streamforge-api"],
    )
    version: str = Field(
        ...,
        description="Application version",
        examples=["0.1.0"],
    )
    uptime_seconds: float = Field(
        ...,
        description="Seconds since the application started",
        ge=0,
    )
    state_backend: str = Field(
        ...,
        description="Currently active state-store backend",
        examples=["in_memory"],
    )
    components: list[ComponentHealth] = Field(
        default_factory=list,
        description="Per-component health details",
    )
