"""Health-check route.

Reports overall system health including the status of the configured
state-store backend.
"""

from __future__ import annotations

import time

from fastapi import APIRouter, Depends

from streamforge.schemas.health import ComponentHealth, HealthResponse
from streamforge.api.dependencies import get_state_store
from streamforge.config import get_settings
from streamforge.services.state_store import BaseStateStore

router = APIRouter(tags=["Health"])

# Captured once at import time — close enough for uptime reporting.
_start_time: float = time.monotonic()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="System health check",
    description=(
        "Returns the operational status of the StreamForge API and its "
        "backend components.  Returns HTTP 200 when healthy or degraded, "
        "and HTTP 503 when a critical component is unavailable."
    ),
    responses={
        200: {"description": "System is healthy or degraded"},
        503: {"description": "A critical backend component is unavailable"},
    },
)
async def health(
    store: BaseStateStore = Depends(get_state_store),
) -> HealthResponse:
    settings = get_settings()

    # Check state-store health
    store_ok = await store.health_check()
    store_status = "healthy" if store_ok else "unavailable"

    components = [
        ComponentHealth(
            name="state_store",
            status=store_status,
            detail=f"Backend: {store.backend_name}",
        ),
        # Kafka / RocksDB components will be added here in the future.
        ComponentHealth(
            name="kafka",
            status="not_configured",
            detail="Kafka integration is not yet implemented",
        ),
    ]

    overall = "healthy" if store_ok else "degraded"

    return HealthResponse(
        status=overall,
        service="streamforge-api",
        version=settings.version,
        uptime_seconds=round(time.monotonic() - _start_time, 2),
        state_backend=store.backend_name,
        components=components,
    )
