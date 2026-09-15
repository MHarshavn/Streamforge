"""Health-check route with Kafka/worker readiness information."""
from __future__ import annotations
import time
from fastapi import APIRouter, Depends
from streamforge.api.dependencies import get_state_store
from streamforge.config import get_settings
from streamforge.metrics import registry
from streamforge.schemas.health import ComponentHealth, HealthResponse
from streamforge.services.state_store import BaseStateStore

router = APIRouter(tags=["Health"])
_start_time = time.monotonic()


@router.get("/health", response_model=HealthResponse)
async def health(store: BaseStateStore = Depends(get_state_store)) -> HealthResponse:
    settings = get_settings()
    store_ok = await store.health_check()
    workers = registry.snapshot()
    return HealthResponse(
        status="healthy" if store_ok else "degraded",
        service="streamforge-api",
        version=settings.version,
        uptime_seconds=round(time.monotonic() - _start_time, 2),
        state_backend=store.backend_name,
        components=[
            ComponentHealth(
                name="state_store",
                status="healthy" if store_ok else "unavailable",
                detail=f"Backend: {store.backend_name}",
            ),
            ComponentHealth(
                name="kafka",
                status="configured",
                detail=(
                    f"{settings.kafka_bootstrap_servers} / "
                    f"{settings.kafka_telemetry_topic} / "
                    f"{settings.kafka_partitions} partitions"
                ),
            ),
            ComponentHealth(
                name="workers",
                status="healthy" if workers else "waiting",
                detail=f"{len(workers)} active worker(s)",
            ),
            ComponentHealth(
                name="prometheus",
                status="healthy",
                detail="Metrics available at /metrics",
            ),
        ],
    )
