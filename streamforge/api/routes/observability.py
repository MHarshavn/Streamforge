"""Prometheus and distributed-worker observability endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field

from streamforge.metrics import (
    EVENTS_PER_SECOND,
    PROCESSING_LAG_SECONDS,
    metrics_payload,
    registry,
)

router = APIRouter(tags=["Observability"])


class WorkerHeartbeat(BaseModel):
    worker_id: str
    host: str
    group_id: str
    partitions: list[int] = Field(default_factory=list)
    failed_events: int = Field(default=0, ge=0)
    filtered_events: int = Field(default=0, ge=0)
    processing_lag_seconds: float = Field(default=0.0, ge=0.0)


@router.post("/workers/heartbeat", status_code=204)
async def worker_heartbeat(payload: WorkerHeartbeat) -> Response:
    registry.heartbeat(
        payload.worker_id,
        payload.host,
        payload.group_id,
        payload.partitions,
    )
    registry.report_worker_metrics(
        payload.worker_id,
        failed_events=payload.failed_events,
        filtered_events=payload.filtered_events,
        processing_lag_seconds=payload.processing_lag_seconds,
    )
    return Response(status_code=204)



@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(content=metrics_payload(), media_type="text/plain; version=0.0.4")


@router.get("/workers")
async def workers() -> dict:
    snapshot = registry.snapshot()
    return {
        "workers": [
            {
                "worker_id": w.worker_id,
                "host": w.host,
                "group_id": w.group_id,
                "partitions": w.partitions,
                "last_seen": w.last_seen,
            }
            for w in snapshot
        ],
        "active_workers": len(snapshot),
    }


@router.get("/topology")
async def topology() -> dict:
    metrics_payload()
    snapshot = registry.snapshot()
    worker_nodes = [
        {
            "id": w.worker_id,
            "type": "worker",
            "label": f"{w.worker_id} ({len(w.partitions)} partitions)",
            "partitions": w.partitions,
            "status": "healthy",
        }
        for w in snapshot
    ]
    partitions = sorted({p for w in snapshot for p in w.partitions})
    nodes = [
        {"id": "kafka", "type": "broker", "label": "Kafka / telemetry-events"},
        *[
            {"id": f"partition-{p}", "type": "partition", "label": f"Partition {p}"}
            for p in partitions
        ],
        *worker_nodes,
        {"id": "state", "type": "state", "label": "RocksDB State"},
        {"id": "backend", "type": "backend", "label": "FastAPI Backend"},
    ]
    edges = []
    for p in partitions:
        edges.append({"id": f"kafka-p{p}", "source": "kafka", "target": f"partition-{p}"})
    for w in snapshot:
        for p in w.partitions:
            edges.append({"id": f"p{p}-{w.worker_id}", "source": f"partition-{p}", "target": w.worker_id})
        edges.append({"id": f"{w.worker_id}-state", "source": w.worker_id, "target": "state"})
    edges.append({"id": "state-backend", "source": "state", "target": "backend"})
    return {
        "nodes": nodes,
        "edges": edges,
        "active_workers": len(snapshot),
        "events_per_second": EVENTS_PER_SECOND._value.get(),
        "processing_lag_seconds": PROCESSING_LAG_SECONDS._value.get(),
    }
