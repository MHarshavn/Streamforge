"""Prometheus and distributed-worker observability endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Response

from streamforge.metrics import metrics_payload, registry

router = APIRouter(tags=["Observability"])


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
    return {"nodes": nodes, "edges": edges, "active_workers": len(snapshot)}
