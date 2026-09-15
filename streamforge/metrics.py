"""Prometheus metrics and worker registry for StreamForge."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from prometheus_client import Counter, Gauge, Histogram, generate_latest

EVENTS_PROCESSED = Counter(
    "streamforge_events_processed_total",
    "Telemetry events successfully processed",
)
EVENTS_FAILED = Counter(
    "streamforge_events_failed_total",
    "Telemetry events rejected or failed",
)
PROCESSING_SECONDS = Histogram(
    "streamforge_processing_seconds",
    "End-to-end event processing time",
)
PROCESSING_LAG_SECONDS = Gauge(
    "streamforge_processing_lag_seconds",
    "Latest event-time processing lag in seconds",
)
ACTIVE_WORKERS = Gauge(
    "streamforge_active_workers",
    "Number of workers with a live heartbeat",
)
PARTITIONS_ASSIGNED = Gauge(
    "streamforge_partitions_assigned",
    "Total Kafka partitions assigned to registered workers",
)


@dataclass
class WorkerInfo:
    worker_id: str
    host: str
    group_id: str
    partitions: list[int] = field(default_factory=list)
    last_seen: float = field(default_factory=time.time)


class WorkerRegistry:
    def __init__(self, ttl_seconds: float = 30.0) -> None:
        self._ttl = ttl_seconds
        self._workers: dict[str, WorkerInfo] = {}
        self._lock = threading.Lock()

    def heartbeat(self, worker_id: str, host: str, group_id: str, partitions: list[int]) -> None:
        with self._lock:
            self._workers[worker_id] = WorkerInfo(
                worker_id=worker_id,
                host=host,
                group_id=group_id,
                partitions=sorted(set(partitions)),
            )

    def snapshot(self) -> list[WorkerInfo]:
        now = time.time()
        with self._lock:
            self._workers = {
                key: value
                for key, value in self._workers.items()
                if now - value.last_seen <= self._ttl
            }
            workers = list(self._workers.values())
        ACTIVE_WORKERS.set(len(workers))
        PARTITIONS_ASSIGNED.set(sum(len(w.partitions) for w in workers))
        return workers


registry = WorkerRegistry()


def metrics_payload() -> bytes:
    registry.snapshot()
    return generate_latest()
