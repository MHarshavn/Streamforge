"""StreamForge FastAPI application factory and lifecycle management."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from streamforge.api.dependencies import close_state_store, init_state_store
from streamforge.api.routes import health, observability, telemetry
from streamforge.config import Settings, StateBackend, get_settings
from streamforge.exceptions import register_exception_handlers
from streamforge.services.rocksdb_state_store import RocksDBStateStore
from streamforge.services.state_store import BaseStateStore, InMemoryStateStore

logger = logging.getLogger("streamforge")


def _build_state_store(settings: Settings) -> BaseStateStore:
    if settings.state_backend == StateBackend.IN_MEMORY:
        return InMemoryStateStore(window_size_seconds=settings.window_size_seconds)
    if settings.state_backend == StateBackend.ROCKSDB:
        return RocksDBStateStore(
            path=settings.rocksdb_path,
            window_size_seconds=settings.window_size_seconds,
        )
    if settings.state_backend == StateBackend.KAFKA:
        raise NotImplementedError(
            "Kafka is the changelog transport, not a direct state-store backend. "
            "Use STREAMFORGE_STATE_BACKEND=rocksdb."
        )
    raise ValueError(f"Unknown state backend: {settings.state_backend}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    store = _build_state_store(settings)
    init_state_store(store)
    logger.info(
        "StreamForge started - state_backend=%s window=%ds",
        store.backend_name,
        settings.window_size_seconds,
    )
    try:
        yield
    finally:
        await store.close()
        close_state_store()
        logger.info("StreamForge shut down.")


def create_app() -> FastAPI:
    settings = get_settings()
    application = FastAPI(
        title=settings.app_name,
        version=settings.version,
        description=(
            "Distributed StreamForge telemetry processor with Kafka, "
            "event-time 5-minute windows, RocksDB state, worker observability, "
            "and Prometheus metrics."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    register_exception_handlers(application)
    application.include_router(health.router)
    application.include_router(telemetry.router)
    application.include_router(observability.router)
    return application


app = create_app()
