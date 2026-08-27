"""StreamForge FastAPI application factory and lifespan management.

The :func:`create_app` factory wires together configuration, state-store
initialisation, exception handlers, and route registration.  The
``lifespan`` context manager ensures the state store is properly
initialised on startup and torn down on shutdown.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from streamforge import __version__
from streamforge.api.dependencies import close_state_store, init_state_store
from streamforge.api.routes import health, telemetry
from streamforge.config import Settings, StateBackend, get_settings
from streamforge.exceptions import register_exception_handlers
from streamforge.services.state_store import BaseStateStore, InMemoryStateStore

logger = logging.getLogger("streamforge")


def _build_state_store(settings: Settings) -> BaseStateStore:
    """Instantiate the state store dictated by configuration.

    Future backends (RocksDB, Kafka) will be added as ``elif`` branches
    here without changing any API-layer code.
    """
    if settings.state_backend == StateBackend.IN_MEMORY:
        return InMemoryStateStore(
            window_size_seconds=settings.window_size_seconds,
        )

    # Placeholder for future implementations
    if settings.state_backend == StateBackend.ROCKSDB:
        raise NotImplementedError(
            "RocksDB state backend is not yet implemented. "
            "Set STREAMFORGE_STATE_BACKEND=in_memory for development."
        )
    if settings.state_backend == StateBackend.KAFKA:
        raise NotImplementedError(
            "Kafka state backend is not yet implemented. "
            "Set STREAMFORGE_STATE_BACKEND=in_memory for development."
        )

    raise ValueError(f"Unknown state backend: {settings.state_backend}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: initialise and tear down shared resources."""
    settings = get_settings()
    store = _build_state_store(settings)
    init_state_store(store)
    logger.info(
        "StreamForge started — state_backend=%s  window=%ds",
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
    """Build and return the fully configured FastAPI application."""
    settings = get_settings()

    application = FastAPI(
        title=settings.app_name,
        version=settings.version,
        description=(
            "**StreamForge** is a distributed Python event processor for "
            "IoT truck fleet telemetry.\n\n"
            "This API exposes endpoints for ingesting raw telemetry events "
            "and retrieving processed state — including 5-minute rolling "
            "average temperatures per truck calculated over event-time windows."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Exception handlers
    register_exception_handlers(application)

    # Routes
    application.include_router(health.router)
    application.include_router(telemetry.router)

    return application


# The module-level ``app`` instance is what ``uvicorn streamforge.main:app``
# picks up.
app = create_app()
