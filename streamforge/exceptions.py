"""Custom exception types and FastAPI error-handling middleware."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("streamforge.exceptions")


# ── Domain Exceptions ────────────────────────────────────────────────────────


class StreamForgeError(Exception):
    """Base exception for all StreamForge domain errors."""

    def __init__(self, detail: str = "An internal error occurred") -> None:
        self.detail = detail
        super().__init__(detail)


class TruckNotFoundError(StreamForgeError):
    """Raised when telemetry state for a requested truck does not exist."""

    def __init__(self, truck_id: str) -> None:
        self.truck_id = truck_id
        super().__init__(f"No telemetry state found for truck '{truck_id}'")


class StateStoreUnavailableError(StreamForgeError):
    """Raised when the configured state-store backend is unreachable."""

    def __init__(self, backend: str = "unknown") -> None:
        self.backend = backend
        super().__init__(
            f"State store backend '{backend}' is currently unavailable"
        )


class BatchTooLargeError(StreamForgeError):
    """Raised when a batch ingestion request exceeds the maximum size."""

    def __init__(self, size: int, max_size: int) -> None:
        self.size = size
        self.max_size = max_size
        super().__init__(
            f"Batch size {size} exceeds maximum allowed size of {max_size}"
        )


# ── Error Handlers ───────────────────────────────────────────────────────────


def _error_response(status_code: int, detail: str, **extra: Any) -> JSONResponse:
    """Build a consistent JSON error response."""
    body: dict[str, Any] = {"detail": detail}
    body.update(extra)
    return JSONResponse(status_code=status_code, content=body)


async def truck_not_found_handler(
    _request: Request, exc: TruckNotFoundError
) -> JSONResponse:
    return _error_response(404, exc.detail, truck_id=exc.truck_id)


async def state_store_unavailable_handler(
    _request: Request, exc: StateStoreUnavailableError
) -> JSONResponse:
    logger.error("State store unavailable: %s", exc.detail)
    return _error_response(503, "Telemetry state is currently unavailable")


async def batch_too_large_handler(
    _request: Request, exc: BatchTooLargeError
) -> JSONResponse:
    return _error_response(
        422, exc.detail, size=exc.size, max_size=exc.max_size
    )


async def generic_error_handler(
    _request: Request, exc: StreamForgeError
) -> JSONResponse:
    logger.error("Unhandled StreamForge error: %s", exc.detail)
    return _error_response(500, "An internal error occurred")


async def unhandled_exception_handler(
    _request: Request, exc: Exception
) -> JSONResponse:
    logger.exception("Unexpected server error: %s", exc)
    return _error_response(500, "An unexpected error occurred")


def register_exception_handlers(app: FastAPI) -> None:
    """Attach all custom exception handlers to the FastAPI application."""
    app.add_exception_handler(TruckNotFoundError, truck_not_found_handler)  # type: ignore[arg-type]
    app.add_exception_handler(
        StateStoreUnavailableError, state_store_unavailable_handler  # type: ignore[arg-type]
    )
    app.add_exception_handler(BatchTooLargeError, batch_too_large_handler)  # type: ignore[arg-type]
    app.add_exception_handler(StreamForgeError, generic_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, unhandled_exception_handler)
