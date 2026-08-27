"""FastAPI dependency-injection providers.

Route handlers depend on :func:`get_telemetry_service` which itself
depends on :func:`get_state_store`.  Neither function references a
concrete implementation — the actual backend is wired up once in
:func:`init_state_store` (called during application lifespan).
"""

from __future__ import annotations

from fastapi import Depends

from streamforge.services.state_store import BaseStateStore
from streamforge.services.telemetry_service import TelemetryService

# ── Module-level singleton ───────────────────────────────────────────────────
# Set once during app startup via ``init_state_store()`` and cleared on
# shutdown.  Route handlers never touch this directly — they go through
# the ``Depends(get_state_store)`` chain.

_state_store: BaseStateStore | None = None


def init_state_store(store: BaseStateStore) -> None:
    """Wire the application-wide state store (called once at startup)."""
    global _state_store  # noqa: PLW0603
    _state_store = store


def close_state_store() -> None:
    """Clear the module-level reference (called at shutdown)."""
    global _state_store  # noqa: PLW0603
    _state_store = None


# ── FastAPI Dependencies ─────────────────────────────────────────────────────


def get_state_store() -> BaseStateStore:
    """Provide the active :class:`BaseStateStore` to downstream dependencies."""
    if _state_store is None:
        raise RuntimeError(
            "State store has not been initialised. "
            "Ensure the application lifespan has started."
        )
    return _state_store


def get_telemetry_service(
    store: BaseStateStore = Depends(get_state_store),
) -> TelemetryService:
    """Provide a :class:`TelemetryService` bound to the current state store."""
    return TelemetryService(state_store=store)
