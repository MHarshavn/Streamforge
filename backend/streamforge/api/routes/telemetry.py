"""Telemetry routes — ingestion and processed-state retrieval.

All business logic is delegated to :class:`TelemetryService`; route
handlers are deliberately thin.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query

from streamforge.api.dependencies import get_telemetry_service
from streamforge.schemas.telemetry import (
    TelemetryBatchRequest,
    TelemetryEvent,
    TelemetryIngestResponse,
    TruckListResponse,
    TruckTelemetryState,
)
from streamforge.services.telemetry_service import TelemetryService

router = APIRouter(prefix="/telemetry", tags=["Telemetry"])


# ── Ingestion ────────────────────────────────────────────────────────────────


@router.post(
    "/ingest",
    response_model=TelemetryIngestResponse,
    status_code=201,
    summary="Ingest a single telemetry event",
    description=(
        "Accept a single truck telemetry reading and store it in the "
        "configured state backend.  The event's timestamp is used to "
        "calculate the 5-minute rolling-average temperature."
    ),
    responses={
        201: {"description": "Event accepted successfully"},
        422: {"description": "Validation error in request body"},
        503: {"description": "State store is unavailable"},
    },
)
async def ingest_single(
    event: TelemetryEvent,
    service: TelemetryService = Depends(get_telemetry_service),
) -> TelemetryIngestResponse:
    return await service.ingest(event)


@router.post(
    "/ingest/batch",
    response_model=TelemetryIngestResponse,
    status_code=201,
    summary="Ingest a batch of telemetry events",
    description=(
        "Accept between 1 and 1 000 telemetry events in a single request "
        "for high-throughput ingestion.  Each event is processed using the "
        "same logic as the single-event endpoint."
    ),
    responses={
        201: {"description": "Batch accepted successfully"},
        422: {"description": "Validation error in request body"},
        503: {"description": "State store is unavailable"},
    },
)
async def ingest_batch(
    batch: TelemetryBatchRequest,
    service: TelemetryService = Depends(get_telemetry_service),
) -> TelemetryIngestResponse:
    return await service.ingest_batch(batch.events)


# ── Retrieval ────────────────────────────────────────────────────────────────


@router.get(
    "/",
    response_model=TruckListResponse,
    summary="List all tracked trucks",
    description=(
        "Return a paginated list of all trucks that have reported telemetry. "
        "Each entry includes the latest temperature and 5-minute rolling "
        "average."
    ),
    responses={
        200: {"description": "Paginated truck list"},
        503: {"description": "State store is unavailable"},
    },
)
async def list_trucks(
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of trucks to return",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Number of trucks to skip",
    ),
    service: TelemetryService = Depends(get_telemetry_service),
) -> TruckListResponse:
    return await service.list_trucks(limit=limit, offset=offset)


@router.get(
    "/{truck_id}",
    response_model=TruckTelemetryState,
    summary="Get processed state for a truck",
    description=(
        "Return the latest telemetry reading and the 5-minute "
        "rolling-average temperature for the specified truck."
    ),
    responses={
        200: {"description": "Truck telemetry state"},
        404: {"description": "Truck not found"},
        503: {"description": "State store is unavailable"},
    },
)
async def get_truck_state(
    truck_id: str = Path(
        ...,
        min_length=1,
        max_length=50,
        description="Truck identifier",
        examples=["TRUCK-00001"],
    ),
    service: TelemetryService = Depends(get_telemetry_service),
) -> TruckTelemetryState:
    return await service.get_truck_state(truck_id)
