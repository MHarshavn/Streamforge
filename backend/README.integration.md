# Backend integration note

This FastAPI backend is integrated into StreamForge under `backend/`.

The dashboard calls this service instead of consuming Kafka directly.

Telemetry fields are intentionally limited to:

- `truck_id`
- `temperature`
- `timestamp`

The state layer additionally returns the processed 5-minute rolling average and reading count.
