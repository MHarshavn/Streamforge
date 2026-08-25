# StreamForge

Real-time stream processing system built with Python, Apache Kafka,
stream processing, state management, FastAPI, React, and Prometheus.

## Project Structure

- `producer/` - Telemetry event producer
- `stream_processor/` - Kafka consumer and telemetry processing
- `state/` - State management
- `backend/` - Backend services
- `dashboard/` - Monitoring/dashboard application
- `monitoring/` - Application, Kafka, and system health checks
- `tests/` - Unit and integration tests
- `docs/` - Project documentation
- `docker/` - Docker/Kafka configuration

## Requirements

- Python 3.13+
- Docker Desktop
- Apache Kafka
- pytest
- FastAPI
- Uvicorn

## Kafka Setup

Start Docker Desktop and make sure the Kafka container is running.

Check running containers:

```powershell
docker ps