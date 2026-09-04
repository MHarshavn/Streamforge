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
```

Kafka should be available on:

```text
localhost:9092
```

Check whether the Kafka port is reachable:

```powershell
Test-NetConnection localhost -Port 9092
```

`TcpTestSucceeded : True` means Kafka is reachable.

## Testing

### Run all tests

Run the complete test suite:

```powershell
python -m pytest -v
```

### Run unit tests

Run only the unit tests:

```powershell
python -m pytest .\tests\unit -v
```

### Run integration tests

Run only the Kafka integration tests:

```powershell
python -m pytest .\tests\integration -v
```

> Kafka must be running before running the integration tests.

The integration tests verify:

- Producer-to-Kafka-to-consumer message flow
- Telemetry message delivery
- Invalid telemetry handling
- Telemetry processing

## Monitoring

StreamForge provides application, Kafka, and system health monitoring through a FastAPI service.

### Start the monitoring service

Run:

```powershell
python -m uvicorn monitoring.health:app --host 0.0.0.0 --port 8000
```

The monitoring service runs on:

```text
http://localhost:8000
```

Keep the monitoring server running while checking the health endpoints.

### Application Health Check

Check whether the StreamForge application is running:

```powershell
Invoke-RestMethod http://localhost:8000/health
```

Expected status:

```text
healthy
```

### Kafka Health Check

Check whether the Kafka broker is available:

```powershell
Invoke-RestMethod http://localhost:8000/health/kafka
```

A healthy Kafka response reports:

```text
status  : healthy
service : kafka
broker  : localhost:9092
```

If Kafka is unavailable, the endpoint reports:

```text
status : unhealthy
```

and includes the Kafka connection error.

### System Health Check

Check the basic StreamForge system status:

```powershell
Invoke-RestMethod http://localhost:8000/health/system
```

This reports:

- Application status
- Python version
- Operating system/platform

### Monitoring Logs

Kafka monitoring failures are logged with the broker address and error details.

For example, when Kafka is unavailable, the monitoring logs report a Kafka health-check failure and the connection error.

## Test and Monitoring Requirements

Before running the Kafka integration tests or Kafka health check:

1. Start Docker Desktop.
2. Make sure the `streamforge-kafka` container is running.
3. Verify that port `9092` is reachable.
4. Run the required tests or start the monitoring service.

Example:

```powershell
docker ps
Test-NetConnection localhost -Port 9092
python -m pytest -v
```

To run monitoring:

```powershell
python -m uvicorn monitoring.health:app --host 0.0.0.0 --port 8000
```

Then, from another PowerShell terminal:

```powershell
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/health/kafka
Invoke-RestMethod http://localhost:8000/health/system
```

## Test Results

The StreamForge test suite includes unit tests and Kafka integration tests covering telemetry validation, monitoring, system status, and producer-to-consumer message flow.

Run:

```powershell
python -m pytest -v
```

A successful run should report all tests as passed.