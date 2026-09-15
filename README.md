# StreamForge

**Distributed real-time IoT telemetry streaming, processing, state management and observability platform.**

StreamForge processes telemetry from IoT trucks through Kafka, validates and processes events with Python workers, maintains durable state in RocksDB, exposes FastAPI APIs and Prometheus metrics, and provides a React Flow topology view.

## Architecture

```text
Telemetry Producer
       |
       | JSON telemetry
       v
Apache Kafka
20 partitions
       |
       +-----------------------------+
       |                             |
       v                             v
Kafka Workers                 Bytewax Dataflow
(validate + persist)          event-time windows
       |                             |
       |                             v
       |                    streamforge-windowed
       v
RocksDB State
       |
       +-----------> FastAPI
       |             /health
       |             /telemetry/*
       |             /workers
       |             /topology
       |             /metrics
       |
       v
React Flow Topology UI

Kafka state-changelog snapshots and manual commits support recovery and replay.
```

## Distributed Processing Features

- **20 Kafka partitions** for parallel consumption.
- **Shared consumer group** with configurable worker IDs.
- **Manual Kafka offset commits** only after successful processing.
- **Kafka state-changelog snapshots** for worker/state recovery.
- **RocksDB persistent state** with idempotent event application.
- **5-minute event-time rolling temperature windows**.
- **Bytewax** dataflow for event-time sliding-window processing.
- **Worker heartbeats** and partition-assignment visibility.
- **Prometheus metrics** for throughput, failures, latency, lag and active workers.
- **FastAPI observability APIs** for workers and topology.
- **React Flow dashboard** for visualizing Kafka partitions, workers and state.
- **Throughput benchmark** for measuring the review environment.
- **Recovery test coverage** for the RocksDB state store and observability endpoints.

> **Benchmark note:** 100,000 events/sec is a target to be measured, not an assumed performance claim. Use the supplied benchmark and report the measured result from the review machine.

## Technology Stack

| Technology | Role |
|---|---|
| Python 3.11+ | Application and stream processing |
| Apache Kafka 4.3.1 | Event streaming and partitioning |
| kafka-python | Kafka producer/consumer |
| Bytewax 0.21.1 | Event-time stream processing |
| RocksDB / rocksdict | Durable local state |
| FastAPI + Uvicorn | Backend and APIs |
| Prometheus client | Metrics |
| React + React Flow | Topology visualization |
| Docker Compose | Local Kafka environment |
| Pytest | Automated testing |

## Repository Structure

```text
Streamforge/
├── producer/
│   └── telemetry_producer.py
├── stream_processor/
│   ├── consumer.py
│   └── bytewax_flow.py
├── streamforge/
│   ├── api/routes/
│   │   ├── health.py
│   │   ├── telemetry.py
│   │   └── observability.py
│   ├── services/
│   │   ├── state_store.py
│   │   └── rocksdb_state_store.py
│   ├── config.py
│   ├── metrics.py
│   └── main.py
├── topology-ui/
├── monitoring/
├── scripts/
│   └── throughput_benchmark.py
├── tests/
├── docs/
│   └── distributed-review.md
├── docker/
│   └── compose.yaml
├── .github/workflows/tests.yml
└── pyproject.toml
```

## Quick Start — Windows PowerShell

### 1. Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

### 2. Start Kafka

```powershell
docker compose -f docker/compose.yaml up -d
docker ps
Test-NetConnection localhost -Port 9092
```

The Compose setup creates the following 20-partition topics:

- `telemetry-events`
- `streamforge-state-changelog`
- `streamforge-windowed`

### 3. Start the distributed FastAPI backend

```powershell
$env:STREAMFORGE_STATE_BACKEND="rocksdb"
$env:STREAMFORGE_ROCKSDB_PATH="./data/rocksdb"
python -m uvicorn streamforge.main:app --host 127.0.0.1 --port 8000
```

Useful endpoints:

```text
GET  /health
GET  /telemetry/
GET  /telemetry/{truck_id}
GET  /workers
GET  /topology
GET  /metrics
POST /workers/heartbeat
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

### 4. Start workers

Use separate terminals. Keep the same consumer group and give each worker a different ID:

```powershell
$env:STREAMFORGE_WORKER_ID="worker-1"; python -m stream_processor.consumer
```

```powershell
$env:STREAMFORGE_WORKER_ID="worker-2"; python -m stream_processor.consumer
```

```powershell
$env:STREAMFORGE_WORKER_ID="worker-3"; python -m stream_processor.consumer
```

Kafka distributes the 20 partitions among workers in the `streamforge-workers` consumer group.

### 5. Start the telemetry producer

In another terminal:

```powershell
python -m producer.telemetry_producer
```

The producer publishes JSON telemetry to `telemetry-events`.

### 6. Optional Bytewax path

```powershell
python -m bytewax.run stream_processor.bytewax_flow:flow -w 20
```

The flow consumes telemetry, groups events by truck, applies a 5-minute event-time sliding window and publishes windowed results to `streamforge-windowed`.

### 7. Start the topology UI

```powershell
cd topology-ui
npm install
npm run dev
```

The UI polls `/topology` and displays Kafka partitions, active workers, state and backend relationships.

## State and Recovery

RocksDB is the durable state backend used by the distributed review path.

For each truck, the state store keeps recent readings, removes readings outside the configured 5-minute event-time window, and calculates the rolling average.

Event application is idempotent: a deterministic event ID prevents the same telemetry event from being applied twice.

The worker also publishes a state-changelog record and commits the Kafka offset only after successful backend processing and changelog flush. This provides a clear processing boundary for recovery.

See **`docs/distributed-review.md`** for the full failure/recovery procedure.

## Observability

The backend exposes:

- `/health` — application/state health.
- `/workers` — active worker heartbeats and assigned partitions.
- `/topology` — graph data used by the React Flow UI.
- `/metrics` — Prometheus-compatible metrics.

Important metrics include:

```text
streamforge_events_processed_total
streamforge_events_failed_total
streamforge_processing_seconds
streamforge_processing_lag_seconds
streamforge_active_workers
streamforge_partitions_assigned
```

Prometheus configuration is in `monitoring/prometheus.yml`.

## Testing

Run:

```powershell
python -m pytest -v
```

Targeted distributed-state tests:

```powershell
python -m pytest tests/test_rocksdb_state_store.py -v
python -m pytest tests/test_observability.py -v
python -m pytest tests/unit/test_consumer.py -v
```

The GitHub Actions workflow in `.github/workflows/tests.yml` runs the test suite on pushes to `main` and the distributed-system branch, and on pull requests.

## Throughput Audit

Run:

```powershell
python scripts/throughput_benchmark.py --events 100000
```

Report the measured `producer_events_per_second` from the review machine. Do not present the 100,000 events/sec target as a measured result unless the benchmark actually reaches it.

## Review Demonstration

For a short project review, demonstrate the system in this order:

1. Show the architecture: **Producer → Kafka → Workers → RocksDB/FastAPI → Observability/UI**.
2. Start Kafka and show the 20-partition topics.
3. Start FastAPI with RocksDB.
4. Start two or three workers in the same consumer group.
5. Start the telemetry producer and show events being processed.
6. Open `/workers` and `/topology` to show worker/partition assignments.
7. Stop one worker and explain Kafka partition reassignment.
8. Restart the worker and explain state/changelog recovery.
9. Show Prometheus metrics.
10. Run or show the throughput benchmark and report only the measured result.
11. Finish with the test suite and the failure/recovery runbook.

## Project Documentation

- `docs/distributed-review.md` — distributed review and recovery procedure.
- `docs/stream_processor.md` — stream processor details.
- `docs/dashboard.md` — dashboard details.
- `backend/README.md` — backend documentation.

## Project Status

The distributed processing path is implemented on `feature/distributed-system-completion`. The final review should verify the GitHub Actions test result and the measured local benchmark before making performance or production-readiness claims.

## License

MIT
