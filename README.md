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

Kafka state-changelog events and manual commits support recovery and replay.
```

## Distributed Processing Features

- **20 Kafka partitions** for parallel consumption.
- **Canonical telemetry schema**: `truck_id`, `temperature`, and timezone-aware `timestamp`.
- **Positive-temperature filter** shared by the Kafka worker and Bytewax flow.
- **Shared consumer group** with configurable worker IDs.
- **Manual Kafka offset commits** only after successful processing.
- **Kafka state-changelog events** for worker/state recovery.
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
| Confluent Kafka | Batched, idempotent telemetry producer |
| kafka-python | Consumer groups and changelog replay |
| Bytewax 0.21.1 | Event-time stream processing |
| RocksDB / rocksdict | Durable local state |
| FastAPI + Uvicorn | Backend and APIs |
| Prometheus client | Metrics |
| React + React Flow | Topology visualization |
| Docker Compose | Local Kafka and Prometheus services |
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

## How to Execute the Code (Windows PowerShell)

This walkthrough runs the full local pipeline: Kafka → FastAPI → workers → telemetry producer. Open a separate PowerShell terminal for each long-running process. You need Python 3.11+, Docker Desktop with Docker Compose, and (for the optional dashboard) Node.js with npm.

### 1. Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

### 2. Start Kafka

From the repository root, start the broker and create the project topics:

```powershell
docker compose -f docker/compose.yaml up -d
docker ps
Test-NetConnection localhost -Port 9092
```

Wait for `Test-NetConnection` to report `TcpTestSucceeded : True` before starting the application processes.

The Compose setup creates the following 20-partition topics:

- `telemetry-events`
- `streamforge-state-changelog`
- `streamforge-windowed`

### 3. Start the FastAPI backend (Terminal 1)

```powershell
$env:STREAMFORGE_STATE_BACKEND="rocksdb"
$env:STREAMFORGE_ROCKSDB_PATH="./data/rocksdb"
$env:STREAMFORGE_RECOVER_STATE_FROM_CHANGELOG="true"
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

When Uvicorn reports that the application has started, open `http://127.0.0.1:8000/docs` to explore the API.

### 4. Start workers (Terminal 2 or more)

Start one or more workers in separate terminals. Give each worker a different ID; Kafka shares the 20 partitions across workers in the `streamforge-workers` consumer group:

```powershell
$env:STREAMFORGE_WORKER_ID="worker-1"; python -m stream_processor.consumer
```

```powershell
$env:STREAMFORGE_WORKER_ID="worker-2"; python -m stream_processor.consumer
```

```powershell
$env:STREAMFORGE_WORKER_ID="worker-3"; python -m stream_processor.consumer
```

### 5. Start the telemetry producer (another terminal)

In another terminal:

```powershell
python -m producer.telemetry_producer
```

The producer publishes JSON telemetry to `telemetry-events`.

The producer runs continuously until you press `Ctrl+C`. Check processed state at `http://127.0.0.1:8000/telemetry/`, worker assignments at `http://127.0.0.1:8000/workers`, and the topology at `http://127.0.0.1:8000/topology`.

### 6. Optional Bytewax processing path

```powershell
python -m bytewax.run stream_processor.bytewax_flow:flow -w 20
```

The flow consumes telemetry, groups events by truck, applies a 5-minute event-time sliding window and publishes windowed results to `streamforge-windowed`.

### 7. Optional topology UI

```powershell
cd topology-ui
npm install
npm run dev
```

The UI polls `/topology` and displays Kafka partitions, active workers, state and backend relationships.

To stop the local services, press `Ctrl+C` in each application terminal, then stop Docker Compose from the repository root:

```powershell
docker compose -f docker/compose.yaml down
```

## State and Recovery

RocksDB is the durable state backend used by the distributed review path.

For each truck, the state store keeps recent readings, removes readings outside the configured 5-minute event-time window, and calculates the rolling average.

Event application is idempotent: a deterministic event ID prevents the same telemetry event from being applied twice.

The worker publishes accepted positive-temperature events to the keyed changelog and commits the input Kafka offset only after backend processing and changelog flush. At API startup, the service replays retained changelog events into RocksDB; deterministic event IDs make replay idempotent. If Kafka is unavailable, the API logs the recovery failure and continues with its local RocksDB contents. Set `STREAMFORGE_RECOVER_STATE_FROM_CHANGELOG=false` to disable replay.

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
streamforge_events_filtered_total
streamforge_events_per_second
streamforge_processing_seconds
streamforge_processing_lag_seconds
streamforge_active_workers
streamforge_partitions_assigned
```

Prometheus configuration is in `monitoring/prometheus.yml`.
The Compose file exposes Prometheus at `http://127.0.0.1:9090`.

## Testing

The Kafka integration tests require the local broker and initialized topics. Start them first:

```powershell
docker compose -f docker/compose.yaml up -d kafka kafka-init
```

The `test_kafka_flow` integration test starts its own FastAPI server on an available local port and uses in-memory state. It still sends telemetry through Kafka and the worker's real HTTP ingestion path.

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

## Project Status

Local validation on 2026-10-17:

- Pytest: **83 passed**, **0 failed**, with **5 warnings**.
- Producer benchmark: **100,000 delivered** in **1.182 seconds**, or **84,577 producer events/sec**. This measures producer delivery to Kafka, not end-to-end worker throughput; the 100,000 events/sec target was not reached.
- An isolated 20-partition Kafka test assigned partitions across three workers (7/7/6). Stopping a worker caused reassignment and its heartbeat expired; restarting it restored three visible workers.
- Three telemetry events reached FastAPI/RocksDB and the changelog. A fresh RocksDB path recovered all three readings from the changelog.
- Bytewax consumed two current-time events and emitted windows with average temperature **31.0** and reading count **2**.
- The React Flow dashboard production build completed; Vite reported an ignored `use client` directive warning from React Flow.

These are local single-broker checks. They do not establish multi-broker fault tolerance or production readiness. The benchmark and full runbook are in `docs/distributed-review.md`.

## License

MIT
