# StreamForge

**Real-Time IoT Telemetry Streaming, Processing, Monitoring and Dashboard Platform**

StreamForge is a Python-based real-time telemetry platform for an IoT truck fleet. It generates telemetry events, publishes them to Apache Kafka, consumes and validates those events, forwards valid telemetry to a FastAPI backend, maintains processed truck state with a 5-minute event-time rolling temperature average, and displays the processed state in a Streamlit dashboard.

## End-to-End Architecture

```text
┌────────────────────────┐
│  Telemetry Producer    │
│  producer/             │
│  telemetry_producer.py │
└────────────┬───────────┘
             │ JSON telemetry
             ▼
┌────────────────────────┐
│      Apache Kafka      │
│  localhost:9092        │
│  telemetry-events      │
└────────────┬───────────┘
             │
             ▼
┌────────────────────────┐
│   Stream Processor     │
│ stream_processor/      │
│ consumer.py            │
│                        │
│ • Deserialize JSON     │
│ • Validate telemetry   │
│ • Reject bad events    │
└────────────┬───────────┘
             │ HTTP POST
             ▼
┌────────────────────────┐
│      FastAPI API       │
│  localhost:8001        │
│                        │
│ /health                │
│ /telemetry/ingest     │
│ /telemetry/            │
│ /telemetry/{truck_id} │
└────────────┬───────────┘
             │
             ▼
┌────────────────────────┐
│    InMemoryStateStore  │
│                        │
│ • Latest telemetry     │
│ • 5-minute rolling avg │
│ • Reading count        │
└────────────┬───────────┘
             │ HTTP GET
             ▼
┌────────────────────────┐
│  Streamlit Dashboard   │
│  localhost:8501        │
│                        │
│ • Live telemetry       │
│ • Truck filter         │
│ • Alerts               │
│ • Health status        │
│ • Charts               │
└────────────────────────┘
```

### Important data-flow detail

The dashboard **does not consume Kafka directly**. It reads processed telemetry from the FastAPI/state layer through `dashboard/backend_client.py`.

The stream-processing path is:

**Producer → Kafka → Stream Processor → FastAPI → State Store → Dashboard**

---

## Features

- Real-time telemetry generation for truck/device sensors
- Apache Kafka event streaming
- Kafka producer and consumer integration
- JSON serialization/deserialization
- Telemetry validation and malformed-event handling
- FastAPI REST API
- Single-event and batch telemetry ingestion
- 5-minute event-time rolling temperature average
- Per-truck processed state retrieval
- In-memory state store for development/testing
- Application, Kafka and system health checks
- Streamlit live monitoring dashboard
- Truck filtering and telemetry charts
- High-temperature alerts
- Unit and integration tests
- OpenAPI/Swagger API documentation

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Application and stream-processing language |
| Apache Kafka | Real-time event streaming |
| kafka-python | Kafka producer/consumer client |
| FastAPI | REST API/backend |
| Pydantic | Request and telemetry validation |
| Uvicorn | ASGI server |
| Streamlit | Monitoring dashboard |
| Pandas | Dashboard data processing |
| Docker | Local Kafka deployment |
| Pytest | Unit and integration testing |
| Git/GitHub | Version control |

---

## Project Structure

```text
StreamForge/
│
├── producer/
│   └── telemetry_producer.py       # Generates and publishes telemetry
│
├── stream_processor/
│   └── consumer.py                 # Kafka consumer and event processor
│
├── streamforge/                    # Main integrated FastAPI package
│   ├── api/
│   │   ├── dependencies.py
│   │   └── routes/
│   │       ├── health.py
│   │       └── telemetry.py
│   ├── schemas/
│   │   ├── health.py
│   │   └── telemetry.py
│   ├── services/
│   │   ├── state_store.py
│   │   └── telemetry_service.py
│   ├── config.py
│   ├── exceptions.py
│   └── main.py
│
├── backend/                        # Backend-focused package/docs
│
├── dashboard/
│   ├── app.py                      # Streamlit dashboard
│   ├── backend_client.py           # Calls FastAPI
│   └── kafka_client.py             # Legacy/helper Kafka client
│
├── monitoring/
│   ├── health.py                   # Monitoring API
│   ├── kafka_health.py             # Kafka health check
│   └── system_status.py            # System information
│
├── tests/
│   ├── integration/
│   │   ├── test_kafka_flow.py
│   │   └── test_kafka_pipeline.py
│   ├── unit/
│   │   ├── test_consumer.py
│   │   ├── test_health.py
│   │   ├── test_kafka_health.py
│   │   └── test_system_status.py
│   ├── test_error_handling.py
│   ├── test_health.py
│   └── test_telemetry.py
│
├── docs/
│   ├── dashboard.md
│   └── stream_processor.md
│
├── docker/
│   └── compose.yaml                # Kafka Docker configuration
│
├── pyproject.toml
└── README.md
```

---

# Requirements

- Python **3.11 or newer**
- Docker Desktop
- Git
- PowerShell on Windows (commands below use PowerShell)
- Internet access during initial package installation

Python/Kafka dependencies used by the complete application include FastAPI, Uvicorn, Pydantic, pytest, kafka-python, Streamlit, Pandas and Requests.

---

# Installation

Clone the repository:

```powershell
git clone https://github.com/MHarshavn/Streamforge.git
cd Streamforge
```

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project and development dependencies:

```powershell
pip install -e ".[dev]"
```

Install the Kafka/dashboard dependencies used by the complete application:

```powershell
pip install kafka-python streamlit pandas requests
```

If PowerShell blocks virtual-environment activation, use:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

---

# 1. Start Apache Kafka

Start Docker Desktop first.

From the repository root:

```powershell
docker compose -f docker/compose.yaml up -d
```

Check the Kafka container:

```powershell
docker ps
```

The Kafka container is named:

```text
streamforge-kafka
```

Kafka is configured for:

```text
Broker: localhost:9092
Topic: telemetry-events
```

Verify that port 9092 is reachable:

```powershell
Test-NetConnection localhost -Port 9092
```

Expected:

```text
TcpTestSucceeded : True
```

To stop Kafka later:

```powershell
docker compose -f docker/compose.yaml down
```

---

# 2. Start the FastAPI Backend

Open a **new terminal**.

Go to the project root and activate the environment:

```powershell
cd C:\Users\<your-user>\StreamForge
.\.venv\Scripts\Activate.ps1
```

Start the integrated FastAPI application on port 8001:

```powershell
python -m uvicorn streamforge.main:app --host 127.0.0.1 --port 8001
```

The backend is available at:

```text
http://127.0.0.1:8001
```

Swagger UI:

```text
http://127.0.0.1:8001/docs
```

ReDoc:

```text
http://127.0.0.1:8001/redoc
```

OpenAPI specification:

```text
http://127.0.0.1:8001/openapi.json
```

Health check:

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health
```

> **Why port 8001?** The integrated stream processor and dashboard are configured to send/read backend requests from `127.0.0.1:8001`. The separate monitoring service uses port 8000.

---

# 3. Start the Stream Processor

Open another terminal and activate the virtual environment.

Run:

```powershell
python -m stream_processor.consumer
```

It connects to:

```text
Kafka Broker : localhost:9092
Kafka Topic  : telemetry-events
```

The processor:

1. Connects to Kafka.
2. Subscribes to `telemetry-events`.
3. Receives Kafka messages.
4. Decodes UTF-8 JSON.
5. Validates the event structure.
6. Validates required fields.
7. Converts temperature and humidity to numeric values.
8. Checks allowed ranges.
9. Converts `device_id` to the backend's `truck_id`.
10. Sends valid telemetry to `POST /telemetry/ingest`.
11. Prints a successful `Processed` message.
12. Logs and rejects invalid events without crashing the consumer.

Example processed output:

```text
Processed | device_004 | Temp: 27.73°C | Humidity: 44.03% | Time: ... | Backend: OK
```

---

# 4. Start the Telemetry Producer

Open another terminal and activate the environment.

Run:

```powershell
python -m producer.telemetry_producer
```

The producer generates a new telemetry event approximately every second.

Example event:

```json
{
  "device_id": "device_001",
  "temperature": 28.5,
  "humidity": 65.2,
  "timestamp": "2026-08-25T12:00:00+00:00"
}
```

It publishes the event to:

```text
Kafka → telemetry-events
```

The producer uses Kafka acknowledgements and retries for reliable publishing.

---

# 5. Start the Streamlit Dashboard

Open another terminal and activate the environment.

Run:

```powershell
streamlit run dashboard/app.py
```

The dashboard normally opens at:

```text
http://localhost:8501
```

The dashboard:

- Checks FastAPI backend health
- Retrieves processed telemetry through the backend
- Maintains recent dashboard history
- Filters by truck
- Displays average temperature and humidity
- Displays truck and reading counts
- Detects high-temperature telemetry
- Displays system status
- Shows temperature and humidity charts
- Shows truck-wise analysis
- Displays recent processed telemetry
- Displays the 5-minute rolling average when available

**Do not use the old dashboard Kafka consumer as the dashboard's primary data source.** The current dashboard uses `dashboard/backend_client.py` and the FastAPI/state layer.

---

# 6. Optional: Start Monitoring

The project also contains a separate lightweight monitoring FastAPI service.

Open another terminal and run:

```powershell
python -m uvicorn monitoring.health:app --host 0.0.0.0 --port 8000
```

Monitoring service:

```text
http://localhost:8000
```

Application health:

```powershell
Invoke-RestMethod http://localhost:8000/health
```

Kafka health:

```powershell
Invoke-RestMethod http://localhost:8000/health/kafka
```

System health:

```powershell
Invoke-RestMethod http://localhost:8000/health/system
```

Kafka health reports the broker status for `localhost:9092`.

System health reports:

- Application status
- Python version
- Operating system/platform

---

# Complete Execution Order

For a complete live demonstration, use separate terminals.

### Terminal 1 — Kafka

```powershell
docker compose -f docker/compose.yaml up -d
```

### Terminal 2 — FastAPI Backend

```powershell
.\.venv\Scripts\Activate.ps1
python -m uvicorn streamforge.main:app --host 127.0.0.1 --port 8001
```

### Terminal 3 — Stream Processor

```powershell
.\.venv\Scripts\Activate.ps1
python -m stream_processor.consumer
```

### Terminal 4 — Telemetry Producer

```powershell
.\.venv\Scripts\Activate.ps1
python -m producer.telemetry_producer
```

### Terminal 5 — Dashboard

```powershell
.\.venv\Scripts\Activate.ps1
streamlit run dashboard/app.py
```

### Optional Terminal 6 — Monitoring

```powershell
.\.venv\Scripts\Activate.ps1
python -m uvicorn monitoring.health:app --host 0.0.0.0 --port 8000
```

### Final data flow

```text
Producer
   │
   │ JSON event
   ▼
Kafka:9092
   │
   │ telemetry-events
   ▼
Stream Processor
   │
   │ validated HTTP request
   ▼
FastAPI:8001
   │
   ▼
InMemoryStateStore
   │
   │ processed truck state
   ▼
Streamlit Dashboard:8501
```

---

# Telemetry Validation

The stream processor validates telemetry before sending it to the backend.

A valid event must contain:

- `device_id`
- `temperature`
- `humidity`
- `timestamp`

### Temperature

Temperature must be numeric and between:

```text
-50°C to 100°C
```

### Humidity

Humidity must be numeric and between:

```text
0% to 100%
```

### Invalid events

The processor safely rejects:

- Missing `device_id`
- Missing `temperature`
- Missing `humidity`
- Missing `timestamp`
- Non-numeric temperature
- Non-numeric humidity
- Temperature outside the allowed range
- Humidity outside the allowed range
- Malformed JSON / invalid event structures

The consumer continues processing subsequent messages after an invalid event.

---

# FastAPI API

The main API is exposed by:

```text
http://127.0.0.1:8001
```

## Health

```text
GET /health
```

Checks application health.

## Single telemetry ingestion

```text
POST /telemetry/ingest
```

Accepts one telemetry event.

## Batch telemetry ingestion

```text
POST /telemetry/ingest/batch
```

Accepts a batch of telemetry events.

## List tracked trucks

```text
GET /telemetry/
```

Returns paginated processed truck states.

## Get one truck

```text
GET /telemetry/{truck_id}
```

Returns the processed state for a specific truck.

Use Swagger UI at:

```text
http://127.0.0.1:8001/docs
```

to test the endpoints interactively.

---

# State Management and 5-Minute Rolling Average

The current development state backend is:

```text
InMemoryStateStore
```

For every truck, the state store keeps telemetry readings and calculates a rolling temperature average using **event time**.

The configured window is:

```text
300 seconds = 5 minutes
```

When a new event arrives:

1. The event is stored.
2. Readings are ordered by event timestamp.
3. Readings older than the 5-minute window are removed.
4. The average temperature is recalculated.
5. The latest truck state is available through the API.

The state response can include:

- Truck ID
- Latest temperature
- Latest humidity
- Rolling average temperature
- Window start
- Window end
- Reading count
- Last updated timestamp

> **Current limitation:** `InMemoryStateStore` is intended for development/testing. RocksDB and Kafka-backed state stores are represented as future backend options and are not currently implemented.

---

# Dashboard

The Streamlit dashboard reads processed state from FastAPI.

It provides:

### Backend health

Shows whether the StreamForge backend is healthy.

### Truck filter

Select:

```text
All Trucks
```

or an individual truck.

### Telemetry overview

Displays:

- Average temperature
- Average humidity
- Number of trucks
- Number of processed readings

### Alerts

A high-temperature alert is generated when:

```text
temperature > 35°C
```

A critical system condition is detected when:

```text
temperature > 38°C
```

### Charts

Displays:

- Temperature over time
- Humidity over time

### Truck-wise analysis

Displays the average temperature and reading count for each truck.

### Recent processed telemetry

Displays recent truck readings and the available 5-minute rolling average.

---

# Monitoring

The monitoring service provides:

```text
GET /health
GET /health/kafka
GET /health/system
```

The Kafka health check attempts to connect to:

```text
localhost:9092
```

and reports either:

```text
status: healthy
```

or:

```text
status: unhealthy
```

with the connection error when Kafka is unavailable.

---

# Testing

Run the complete test suite:

```powershell
python -m pytest -v
```

Run unit tests:

```powershell
python -m pytest .\tests\unit -v
```

Run integration tests:

```powershell
python -m pytest .\tests\integration -v
```

Run the main Kafka producer/consumer integration test:

```powershell
python -m pytest tests/integration/test_kafka_flow.py::test_producer_kafka_consumer_flow -v -s
```

Kafka must be running for Kafka integration tests.

## Verified test result

The current project validation completed with:

```text
64 passed
0 failed
7 warnings
```

The Kafka integration flow also passed successfully.

The test suite covers:

- Telemetry validation
- Error handling
- FastAPI health endpoints
- Telemetry API behavior
- State-store behavior
- Kafka health monitoring
- System health monitoring
- Kafka producer/consumer flow
- Stream-processing behavior

---

# Kafka Integration Test

The main integration test verifies the complete producer-to-consumer Kafka path.

The test:

1. Creates a unique device ID.
2. Creates a Kafka consumer with a unique test consumer group.
3. Connects the consumer to its Kafka partition.
4. Creates the Kafka producer.
5. Sends a telemetry event to `telemetry-events`.
6. Waits for Kafka acknowledgement.
7. Polls the consumer.
8. Finds the event using its unique `device_id`.
9. Verifies temperature and humidity.
10. Calls `process_event()`.
11. Verifies the processing output.

A unique consumer group is important for the integration test because the consumer uses `auto_offset_reset="latest"`. It prevents stale consumer-group offsets from making a newly produced test event invisible to the test consumer.

---

# Troubleshooting

## Kafka is not reachable

Check:

```powershell
docker ps
Test-NetConnection localhost -Port 9092
```

If the connection fails:

```powershell
docker compose -f docker/compose.yaml up -d
```

## Backend connection error in the stream processor

Make sure the FastAPI server is running on port 8001:

```powershell
python -m uvicorn streamforge.main:app --host 127.0.0.1 --port 8001
```

Then check:

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health
```

## Dashboard shows no telemetry

Check, in order:

1. Kafka is running.
2. FastAPI is running on port 8001.
3. Stream processor is running.
4. Producer is running.
5. The processor prints `Processed` messages.
6. FastAPI contains telemetry state.
7. Dashboard is running.

## Integration test cannot receive a Kafka event

Make sure:

- Kafka is running.
- Port 9092 is reachable.
- Topic is `telemetry-events`.
- The test consumer is created before the producer sends the event.
- The test uses a unique consumer group.

## Run everything from a clean environment

```powershell
python -m pytest -v
```

If dependencies are missing:

```powershell
pip install -e ".[dev]"
pip install kafka-python streamlit pandas requests
```

---

# Known Warnings

The current test suite passes successfully, but Kafka serializer/deserializer deprecation warnings are currently emitted by the Kafka Python client configuration.

These warnings do **not** currently cause test failures.

Current validation status:

```text
64 passed
0 failed
7 warnings
```

---

# Team / Contribution

StreamForge is a team project.

### Stream-processing / Kafka integration contribution

The Kafka/stream-processing work includes:

- Kafka consumer configuration
- Consumer-group handling
- Kafka producer/consumer integration
- JSON deserialization
- Telemetry validation
- Invalid-event handling
- FastAPI backend forwarding
- Kafka integration testing

A key integration issue was the consumer-group handling in the Kafka integration test. The consumer was made configurable through `create_consumer(group_id=...)`, and the integration test uses a unique consumer group so that newly produced test events can be consumed reliably.

---

# Important Project Concepts for Presentation

If explaining StreamForge to an instructor, describe the project in this order:

1. **Problem:** IoT trucks continuously generate telemetry that needs to be processed and monitored in real time.
2. **Producer:** Python generates temperature, humidity, device ID and timestamp.
3. **Kafka:** Kafka provides the event-streaming layer and decouples producers from consumers.
4. **Consumer:** The stream processor consumes Kafka events and validates them.
5. **Backend:** Valid events are converted to the API's truck telemetry format and sent to FastAPI.
6. **State Store:** FastAPI stores the latest state and calculates a 5-minute event-time rolling temperature average.
7. **Dashboard:** Streamlit reads processed state through FastAPI and displays telemetry, charts, alerts and system status.
8. **Monitoring:** Separate health endpoints check the application, Kafka and system.
9. **Testing:** Unit and integration tests verify individual components and the Kafka message flow.

### One-line explanation

> **StreamForge is a real-time IoT telemetry pipeline that uses Kafka for event streaming, a Python stream processor for validation, FastAPI for backend/state management, and Streamlit for monitoring and visualization.**

---

# Documentation

Additional detailed documentation:

- `docs/stream_processor.md` — Stream processor, Kafka configuration, validation and processing
- `docs/dashboard.md` — Dashboard architecture and usage
- `backend/README.md` — Backend-focused API documentation
- `backend/README.integration.md` — Backend integration notes

---

# License

MIT
