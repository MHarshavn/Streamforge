# StreamForge Distributed Review Runbook

## 1. Kafka

Start Kafka:

    docker compose -f docker/compose.yaml up -d

The broker is configured with 20 partitions for parallel workers.

## 2. Backend with RocksDB

    pip install -e ".[dev]"
    $env:STREAMFORGE_STATE_BACKEND="rocksdb"
    $env:STREAMFORGE_RECOVER_STATE_FROM_CHANGELOG="true"
    $env:STREAMFORGE_ROCKSDB_PATH="./data/rocksdb"
    python -m uvicorn streamforge.main:app --port 8000

Verify /health, /metrics, /workers, and /topology.

At API startup, retained positive-temperature events are replayed from
`streamforge-state-changelog` into RocksDB. If Kafka is unavailable, the API
continues with the local RocksDB state. Use a clean temporary RocksDB path when
demonstrating a full rebuild from the changelog.

## 3. Distributed workers

Run several terminals with the same consumer group and different IDs:

    $env:STREAMFORGE_WORKER_ID="worker-1"; python -m stream_processor.consumer
    $env:STREAMFORGE_WORKER_ID="worker-2"; python -m stream_processor.consumer
    $env:STREAMFORGE_WORKER_ID="worker-3"; python -m stream_processor.consumer

Kafka assigns partitions across the group. Stop a worker with Ctrl+C and observe reassignment.

## 4. Bytewax path

    python -m bytewax.run stream_processor.bytewax_flow:flow -w 20

For recovery:

    python -m bytewax.recovery recovery 20
    python -m bytewax.run stream_processor.bytewax_flow:flow -w 20 -r recovery -s 10 -b 60

Restarting with the same recovery directory restores coordinated Bytewax state.

## 5. React Flow topology

    cd topology-ui
    npm install
    npm run dev

The graph polls /topology every five seconds.

## 6. Prometheus

Use monitoring/prometheus.yml and inspect:

- streamforge_events_processed_total
- streamforge_events_failed_total
- streamforge_events_filtered_total
- streamforge_events_per_second
- streamforge_processing_seconds
- streamforge_processing_lag_seconds
- streamforge_active_workers
- streamforge_partitions_assigned

## 7. Throughput audit

    python scripts/throughput_benchmark.py --events 100000

Record the measured producer rate and Prometheus counters. Only claim 100,000 events/sec if the measured environment proves it.

## 8. Recovery proof

1. Ingest several readings for one truck.
2. Query /telemetry/TRUCK-00001 and record its 5-minute average.
3. Stop a worker.
4. Confirm Kafka reassigns its partition.
5. Restart the worker/dataflow.
6. Query the truck again and confirm the rolling state remains consistent.

## 9. Verified local results (2026-10-17)

- `python -m pytest -q`: 83 passed, 0 failed, 5 warnings.
- The 100,000-event producer benchmark delivered all events in 1.182 seconds (84,577 producer events/sec). This is producer-to-Kafka throughput, not end-to-end processing throughput; the 100,000 events/sec target was not reached.
- A dedicated 20-partition test topic was assigned across three workers (7/7/6). After one worker was stopped, Kafka reassigned its partitions and the API expired its heartbeat. Restarting the worker restored three registered workers.
- Three events were processed into RocksDB and written to the changelog. Starting the API with a fresh temporary RocksDB directory restored all three events from that changelog.
- The API metrics endpoint reported 3 processed events, 1 filtered event, 3 latency observations, 3 active workers, and 20 assigned partitions. The Prometheus target reported `up` while FastAPI was running.
- Bytewax consumed two current-time events and emitted sliding-window results with average temperature 31.0 and reading count 2.
- The React Flow UI production build completed; Vite reported an ignored `use client` directive warning from React Flow.

The validation used one local Kafka broker with replication factor 1. It verifies the local recovery path, not broker loss or multi-node availability.
