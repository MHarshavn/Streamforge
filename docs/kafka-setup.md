# StreamForge Kafka Telemetry Setup

## Overview

StreamForge uses Apache Kafka to transfer telemetry events from the telemetry producer to the stream processor.

The telemetry pipeline is:

Producer → Kafka → Stream Processor

## Kafka Configuration

Kafka runs locally using Docker.

- Kafka broker: `localhost:9092`
- Kafka topic: `telemetry-events`
- Kafka container: `streamforge-kafka`

## Start Kafka

From the `docker` directory:

```powershell
docker compose up -d