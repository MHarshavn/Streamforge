# StreamForge Stream Processor

## Overview

The StreamForge stream processor consumes telemetry events from Apache Kafka, validates the incoming data, processes temperature and humidity values, and safely handles invalid or malformed events.

The processing pipeline is:

**Producer → Kafka → Stream Processor**

## Kafka Configuration

The stream processor connects to the local Kafka broker:

* **Kafka Broker:** `localhost:9092`
* **Kafka Topic:** `telemetry-events`
* **Consumer Group:** `streamforge-consumer`

## Consumer Workflow

The stream processor performs the following steps:

1. Connects to the Kafka broker.
2. Subscribes to the `telemetry-events` topic.
3. Receives telemetry messages.
4. Deserializes the JSON message into a Python object.
5. Validates the telemetry fields.
6. Converts temperature and humidity values to numeric values.
7. Checks that temperature and humidity are within valid ranges.
8. Logs successfully processed telemetry events.
9. Rejects invalid events without stopping the consumer.

## Telemetry Event Format

A valid telemetry event contains:

```json
{
  "device_id": "device_001",
  "temperature": 25.5,
  "humidity": 60.0,
  "timestamp": "2026-08-17T12:00:00"
}
```

## Validation Rules

The processor validates the following fields:

### Device ID

`device_id` must be present and non-empty.

### Temperature

Temperature must:

* Be present.
* Be numeric.
* Be between `-50°C` and `100°C`.

### Humidity

Humidity must:

* Be present.
* Be numeric.
* Be between `0%` and `100%`.

### Timestamp

A timestamp must be present in the telemetry event.

## JSON Deserialization

Kafka messages are received as bytes. The consumer decodes the message as UTF-8 and converts the JSON payload into a Python object.

Invalid event structures are handled safely and do not terminate the stream processor.

## Malformed Event Handling

Invalid telemetry events are rejected with a clear log message.

Examples include:

* Missing `device_id`
* Missing `temperature`
* Missing `humidity`
* Missing `timestamp`
* Non-numeric temperature or humidity
* Temperature outside the allowed range
* Humidity outside the allowed range
* Invalid JSON object structure

The processor continues consuming subsequent Kafka messages after an invalid event.

## Processing Logs

Valid events are logged in the following format:

```text
Processed | device_001 | Temp: 25.5°C | Humidity: 60.0% | Time: 2026-08-17T12:00:00
```

Invalid events generate descriptive messages explaining the validation failure.

## Running the Stream Processor

### 1. Start Kafka

From the `docker` directory:

```bash
docker compose up -d
```

### 2. Start the telemetry producer

Run the producer so that telemetry events are published to Kafka.

### 3. Start the stream processor

From the project root, run:

```bash
python -m stream_processor.consumer
```

The processor will connect to Kafka and wait for telemetry events.

## Expected Output

When valid telemetry is received:

```text
StreamForge Stream Processor
Kafka Broker : localhost:9092
Kafka Topic  : telemetry-events
Waiting for telemetry events...

Processed | device_001 | Temp: 28.45°C | Humidity: 67.21% | Time: ...
```

When invalid telemetry is received, the processor prints an appropriate validation message and continues running.

## Summary

The StreamForge stream processor provides a reliable Kafka consumer for telemetry data. It deserializes JSON events, validates telemetry values, processes valid events, logs processing results, and safely rejects malformed data without crashing the consumer.

## Testing

The StreamForge stream processor was tested with Apache Kafka running locally through Docker.

The consumer successfully connected to the Kafka broker at `localhost:9092` and consumed telemetry events from the `telemetry-events` topic.

### Valid Telemetry

Valid telemetry events were successfully processed and displayed with device ID, temperature, humidity, and timestamp.

Example:

```text
Processed | device_004 | Temp: 27.73°C | Humidity: 44.03%
```

### Invalid Telemetry

Invalid telemetry events are handled safely without crashing the consumer.

The consumer validates:

- Missing `device_id`
- Missing `temperature`
- Missing `humidity`
- Missing `timestamp`
- Non-numeric temperature or humidity
- Temperature outside the valid range
- Humidity outside the valid range

### Test Result

The producer successfully sent telemetry events to Kafka, and the stream processor successfully consumed and processed the events.

The consumer remained stable while handling telemetry validation and processing.

Unit tests were executed using pytest, with all 16 tests passing successfully.

Test Result: 16 passed in 0.52 seconds.