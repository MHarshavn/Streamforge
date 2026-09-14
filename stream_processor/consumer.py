import json
import urllib.request
import logging
import os

from kafka import KafkaConsumer



KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetry-events"
BACKEND_URL = "http://127.0.0.1:8001/telemetry/ingest"
CONSUMER_GROUP = os.getenv(
    "STREAMFORGE_CONSUMER_GROUP",
    "streamforge-consumer",
)

WORKER_ID = os.getenv(
    "STREAMFORGE_WORKER_ID",
    "worker-1",
)

logger = logging.getLogger("streamforge.consumer")


def deserialize_json(value):
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        logger.error("Malformed JSON message received: %s", error)
        return None


def create_consumer(group_id="streamforge-consumer"):
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=deserialize_json,
    )


def process_event(event):
    if not isinstance(event, dict):
        message = "Invalid event: expected a JSON object"
        logger.error(message)
        print(message)
        return False

    device_id = event.get("device_id")
    temperature = event.get("temperature")
    humidity = event.get("humidity")
    timestamp = event.get("timestamp")

    # Required-field validation
    if not device_id:
        message = "Invalid telemetry event: missing device_id"
        logger.error(message)
        print(message)
        return False

    if temperature is None:
        message = "Invalid telemetry event: missing temperature"
        logger.error(message)
        print(message)
        return False

    if humidity is None:
        message = "Invalid telemetry event: missing humidity"
        logger.error(message)
        print(message)
        return False

    if not timestamp:
        message = "Invalid telemetry event: missing timestamp"
        logger.error(message)
        print(message)
        return False

    # Numeric validation
    try:
        temperature = float(temperature)
    except (TypeError, ValueError):
        message = "Invalid telemetry event: temperature must be numeric"
        logger.error(message)
        print(message)
        return False

    try:
        humidity = float(humidity)
    except (TypeError, ValueError):
        message = "Invalid telemetry event: humidity must be numeric"
        logger.error(message)
        print(message)
        return False

    # Range validation
    if temperature < -50 or temperature > 100:
        message = "Invalid telemetry event: temperature out of range"
        logger.error(message)
        print(message)
        return False

    if humidity < 0 or humidity > 100:
        message = "Invalid telemetry event: humidity out of range"
        logger.error(message)
        print(message)
        return False

    # Convert device_id to the truck_id expected by FastAPI.
    backend_event = {
        "truck_id": device_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": timestamp,
    }

    try:
        request_body = json.dumps(backend_event).encode("utf-8")

        request = urllib.request.Request(
            BACKEND_URL,
            data=request_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urllib.request.urlopen(request, timeout=5) as response:
            if 200 <= response.status < 300:
                print(
                    f"Processed | {device_id} | "
                    f"Temp: {temperature}°C | "
                    f"Humidity: {humidity}% | "
                    f"Time: {timestamp} | "
                    f"Backend: OK"
                )
                return True

    except Exception as exc:
        logger.error("Backend error for %s: %s", device_id, exc)
        print(f"Backend error for {device_id}: {exc}")
        return False

    return False


def main():
    print("====================================")
    print(" StreamForge Stream Processor")
    print("====================================")
    print(f"Worker ID    : {WORKER_ID}")
    print(f"Consumer Group: {CONSUMER_GROUP}")
    print(f"Kafka Broker : {KAFKA_BROKER}")
    print(f"Kafka Topic  : {KAFKA_TOPIC}")
    print("Waiting for telemetry events...\n")

    logger.info("Starting StreamForge stream processor")
    logger.info("Worker ID: %s", WORKER_ID)
    logger.info("Consumer group: %s", CONSUMER_GROUP)
    logger.info("Kafka broker: %s", KAFKA_BROKER)
    logger.info("Kafka topic: %s", KAFKA_TOPIC)

    consumer = create_consumer(group_id=CONSUMER_GROUP)

    try:
        for message in consumer:
            event = message.value
            process_event(event)

    except KeyboardInterrupt:
        logger.info("Stream processor stopped by user")
        print("\nStream processor stopped.")

    except Exception as error:
        logger.exception("Stream processor failed: %s", error)
        raise

    finally:
        consumer.close()
        logger.info("Kafka consumer closed")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    main()