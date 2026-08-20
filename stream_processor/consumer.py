import json
import logging

from kafka import KafkaConsumer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetry-events"

logger = logging.getLogger("streamforge.consumer")


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="streamforge-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def process_event(event):
    if not isinstance(event, dict):
        logger.error("Invalid event: expected a JSON object")
        print("Invalid event: expected a JSON object")
        return

    device_id = event.get("device_id")
    temperature = event.get("temperature")
    humidity = event.get("humidity")
    timestamp = event.get("timestamp")

    if not device_id:
        logger.error("Invalid event: missing device_id")
        print("Invalid event: missing device_id")
        return

    if temperature is None:
        logger.error(
            "Invalid event from %s: missing temperature",
            device_id,
        )
        print(f"Invalid event from {device_id}: missing temperature")
        return

    if humidity is None:
        logger.error(
            "Invalid event from %s: missing humidity",
            device_id,
        )
        print(f"Invalid event from {device_id}: missing humidity")
        return

    if not timestamp:
        logger.error(
            "Invalid event from %s: missing timestamp",
            device_id,
        )
        print(f"Invalid event from {device_id}: missing timestamp")
        return

    try:
        temperature = float(temperature)
        humidity = float(humidity)

    except (TypeError, ValueError):
        logger.error(
            "Invalid event from %s: temperature/humidity must be numeric",
            device_id,
        )
        print(
            f"Invalid event from {device_id}: "
            "temperature/humidity must be numeric"
        )
        return

    if not -50 <= temperature <= 100:
        logger.error(
            "Invalid event from %s: temperature out of range",
            device_id,
        )
        print(
            f"Invalid event from {device_id}: "
            "temperature out of range"
        )
        return

    if not 0 <= humidity <= 100:
        logger.error(
            "Invalid event from %s: humidity out of range",
            device_id,
        )
        print(
            f"Invalid event from {device_id}: "
            "humidity out of range"
        )
        return

    logger.info(
        "Processed telemetry event from %s",
        device_id,
    )

    print(
        f"Processed | {device_id} | "
        f"Temp: {temperature}°C | "
        f"Humidity: {humidity}% | "
        f"Time: {timestamp}"
    )


def main():
    print("====================================")
    print(" StreamForge Stream Processor")
    print("====================================")
    print(f"Kafka Broker : {KAFKA_BROKER}")
    print(f"Kafka Topic  : {KAFKA_TOPIC}")
    print("Waiting for telemetry events...\n")

    logger.info("Starting StreamForge stream processor")
    logger.info("Kafka broker: %s", KAFKA_BROKER)
    logger.info("Kafka topic: %s", KAFKA_TOPIC)

    consumer = create_consumer()

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