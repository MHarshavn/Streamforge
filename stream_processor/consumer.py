import json
from kafka import KafkaConsumer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetry-events"


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
        print("Invalid event: expected a JSON object")
        return

    device_id = event.get("device_id")
    temperature = event.get("temperature")
    humidity = event.get("humidity")
    timestamp = event.get("timestamp")

    if not device_id:
        print("Invalid event: missing device_id")
        return

    if temperature is None:
        print(f"Invalid event from {device_id}: missing temperature")
        return

    if humidity is None:
        print(f"Invalid event from {device_id}: missing humidity")
        return

    if not timestamp:
        print(f"Invalid event from {device_id}: missing timestamp")
        return

    try:
        temperature = float(temperature)
        humidity = float(humidity)
    except (TypeError, ValueError):
        print(f"Invalid event from {device_id}: temperature/humidity must be numeric")
        return

    if not -50 <= temperature <= 100:
        print(f"Invalid event from {device_id}: temperature out of range")
        return

    if not 0 <= humidity <= 100:
        print(f"Invalid event from {device_id}: humidity out of range")
        return

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

    consumer = create_consumer()

    try:
        for message in consumer:
            event = message.value
            process_event(event)

    except KeyboardInterrupt:
        print("\nStream processor stopped.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()