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
    device_id = event.get("device_id")
    temperature = event.get("temperature")
    humidity = event.get("humidity")
    timestamp = event.get("timestamp")

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