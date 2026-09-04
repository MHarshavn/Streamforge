import json
import urllib.request
from kafka import KafkaConsumer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetry-events"
BACKEND_URL = "http://127.0.0.1:8000/telemetry/ingest"


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="streamforge-consumer-v4",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def process_event(event):
    device_id = event.get("device_id")
    temperature = event.get("temperature")
    humidity = event.get("humidity")
    timestamp = event.get("timestamp")

    if not device_id or temperature is None or humidity is None or not timestamp:
        print("Invalid telemetry event:", event)
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
            if response.status == 200:
                print(
                    f"Processed | {device_id} | "
                    f"Temp: {temperature}°C | "
                    f"Humidity: {humidity}% | "
                    f"Time: {timestamp} | "
                    f"Backend: OK"
                )
                return True

    except Exception as exc:
        print(f"Backend error for {device_id}: {exc}")
        return False

    return False


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