import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9092"
TOPIC = "telemetry-events"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )


def generate_event():
    device_id = f"device_{random.randint(1, 10):03d}"

    return {
        "device_id": device_id,
        "temperature": round(random.uniform(20.0, 40.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = create_producer()

    print("StreamForge Telemetry Producer started.")
    print(f"Kafka: {KAFKA_SERVER}")
    print(f"Topic: {TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            event = generate_event()

            producer.send(
                TOPIC,
                key=event["device_id"],
                value=event,
            )

            producer.flush()

            print(
                f"Sent | {event['device_id']} | "
                f"Temp: {event['temperature']}°C | "
                f"Humidity: {event['humidity']}%"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main()