from kafka import KafkaAdminClient


KAFKA_BROKER = "localhost:9092"


def check_kafka_health():
    try:
        client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BROKER,
    request_timeout_ms=3000,
    bootstrap_timeout_ms=3000,
        )

        client.list_topics()
        client.close()

        return {
            "status": "healthy",
            "service": "kafka",
            "broker": KAFKA_BROKER,
        }

    except Exception as error:
        return {
            "status": "unhealthy",
            "service": "kafka",
            "broker": KAFKA_BROKER,
            "error": str(error),
        }