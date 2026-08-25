import logging

from kafka import KafkaAdminClient


KAFKA_BROKER = "localhost:9092"

logger = logging.getLogger("streamforge.monitoring.kafka")


def check_kafka_health():
    client = None

    try:
        client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            request_timeout_ms=3000,
            bootstrap_timeout_ms=3000,
        )

        client.list_topics()

        logger.info(
            "Kafka health check successful: broker=%s",
            KAFKA_BROKER,
        )

        return {
            "status": "healthy",
            "service": "kafka",
            "broker": KAFKA_BROKER,
        }

    except Exception as error:
        logger.error(
            "Kafka health check failed: broker=%s error=%s",
            KAFKA_BROKER,
            error,
        )

        return {
            "status": "unhealthy",
            "service": "kafka",
            "broker": KAFKA_BROKER,
            "error": str(error),
        }

    finally:
        if client is not None:
            client.close()