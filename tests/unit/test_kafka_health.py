from unittest.mock import MagicMock, patch

from monitoring.kafka_health import check_kafka_health


def test_kafka_health_when_kafka_is_available():
    mock_client = MagicMock()

    with patch(
        "monitoring.kafka_health.KafkaAdminClient",
        return_value=mock_client
    ):
        result = check_kafka_health()

    assert result["status"] == "healthy"
    assert result["service"] == "kafka"
    assert result["broker"] == "localhost:9092"

    mock_client.list_topics.assert_called_once()
    mock_client.close.assert_called_once()


def test_kafka_health_when_kafka_is_unavailable():
    with patch(
        "monitoring.kafka_health.KafkaAdminClient",
        side_effect=Exception("Kafka unavailable")
    ):
        result = check_kafka_health()

    assert result["status"] == "unhealthy"
    assert result["service"] == "kafka"
    assert result["broker"] == "localhost:9092"
    assert "Kafka unavailable" in result["error"]