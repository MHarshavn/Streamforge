from monitoring.kafka_health import check_kafka_health


def test_kafka_health_returns_status():
    result = check_kafka_health()

    assert result["status"] in ["healthy", "unhealthy"]
    assert result["service"] == "kafka"
    assert result["broker"] == "localhost:9092"