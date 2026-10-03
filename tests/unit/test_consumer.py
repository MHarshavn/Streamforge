from unittest.mock import patch
from stream_processor.consumer import (
    create_consumer,
    deserialize_json,
    process_event,
)


def test_valid_event(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.status = 201
        process_event(event)

    captured = capsys.readouterr()

    assert "Processed" in captured.out
    assert "TRUCK-001" in captured.out


def test_missing_device_id(capsys):
    event = {
        "temperature": 25.5,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing device_id" in captured.out


def test_missing_temperature(capsys):
    event = {
        "device_id": "TRUCK-001",
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing temperature" in captured.out


def test_invalid_temperature(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": "invalid",
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "must be numeric" in captured.out


def test_temperature_out_of_range(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 150,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "temperature out of range" in captured.out
def test_humidity_is_optional_for_canonical_telemetry(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.status = 201
        assert process_event(event) is True

    payload = mock_urlopen.call_args.args[0].data
    assert b'"truck_id": "TRUCK-001"' in payload
    assert b"humidity" not in payload


def test_missing_timestamp(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": 60
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing timestamp" in captured.out


def test_naive_timestamp_is_rejected(capsys):
    event = {
        "truck_id": "TRUCK-001",
        "temperature": 25.5,
        "timestamp": "2026-08-17T12:00:00",
    }
    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        assert process_event(event) is False
        mock_urlopen.assert_not_called()
    assert "must include a timezone" in capsys.readouterr().out


def test_malformed_timestamp_is_rejected(capsys):
    event = {
        "truck_id": "TRUCK-001",
        "temperature": 25.5,
        "timestamp": "not-a-date",
    }
    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        assert process_event(event) is False
        mock_urlopen.assert_not_called()
    assert "timestamp must be ISO-8601" in capsys.readouterr().out


def test_invalid_humidity(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": "invalid",
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "must be numeric" in captured.out


def test_temperature_below_range(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": -60,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "temperature out of range" in captured.out


def test_humidity_above_range(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": 120,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "humidity out of range" in captured.out


def test_invalid_event_type(capsys):
    event = "invalid telemetry"

    process_event(event)

    captured = capsys.readouterr()

    assert "expected a JSON object" in captured.out
def test_temperature_none(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": None,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing temperature" in captured.out


def test_null_humidity_is_optional(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": None,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.status = 201
        assert process_event(event) is True


def test_empty_device_id(capsys):
    event = {
        "device_id": "",
        "temperature": 25.5,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing device_id" in captured.out


def test_empty_timestamp(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": 60,
        "timestamp": ""
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing timestamp" in captured.out
def test_invalid_event_logs_error(caplog):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 150,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    with caplog.at_level("ERROR"):
        process_event(event)

        assert "temperature out of range" in caplog.text


def test_temperature_boundary_values(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": -50,
        "humidity": 0,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        assert process_event(event) is True
        mock_urlopen.assert_not_called()

    captured = capsys.readouterr()

    assert "Filtered" in captured.out
    assert "TRUCK-001" in captured.out


def test_temperature_and_humidity_upper_boundary(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 100,
        "humidity": 100,
        "timestamp": "2026-08-17T12:00:00Z"
    }

    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.status = 201
        process_event(event)

    captured = capsys.readouterr()

    assert "Processed" in captured.out
    assert "TRUCK-001" in captured.out


def test_zero_temperature_is_filtered_and_acked(capsys):
    event = {
        "truck_id": "TRUCK-ZERO",
        "temperature": 0,
        "timestamp": "2026-08-17T12:00:00Z",
    }
    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        assert process_event(event) is True
        mock_urlopen.assert_not_called()
    assert "Filtered" in capsys.readouterr().out

def test_create_consumer_configuration():
    with patch("stream_processor.consumer.KafkaConsumer") as mock_consumer:
        create_consumer()

        mock_consumer.assert_called_once()

        args, kwargs = mock_consumer.call_args

        assert args[0] == "telemetry-events"
        assert kwargs["bootstrap_servers"] == "localhost:9092"
        assert kwargs["group_id"] == "streamforge-workers"
        assert kwargs["auto_offset_reset"] == "earliest"
        assert kwargs["enable_auto_commit"] is False

def test_malformed_json_returns_none(caplog):
    malformed_message = b'{"device_id": "TRUCK-001", invalid}'

    with caplog.at_level("ERROR"):
        result = deserialize_json(malformed_message)

    assert result is None
    assert "Malformed JSON message received" in caplog.text

def test_create_consumer_custom_group():
    with patch("stream_processor.consumer.KafkaConsumer") as mock_consumer:
        create_consumer(group_id="streamforge-worker-group")

        _, kwargs = mock_consumer.call_args

        assert kwargs["group_id"] == "streamforge-worker-group"


def test_truck_id_event_is_forwarded_with_canonical_backend_field():
    event = {
        "truck_id": "TRUCK-CANONICAL",
        "temperature": 25.5,
        "timestamp": "2026-08-17T12:00:00+00:00",
    }

    with patch("stream_processor.consumer.urllib.request.urlopen") as mock_urlopen:
        mock_response = mock_urlopen.return_value.__enter__.return_value
        mock_response.status = 201
        assert process_event(event) is True

    request = mock_urlopen.call_args.args[0]
    import json
    assert json.loads(request.data)["truck_id"] == "TRUCK-CANONICAL"
    assert "humidity" not in json.loads(request.data)
