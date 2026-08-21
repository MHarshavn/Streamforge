from stream_processor.consumer import process_event


def test_valid_event(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "Processed" in captured.out
    assert "TRUCK-001" in captured.out


def test_missing_device_id(capsys):
    event = {
        "temperature": 25.5,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing device_id" in captured.out


def test_missing_temperature(capsys):
    event = {
        "device_id": "TRUCK-001",
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing temperature" in captured.out


def test_invalid_temperature(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": "invalid",
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "must be numeric" in captured.out


def test_temperature_out_of_range(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 150,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "temperature out of range" in captured.out
def test_missing_humidity(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing humidity" in captured.out


def test_missing_timestamp(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": 60
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing timestamp" in captured.out


def test_invalid_humidity(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": "invalid",
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "must be numeric" in captured.out


def test_temperature_below_range(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": -60,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "temperature out of range" in captured.out


def test_humidity_above_range(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": 120,
        "timestamp": "2026-08-17T12:00:00"
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
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing temperature" in captured.out


def test_humidity_none(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 25.5,
        "humidity": None,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "missing humidity" in captured.out


def test_empty_device_id(capsys):
    event = {
        "device_id": "",
        "temperature": 25.5,
        "humidity": 60,
        "timestamp": "2026-08-17T12:00:00"
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
        "timestamp": "2026-08-17T12:00:00"
    }

    with caplog.at_level("ERROR"):
        process_event(event)

        assert "temperature out of range" in caplog.text


def test_temperature_boundary_values(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": -50,
        "humidity": 0,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "Processed" in captured.out
    assert "TRUCK-001" in captured.out


def test_temperature_and_humidity_upper_boundary(capsys):
    event = {
        "device_id": "TRUCK-001",
        "temperature": 100,
        "humidity": 100,
        "timestamp": "2026-08-17T12:00:00"
    }

    process_event(event)

    captured = capsys.readouterr()

    assert "Processed" in captured.out
    assert "TRUCK-001" in captured.out