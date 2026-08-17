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