from datetime import datetime, timezone

from stream_processor.bytewax_flow import add, event_time, has_positive_temperature, merge


def test_filter_requires_positive_numeric_temperature():
    assert has_positive_temperature({"temperature": "2.5"})
    assert not has_positive_temperature({"temperature": 0})
    assert not has_positive_temperature({"temperature": -1})
    assert not has_positive_temperature({"temperature": "invalid"})
    assert not has_positive_temperature({})


def test_window_accumulator_handles_events_and_merges_by_event_time():
    older = {
        "truck_id": "TRUCK-1",
        "temperature": 20,
        "timestamp": "2026-10-03T12:00:00Z",
    }
    newer = {
        "truck_id": "TRUCK-1",
        "temperature": 24,
        "timestamp": "2026-10-03T12:02:00Z",
    }
    left = add({"sum": 0.0, "count": 0, "latest": None}, newer)
    right = add({"sum": 0.0, "count": 0, "latest": None}, older)

    combined = merge(left, right)

    assert combined["sum"] == 44
    assert combined["count"] == 2
    assert combined["latest"] == newer
    assert event_time(combined["latest"]) == datetime(
        2026, 10, 3, 12, 2, tzinfo=timezone.utc
    )
