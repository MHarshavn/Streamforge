from monitoring.system_status import get_system_status


def test_system_status():
    result = get_system_status()

    assert result["status"] == "healthy"
    assert result["service"] == "streamforge"
    assert "python_version" in result
    assert "platform" in result