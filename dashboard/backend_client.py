import os

import requests


BACKEND_URL = os.getenv("STREAMFORGE_BACKEND_URL", "http://127.0.0.1:8000").rstrip("/")


def check_backend_health():
    """Check whether the StreamForge backend is healthy."""

    try:
        response = requests.get(
            f"{BACKEND_URL}/health",
            timeout=5
        )
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }


def get_processed_telemetry(limit=500):
    """
    Get processed telemetry from the StreamForge backend/state layer.

    The dashboard does NOT connect directly to Kafka.
    """

    try:
        response = requests.get(
            f"{BACKEND_URL}/telemetry/",
            params={
                "limit": limit,
                "offset": 0
            },
            timeout=5
        )

        response.raise_for_status()

        data = response.json()

        # The API normally returns a list of truck states.
        if isinstance(data, dict):
            trucks = data.get("items", data.get("trucks", []))
        else:
            trucks = data

        processed_events = []

        for truck in trucks:

            if not isinstance(truck, dict):
                continue

            truck_id = truck.get("truck_id")

            # Backend may expose the latest temperature under
            # slightly different field names.
            temperature = truck.get(
                "latest_temperature",
                truck.get("temperature")
            )
            rolling_avg = truck.get(
                "rolling_avg_temperature",
                truck.get("rolling_average_temperature")
            )

            timestamp = truck.get(
                "timestamp",
                truck.get(
                    "latest_timestamp",
                    truck.get("last_updated")
                )
            )

            # Only keep the fields required by the dashboard.
            processed_events.append({
                "truck_id": truck_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "rolling_avg_temperature": rolling_avg
            })

        return processed_events

    except requests.RequestException:
        return []
