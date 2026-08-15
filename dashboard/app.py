import streamlit as st

from kafka_client import get_recent_events


st.set_page_config(
    page_title="StreamForge Dashboard",
    page_icon="📡",
    layout="wide",
)

st.title("StreamForge Telemetry Dashboard")
st.write("Live telemetry from Kafka")

events = get_recent_events(30)

if events:
    st.subheader("Recent Telemetry")

    for event in events:
        st.write(
            f"**{event.get('device_id', 'N/A')}** | "
            f"Temperature: {event.get('temperature', 'N/A')} °C | "
            f"Humidity: {event.get('humidity', 'N/A')} % | "
            f"Time: {event.get('timestamp', 'N/A')}"
        )
else:
    st.warning("No telemetry events available.")