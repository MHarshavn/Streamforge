import streamlit as st
import pandas as pd

from kafka_client import get_recent_events


st.set_page_config(
    page_title="StreamForge Dashboard",
    page_icon="📡",
    layout="wide"
)

st.title("StreamForge Telemetry Dashboard")
st.write("Live telemetry from Kafka")


@st.fragment(run_every="5s")
def live_dashboard():

    events = get_recent_events(30)

    if events:
        df = pd.DataFrame(events)

        st.subheader("Telemetry Overview")

        avg_temperature = df["temperature"].mean()
        avg_humidity = df["humidity"].mean()
        device_count = df["device_id"].nunique()
        event_count = len(df)

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Average Temperature",
                f"{avg_temperature:.2f} °C"
            )

        with col2:
            st.metric(
                "Average Humidity",
                f"{avg_humidity:.2f} %"
            )

        with col3:
            st.metric(
                "Devices",
                device_count
            )

        with col4:
            st.metric(
                "Events",
                event_count
            )

            st.subheader("Telemetry Charts")

        chart_data = df[["temperature", "humidity"]].copy()

        col1, col2 = st.columns(2)

        with col1:
            st.write("Temperature")
            st.line_chart(chart_data["temperature"])

        with col2:
            st.write("Humidity")
            st.line_chart(chart_data["humidity"])

        st.subheader("Device-wise Analysis")

        device_analysis = (
            df.groupby("device_id")
            .agg(
                Average_Temperature=("temperature", "mean"),
                Average_Humidity=("humidity", "mean"),
                Events=("device_id", "count")
            )
            .reset_index()
        )

        device_analysis["Average_Temperature"] = (
            device_analysis["Average_Temperature"].round(2)
        )

        device_analysis["Average_Humidity"] = (
            device_analysis["Average_Humidity"].round(2)
        )

        st.dataframe(
            device_analysis,
            use_container_width=True
        )

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

live_dashboard()