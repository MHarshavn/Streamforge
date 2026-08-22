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

        # -----------------------------
        # Device Filter
        # -----------------------------

        st.subheader("Device Filter")

        devices = sorted(df["device_id"].dropna().unique().tolist())

        selected_device = st.selectbox(
            "Select Device",
            ["All Devices"] + devices
        )

        if selected_device != "All Devices":
            df = df[df["device_id"] == selected_device].copy()

        if df.empty:
            st.warning("No telemetry available for the selected device.")
            return

        # -----------------------------
        # Telemetry Overview
        # -----------------------------

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

        # -----------------------------
        # Telemetry Alerts
        # -----------------------------

        st.subheader("Telemetry Alerts")

        alerts = []

        for _, event in df.iterrows():

            if event["temperature"] > 35:
                alerts.append({
                    "device_id": event["device_id"],
                    "type": "High Temperature",
                    "value": f"{event['temperature']:.2f} °C",
                    "timestamp": event.get("timestamp", "N/A")
                })

            if event["humidity"] > 80:
                alerts.append({
                    "device_id": event["device_id"],
                    "type": "High Humidity",
                    "value": f"{event['humidity']:.2f} %",
                    "timestamp": event.get("timestamp", "N/A")
                })

        if alerts:

            st.warning(f"⚠️ {len(alerts)} alert(s) detected")

            alert_df = pd.DataFrame(alerts)

            st.dataframe(
                alert_df,
                use_container_width=True
            )

        else:

            st.success("✅ No telemetry alerts detected.")

        # -----------------------------
        # System Health Status
        # -----------------------------

        st.subheader("System Health")

        active_alerts = len(alerts)

        affected_devices = len(
            set(alert["device_id"] for alert in alerts)
        )

        critical_condition = (
            (df["temperature"] > 38).any()
            or
            (df["humidity"] > 85).any()
        )

        if critical_condition:
            system_status = "🔴 Critical"
        elif active_alerts > 0:
            system_status = "🟡 Warning"
        else:
            system_status = "🟢 Healthy"

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "System Status",
                system_status
            )

        with col2:
            st.metric(
                "Active Alerts",
                active_alerts
            )

        with col3:
            st.metric(
                "Affected Devices",
                affected_devices
            )

        # -----------------------------
        # Telemetry Charts
        # -----------------------------

        st.subheader("Telemetry Charts")

        chart_data = df[["temperature", "humidity"]].copy()

        col1, col2 = st.columns(2)

        with col1:
            st.write("Temperature")
            st.line_chart(chart_data["temperature"])

        with col2:
            st.write("Humidity")
            st.line_chart(chart_data["humidity"])

        # -----------------------------
        # Device-wise Analysis
        # -----------------------------

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

        # -----------------------------
        # Recent Telemetry
        # -----------------------------

        st.subheader("Recent Telemetry")

        filtered_events = df.to_dict("records")

        for event in filtered_events:
            st.write(
                f"**{event.get('device_id', 'N/A')}** | "
                f"Temperature: {event.get('temperature', 'N/A')} °C | "
                f"Humidity: {event.get('humidity', 'N/A')} % | "
                f"Time: {event.get('timestamp', 'N/A')}"
            )

    else:
        st.warning("No telemetry events available.")


live_dashboard()