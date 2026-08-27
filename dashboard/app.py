import streamlit as st
import pandas as pd

from backend_client import get_processed_telemetry, check_backend_health


st.set_page_config(
    page_title="StreamForge Dashboard",
    page_icon="📡",
    layout="wide"
)


st.title("StreamForge Telemetry Dashboard")
st.write("Processed truck telemetry from the StreamForge backend")


# Keep processed telemetry history between dashboard refreshes.
if "telemetry_history" not in st.session_state:
    st.session_state.telemetry_history = []


@st.fragment(run_every="5s")
def live_dashboard():

    # -----------------------------
    # Backend / State Health
    # -----------------------------

    health = check_backend_health()

    if health.get("status") == "healthy":
        st.success("🟢 StreamForge Backend: Healthy")
    else:
        st.error("🔴 StreamForge Backend: Unhealthy")

    # Read processed telemetry through FastAPI/state layer.
    new_events = get_processed_telemetry(limit=500)

    # Add only new backend snapshots to the local history.  The backend
    # exposes last_updated, so the same state is not added repeatedly.
    existing_keys = {
        (event.get("truck_id"), event.get("timestamp"))
        for event in st.session_state.telemetry_history
    }

    for event in new_events:
        key = (event.get("truck_id"), event.get("timestamp"))
        if key not in existing_keys:
            st.session_state.telemetry_history.append(event)
            existing_keys.add(key)

    # Keep only the latest 100 processed snapshots.
    st.session_state.telemetry_history = (
        st.session_state.telemetry_history[-100:]
    )

    df = pd.DataFrame(st.session_state.telemetry_history)

    if not df.empty:

        # -----------------------------
        # Truck Filter
        # -----------------------------

        st.subheader("Truck Filter")

        trucks = sorted(
            df["truck_id"].dropna().unique().tolist()
        )
        truck_options = ["All Trucks"] + trucks

        if "selected_truck" not in st.session_state:
            st.session_state.selected_truck = "All Trucks"

        if (
            st.session_state.selected_truck != "All Trucks"
            and st.session_state.selected_truck not in truck_options
        ):
            truck_options.append(st.session_state.selected_truck)

        selected_truck = st.selectbox(
            "Select Truck",
            truck_options,
            key="selected_truck"
        )

        if selected_truck != "All Trucks":
            df = df[df["truck_id"] == selected_truck].copy()

        if df.empty:
            st.warning("No telemetry available for the selected truck.")
            return

        # -----------------------------
        # Telemetry Overview
        # -----------------------------

        st.subheader("Telemetry Overview")

        avg_temperature = df["temperature"].mean()
        truck_count = df["truck_id"].nunique()
        reading_count = len(df)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Average Temperature",
                f"{avg_temperature:.2f} °C"
            )

        with col2:
            st.metric(
                "Trucks",
                truck_count
            )

        with col3:
            st.metric(
                "Processed Readings",
                reading_count
            )

        # -----------------------------
        # Telemetry Alerts
        # -----------------------------

        st.subheader("Telemetry Alerts")

        alerts = []

        for _, event in df.iterrows():
            if (
                (selected_truck == "All Trucks" or event["truck_id"] == selected_truck)
                and event["temperature"] > 35
            ):
                alerts.append({
                    "truck_id": event["truck_id"],
                    "type": "High Temperature",
                    "value": f"{event['temperature']:.2f} °C",
                    "timestamp": event.get("timestamp", "N/A")
                })

        if alerts:
            st.warning(f"⚠️ {len(alerts)} alert(s) detected")
            alert_df = pd.DataFrame(alerts)
            st.dataframe(alert_df, use_container_width=True)
        else:
            st.success("✅ No telemetry alerts detected.")

        # -----------------------------
        # System Health Status
        # -----------------------------

        st.subheader("System Health")

        active_alerts = len(alerts)
        affected_trucks = len(
            set(alert["truck_id"] for alert in alerts)
        )

        critical_condition = (df["temperature"] > 38).any()

        if critical_condition:
            system_status = "🔴 Critical"
        elif active_alerts > 0:
            system_status = "🟡 Warning"
        else:
            system_status = "🟢 Healthy"

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("System Status", system_status)

        with col2:
            st.metric("Active Alerts", active_alerts)

        with col3:
            st.metric("Affected Trucks", affected_trucks)

        # -----------------------------
        # Telemetry Charts
        # -----------------------------

        st.subheader("Telemetry Charts")

        chart_data = df.copy()
        chart_data["timestamp"] = pd.to_datetime(
            chart_data["timestamp"], errors="coerce"
        )
        chart_data = chart_data.sort_values("timestamp")

        st.write("Temperature")
        st.line_chart(
            chart_data.set_index("timestamp")["temperature"]
        )

        # -----------------------------
        # Truck-wise Analysis
        # -----------------------------

        st.subheader("Truck-wise Analysis")

        truck_analysis = (
            df.groupby("truck_id")
            .agg(
                Average_Temperature=("temperature", "mean"),
                Readings=("truck_id", "count")
            )
            .reset_index()
        )

        truck_analysis["Average_Temperature"] = (
            truck_analysis["Average_Temperature"].round(2)
        )

        st.dataframe(
            truck_analysis,
            use_container_width=True
        )

        # -----------------------------
        # Recent Processed Telemetry
        # -----------------------------

        st.subheader("Recent Processed Telemetry")

        recent_events = df.sort_values(
            "timestamp", ascending=False
        ).head(30)

        for _, event in recent_events.iterrows():
            rolling_avg = event.get("rolling_avg_temperature")
            rolling_text = (
                f" | 5-min Avg: {rolling_avg:.2f} °C"
                if pd.notna(rolling_avg)
                else ""
            )

            st.write(
                f"**{event.get('truck_id', 'N/A')}** | "
                f"Temperature: {event.get('temperature', 'N/A')} °C"
                f"{rolling_text} | "
                f"Time: {event.get('timestamp', 'N/A')}"
            )

    else:
        st.warning("No processed telemetry available from the backend.")


live_dashboard()
