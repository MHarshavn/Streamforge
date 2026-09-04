# StreamForge Dashboard

The Streamlit dashboard consumes **processed telemetry through the StreamForge FastAPI/state layer**. It does not connect directly to Kafka.

## Telemetry contract

The dashboard uses only:

- `truck_id`
- `temperature`
- `timestamp`

The backend also exposes the processed `rolling_avg_temperature` and `reading_count` values for each truck.

## Architecture

```text
Telemetry Producer / Stream Processor
              |
              v
       StreamForge state
              |
              v
        FastAPI :8000
              |
              v
      Streamlit Dashboard
              :8501
```

## Run the backend

From the repository root:

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
uvicorn streamforge.main:app --reload --port 8000
```

Check:

```text
http://localhost:8001/health
http://localhost:8001/docs
```

## Run the dashboard

Open a second terminal at the repository root:

```powershell
pip install streamlit pandas
$env:STREAMFORGE_API_URL="http://localhost:8001"
streamlit run dashboard/app.py
```

The dashboard opens at:

```text
http://localhost:8502
```

## Important

Do not start the dashboard's old Kafka consumer for this version. `dashboard/kafka_client.py` is no longer the dashboard data source. The dashboard reads processed state through `dashboard/backend_client.py`.
