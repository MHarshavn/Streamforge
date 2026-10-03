# StreamForge Dashboard

The Streamlit dashboard reads processed state from FastAPI. It does not
connect directly to Kafka.

## Data shown

- Truck ID
- Latest temperature and event timestamp
- Five-minute rolling temperature average
- Truck and reading counts

Humidity is not part of the current telemetry contract. The backend accepts it
only as an optional legacy field and the dashboard does not display it.

## Run the services

Start FastAPI from the repository root, with Kafka available for recovery:

```powershell
$env:STREAMFORGE_STATE_BACKEND="rocksdb"
$env:STREAMFORGE_ROCKSDB_PATH="./data/rocksdb"
python -m uvicorn streamforge.main:app --host 127.0.0.1 --port 8000
```

In a second terminal from the repository root:

```powershell
$env:STREAMFORGE_BACKEND_URL="http://127.0.0.1:8000"
streamlit run dashboard/app.py
```

The dashboard opens on Streamlit's default port, `http://127.0.0.1:8501`.
