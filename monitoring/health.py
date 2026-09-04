from fastapi import FastAPI

from monitoring.kafka_health import check_kafka_health
from monitoring.system_status import get_system_status


app = FastAPI(title="StreamForge Monitoring")


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "streamforge"
    }


@app.get("/health/kafka")
def kafka_health_check():
    return check_kafka_health()


@app.get("/health/system")
def system_health_check():
    return get_system_status()