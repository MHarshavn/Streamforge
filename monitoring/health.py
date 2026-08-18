from fastapi import FastAPI

from monitoring.kafka_health import check_kafka_health


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