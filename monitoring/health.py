from fastapi import FastAPI

app = FastAPI(title="StreamForge Monitoring")


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "streamforge"
    }