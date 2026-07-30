from fastapi import FastAPI


app = FastAPI(
    title="StreamForge API",
    description="Backend API for the StreamForge real-time event streaming platform",
    version="1.0.0",
)


@app.get("/")
def root() -> dict:
    """Return basic information about the API."""

    return {
        "message": "Welcome to StreamForge API",
    }


@app.get("/health")
def health_check() -> dict:
    """Return the current health status of the API."""

    return {
        "status": "healthy",
        "service": "StreamForge API",
    }