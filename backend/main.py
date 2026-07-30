import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.event_store import events
from backend.kafka_consumer import consume_kafka_events


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_thread = threading.Thread(
        target=consume_kafka_events,
        daemon=True,
        name="streamforge-kafka-consumer",
    )

    consumer_thread.start()

    yield


app = FastAPI(
    title="StreamForge API",
    description="Backend API for the StreamForge real-time event streaming platform",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
def root() -> dict:
    return {
        "message": "Welcome to StreamForge API",
    }


@app.get("/health")
def health_check() -> dict:
    return {
        "status": "healthy",
        "service": "StreamForge API",
    }


@app.get("/events")
def get_events() -> list[dict]:
    return events.copy()