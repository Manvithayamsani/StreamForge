import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import REGISTRY

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_metric_value(metric_name: str):
    for metric in REGISTRY.collect():
        if metric.name == metric_name:
            for sample in metric.samples:
                if sample.name == metric_name:
                    return sample.value
    return 0


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


@app.get("/metrics-summary")
def metrics_summary():
    return {
        "events_processed": get_metric_value(
            "streamforge_events_processed_total"
        ),
        "events_filtered": get_metric_value(
            "streamforge_events_filtered_total"
        ),
        "windows_closed": get_metric_value(
            "streamforge_windows_closed_total"
        ),
        "active_windows": get_metric_value(
            "streamforge_active_windows"
        ),
    }