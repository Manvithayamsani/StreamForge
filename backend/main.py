import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.kafka_consumer import consume_kafka_events
from backend.routes.events import router as events_router
from backend.routes.health import router as health_router
from backend.routes.topology import router as topology_router
from backend.routes.workers import router as workers_router


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
    description=(
        "Backend API for the StreamForge "
        "real-time event streaming platform"
    ),
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


app.include_router(health_router)
app.include_router(events_router)
app.include_router(topology_router)
app.include_router(workers_router)


@app.get("/")
def root() -> dict:
    return {
        "message": "Welcome to StreamForge API",
        "status": "running",
    }