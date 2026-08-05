from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.worker_registry import (
    get_workers,
    heartbeat,
    register_worker,
)


router = APIRouter(
    prefix="/workers",
    tags=["workers"],
)


class WorkerRegistration(BaseModel):
    worker_id: str
    metrics_url: str


class WorkerHeartbeat(BaseModel):
    worker_id: str


@router.post("/register")
def register(data: WorkerRegistration) -> dict:
    worker = register_worker(
        worker_id=data.worker_id,
        metrics_url=data.metrics_url,
    )

    return {
        "status": "registered",
        "worker": worker,
    }


@router.post("/heartbeat")
def worker_heartbeat(data: WorkerHeartbeat) -> dict:
    success = heartbeat(data.worker_id)

    if not success:
        raise HTTPException(
            status_code=404,
            detail="Worker is not registered",
        )

    return {
        "status": "alive",
        "worker_id": data.worker_id,
    }


@router.get("")
def list_workers() -> dict:
    return {
        "workers": get_workers(),
    }