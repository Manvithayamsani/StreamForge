from fastapi import APIRouter, HTTPException

from backend.worker_control import (
    start_worker,
    stop_worker,
    worker_status,
)


router = APIRouter(
    prefix="/cluster",
    tags=["cluster-control"],
)


@router.get("/workers")
def get_worker_processes():
    return worker_status()


@router.post("/workers/start")
def add_worker():
    try:
        return start_worker()
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=str(error),
        )


@router.post(
    "/workers/{worker_id}/start"
)
def start_specific_worker(
    worker_id: str,
):
    try:
        return start_worker(worker_id)
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=str(error),
        )


@router.post(
    "/workers/{worker_id}/stop"
)
def stop_specific_worker(
    worker_id: str,
):
    try:
        return stop_worker(worker_id)
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=str(error),
        )