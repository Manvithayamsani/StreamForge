from fastapi import APIRouter

from backend.producer_control import (
    producer_status,
    start_producer,
    stop_producer,
)


router = APIRouter(
    prefix="/stream",
    tags=["stream-control"],
)


@router.get("/status")
def get_stream_status():
    return producer_status()


@router.post("/start")
def start_stream():
    return start_producer()


@router.post("/stop")
def stop_stream():
    return stop_producer()