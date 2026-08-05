from fastapi import APIRouter

from backend.event_store import events

router = APIRouter()


@router.get("/events")
def get_events() -> list[dict]:
    return events.copy()