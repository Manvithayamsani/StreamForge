from fastapi import APIRouter
from pydantic import BaseModel

from backend.aggregation_store import (
    get_aggregations,
    update_aggregation,
)


router = APIRouter(
    prefix="/aggregations",
    tags=["aggregations"],
)


class AggregationUpdate(BaseModel):
    worker_id: str
    truck_id: str
    window_start: str
    window_end: str
    temperature_sum: float
    reading_count: int
    average_temperature: float


@router.post("")
def save_aggregation(
    data: AggregationUpdate,
) -> dict:
    update_aggregation(
        data.model_dump()
    )

    return {
        "status": "updated",
    }


@router.get("")
def list_aggregations() -> list[dict]:
    return get_aggregations()