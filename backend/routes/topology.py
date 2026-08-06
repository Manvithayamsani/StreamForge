from fastapi import APIRouter

from backend.metrics_service import get_cluster_metrics

router = APIRouter()


@router.get("/metrics-summary")
def metrics_summary() -> dict:
    return get_cluster_metrics()


@router.get("/topology")
def topology() -> dict:
    metrics = get_cluster_metrics()

    worker_nodes = [
        {
            "id": worker_id,
            "type": "worker",
            **worker_data,
        }
        for worker_id, worker_data in metrics["workers"].items()
    ]

    pipeline = [
        {
            "id": "producer",
            "type": "producer",
            "status": "active",
        },
        {
            "id": "kafka",
            "type": "broker",
            "status": "active",
        },
        *worker_nodes,
    ]

    return {
        "pipeline": pipeline,
        "summary": metrics["summary"],
    }