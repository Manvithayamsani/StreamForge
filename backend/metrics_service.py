from urllib.error import URLError
from urllib.request import urlopen

from prometheus_client.parser import text_string_to_metric_families

from backend.worker_registry import get_workers


STREAMFORGE_METRICS = {
    "streamforge_events_processed",
    "streamforge_events_filtered",
    "streamforge_processing_rate",
    "streamforge_worker_up",
    "streamforge_active_partitions",
    "streamforge_processing_lag",
}


def fetch_worker_metrics(metrics_url: str) -> dict:
    """
    Fetch and parse Prometheus metrics from one Faust worker.
    """

    try:
        with urlopen(metrics_url, timeout=2) as response:
            metrics_text = response.read().decode("utf-8")

    except (URLError, TimeoutError, OSError):
        return {
            "available": False,
            "events_processed": 0,
            "events_filtered": 0,
            "processing_rate": 0,
            "worker_up": 0,
            "active_partitions": 0,
            "processing_lag": 0,
        }

    values = {}

    for family in text_string_to_metric_families(metrics_text):
        if family.name not in STREAMFORGE_METRICS:
            continue

        for sample in family.samples:
            values[sample.name] = sample.value

    return {
        "available": True,

        "events_processed": values.get(
            "streamforge_events_processed_total",
            0,
        ),

        "events_filtered": values.get(
            "streamforge_events_filtered_total",
            0,
        ),

        "processing_rate": values.get(
            "streamforge_processing_rate",
            0,
        ),

        "worker_up": values.get(
            "streamforge_worker_up",
            0,
        ),

        "active_partitions": values.get(
            "streamforge_active_partitions",
            0,
        ),

        "processing_lag": values.get(
            "streamforge_processing_lag",
            0,
        ),
    }


def get_cluster_metrics() -> dict:
    workers = {}

    registered_workers = get_workers()

    for registered_worker in registered_workers:
        worker_id = registered_worker["worker_id"]

        if registered_worker["online"]:
            worker_metrics = fetch_worker_metrics(
                registered_worker["metrics_url"]
            )
        else:
            worker_metrics = {
                "available": False,
                "events_processed": 0,
                "events_filtered": 0,
                "processing_rate": 0,
                "worker_up": 0,
                "active_partitions": 0,
                "processing_lag": 0,
            }

        workers[worker_id] = {
            **worker_metrics,
            "worker_id": worker_id,
            "metrics_url": registered_worker["metrics_url"],
            "online": registered_worker["online"],
            "last_seen": registered_worker["last_seen"],
        }

    available_workers = [
        worker
        for worker in workers.values()
        if worker["available"] and worker["online"]
    ]

    return {
        "workers": workers,

        "summary": {
            "workers_online": sum(
                1
                for worker in available_workers
                if worker["worker_up"] == 1
            ),

            "events_processed": sum(
                worker["events_processed"]
                for worker in available_workers
            ),

            "events_filtered": sum(
                worker["events_filtered"]
                for worker in available_workers
            ),

            "processing_rate": sum(
                worker["processing_rate"]
                for worker in available_workers
            ),

            "active_partitions": sum(
                worker["active_partitions"]
                for worker in available_workers
            ),

            "max_processing_lag": max(
                (
                    worker["processing_lag"]
                    for worker in available_workers
                ),
                default=0,
            ),
        },
    }