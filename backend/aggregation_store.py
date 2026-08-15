from threading import Lock

_lock = Lock()
_aggregations: dict[str, dict] = {}


def update_aggregation(data: dict) -> None:
    key = (
        f"{data['worker_id']}|"
        f"{data['truck_id']}|"
        f"{data['window_start']}"
    )

    with _lock:
        _aggregations[key] = data


def get_aggregations() -> list[dict]:
    with _lock:
        values = list(_aggregations.values())

    return sorted(
        values,
        key=lambda item: item["window_start"],
        reverse=True,
    )