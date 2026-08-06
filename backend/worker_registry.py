import threading
from datetime import datetime, timezone


WORKER_TIMEOUT_SECONDS = 15

_workers: dict[str, dict] = {}
_registry_lock = threading.Lock()


def register_worker(
    worker_id: str,
    metrics_url: str,
) -> dict:
    now = datetime.now(timezone.utc)

    with _registry_lock:
        _workers[worker_id] = {
            "worker_id": worker_id,
            "metrics_url": metrics_url,
            "registered_at": now,
            "last_seen": now,
        }

        return _workers[worker_id].copy()


def heartbeat(worker_id: str) -> bool:
    with _registry_lock:
        worker = _workers.get(worker_id)

        if worker is None:
            return False

        worker["last_seen"] = datetime.now(timezone.utc)

        return True


def get_workers() -> list[dict]:
    now = datetime.now(timezone.utc)
    result = []

    with _registry_lock:
        for worker in _workers.values():
            age = (
                now - worker["last_seen"]
            ).total_seconds()

            worker_data = worker.copy()
            worker_data["online"] = (
                age <= WORKER_TIMEOUT_SECONDS
            )

            # Convert datetime objects for API-friendly output
            worker_data["registered_at"] = (
                worker["registered_at"].isoformat()
            )
            worker_data["last_seen"] = (
                worker["last_seen"].isoformat()
            )

            result.append(worker_data)

    return result