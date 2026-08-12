from datetime import datetime, timedelta, timezone

import backend.worker_registry as registry

from backend.worker_registry import (
    register_worker,
    heartbeat,
    get_workers,
    WORKER_TIMEOUT_SECONDS,
)


def test_worker_is_online_after_registration():
    register_worker(
        "test-timeout-worker",
        "http://localhost:8002/metrics",
    )

    workers = get_workers()

    worker = next(
        worker
        for worker in workers
        if worker["worker_id"] == "test-timeout-worker"
    )

    assert worker["online"] is True


def test_heartbeat_updates_worker():
    register_worker(
        "test-heartbeat-worker",
        "http://localhost:8003/metrics",
    )

    assert heartbeat("test-heartbeat-worker") is True


def test_unknown_worker_heartbeat_fails():
    assert heartbeat("does-not-exist") is False


def test_worker_goes_offline_after_timeout():
    worker_id = "test_offline-worker"

    register_worker(
        worker_id,
        "http://localhost:8004/metrics",
    )

    old_time = datetime.now(timezone.utc) - timedelta(
        seconds=WORKER_TIMEOUT_SECONDS + 1
    )

    registry._workers[worker_id]["last_seen"] = old_time

    workers = get_workers()

    worker = next(
        worker
        for worker in workers
        if worker["worker_id"] == worker_id
    )

    assert worker["online"] is False 


def test_worker_recovers_after_heartbeat():
    worker_id = "test-recovery-worker"

    register_worker(
        worker_id,
        "http://localhost:8005/metrics",
    ) 

    old_time = datetime.now(timezone.utc) - timedelta(
        seconds=WORKER_TIMEOUT_SECONDS + 1
    )

    registry._workers[worker_id]["last_seen"] = old_time

    workers = get_workers()

    worker = next(
        worker
        for worker in workers
        if worker["worker_id"] == worker_id
    )

    assert worker["online"] is False

    assert heartbeat(worker_id) is True

    workers = get_workers()

    worker = next(
        worker
        for worker in workers
        if worker["worker_id"] == worker_id
    )

    assert worker["online"] is True

                  