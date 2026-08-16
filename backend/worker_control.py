import os
import subprocess
import sys
import threading
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

_lock = threading.Lock()

_worker_processes: dict[str, subprocess.Popen] = {}


def _worker_number(worker_id: str) -> int:
    """
    worker-a -> 1
    worker-b -> 2
    worker-c -> 3
    """
    suffix = worker_id.split("-")[-1].lower()

    if len(suffix) != 1 or not suffix.isalpha():
        raise ValueError(
            f"Invalid worker id: {worker_id}"
        )

    return ord(suffix) - ord("a") + 1


def _worker_ports(worker_id: str) -> tuple[int, int]:
    number = _worker_number(worker_id)

    metrics_port = 9100 + number
    web_port = 6065 + number

    return metrics_port, web_port


def _next_worker_id() -> str:
    for index in range(26):
        worker_id = f"worker-{chr(ord('a') + index)}"

        process = _worker_processes.get(worker_id)

        if (
            process is None
            or process.poll() is not None
        ):
            return worker_id

    raise RuntimeError(
        "Maximum worker count reached"
    )


def worker_status() -> list[dict]:
    workers = []

    with _lock:
        for worker_id, process in list(
            _worker_processes.items()
        ):
            running = process.poll() is None

            metrics_port, web_port = (
                _worker_ports(worker_id)
            )

            workers.append(
                {
                    "worker_id": worker_id,
                    "running": running,
                    "pid": (
                        process.pid
                        if running
                        else None
                    ),
                    "metrics_port": metrics_port,
                    "web_port": web_port,
                }
            )

    return workers


def start_worker(
    worker_id: str | None = None,
) -> dict:
    with _lock:
        if worker_id is None:
            worker_id = _next_worker_id()

        existing = _worker_processes.get(
            worker_id
        )

        if (
            existing is not None
            and existing.poll() is None
        ):
            metrics_port, web_port = (
                _worker_ports(worker_id)
            )

            return {
                "worker_id": worker_id,
                "running": True,
                "pid": existing.pid,
                "metrics_port": metrics_port,
                "web_port": web_port,
                "message": (
                    f"{worker_id} already running"
                ),
            }

        metrics_port, web_port = (
            _worker_ports(worker_id)
        )

        environment = os.environ.copy()

        environment.update(
            {
                "STREAMFORGE_WORKER_ID":
                    worker_id,

                "STREAMFORGE_DATADIR":
                    (
                        f"streamforge-faust-"
                        f"{worker_id}-data"
                    ),

                "STREAMFORGE_METRICS_PORT":
                    str(metrics_port),

                "STREAMFORGE_BENCHMARK":
                    "false",

                "STREAMFORGE_BROKER":
                    (
                        "kafka://"
                        "127.0.0.1:9092"
                    ),
            }
        )

        command = [
            sys.executable,
            "-m",
            "processor.faust_processor",
            "worker",
            "-l",
            "info",
        ]

        if worker_id != "worker-a":
            command.extend(
                [
                    "--web-port",
                    str(web_port),
                ]
            )

        process = subprocess.Popen(
            command,
            cwd=str(PROJECT_ROOT),
            env=environment,
        )

        _worker_processes[
            worker_id
        ] = process

        return {
            "worker_id": worker_id,
            "running": True,
            "pid": process.pid,
            "metrics_port": metrics_port,
            "web_port": web_port,
            "message": (
                f"{worker_id} started"
            ),
        }


def stop_worker(
    worker_id: str,
) -> dict:
    with _lock:
        process = _worker_processes.get(
            worker_id
        )

        if (
            process is None
            or process.poll() is not None
        ):
            return {
                "worker_id": worker_id,
                "running": False,
                "pid": None,
                "message": (
                    f"{worker_id} already stopped"
                ),
            }

        process.terminate()

        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

        return {
            "worker_id": worker_id,
            "running": False,
            "pid": None,
            "message": (
                f"{worker_id} stopped"
            ),
        }