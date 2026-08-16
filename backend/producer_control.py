import subprocess
import sys
import threading
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

_producer_process: subprocess.Popen | None = None
_lock = threading.Lock()


def producer_status() -> dict:
    global _producer_process

    running = (
        _producer_process is not None
        and _producer_process.poll() is None
    )

    return {
        "running": running,
        "pid": (
            _producer_process.pid
            if running
            else None
        ),
    }


def start_producer() -> dict:
    global _producer_process

    with _lock:
        if (
            _producer_process is not None
            and _producer_process.poll() is None
        ):
            return {
                "running": True,
                "pid": _producer_process.pid,
                "message": "Event stream already running",
            }

        _producer_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "producer.producer",
            ],
            cwd=str(PROJECT_ROOT),
        )

        return {
            "running": True,
            "pid": _producer_process.pid,
            "message": "Event stream started",
        }


def stop_producer() -> dict:
    global _producer_process

    with _lock:
        if (
            _producer_process is None
            or _producer_process.poll() is not None
        ):
            _producer_process = None

            return {
                "running": False,
                "pid": None,
                "message": "Event stream already stopped",
            }

        pid = _producer_process.pid

        _producer_process.terminate()

        try:
            _producer_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _producer_process.kill()
            _producer_process.wait()

        _producer_process = None

        return {
            "running": False,
            "pid": None,
            "message": f"Event stream stopped (PID {pid})",
        }