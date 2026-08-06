import json
import threading
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


def post_json(url: str, payload: dict) -> bool:
    data = json.dumps(payload).encode("utf-8")

    request = Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=2):
            return True
    except (URLError, TimeoutError, OSError):
        return False


def start_worker_registration(
    worker_id: str,
    metrics_url: str,
    backend_url: str,
    heartbeat_seconds: int,
) -> None:

    def run():
        register_url = f"{backend_url}/workers/register"
        heartbeat_url = f"{backend_url}/workers/heartbeat"

        registered = False

        while True:
            if not registered:
                registered = post_json(
                    register_url,
                    {
                        "worker_id": worker_id,
                        "metrics_url": metrics_url,
                    },
                )

                if registered:
                    print(
                        f"[DISCOVERY] Registered {worker_id}"
                    )
                else:
                    time.sleep(heartbeat_seconds)
                    continue

            time.sleep(heartbeat_seconds)

            alive = post_json(
                heartbeat_url,
                {
                    "worker_id": worker_id,
                },
            )

            if not alive:
                # Backend may have restarted and lost
                # its in-memory registry.
                registered = False

    thread = threading.Thread(
        target=run,
        daemon=True,
        name=f"{worker_id}-heartbeat",
    )

    thread.start()