import json
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.request import Request, urlopen

from processor.config import (
    BACKEND_URL,
    WINDOW_SIZE_SECONDS,
    WORKER_ID,
)


def publish_aggregation(
    truck_id: str,
    event_timestamp: float,
    total: float,
    count: int,
    average: float,
) -> bool:
    if event_timestamp <= 0:
        return False

    window_start_epoch = (
        int(event_timestamp)
        // WINDOW_SIZE_SECONDS
    ) * WINDOW_SIZE_SECONDS

    window_end_epoch = (
        window_start_epoch
        + WINDOW_SIZE_SECONDS
    )

    payload = {
        "worker_id": WORKER_ID,
        "truck_id": truck_id,
        "window_start": datetime.fromtimestamp(
            window_start_epoch,
            tz=timezone.utc,
        ).isoformat(),
        "window_end": datetime.fromtimestamp(
            window_end_epoch,
            tz=timezone.utc,
        ).isoformat(),
        "temperature_sum": total,
        "reading_count": count,
        "average_temperature": average,
    }

    request = Request(
        f"{BACKEND_URL}/aggregations",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=1):
            return True
    except (URLError, TimeoutError, OSError) as error:
        print(
            f"[AGGREGATION] Publish failed: {error}"
        )
        return False