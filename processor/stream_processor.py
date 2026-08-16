import json
import os
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
from prometheus_client import start_http_server
from processor.metrics import (
    EVENTS_PROCESSED,
    EVENTS_FILTERED,
    WINDOWS_CLOSED,
    ACTIVE_WINDOWS,
)
from processor.state_store import StateStore


BROKER = os.getenv(
    "STREAMFORGE_KAFKA_BOOTSTRAP",
    "127.0.0.1:9092",
)

TOPIC = "streamforge-events"
WINDOW_SIZE_SECONDS = 300  # 5 minutes


def get_window_start(timestamp: str) -> datetime:
    event_time = datetime.fromisoformat(timestamp)

    epoch_seconds = int(event_time.timestamp())
    window_start_epoch = (
        epoch_seconds // WINDOW_SIZE_SECONDS
    ) * WINDOW_SIZE_SECONDS

    return datetime.fromtimestamp(
        window_start_epoch,
        tz=timezone.utc,
    )


def calculate_average(stats) -> float:
    count = stats["reading_count"]

    if count == 0:
        return 0.0

    return stats["temperature_sum"] / count


def close_completed_windows(windows, state_store):
    current_time = datetime.now(timezone.utc)
    windows_to_remove = []

    for (truck_id, window_start), stats in windows.items():

        window_end = datetime.fromtimestamp(
            window_start.timestamp() + WINDOW_SIZE_SECONDS,
            tz=timezone.utc,
        )

        if current_time >= window_end:
            average = calculate_average(stats)

            print("\n" + "=" * 50)
            print("WINDOW CLOSED")
            print(f"Truck ID : {truck_id}")
            print(
                f"Window   : {window_start.isoformat()} "
                f"-> {window_end.isoformat()}"
            )
            print(f"Readings : {stats['reading_count']}")
            print(f"Average  : {average:.2f}°C")
            print("=" * 50)

            windows_to_remove.append(
                (truck_id, window_start)
            )

            # Increment closed windows counter
            WINDOWS_CLOSED.inc()

    # Remove completed windows from RAM AND RocksDB
    for truck_id, window_start in windows_to_remove:
        del windows[(truck_id, window_start)]

        state_store.delete_window(
            truck_id,
            window_start,
        )

    # Update active windows count
    ACTIVE_WINDOWS.set(len(windows))


def main() -> None:
    # Start Prometheus HTTP server
    start_http_server(8000)
    print("Prometheus metrics available at http://localhost:8000/metrics")

    # Connect to Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset="latest",
    )

    # Open RocksDB
    state_store = StateStore()

    # Create RAM state
    windows = defaultdict(
        lambda: {
            "temperature_sum": 0.0,
            "reading_count": 0,
        }
    )

    # Recover previous state from RocksDB
    for saved_window in state_store.load_windows():

        truck_id = saved_window["truck_id"]

        window_start = datetime.fromisoformat(
            saved_window["window_start"]
        )

        windows[(truck_id, window_start)] = {
            "temperature_sum":
                saved_window["temperature_sum"],
            "reading_count":
                saved_window["reading_count"],
        }

    print(
        f"Recovered {len(windows)} windows from RocksDB."
    )

    print("Stream processor started.")
    print("Waiting for truck telemetry...\n")

    try:
        for message in consumer:

            event = json.loads(
                message.value.decode("utf-8")
            )

            # Track event processed
            EVENTS_PROCESSED.inc()

            truck_id = event.get("truck_id")
            temperature = event.get("temperature")
            timestamp = event.get("timestamp")

            # FILTER
            if (
                not truck_id
                or temperature is None
                or not timestamp
                or temperature < -50
                or temperature > 100
            ):
                EVENTS_FILTERED.inc()
                print(
                    f"Filtered invalid event: {event}"
                )
                continue

            # MAP
            normalized_event = {
                "truck_id": truck_id,
                "temperature": float(temperature),
                "timestamp": timestamp,
            }

            # WINDOW
            window_start = get_window_start(
                normalized_event["timestamp"]
            )

            window_key = (
                normalized_event["truck_id"],
                window_start,
            )

            # Update RAM state
            windows[window_key][
                "temperature_sum"
            ] += normalized_event["temperature"]

            windows[window_key][
                "reading_count"
            ] += 1

            # Update gauge for active windows
            ACTIVE_WINDOWS.set(len(windows))

            # Persist the updated window in RocksDB
            state_store.save_window(
                normalized_event["truck_id"],
                window_start,
                windows[window_key],
            )

            # Close completed windows
            close_completed_windows(
                windows,
                state_store,
            )

    except KeyboardInterrupt:
        print("\nStream processor stopped.")

    finally:
        consumer.close()
        state_store.close()
        print("Kafka consumer and RocksDB closed.")


if __name__ == "__main__":
    main()