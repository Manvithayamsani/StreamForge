import json
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer


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

def close_completed_windows(windows):
    current_time = datetime.now(timezone.utc)

    windows_to_remove = []

    for (truck_id, window_start), stats in windows.items():

        window_end = window_start.timestamp() + WINDOW_SIZE_SECONDS
        window_end = datetime.fromtimestamp(
            window_end,
            tz=timezone.utc,
        )

        if current_time >= window_end:

            average = (
                stats["temperature_sum"]
                / stats["reading_count"]
            )

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

            windows_to_remove.append((truck_id, window_start))

    for key in windows_to_remove:
        del windows[key]

def main() -> None:
    consumer = KafkaConsumer(
       TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    )

    windows = defaultdict(
        lambda: {
            "temperature_sum": 0.0,
            "reading_count": 0,
        }
    )

    print("Stream processor started.")
    print("Waiting for truck telemetry...\n")

    try:
        for message in consumer:
            event = json.loads(message.value.decode("utf-8"))

            truck_id = event.get("truck_id")
            temperature = event.get("temperature")
            timestamp = event.get("timestamp")

            # Filter stage
            if (
                not truck_id
                or temperature is None
                or not timestamp
                or temperature <= 0
                or temperature > 100
            ):
                print(f"Filtered invalid event: {event}")
                continue

            # Map stage
            normalized_event = {
                "truck_id": truck_id,
                "temperature": float(temperature),
                "timestamp": timestamp,
            }

            # Window stage
            window_start = get_window_start(
                normalized_event["timestamp"]
            )

            window_key = (
                normalized_event["truck_id"],
                window_start,
            )

            windows[window_key]["temperature_sum"] += (
                normalized_event["temperature"]
            )
            windows[window_key]["reading_count"] += 1
            close_completed_windows(windows)
    except KeyboardInterrupt:
        print("\nStream processor stopped.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()