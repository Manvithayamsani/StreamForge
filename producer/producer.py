import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def generate_truck_telemetry() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "truck_id": f"TRUCK-{random.randint(1001, 1010)}",
        "temperature": round(random.uniform(-5.0, 45.0), 2),
        "timestamp": now.isoformat(),
        "event_timestamp": now.timestamp(),  # UTC Unix timestamp float for Faust event-time windowing
    }


def main() -> None:
    producer = create_producer()
    topic = "streamforge-events"

    print("Connected to Kafka at localhost:9092")
    print(f"Sending truck telemetry to topic: {topic}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            event = generate_truck_telemetry()

            producer.send(
                topic,
                key=event["truck_id"].encode("utf-8"),
                value=event,
            )
            producer.flush()

            print(f"Sent: {event}")

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()