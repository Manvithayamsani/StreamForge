import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "streamforge-events"


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def generate_event() -> dict:
    """Generate one random StreamForge event."""

    event_type = random.choice(
        [
            "user_login",
            "purchase",
            "payment_success",
            "user_logout",
        ]
    )

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": random.randint(1001, 1100),
        "source": random.choice(["web-app", "mobile-app"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {},
    }

    if event_type == "user_login":
        event["data"] = {
            "device": random.choice(["desktop", "mobile", "tablet"]),
            "location": random.choice(
                ["Hyderabad", "Bengaluru", "Chennai", "Mumbai"]
            ),
        }

    elif event_type == "purchase":
        event["data"] = {
            "product": random.choice(
                ["Laptop", "Mobile Phone", "Headphones", "Keyboard"]
            ),
            "price": random.randint(1000, 100000),
            "quantity": random.randint(1, 3),
        }

    elif event_type == "payment_success":
        event["data"] = {
            "payment_method": random.choice(
                ["UPI", "Credit Card", "Debit Card", "Net Banking"]
            ),
            "amount": random.randint(500, 100000),
            "status": "success",
        }

    elif event_type == "user_logout":
        event["data"] = {
            "session_duration_minutes": random.randint(1, 180),
        }

    return event


def main() -> None:
    producer = None

    try:
        producer = create_producer()

        print(f"Connected to Kafka at {KAFKA_SERVER}")
        print(f"Producing events to topic: {TOPIC_NAME}")
        print("Press Ctrl+C to stop.\n")

        while True:
            event = generate_event()

            future = producer.send(TOPIC_NAME, value=event)
            metadata = future.get(timeout=10)

            print(
                f"Sent {event['event_type']} event "
                f"to partition {metadata.partition}, "
                f"offset {metadata.offset}"
            )

            print(json.dumps(event, indent=2))
            print("-" * 50)

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    except KafkaError as error:
        print(f"Kafka error: {error}")

    except Exception as error:
        print(f"Unexpected error: {type(error).__name__}: {error}")

    finally:
        if producer is not None:
            producer.flush()
            producer.close()
            print("Producer closed.")


if __name__ == "__main__":
    main()