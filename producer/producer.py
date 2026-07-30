import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "streamforge-events"


def create_producer() -> KafkaProducer:
    """Create a Kafka producer that sends JSON messages."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=3,
    )


def main() -> None:
    producer = None

    try:
        producer = create_producer()

        print(f"Connected to Kafka at {KAFKA_SERVER}")
        print(f"Sending events to topic: {TOPIC_NAME}")

        for event_number in range(1, 6):
            event = {
                "event_id": event_number,
                "source": "streamforge-producer",
                "message": f"StreamForge event {event_number}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            future = producer.send(TOPIC_NAME, value=event)
            metadata = future.get(timeout=10)

            print(
                f"Sent event {event_number} "
                f"to partition {metadata.partition}, "
                f"offset {metadata.offset}"
            )

            time.sleep(1)

    except KafkaError as error:
        print(f"Kafka error: {error}")

    except Exception as error:
        print(f"Unexpected error: {error}")

    finally:
        if producer is not None:
            producer.flush()
            producer.close()
            print("Producer closed.")


if __name__ == "__main__":
    main()