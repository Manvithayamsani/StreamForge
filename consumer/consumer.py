import json

from kafka import KafkaConsumer
from kafka.errors import KafkaError


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "streamforge-events"
CONSUMER_GROUP = "streamforge-consumer-group-v2"
def deserialize_message(value: bytes):
    """Decode JSON messages and safely handle older plain-text messages."""
    decoded_value = value.decode("utf-8")

    try:
        return json.loads(decoded_value)
    except json.JSONDecodeError:
        return {
            "type": "plain-text",
            "message": decoded_value,
        }

def create_consumer() -> KafkaConsumer:
    """Create a Kafka consumer that reads JSON events."""

    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=deserialize_message,
    )


def main() -> None:
    consumer = None

    try:
        consumer = create_consumer()

        print(f"Connected to Kafka at {KAFKA_SERVER}")
        print(f"Listening to topic: {TOPIC_NAME}")
        print("Press Ctrl+C to stop.\n")

        for message in consumer:
            event = message.value

            print(
                f"Received from partition {message.partition}, "
                f"offset {message.offset}:"
            )
            print(json.dumps(event, indent=2))
            print("-" * 50)

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    except KafkaError as error:
        print(f"Kafka error: {error}")

    except Exception as error:
        print(f"Unexpected error: {error}")

    finally:
        if consumer is not None:
            consumer.close()
            print("Consumer closed.")


if __name__ == "__main__":
    main()