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

def process_event(event: dict) -> None:
    """Process an event based on its event type."""

    event_type = event.get("event_type")
    user_id = event.get("user_id")
    data = event.get("data", {})

    if event_type == "user_login":
        print("LOGIN EVENT")
        print(f"User: {user_id}")
        print(f"Device: {data.get('device')}")
        print(f"Location: {data.get('location')}")

    elif event_type == "purchase":
        price = data.get("price", 0)
        quantity = data.get("quantity", 0)
        total_amount = price * quantity

        print("PURCHASE EVENT")
        print(f"User: {user_id}")
        print(f"Product: {data.get('product')}")
        print(f"Price: {price}")
        print(f"Quantity: {quantity}")
        print(f"Total amount: {total_amount}")

    elif event_type == "payment_success":
        print("PAYMENT EVENT")
        print(f"User: {user_id}")
        print(f"Method: {data.get('payment_method')}")
        print(f"Amount: {data.get('amount')}")
        print(f"Status: {data.get('status')}")

    elif event_type == "user_logout":
        print("LOGOUT EVENT")
        print(f"User: {user_id}")
        print(
            f"Session duration: "
            f"{data.get('session_duration_minutes')} minutes"
        )

    else:
        print("UNKNOWN EVENT")
        print(json.dumps(event, indent=2))

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
            process_event(event)
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