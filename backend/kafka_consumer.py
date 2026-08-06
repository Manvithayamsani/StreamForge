import json

from kafka import KafkaConsumer

from backend.event_store import events


def consume_kafka_events() -> None:
    consumer = KafkaConsumer(
        "streamforge-events",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="streamforge-api-consumer",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    print("FastAPI background consumer connected to Kafka.")

    for message in consumer:
        event = message.value

        events.append(event)

        if len(events) > 100:
            events.pop(0)

        print(
            f"Stored event from offset {message.offset}: "
            f"{event.get('event_type')}"
        )