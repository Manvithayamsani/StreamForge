import json
import os
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


BROKER = os.getenv(
    "STREAMFORGE_KAFKA_BOOTSTRAP",
    "127.0.0.1:9092",
)

TOPIC = "streamforge-events"


producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


late_time = datetime.now(timezone.utc) - timedelta(minutes=7)

event = {
    "truck_id": "TRUCK-LATE-001",
    "temperature": 25.0,
    "timestamp": late_time.isoformat(),
    "event_timestamp": late_time.timestamp(),
}


producer.send(
    TOPIC,
    value=event,
)

producer.flush()
producer.close()

print("Late event sent:")
print(event)