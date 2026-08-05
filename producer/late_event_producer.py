import json
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


TOPIC = "streamforge-events"


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
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