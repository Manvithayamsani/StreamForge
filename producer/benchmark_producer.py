import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


TOPIC = "streamforge-events"
TOTAL_EVENTS = 100_000


producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "linger.ms": 10,
    "batch.num.messages": 10000,
    "queue.buffering.max.messages": 500000,
})


start = time.perf_counter()

for i in range(TOTAL_EVENTS):
    now = datetime.now(timezone.utc)

    event = {
        "truck_id": f"BENCH-{i % 1000:04d}",
        "temperature": 25.0,
        "timestamp": now.isoformat(),
        "event_timestamp": now.timestamp(),
    }

    producer.produce(
        TOPIC,
        key=event["truck_id"],
        value=json.dumps(event),
    )

    # Let librdkafka serve delivery callbacks periodically
    if i % 10000 == 0:
        producer.poll(0)

producer.flush()

elapsed = time.perf_counter() - start
rate = TOTAL_EVENTS / elapsed

print(f"Sent {TOTAL_EVENTS:,} events")
print(f"Time: {elapsed:.2f} seconds")
print(f"Producer throughput: {rate:,.0f} events/sec")