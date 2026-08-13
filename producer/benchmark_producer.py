import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


TOPIC = "streamforge-events"
TOTAL_EVENTS = 500_000
TRUCK_COUNT = 1000


producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "linger.ms": 5,
    "batch.num.messages": 10000,
    "queue.buffering.max.messages": 1_000_000,
    "queue.buffering.max.kbytes": 1_048_576,
    "acks": 1,
})


# Pre-build benchmark payloads so we measure Kafka throughput,
# not Python JSON/datetime creation performance.
now = datetime.now(timezone.utc)
timestamp = now.isoformat()
event_timestamp = now.timestamp()

payloads = []

for i in range(TRUCK_COUNT):
    truck_id = f"BENCH-{i:04d}"

    event = {
        "truck_id": truck_id,
        "temperature": 25.0,
        "timestamp": timestamp,
        "event_timestamp": event_timestamp,
    }

    payloads.append(
        (
            truck_id,
            json.dumps(event).encode("utf-8"),
        )
    )


start = time.perf_counter()

for i in range(TOTAL_EVENTS):
    truck_id, payload = payloads[i % TRUCK_COUNT]

    producer.produce(
        TOPIC,
        key=truck_id,
        value=payload,
    )

    if i % 50_000 == 0:
        producer.poll(0)


producer.flush()

elapsed = time.perf_counter() - start
rate = TOTAL_EVENTS / elapsed

print(f"Sent {TOTAL_EVENTS:,} events")
print(f"Time: {elapsed:.2f} seconds")
print(f"Producer throughput: {rate:,.0f} events/sec")