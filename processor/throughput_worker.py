import json
import os
import time
from confluent_kafka import Consumer, KafkaException

BROKER = os.getenv(
    "STREAMFORGE_KAFKA_BOOTSTRAP",
    "127.0.0.1:9092",
)
TOPIC = "streamforge-events"
WORKER_ID = os.getenv("STREAMFORGE_WORKER_ID", "throughput-worker")
GROUP_ID = "streamforge-throughput-benchmark"
BATCH_SIZE = 5000
REPORT_EVERY = 100_000
next_report = REPORT_EVERY

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,

    # Benchmark should consume the events we produce after startup.
    "auto.offset.reset": "latest",

    # Avoid commit overhead during throughput measurement.
    "enable.auto.commit": False,

    # Larger fetches for high-throughput batch consumption.
    "fetch.min.bytes": 1_048_576,
    "fetch.wait.max.ms": 100,
    "queued.max.messages.kbytes": 262_144,
})

consumer.subscribe([TOPIC])

processed = 0
valid = 0
start_time = None

print(f"[{WORKER_ID}] Throughput worker starting...")
print(f"[{WORKER_ID}] Waiting for Kafka partitions...")

try:
    while True:
        messages = consumer.consume(
            num_messages=BATCH_SIZE,
            timeout=1.0,
        )

        if not messages:
            continue

        if start_time is None:
            start_time = time.perf_counter()

        for msg in messages:
            if msg.error():
                raise KafkaException(msg.error())

            processed += 1

            # Consume + deserialize + filter + map
            try:
                event = json.loads(msg.value())

                truck_id = event.get("truck_id")
                temperature = float(event.get("temperature", 0))

                if not truck_id:
                    continue

                if temperature <= 0 or temperature > 100:
                    continue

                valid += 1

            except (ValueError, TypeError, json.JSONDecodeError):
                continue

        if processed >= next_report:
            elapsed = time.perf_counter() - start_time
            rate = processed / elapsed

            assignment = [
                tp.partition
                for tp in consumer.assignment()
            ]

            print(
                f"[{WORKER_ID}] "
                f"Processed: {processed:,} | "
                f"Valid: {valid:,} | "
                f"Rate: {rate:,.0f} events/sec | "
                f"Partitions: {assignment}"
            )

            # Report every additional 100k
            next_report += REPORT_EVERY

except KeyboardInterrupt:
    print(f"\n[{WORKER_ID}] Stopping...")
finally:
    consumer.close()