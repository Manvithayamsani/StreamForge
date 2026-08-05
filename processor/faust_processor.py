import os
import time
import faust

BROKER = "kafka://localhost:9092"
TOPIC = "streamforge-events"
WINDOW_SIZE_SECONDS = 300

# Benchmark Mode Switch
BENCHMARK_MODE = False

# Benchmark Throughput Counters
processed_events = 0
benchmark_start = None


class TruckTelemetry(faust.Record, serializer="json"):
    truck_id: str
    temperature: float
    timestamp: str
    event_timestamp: float = 0.0  # Extracted from producer JSON for event-time processing
WORKER_DATADIR = os.getenv(
    "STREAMFORGE_DATADIR",
    "streamforge-faust-data"
)


app = faust.App(
    "streamforge-faust",
    broker=BROKER,
    store="rocksdb://",
    topic_partitions=8,
    datadir=WORKER_DATADIR,
)

telemetry_topic = app.topic(
    TOPIC,
    value_type=TruckTelemetry,
)


temperature_sum = (
    app.Table(
        "temperature_sum",
        default=float,
        options={
            "driver": "rocksdict",
            "max_open_files": 1000,
        },
    )
    .tumbling(
        WINDOW_SIZE_SECONDS,
        expires=86400,
    )
    .relative_to_field(TruckTelemetry.event_timestamp)
)
reading_count = (
    app.Table(
        "reading_count",
        default=int,
        options={
            "driver": "rocksdict",
            "max_open_files": 1000,
        },
    )
    .tumbling(
        WINDOW_SIZE_SECONDS,
        expires=86400,
    )
    .relative_to_field(TruckTelemetry.event_timestamp)
)


@app.agent(telemetry_topic)
async def process_telemetry(stream):
    global processed_events, benchmark_start

    async for event in stream:

        if benchmark_start is None:
            benchmark_start = time.perf_counter()

        processed_events += 1

        # FILTER
        if (
            not event.truck_id
            or event.temperature <= 0
            or event.temperature > 100
        ):
            continue

        # MAP
        truck_id = event.truck_id
        temperature = float(event.temperature)

        # BENCHMARK MODE BYPASS
        if BENCHMARK_MODE:
            if processed_events % 10_000 == 0:
                elapsed = time.perf_counter() - benchmark_start
                rate = processed_events / elapsed

                print(
                    f"[RAW THROUGHPUT] "
                    f"Processed: {processed_events:,} | "
                    f"Rate: {rate:,.0f} events/sec"
                )
            continue

        # WINDOWED STATE
        temperature_sum[truck_id] += temperature
        reading_count[truck_id] += 1

        total = temperature_sum[truck_id].current()
        count = reading_count[truck_id].current()

        # LATE EVENT / EXPIRED WINDOW SAFEGUARD
        if count == 0:
            continue

        average = total / count

        # Keep normal event logging disabled during benchmarking
        # print(
        #     f"Truck: {truck_id} | "
        #     f"Temperature: {temperature:.2f}°C | "
        #     f"Readings: {count} | "
        #     f"Window Average: {average:.2f}°C"
        # )

        # THROUGHPUT REPORT EVERY 10,000 EVENTS
        if processed_events % 10_000 == 0:
            elapsed = time.perf_counter() - benchmark_start
            rate = processed_events / elapsed

            print(
                f"[THROUGHPUT] "
                f"Processed: {processed_events:,} | "
                f"Rate: {rate:,.0f} events/sec"
            )


if __name__ == "__main__":
    app.main()