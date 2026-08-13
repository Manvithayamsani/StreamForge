import time
from datetime import datetime, timezone
import faust
from prometheus_client import start_http_server

from processor.config import (
    BACKEND_URL,
    BENCHMARK_MODE,
    BENCHMARK_REPORT_INTERVAL,
    BROKER,
    METRICS_HOST,
    PROMETHEUS_PORT,
    TOPIC,
    TOPIC_PARTITIONS,
    WINDOW_EXPIRES_SECONDS,
    WINDOW_SIZE_SECONDS,
    WORKER_DATADIR,
    WORKER_HEARTBEAT_SECONDS,
    WORKER_ID,
)
from processor.metrics import (
    ACTIVE_PARTITIONS,
    EVENTS_FILTERED,
    EVENTS_PROCESSED,
    PROCESSING_LAG,
    PROCESSING_RATE,
    WORKER_UP,
)
from processor.worker_registration import start_worker_registration

processed_events = 0
benchmark_start = None

rate_window_start = time.perf_counter()
rate_window_events = 0
RATE_UPDATE_SECONDS = 5


class TruckTelemetry(faust.Record, serializer="json"):
    truck_id: str
    temperature: float
    timestamp: str
    event_timestamp: float = 0.0


def is_valid_telemetry(event: TruckTelemetry) -> bool:
    return (
        bool(event.truck_id)
        and event.temperature > 0
        and event.temperature <= 100
    ) 


app = faust.App(
    "streamforge-faust",
    broker=BROKER,
    store="rocksdb://",
    topic_partitions=TOPIC_PARTITIONS,
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
        expires=WINDOW_EXPIRES_SECONDS,
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
        expires=WINDOW_EXPIRES_SECONDS,
    )
    .relative_to_field(TruckTelemetry.event_timestamp)
)


@app.on_partitions_assigned.connect
async def on_partitions_assigned(sender, assigned, **kwargs):
    input_partitions = {
        tp for tp in assigned
        if tp.topic == TOPIC
    }

    ACTIVE_PARTITIONS.set(len(input_partitions))
    WORKER_UP.set(1)

    print(
        f"[WORKER] StreamForge partitions assigned: "
        f"{sorted(tp.partition for tp in input_partitions)}"
    )


@app.on_partitions_revoked.connect
async def on_partitions_revoked(sender, revoked, **kwargs):
    input_partitions = {
        tp for tp in revoked
        if tp.topic == TOPIC
    }

    ACTIVE_PARTITIONS.set(0)

    print(
        f"[WORKER] StreamForge partitions revoked: "
        f"{sorted(tp.partition for tp in input_partitions)}"
    )


@app.agent(telemetry_topic)
async def process_telemetry(stream):
    global processed_events, benchmark_start
    global rate_window_start, rate_window_events

    async for event in stream:

        # =========================================================
        # RAW BENCHMARK PATH
        # =========================================================
        if BENCHMARK_MODE:

            if benchmark_start is None:
                benchmark_start = time.perf_counter()

            processed_events += 1

            # Keep the actual Filter step
            if not is_valid_telemetry(event):
                continue

            # Keep the actual Map step
            truck_id = event.truck_id
            temperature = float(event.temperature)

            # Do NOT run Prometheus, lag calculation,
            # RocksDB or window state during raw benchmark.
            if processed_events % BENCHMARK_REPORT_INTERVAL == 0:
                elapsed = time.perf_counter() - benchmark_start
                rate = processed_events / elapsed

                print(
                    f"[RAW THROUGHPUT] "
                    f"Processed: {processed_events:,} | "
                    f"Rate: {rate:,.0f} events/sec"
                )

            continue

        # =========================================================
        # NORMAL STREAMFORGE PROCESSING
        # =========================================================

        if benchmark_start is None:
            benchmark_start = time.perf_counter()

        processed_events += 1
        EVENTS_PROCESSED.inc()

        # LIVE PROCESSING RATE
        rate_window_events += 1

        now = time.perf_counter()
        rate_elapsed = now - rate_window_start

        if rate_elapsed >= RATE_UPDATE_SECONDS:
            live_rate = rate_window_events / rate_elapsed

            PROCESSING_RATE.set(live_rate)

            rate_window_events = 0
            rate_window_start = now

        # FILTER
        if not is_valid_telemetry(event):
            EVENTS_FILTERED.inc()
            continue

        # MAP
        truck_id = event.truck_id
        temperature = float(event.temperature)

        # PROCESSING LAG
        if event.event_timestamp > 0:
            event_time = datetime.fromtimestamp(
                event.event_timestamp,
                tz=timezone.utc,
            )

            lag_seconds = max(
                0.0,
                (
                    datetime.now(timezone.utc) - event_time
                ).total_seconds(),
            )

            PROCESSING_LAG.set(lag_seconds)

        # 5-MINUTE WINDOWED STATE
        temperature_sum[truck_id] += temperature
        reading_count[truck_id] += 1

        total = temperature_sum[truck_id].current()
        count = reading_count[truck_id].current()

        if count == 0:
            continue

        average = total / count

        if processed_events % BENCHMARK_REPORT_INTERVAL == 0:
            elapsed = time.perf_counter() - benchmark_start
            rate = processed_events / elapsed

            print(
                f"[THROUGHPUT] "
                f"Processed: {processed_events:,} | "
                f"Rate: {rate:,.0f} events/sec"
            )

if __name__ == "__main__":
    start_http_server(PROMETHEUS_PORT)

    metrics_url = f"http://{METRICS_HOST}:{PROMETHEUS_PORT}/metrics"

    print(
        f"[METRICS] Prometheus metrics available at "
        f"{metrics_url}"
    )

    start_worker_registration(
        worker_id=WORKER_ID,
        metrics_url=metrics_url,
        backend_url=BACKEND_URL,
        heartbeat_seconds=WORKER_HEARTBEAT_SECONDS,
    )

    app.main()