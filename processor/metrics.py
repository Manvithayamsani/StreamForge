from prometheus_client import Counter, Gauge


EVENTS_PROCESSED = Counter(
    "streamforge_events_processed_total",
    "Total number of telemetry events processed by this worker",
)

EVENTS_FILTERED = Counter(
    "streamforge_events_filtered_total",
    "Total number of telemetry events filtered by this worker",
)

PROCESSING_RATE = Gauge(
    "streamforge_processing_rate",
    "Current processing throughput in events per second",
)

WORKER_UP = Gauge(
    "streamforge_worker_up",
    "Worker health status: 1 means running",
)

ACTIVE_PARTITIONS = Gauge(
    "streamforge_active_partitions",
    "Number of Kafka partitions currently assigned to this worker",
)

PROCESSING_LAG = Gauge(
    "streamforge_processing_lag",
    "Approximate processing lag in seconds based on event timestamp",
)

WINDOWS_CLOSED = Counter(
    "streamforge_windows_closed_total",
    "Total number of completed windows",
)

ACTIVE_WINDOWS = Gauge(
    "streamforge_active_windows",
    "Current number of active windows",
)
