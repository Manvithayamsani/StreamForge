import os

# Kafka
BROKER = os.getenv("STREAMFORGE_BROKER", "kafka://127.0.0.1:9092")
TOPIC = os.getenv("STREAMFORGE_TOPIC", "streamforge-events")
TOPIC_PARTITIONS = int(os.getenv("STREAMFORGE_PARTITIONS", "8"))
PROMETHEUS_PORT = int(
    os.getenv("STREAMFORGE_METRICS_PORT", "9101")
)

# Windowing
WINDOW_SIZE_SECONDS = 300
WINDOW_EXPIRES_SECONDS = 86400

# Faust / RocksDB
WORKER_DATADIR = os.getenv(
    "STREAMFORGE_DATADIR",
    "streamforge-faust-data",
)

# Worker Settings
WORKER_ID = os.getenv(
    "STREAMFORGE_WORKER_ID",
    "worker-a",
)
METRICS_HOST = os.getenv(
    "STREAMFORGE_METRICS_HOST",
    "localhost",
)
BACKEND_URL = os.getenv(
    "STREAMFORGE_BACKEND_URL",
    "http://localhost:8000",
)
WORKER_HEARTBEAT_SECONDS = 5

# Benchmarking
BENCHMARK_MODE = (
    os.getenv("STREAMFORGE_BENCHMARK", "false").lower() == "true"
)
BENCHMARK_REPORT_INTERVAL = 10_000