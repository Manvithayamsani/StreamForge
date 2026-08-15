# ⚡ StreamForge

### Distributed Real-Time Event Streaming & Processing Platform

StreamForge is a distributed real-time event streaming and processing platform built with **Apache Kafka, Faust, RocksDB, FastAPI, Prometheus, and React**.

It demonstrates high-throughput event ingestion, distributed stream processing, stateful event-time aggregation, fault tolerance, observability, and real-time visualization using simulated truck telemetry.

---

## 📌 Overview

StreamForge demonstrates a scalable event-streaming architecture capable of ingesting, processing, aggregating, monitoring, and recovering real-time telemetry data.

The current implementation uses simulated truck telemetry events containing:

- Truck identifier
- Temperature
- Timestamp
- Event-time timestamp

Events are published to an **8-partition Apache Kafka topic** and distributed across multiple Faust processing workers.

Workers perform:

- Event validation
- Filtering
- Transformation
- Event-time processing
- 5-minute windowed aggregation
- Persistent state management
- Processing-lag calculation
- Prometheus metrics export

The processed information is exposed through a FastAPI backend and visualized using a React monitoring dashboard.

---

## 🏗️ Architecture

```text
Telemetry Producer
       |
       v
Apache Kafka
streamforge-events
8 partitions
       |
       v
Faust Consumer Group
streamforge-faust
       |
       +-------------------+
       |                   |
       v                   v
   Worker A            Worker B
   Partitions          Partitions
       |                   |
       +---------+---------+
                 |
                 v
        Stateful Processing
       Filter -> Map -> Window
                 |
                 v
        RocksDB Local State
                 |
                 v
       Kafka Changelog Topics
                 |
          +------+------+
          |             |
          v             v
      Prometheus     FastAPI
       Metrics        Backend
                         |
                         v
                  React Dashboard
```

---

## 🔄 Processing Pipeline

Each telemetry event passes through the following processing stages:

```text
Kafka Event
    |
    v
Consume
    |
    v
Validate / Filter
    |
    v
Map / Transform
    |
    v
Event-Time Processing
    |
    v
5-Minute Tumbling Window
    |
    v
Update Stateful Aggregation
    |
    v
RocksDB
```

Each valid telemetry event contributes to a five-minute event-time window associated with its truck.

---

## 📊 Windowed Aggregations

For every truck and five-minute window, StreamForge maintains:

- Temperature sum
- Reading count
- Average temperature
- Window start
- Window end
- Processing worker

The average temperature is calculated as:

```text
Average Temperature = Temperature Sum / Reading Count
```

### Example

```text
Readings: 20°C, 30°C, 40°C

Temperature Sum = 90
Reading Count   = 3

Average = 90 / 3
        = 30°C
```

Aggregated results are published to the FastAPI backend and displayed in the React dashboard.

---

## ⏱️ Event-Time Processing

StreamForge performs window calculations using the timestamp associated with the event rather than simply relying on the time at which the worker receives it.

This allows telemetry events to be assigned to their appropriate five-minute processing windows.

The implementation was also validated using late-arriving events to ensure that event-time window assignment behaves correctly.

---

## ⚙️ Distributed Processing

The `streamforge-events` Kafka topic contains **8 partitions**.

Multiple Faust workers operate within the same `streamforge-faust` consumer group.

Kafka distributes topic partitions among the available workers, enabling processing to occur in parallel.

Example:

```text
                 Kafka
                   |
       +-----------+-----------+
       |                       |
       v                       v

    Worker A                Worker B

 Partition 0             Partition 4
 Partition 1             Partition 5
 Partition 2             Partition 6
 Partition 3             Partition 7
```

The exact partition ownership can change whenever Kafka performs consumer-group rebalancing.

If a worker becomes unavailable, Kafka can reassign its partitions to another available worker.

This enables StreamForge to scale horizontally by adding additional processing workers.

---

## 💾 Stateful Processing

StreamForge performs stateful window aggregation using **RocksDB**.

For each active truck/window combination, processing state includes values such as:

```text
temperature_sum
reading_count
```

These values allow the average temperature to be updated incrementally without recalculating every previous event.

---

## 🔁 State Recovery

Faust changelog topics provide recoverable state for the window tables.

This allows another worker to restore processing state when partition ownership changes.

State recovery was validated through a controlled worker-failure experiment.

### Before Worker Failure

```text
20°C + 30°C

Sum     = 50
Count   = 2
Average = 25°C
```

The worker responsible for the partition was then terminated.

Kafka reassigned the partition to the surviving worker.

Another event was produced:

```text
40°C
```

### Recovered Result

```text
Sum     = 90
Count   = 3
Average = 30°C
```

The aggregation continued from the previous state rather than restarting from zero.

This validates state recovery across worker failure and partition reassignment.

---

## 🛡️ Fault Tolerance

Fault tolerance was tested by deliberately terminating a processing worker while stateful processing was active.

The experiment demonstrated:

1. Worker failure detection
2. Kafka consumer-group rebalancing
3. Partition reassignment
4. Stateful recovery
5. Continued processing after recovery

This demonstrates StreamForge's ability to tolerate an individual processing-worker failure.

---

## 📡 Prometheus Observability

Each processing worker exposes a Prometheus-compatible metrics endpoint.

StreamForge exports metrics including:

```text
streamforge_events_processed_total
streamforge_events_filtered_total
streamforge_processing_rate
streamforge_worker_up
streamforge_active_partitions
streamforge_processing_lag
streamforge_windows_closed_total
streamforge_active_windows
```

These metrics provide visibility into:

- Worker health
- Events processed
- Events filtered
- Processing throughput
- Kafka partition ownership
- Processing lag
- Window activity

The FastAPI backend collects worker-level metrics and produces cluster-level information for the frontend.

---

## 🖥️ React Monitoring Dashboard

StreamForge includes a React-based real-time monitoring dashboard.

The dashboard displays:

- Workers online
- Events processed
- Processing rate
- Active Kafka partitions
- Processing lag
- Events filtered
- System health
- Bottleneck warnings
- Processing topology
- Worker-level analytics
- Live five-minute window aggregations

### Pipeline Visualization

React Flow is used to visualize the distributed processing topology.

Conceptually:

```text
Producer
   |
   v
Kafka
   |
   +----------+
   |          |
   v          v
Worker A   Worker B
   |          |
   +----+-----+
        |
        v
Window Processing
        |
        v
RocksDB
```

The dashboard continuously retrieves updated system information from the FastAPI backend.

---

## 🚨 Bottleneck Detection

StreamForge monitors processing lag to provide a simplified representation of cluster health.

The dashboard can represent conditions such as:

```text
HEALTHY
WARNING
BOTTLENECK
CRITICAL
```

This provides immediate visibility into situations such as:

- No processing workers online
- Elevated processing lag
- Large processing backlog
- Normal pipeline operation

---

## 🚀 Performance Validation

StreamForge was load-tested using a dedicated benchmark workload.

### Benchmark Configuration

```text
Benchmark events : 500,000
Kafka partitions : 8
Workers          : 2
Required target  : 100,000+ events/sec
```

A dedicated benchmark producer was used to rapidly publish events into Kafka.

During testing, producer throughput exceeded:

```text
350,000 events/sec
```

The distributed benchmark workers demonstrated high-rate parallel consumption across the eight Kafka partitions.

The benchmark validated that the architecture can satisfy the project's required **100,000+ events/sec** processing target.

Benchmark workloads are kept separate from normal system verification so that synthetic load does not interfere with regular telemetry processing.

---

## 🧪 Automated Testing

StreamForge includes an automated Pytest test suite.

Run:

```bash
python -m pytest -q
```

Verified result:

```text
34 passed
```

Dependency deprecation warnings may be displayed by the Faust/mode dependency stack, but they do not represent failed StreamForge tests.

---

## ✅ Live System Verification

In addition to automated tests, StreamForge provides a live-system verification utility.

Run:

```bash
python scripts/verify_streamforge.py
```

The verifier checks the actual running distributed system.

Example successful verification:

```text
==========================================================
             STREAMFORGE SYSTEM VERIFICATION
==========================================================
[PASS] FastAPI backend — healthy
[PASS] Distributed workers — 2 online
[PASS] Kafka partition assignment — 8/8 active
[PASS] 5-minute window aggregations
[PASS] Aggregation mathematics
[PASS] Prometheus export (worker-a)
[PASS] Prometheus export (worker-b)
==========================================================
FINAL RESULT: PASS
StreamForge core system is operational.
==========================================================
```

This provides a single health check for the core distributed platform.

---

## 🧰 Technology Stack

| Component | Technology |
|---|---|
| Event Streaming | Apache Kafka |
| Stream Processing | Faust |
| State Management | RocksDB |
| Backend API | FastAPI |
| Observability | Prometheus |
| Frontend | React |
| Pipeline Visualization | React Flow |
| Containerization | Docker |
| Testing | Pytest |
| Languages | Python, JavaScript |

---

## 📁 Project Structure

```text
StreamForge/
│
├── backend/
│   ├── routes/
│   ├── aggregation_store.py
│   ├── main.py
│   ├── metrics_service.py
│   └── worker_registry.py
│
├── processor/
│   ├── aggregation_publisher.py
│   ├── faust_processor.py
│   └── throughput_worker.py
│
├── producer/
│   ├── producer.py
│   └── benchmark_producer.py
│
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   ├── pages/
│   │   └── services/
│   └── package.json
│
├── scripts/
│   └── verify_streamforge.py
│
├── tests/
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

> The structure above highlights the major StreamForge components; additional supporting files may also exist in the repository.

---

# 🚀 Setup and Running StreamForge

## Prerequisites

Before running StreamForge, install:

- Python
- Node.js and npm
- Docker Desktop
- Git

---

## 1. Clone the Repository

```bash
git clone <https://github.com/Manvithayamsani/StreamForge>
cd StreamForge
```


---

## 2. Create a Python Virtual Environment

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

---

## 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

## 4. Start Kafka

StreamForge uses Docker for the local Kafka environment.

From the project root:

```bash
docker compose up -d
```

Verify the containers:

```bash
docker ps
```

---

## 5. Start the FastAPI Backend

Open a new PowerShell terminal in the project root.

Activate the virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Start FastAPI:

```powershell
uvicorn backend.main:app --reload --port 8000
```

Backend:

```text
http://localhost:8000
```

---

## 6. Start Worker A

Open another PowerShell terminal in the project root.

```powershell
.\.venv\Scripts\Activate.ps1

$env:STREAMFORGE_WORKER_ID="worker-a"
$env:STREAMFORGE_DATADIR="streamforge-faust-worker-a-data"
$env:STREAMFORGE_METRICS_PORT="9101"
$env:STREAMFORGE_BENCHMARK="false"

python -m processor.faust_processor worker -l info
```

---

## 7. Start Worker B

Open another PowerShell terminal in the project root.

```powershell
.\.venv\Scripts\Activate.ps1

$env:STREAMFORGE_WORKER_ID="worker-b"
$env:STREAMFORGE_DATADIR="streamforge-faust-worker-b-data"
$env:STREAMFORGE_METRICS_PORT="9102"
$env:STREAMFORGE_BENCHMARK="false"

python -m processor.faust_processor worker -l info --web-port 6067
```

After both workers join the same consumer group, Kafka distributes the eight input partitions between them.

---

## 8. Start the React Frontend

Open another terminal:

```powershell
cd frontend
npm install
npm run dev
```

Dashboard:

```text
http://localhost:5173
```

`npm install` is only required initially or when frontend dependencies change.

---

## 9. Start the Telemetry Producer

Open another terminal in the project root:

```powershell
.\.venv\Scripts\Activate.ps1
python -m producer.producer
```

The producer generates simulated truck telemetry and publishes events to:

```text
streamforge-events
```

Stop the producer with:

```text
Ctrl+C
```

when sufficient telemetry has been generated.

---

## 10. Verify StreamForge

Run:

```bash
python -m pytest -q
```

Then:

```bash
python scripts/verify_streamforge.py
```

A healthy running system should finish with:

```text
FINAL RESULT: PASS
StreamForge core system is operational.
```

---

## 🌐 Local Service Ports

| Service | Port |
|---|---:|
| Kafka | 9092 |
| FastAPI | 8000 |
| React Dashboard | 5173 |
| Worker A Prometheus | 9101 |
| Worker B Prometheus | 9102 |
| Worker B Faust Web | 6067 |

---

## 🔌 Useful API Endpoints

```text
GET http://localhost:8000/health
GET http://localhost:8000/events
GET http://localhost:8000/topology
GET http://localhost:8000/metrics-summary
GET http://localhost:8000/workers
GET http://localhost:8000/aggregations
```

### Worker Prometheus Metrics

```text
http://localhost:9101/metrics
http://localhost:9102/metrics
```

---

## 📸 Project Evidence

The following evidence can be captured for demonstration and evaluation:

- Live StreamForge dashboard
- Two-worker distributed topology
- Kafka 8/8 partition assignment
- Five-minute window aggregations
- Prometheus worker metrics
- Automated test result
- Unified system verification result
- 500,000-event benchmark
- Worker failover and Kafka rebalancing
- Stateful recovery demonstration

Recommended repository organization:

```text
docs/
└── evidence/
    ├── dashboard.png
    ├── system-verification.png
    ├── partition-assignment.png
    ├── prometheus-worker-a.png
    ├── prometheus-worker-b.png
    ├── throughput-benchmark.png
    ├── worker-failover.png
    └── state-recovery.png
```

Once screenshots are added, they can also be embedded directly into this README.

---

## 🎯 Project Capabilities

StreamForge demonstrates:

- Distributed event streaming
- Kafka partition-based parallelism
- Multi-worker stream processing
- Event filtering and transformation
- Event-time semantics
- Five-minute tumbling windows
- Stateful aggregation
- RocksDB-backed local state
- Kafka changelog-based recovery
- Consumer-group rebalancing
- Worker failure recovery
- Processing-lag monitoring
- Prometheus observability
- FastAPI-based cluster APIs
- Real-time React visualization
- Automated system verification
- High-throughput performance validation

---

## 📈 Current Status

### StreamForge Core Implementation: Complete

- ✅ 34 automated tests passing
- ✅ Live-system verification passing
- ✅ FastAPI backend verified
- ✅ 2 distributed workers verified
- ✅ 8/8 Kafka partitions verified
- ✅ Five-minute event-time aggregation verified
- ✅ Aggregation mathematics verified
- ✅ RocksDB-backed state processing implemented
- ✅ Worker failover verified
- ✅ Kafka partition rebalancing verified
- ✅ Stateful recovery verified
- ✅ Prometheus metrics verified on both workers
- ✅ React monitoring dashboard operational
- ✅ Production frontend build verified
- ✅ High-throughput benchmark performed

---

## 🔮 Future Enhancements

Potential extensions include:

- Kubernetes-based worker deployment
- Automatic horizontal worker scaling
- Schema Registry integration
- Persistent aggregation database
- Grafana dashboards
- Distributed tracing
- Alerting based on processing lag
- Additional telemetry event types
- Cloud-managed Kafka deployment
- CI/CD pipeline integration

---

## 📄 License

This project is intended for educational, demonstration, and portfolio purposes.