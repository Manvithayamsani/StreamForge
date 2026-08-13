# StreamForge Project Architecture

## Overview

StreamForge is a distributed real-time telemetry processing platform built with Apache Kafka, Faust, RocksDB, FastAPI, Prometheus, and React.

## Architecture

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
       |                   |
       +---------+---------+
                 |
                 v
        Filter -> Map -> Window
                 |
                 v
       5-Minute Event-Time State
                 |
                 v
             RocksDB
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

## Processing Model

Each Faust worker belongs to the same Kafka consumer group. Kafka automatically distributes input partitions across available workers.

When a worker fails, Kafka rebalances the partitions to surviving workers.

Window state is maintained locally using RocksDB and backed by Kafka changelog topics for recovery.

## Stateful Processing

StreamForge maintains two windowed state tables:

- `temperature_sum`
- `reading_count`

Both use 5-minute tumbling event-time windows.

For each truck, StreamForge calculates:

```text
Average Temperature = Temperature Sum / Reading Count
```

## Partitioning and Scaling

The `streamforge-events` topic contains 8 partitions.

Multiple Faust workers can consume from the same consumer group, allowing Kafka to distribute partitions between workers automatically.

This enables horizontal processing and automatic partition reassignment when workers join or leave.

## Fault Tolerance

The architecture supports:

- Kafka consumer-group rebalancing
- Worker failure detection
- Automatic partition reassignment
- RocksDB local state
- Kafka changelog-backed state recovery
- Worker heartbeat registration

Failover testing verified that stateful window processing can continue after partition ownership moves between workers.

## Observability

Workers expose Prometheus metrics including:

- Events processed
- Events filtered
- Processing rate
- Processing lag
- Active partitions
- Worker availability

The FastAPI backend provides worker and topology information to the React dashboard.

## Benchmarking

StreamForge includes a benchmark mode for measuring raw event-processing throughput.

Benchmark mode isolates the core Filter and Map processing path from RocksDB, Prometheus, and window-state overhead, making bottlenecks easier to identify.