# StreamForge

A distributed real-time event streaming and processing platform built with Apache Kafka, Faust, RocksDB, FastAPI, Prometheus, and React.

## Overview

StreamForge demonstrates a scalable event-streaming architecture capable of ingesting, processing, aggregating, monitoring, and recovering real-time telemetry data.

The current implementation uses simulated truck telemetry events containing a truck identifier, temperature, timestamp, and event-time timestamp.

Events are published to an 8-partition Apache Kafka topic and distributed across multiple Faust processing workers. Workers perform validation, transformation, event-time windowed aggregation, persistent state management, and expose Prometheus metrics for observability.

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