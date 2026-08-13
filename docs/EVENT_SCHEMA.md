# StreamForge Event Schema

This document defines the telemetry event structure used by StreamForge producers, Apache Kafka, Faust processors, and tests.

## Event Format

```json
{
  "truck_id": "TRUCK-1001",
  "temperature": 24.5,
  "timestamp": "2026-08-13T12:00:00+00:00",
  "event_timestamp": 1786622400.0
}
```

## Fields

### truck_id

Unique identifier of the truck producing the telemetry event.

Example:

```text
TRUCK-1001
```

### temperature

Temperature reading associated with the truck.

Valid range:

```text
-50 <= temperature <= 100
```

Events outside this range are rejected by the processing pipeline.

### timestamp

ISO-8601 UTC timestamp representing when the telemetry event was generated.

### event_timestamp

UTC Unix timestamp represented as a floating-point number.

Faust uses this field for event-time window assignment.

## Kafka Topic

Telemetry events are published to:

```text
streamforge-events
```

The topic contains 8 partitions, allowing events to be processed across multiple workers.

## Processing Pipeline

Valid events pass through the following pipeline:

```text
Kafka
  |
  v
Consume
  |
  v
Filter
  |
  v
Map
  |
  v
5-Minute Event-Time Window
  |
  v
RocksDB State
```

Invalid telemetry is filtered before windowed aggregation.

## Windowed Aggregation

For each truck and 5-minute window, StreamForge maintains:

- Temperature sum
- Reading count

The average temperature is calculated as:

```text
average = temperature_sum / reading_count
```

Window state is persisted through Faust state tables backed by RocksDB and Kafka changelog topics.