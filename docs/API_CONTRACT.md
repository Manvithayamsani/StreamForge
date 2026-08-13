# StreamForge API Contract

StreamForge uses FastAPI to expose health, event, metrics, topology, and worker-management endpoints.

## Base URL

```text
http://localhost:8000
```

## Health

### GET /health

Returns the health status of the backend service.

## Events

### GET /events

Returns event information maintained by the backend.

## Metrics Summary

### GET /metrics/summary

Returns aggregated processing metrics collected from StreamForge workers.

Metrics can include:

- Processing rate
- Processing lag
- Active partitions
- Worker availability

## Topology

### GET /topology

Returns information about the current StreamForge processing topology for visualization by the React dashboard.

## Workers

### GET /workers

Returns the currently registered StreamForge workers and their status.

### POST /workers/register

Registers a processing worker with the backend.

Example request:

```json
{
  "worker_id": "worker-a",
  "metrics_url": "http://localhost:9101/metrics"
}
```

### POST /workers/heartbeat

Updates the liveness information of an existing worker.

Example request:

```json
{
  "worker_id": "worker-a"
}
```

Workers periodically send heartbeats so the backend can determine whether they are online or offline.

If the backend restarts and loses its in-memory worker registry, workers automatically attempt registration again.

## Interactive API Documentation

When the FastAPI backend is running, interactive API documentation is available at:

```text
http://localhost:8000/docs
```