# StreamForge Event Schema

This document defines the standard JSON structure used by producers, Kafka topics, consumers, the backend, and the database.

## Event Format

```json
{
  "event_id": "evt_001",
  "event_type": "user_activity",
  "source": "web_application",
  "message": "User logged into the application",
  "timestamp": "2026-07-29T18:00:00Z",
  "metadata": {
    "user_id": "user_101"
  }
}