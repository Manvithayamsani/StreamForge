from fastapi.testclient import TestClient

from backend.main import app


client = TestClient(app)


def test_root_endpoint():
    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {
        "message": "Welcome to StreamForge API",
        "status": "running",
    }


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "StreamForge API"
    }


def test_events_endpoint():
    response = client.get("/events")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_metrics_summary_endpoint():
    response = client.get("/metrics-summary")

    assert response.status_code == 200

    data = response.json()

    assert isinstance(data, dict)
    assert "summary" in data
    assert "workers" in data


def test_topology_endpoint():
    response = client.get("/topology")

    assert response.status_code == 200

    data = response.json()

    assert isinstance(data, dict)
    assert "pipeline" in data
    assert "summary" in data

    assert isinstance(data["pipeline"], list)

    pipeline_types = [
        node["type"]
        for node in data["pipeline"]
    ]

    assert "producer" in pipeline_types
    assert "broker" in pipeline_types


def test_worker_registration():
    response = client.post(
        "/workers/register",
        json={
            "worker_id": "test-worker-1",
            "metrics_url": "http://localhost:8001/metrics",
        },
    )

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "registered"
    assert data["worker"]["worker_id"] == "test-worker-1"
    assert data["worker"]["metrics_url"] == "http://localhost:8001/metrics"


def test_worker_heartbeat():
    response = client.post(
        "/workers/heartbeat",
        json={
            "worker_id": "test-worker-1",
        },
    )

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "alive"
    assert data["worker_id"] == "test-worker-1"


def test_heartbeat_for_unknown_worker():
    response = client.post(
        "/workers/heartbeat",
        json={
            "worker_id": "unknown-worker",
        },
    )

    assert response.status_code == 404

    data = response.json()

    assert data["detail"] == "Worker is not registered"


def test_list_workers():
    response = client.get("/workers")

    assert response.status_code == 200

    data = response.json()

    assert "workers" in data
    assert isinstance(data["workers"], list)

    worker_ids = [
        worker["worker_id"]
        for worker in data["workers"]
    ]

    assert "test-worker-1" in worker_ids


