import json
import sys
import urllib.request


BACKEND = "http://localhost:8000"
WORKER_METRICS = {
    "worker-a": "http://localhost:9101/metrics",
    "worker-b": "http://localhost:9102/metrics",
}


def pass_check(name, detail=""):
    suffix = f" — {detail}" if detail else ""
    print(f"[PASS] {name}{suffix}")


def fail_check(name, detail=""):
    suffix = f" — {detail}" if detail else ""
    print(f"[FAIL] {name}{suffix}")


def get_json(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode())


def get_text(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.read().decode()


def main():
    failures = 0

    print()
    print("=" * 58)
    print("             STREAMFORGE SYSTEM VERIFICATION")
    print("=" * 58)

    # ---------------------------------------------------------
    # Backend
    # ---------------------------------------------------------
    try:
        health = get_json(f"{BACKEND}/health")

        if health.get("status") == "healthy":
            pass_check("FastAPI backend", "healthy")
        else:
            fail_check("FastAPI backend", str(health))
            failures += 1

    except Exception as error:
        fail_check("FastAPI backend", str(error))
        failures += 1

    # ---------------------------------------------------------
    # Topology / workers / partitions
    # ---------------------------------------------------------
    try:
        topology = get_json(f"{BACKEND}/topology")
        summary = topology.get("summary", {})

        workers = int(summary.get("workers_online", 0))
        partitions = int(summary.get("active_partitions", 0))

        if workers >= 2:
            pass_check("Distributed workers", f"{workers} online")
        else:
            fail_check("Distributed workers", f"{workers} online")
            failures += 1

        if partitions == 8:
            pass_check("Kafka partition assignment", "8/8 active")
        else:
            fail_check(
                "Kafka partition assignment",
                f"{partitions}/8 active",
            )
            failures += 1

    except Exception as error:
        fail_check("Topology API", str(error))
        failures += 1

    # ---------------------------------------------------------
    # Aggregations
    # ---------------------------------------------------------
    try:
        aggregations = get_json(f"{BACKEND}/aggregations")

        if aggregations:
            pass_check(
                "5-minute window aggregations",
                f"{len(aggregations)} available",
            )

            valid_math = all(
                abs(
                    float(item["average_temperature"])
                    - (
                        float(item["temperature_sum"])
                        / int(item["reading_count"])
                    )
                ) < 0.0001
                for item in aggregations
                if int(item["reading_count"]) > 0
            )

            if valid_math:
                pass_check("Aggregation mathematics")
            else:
                fail_check("Aggregation mathematics")
                failures += 1

        else:
            fail_check("5-minute window aggregations", "no data")
            failures += 1

    except Exception as error:
        fail_check("Aggregation API", str(error))
        failures += 1

    # ---------------------------------------------------------
    # Prometheus
    # ---------------------------------------------------------
    required_metrics = [
        "streamforge_events_processed_total",
        "streamforge_processing_rate",
        "streamforge_worker_up",
        "streamforge_processing_lag",
        "streamforge_active_partitions",
    ]

    for worker, url in WORKER_METRICS.items():
        try:
            metrics = get_text(url)

            missing = [
                metric
                for metric in required_metrics
                if metric not in metrics
            ]

            if not missing:
                pass_check(
                    f"Prometheus export ({worker})"
                )
            else:
                fail_check(
                    f"Prometheus export ({worker})",
                    f"missing: {', '.join(missing)}",
                )
                failures += 1

        except Exception as error:
            fail_check(
                f"Prometheus export ({worker})",
                str(error),
            )
            failures += 1

    # ---------------------------------------------------------
    # Final result
    # ---------------------------------------------------------
    print("=" * 58)

    if failures == 0:
        print("FINAL RESULT: PASS")
        print("StreamForge core system is operational.")
        print("=" * 58)
        return 0

    print(f"FINAL RESULT: FAIL ({failures} checks failed)")
    print("=" * 58)

    return 1


if __name__ == "__main__":
    sys.exit(main())