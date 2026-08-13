from datetime import datetime, timezone
import pytest
from processor.state_store import StateStore


def test_rocksdb_state_persistence_and_recovery(tmp_path, monkeypatch):
    test_db_dir = tmp_path / "test_streamforge_state"
    monkeypatch.setattr("processor.state_store.DB_PATH", test_db_dir)

    # 1. Open RocksDB state store and save a window state
    store1 = StateStore()
    truck_id = "TRUCK-TEST-99"
    window_start = datetime(2026, 8, 10, 10, 0, 0, tzinfo=timezone.utc)
    stats = {
        "temperature_sum": 120.0,
        "reading_count": 4,
    }

    store1.save_window(truck_id, window_start, stats)
    store1.close()

    # 2. Reopen RocksDB state store and verify state is recovered correctly
    store2 = StateStore()
    recovered_windows = store2.load_windows()

    assert len(recovered_windows) == 1
    recovered = recovered_windows[0]

    assert recovered["truck_id"] == truck_id
    assert recovered["window_start"] == window_start.isoformat()
    assert recovered["temperature_sum"] == 120.0
    assert recovered["reading_count"] == 4

    # 3. Delete window from state store and confirm removal
    store2.delete_window(truck_id, window_start)
    assert len(store2.load_windows()) == 0

    store2.close()