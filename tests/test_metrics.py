from processor.metrics import (
    ACTIVE_PARTITIONS,
    ACTIVE_WINDOWS, 
    EVENTS_FILTERED,
    EVENTS_PROCESSED,
    PROCESSING_LAG,
    PROCESSING_RATE,
    WINDOWS_CLOSED,
    WORKER_UP,
)


def test_metrics_are_defined():
    assert EVENTS_PROCESSED is not None
    assert EVENTS_FILTERED is not None
    assert PROCESSING_RATE is not None
    assert WORKER_UP is not None
    assert ACTIVE_PARTITIONS is not None
    assert PROCESSING_LAG is not None
    assert WINDOWS_CLOSED is not None
    assert ACTIVE_WINDOWS is not None


def test_gauge_metrics_can_be_updated():
    WORKER_UP.set(1)
    ACTIVE_PARTITIONS.set(2)
    ACTIVE_WINDOWS.set(3)
    PROCESSING_RATE.set(100.0)
    PROCESSING_LAG.set(5.0)

    assert WORKER_UP._value.get() == 1
    assert ACTIVE_PARTITIONS._value.get() == 2
    assert ACTIVE_WINDOWS._value.get() == 3
    assert PROCESSING_RATE._value.get() == 100.0
    assert PROCESSING_LAG._value.get() == 5.0
