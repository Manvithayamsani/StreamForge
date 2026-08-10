from datetime import datetime, timezone

from processor.stream_processor import get_window_start


def test_get_window_start():
    timestamp = "2026-08-10T10:03:00+00:00"

    result = get_window_start(timestamp)

    assert result == datetime(
        2026,
        8,
        10,
        10,
        0,
        0,
        tzinfo=timezone.utc,
    )


def test_get_window_start_for_later_timestamp():
    timestamp = "2026-08-10T10:07:30+00:00"

    result = get_window_start(timestamp)

    assert result == datetime(
        2026,
        8,
        10,
        10,
        5,
        0,
        tzinfo=timezone.utc
    )    

