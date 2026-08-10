from processor.faust_processor import TruckTelemetry, is_valid_telemetry


def test_valid_telemetry_is_accepted():
    event = TruckTelemetry(
        truck_id="TRUCK-1001",
        temperature=25.0,
        timestamp="2026-08-10T10:00:00+00:00",
    )

    assert is_valid_telemetry(event) is True


def test_missing_truck_id_is_rejected():
    event = TruckTelemetry(
        truck_id="",
        temperature=25.0,
        timestamp="2026-08-10T10:00:00+00:00",
    ) 

    assert is_valid_telemetry(event) is False


def test_zero_temperature_is_rejected():
    event = TruckTelemetry(
        truck_id="TRUCK-1001",
        temperature=0.0,
        timestamp="2026-08-10T10:00:00+00:00",
    ) 

    assert is_valid_telemetry(event) is False


def test_negative_temperature_is_rejected():
    event = TruckTelemetry(
        truck_id="TRUCK-1001",
        temperature=-5.0,
        timestamp="2026-08-10T10:00:00+00:00",
    )  


def test_temperature_above_100_is_rejected():
    event = TruckTelemetry(
        truck_id="TRUCK-1001",
        temperature=101.0,
        timestamp="2026-08-10T10:00:00+00:00",
    )      

    assert is_valid_telemetry(event) is False
