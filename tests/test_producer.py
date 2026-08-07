from producer.producer import generate_truck_telemetry

def test_truck_telemetry_structure():
    event = generate_truck_telemetry()

    assert "truck_id" in event 
    assert "temperature" in event
    assert "timestamp" in event
    assert "event_timestamp" in event

def test_truck_id_format():
    event = generate_truck_telemetry()

    assert event["truck_id"].startswith("TRUCK-") 

def test_temperature_range():
    event = generate_truck_telemetry() 

    assert -5.0 <= event["temperature"] <= 45.0

def test_event_timestamp_is_float():
    event = generate_truck_telemetry()

    assert isinstance(event["event_timestamp"], float)

                       