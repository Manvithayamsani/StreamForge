from producer.producer import generate_event

def test_generate_event_structure():
    event = generate_event()

    assert "event_id" in event
    assert "event_type" in event
    assert "user_id" in event
    assert "source" in event
    assert "timestamp" in event 
    assert "data" in event 


def test_event_type_is_valid():
    event = generate_event()

    valid_types = [
        "user_login",
        "purchase",
        "payment_success",
        "user_logout"
    ]

    assert event["event_type"] in valid_types     