from consumer.consumer import deserialize_message 


def test_json_message_deserialization():
    message = b'{"event_id":"123","type":"login"}'

    result = deserialize_message(message) 

    assert result["event_id"] == "123"


def test_plain_text_message_deserialization():
    message = b"hello kafka"

    result = deserialize_message(message)

    assert result["type"] == "plain-text"
    assert result["message"] == "hello kafka" 


def test_invalid_message_is_handled():

    message = b"invalid kafka message"

    result = deserialize_message(message)

    assert result["type"] == "plain-text"
    assert result["message"] == "invalid kafka message"

           