import json

import pytest

from kinopio_hub_ros.atom.payload_codec import (
    decode_json_utf8,
    decode_text_or_json_utf8,
    encode_json_utf8,
)
from kinopio_hub_ros.errors import ProtocolError


def test_encode_json_utf8_round_trips_objects():
    payload = encode_json_utf8({"message": "hello", "count": 1})
    assert decode_json_utf8(payload) == {"message": "hello", "count": 1}
    assert json.loads(payload.decode("utf-8"))["message"] == "hello"


def test_decode_text_or_json_utf8_matches_sdk_fallback_behavior():
    assert decode_text_or_json_utf8(b"") is None
    assert decode_text_or_json_utf8(b'{"message":"hello"}') == {"message": "hello"}
    assert decode_text_or_json_utf8(b"hello") == "hello"
    assert decode_text_or_json_utf8(b"\xff\xfe") == b"\xff\xfe"


def test_decode_json_utf8_rejects_invalid_json():
    with pytest.raises(ProtocolError, match="valid JSON"):
        decode_json_utf8(b"hello")
