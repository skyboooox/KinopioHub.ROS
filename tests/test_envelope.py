from datetime import datetime, timezone

import pytest

from kinopio_hub_ros.business.envelope import (
    ENVELOPE_SCHEMA,
    LEGACY_TEXT_ENVELOPE_SCHEMA,
    build_message_envelope,
    build_text_envelope,
    decode_envelope,
    encode_envelope,
)
from kinopio_hub_ros.errors import ProtocolError


def test_message_envelope_round_trip_preserves_fields():
    envelope = build_message_envelope(
        direction="ros_to_nats",
        topic="/odom",
        subject="ros.odom",
        data={"kind": "plain"},
        bridge_id="ubuntu22-ros-bridge",
        sequence=7,
        ros_version=2,
        ros_distro="humble",
        ros_message_type="nav_msgs/msg/Odometry",
        timestamp=datetime(2026, 5, 14, 3, 4, 5, 123456, tzinfo=timezone.utc),
    )

    decoded = decode_envelope(encode_envelope(envelope))

    assert decoded.schema == ENVELOPE_SCHEMA
    assert decoded.direction == "ros_to_nats"
    assert decoded.topic == "/odom"
    assert decoded.subject == "ros.odom"
    assert decoded.text == '{\n  "kind": "plain"\n}'
    assert decoded.json_value == {"kind": "plain"}
    assert decoded.ros.version == 2
    assert decoded.ros.distro == "humble"
    assert decoded.ros.message_type == "nav_msgs/msg/Odometry"
    assert decoded.meta.bridge_id == "ubuntu22-ros-bridge"
    assert decoded.meta.sequence == 7
    assert decoded.stamp.sec == 1778727845
    assert decoded.stamp.nanosec == 123456000
    assert decoded.stamp.iso == "2026-05-14T03:04:05.123456Z"


def test_text_envelope_round_trip_preserves_legacy_shape():
    envelope = build_text_envelope(
        direction="ros_to_nats",
        topic="/chatter",
        subject="ros.chatter",
        text="hello",
        bridge_id="bridge-a",
        sequence=9,
        ros_version=2,
        ros_distro="humble",
        ros_message_type="std_msgs/msg/String",
    )

    decoded = decode_envelope(encode_envelope(envelope))

    assert decoded.schema == LEGACY_TEXT_ENVELOPE_SCHEMA
    assert decoded.text == "hello"
    assert decoded.json_value is None


def test_encode_envelope_accepts_legacy_mapping_payload():
    payload = encode_envelope(
        {
            "schema": LEGACY_TEXT_ENVELOPE_SCHEMA,
            "direction": "nats_to_ros",
            "topic": "/chatter",
            "subject": "ros.chatter",
            "ros": {"version": 1, "distro": "noetic", "type": "std_msgs/String"},
            "stamp": {
                "source": "bridge",
                "sec": 1,
                "nanosec": 2,
                "iso": "1970-01-01T00:00:01.000002Z",
            },
            "data": {"text": "write back", "json": {"value": "write back"}},
            "meta": {"bridgeId": "bridge-a", "sequence": 9},
        }
    )

    decoded = decode_envelope(payload)

    assert decoded.schema == LEGACY_TEXT_ENVELOPE_SCHEMA
    assert decoded.direction == "nats_to_ros"
    assert decoded.text == "write back"
    assert decoded.json_value == {"value": "write back"}


def test_decode_envelope_rejects_invalid_schema():
    with pytest.raises(ProtocolError, match="schema"):
        decode_envelope(
            b'{"schema":"wrong","direction":"ros_to_nats","topic":"/x","subject":"ros.x","ros":{"version":2,"distro":"humble","type":"std_msgs/msg/String"},"stamp":{"source":"bridge","sec":0,"nanosec":0,"iso":"1970-01-01T00:00:00.000000Z"},"data":{"text":"x"},"meta":{"bridgeId":"b","sequence":1}}'
        )


def test_decode_legacy_envelope_rejects_missing_text_field():
    with pytest.raises(ProtocolError, match="data.text"):
        decode_envelope(
            b'{"schema":"kinopio.ros.text.v1","direction":"ros_to_nats","topic":"/x","subject":"ros.x","ros":{"version":2,"distro":"humble","type":"std_msgs/msg/String"},"stamp":{"source":"bridge","sec":0,"nanosec":0,"iso":"1970-01-01T00:00:00.000000Z"},"data":{},"meta":{"bridgeId":"b","sequence":1}}'
        )


def test_decode_message_envelope_uses_direct_data_object():
    decoded = decode_envelope(
        b'{"schema":"kinopio.ros.message.v1","direction":"nats_to_ros","topic":"/odom","subject":"ros.odom","ros":{"version":2,"distro":"humble","type":"nav_msgs/msg/Odometry"},"stamp":{"source":"bridge","sec":0,"nanosec":0,"iso":"1970-01-01T00:00:00.000000Z"},"data":{"pose":{"position":{"x":1.0}}},"meta":{"bridgeId":"b","sequence":1}}'
    )

    assert decoded.schema == ENVELOPE_SCHEMA
    assert decoded.json_value == {"pose": {"position": {"x": 1.0}}}
    assert '"pose"' in decoded.text
    assert '"x": 1.0' in decoded.text


def test_decode_envelope_can_rebuild_text_from_json_field():
    decoded = decode_envelope(
        b'{"schema":"kinopio.ros.text.v1","direction":"nats_to_ros","topic":"/odom","subject":"ros.odom","ros":{"version":2,"distro":"humble","type":"nav_msgs/msg/Odometry"},"stamp":{"source":"bridge","sec":0,"nanosec":0,"iso":"1970-01-01T00:00:00.000000Z"},"data":{"json":{"pose":{"position":{"x":1.0}}}},"meta":{"bridgeId":"b","sequence":1}}'
    )

    assert decoded.json_value == {"pose": {"position": {"x": 1.0}}}
    assert '"pose"' in decoded.text
    assert '"x": 1.0' in decoded.text
