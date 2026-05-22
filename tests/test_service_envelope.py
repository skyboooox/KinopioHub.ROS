import pytest

from kinopio_hub_ros.business.service_envelope import (
    SERVICE_ENVELOPE_SCHEMA,
    build_service_error_envelope,
    build_service_request_envelope,
    build_service_response_envelope,
    decode_service_envelope,
    encode_service_envelope,
)
from kinopio_hub_ros.errors import ProtocolError


def test_service_request_envelope_round_trip():
    envelope = build_service_request_envelope(
        service="/lane_navigation/go_from_to",
        subject="ros_services.lane_navigation.go_from_to",
        data={
            "start_node": "",
            "goal_node": "node2",
            "loop": False,
            "repeat_count": 1,
        },
        bridge_id="sdk",
        sequence=3,
        ros_version=2,
        ros_service_type="lane_navigation/srv/GoFromTo",
    )

    decoded = decode_service_envelope(encode_service_envelope(envelope))

    assert decoded.schema == SERVICE_ENVELOPE_SCHEMA
    assert decoded.direction == "nats_to_ros"
    assert decoded.service == "/lane_navigation/go_from_to"
    assert decoded.subject == "ros_services.lane_navigation.go_from_to"
    assert decoded.ros.version == 2
    assert decoded.ros.service_type == "lane_navigation/srv/GoFromTo"
    assert decoded.data["goal_node"] == "node2"
    assert decoded.ok is None
    assert decoded.error is None


def test_service_success_response_round_trip():
    envelope = build_service_response_envelope(
        service="/lane_navigation/go_from_to",
        subject="ros_services.lane_navigation.go_from_to",
        data={"accepted": True},
        bridge_id="bridge-test",
        sequence=4,
        ros_version=2,
        ros_distro="humble",
        ros_service_type="lane_navigation/GoFromTo",
    )

    decoded = decode_service_envelope(encode_service_envelope(envelope))

    assert decoded.direction == "ros_to_nats"
    assert decoded.ok is True
    assert decoded.data == {"accepted": True}
    assert decoded.ros.distro == "humble"
    assert decoded.ros.service_type == "lane_navigation/srv/GoFromTo"


def test_service_error_response_round_trip():
    envelope = build_service_error_envelope(
        service="/lane_navigation/go_from_to",
        subject="ros_services.lane_navigation.go_from_to",
        code="service_timeout",
        message="timed out",
        bridge_id="bridge-test",
        sequence=5,
        ros_version=2,
        ros_distro="humble",
        ros_service_type="lane_navigation/srv/GoFromTo",
    )

    decoded = decode_service_envelope(encode_service_envelope(envelope))

    assert decoded.ok is False
    assert decoded.data is None
    assert decoded.error.code == "service_timeout"
    assert decoded.error.message == "timed out"


def test_service_envelope_rejects_invalid_request_data():
    with pytest.raises(ProtocolError, match="data"):
        decode_service_envelope(
            b'{"schema":"kinopio.ros.service.v1","direction":"nats_to_ros","service":"/x","subject":"ros_services.x","ros":{"version":2,"type":"pkg/srv/Do"},"data":[],"meta":{"bridgeId":"sdk","sequence":1}}'
        )
