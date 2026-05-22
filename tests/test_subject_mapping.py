import pytest

from kinopio_hub_ros.atom.topic_tools import matches_any_topic_pattern, matches_topic_pattern
from kinopio_hub_ros.business.subject_mapping import service_to_subject, topic_to_subject
from kinopio_hub_ros.errors import ProtocolError


def test_topic_to_subject_maps_slashes_to_dots():
    assert topic_to_subject("/foo/bar") == "ros.foo.bar"


def test_topic_to_subject_sanitizes_non_subject_safe_characters():
    assert topic_to_subject("/robot/camera@front/state?") == "ros.robot.camera_front.state_"


def test_topic_to_subject_supports_custom_prefix():
    assert topic_to_subject("/chatter", subject_prefix="hub.ros") == "hub.ros.chatter"


def test_service_to_subject_uses_service_prefix():
    assert (
        service_to_subject("/lane_navigation/go_from_to")
        == "ros_services.lane_navigation.go_from_to"
    )


def test_topic_to_subject_rejects_invalid_topic():
    with pytest.raises(ProtocolError, match="must start with '/'"):
        topic_to_subject("chatter")


def test_topic_pattern_matches_single_and_multi_segment_wildcards():
    assert matches_topic_pattern("/robot/*/text", "/robot/head/text") is True
    assert matches_topic_pattern("/robot/*/text", "/robot/head/camera/text") is False
    assert matches_topic_pattern("/robot/**/text", "/robot/head/camera/text") is True
    assert matches_topic_pattern("/robot/**", "/robot") is True


def test_matches_any_topic_pattern_checks_multiple_patterns():
    assert matches_any_topic_pattern(
        ("/chatter", "/robot/**/text"),
        "/robot/head/text",
    ) is True
    assert matches_any_topic_pattern(
        ("/chatter", "/robot/**/text"),
        "/robot/head/image",
    ) is False
