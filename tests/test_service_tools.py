import pytest

from kinopio_hub_ros.atom.service_tools import (
    normalize_service_type,
    normalize_service_type_for_ros1,
    normalize_service_type_for_ros2,
)
from kinopio_hub_ros.errors import ProtocolError


def test_service_type_normalizes_ros1_and_ros2_forms():
    assert normalize_service_type("lane_navigation/GoFromTo") == "lane_navigation/srv/GoFromTo"
    assert (
        normalize_service_type("lane_navigation/srv/GoFromTo")
        == "lane_navigation/srv/GoFromTo"
    )
    assert normalize_service_type_for_ros1("lane_navigation/srv/GoFromTo") == "lane_navigation/GoFromTo"
    assert (
        normalize_service_type_for_ros2("lane_navigation/GoFromTo")
        == "lane_navigation/srv/GoFromTo"
    )


def test_service_type_rejects_message_type_shape():
    with pytest.raises(ProtocolError, match="pkg/srv/Name"):
        normalize_service_type("lane_navigation/msg/GoFromTo")
