import logging

import pytest

from kinopio_hub_ros.business.configuration import load_config
from kinopio_hub_ros.business.ros1_adapter import (
    Ros1Adapter,
    restore_logging_handlers,
    snapshot_logging_handlers,
)
from kinopio_hub_ros.errors import AdapterError, RuntimeUnavailableError
from tests.fakes import FakeRos1Driver


def write_config(tmp_path, body):
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return load_config(path)


def test_ros1_adapter_discovers_and_subscribes_to_any_message_type(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: ros1-bridge
ros:
  version: 1
topics:
  mode: include
  patterns:
    - /chatter
    - /odom
    - /robot/**/text
""".strip()
        + "\n",
    )
    messages = []
    driver = FakeRos1Driver(
        distro="noetic",
        topics=(
            ("/chatter", ("std_msgs/String",)),
            ("/odom", ("nav_msgs/Odometry",)),
            ("/camera/image", ("sensor_msgs/Image",)),
            ("/robot/head/text", ("std_msgs/String",)),
        ),
    )
    adapter = Ros1Adapter(config, on_text_message=messages.append, driver=driver)

    adapter.start()
    selected = adapter.refresh_subscriptions()
    driver.emit("/chatter", "hello ros1")

    assert selected == ("/chatter", "/odom", "/robot/head/text")
    assert driver.started_with["node_name"] == "ros1_bridge"
    assert sorted(driver.publishers) == ["/chatter", "/odom", "/robot/head/text"]
    assert driver.subscription_types["/odom"] == "nav_msgs/Odometry"
    assert driver.publisher_types["/odom"] == "nav_msgs/Odometry"
    assert messages[0].message_type == "std_msgs/String"
    assert messages[0].text == "hello ros1"


def test_ros1_adapter_publish_text_reuses_publishers(tmp_path):
    config = write_config(
        tmp_path,
        """
ros:
  version: 1
topics:
  mode: all
""".strip()
        + "\n",
    )
    driver = FakeRos1Driver(distro="noetic")
    adapter = Ros1Adapter(config, driver=driver)

    adapter.start()
    adapter.publish_text("/chatter", "first", message_type="std_msgs/String")
    adapter.publish_text("/chatter", "second", message_type="std_msgs/String")

    assert driver.published == [("/chatter", "first"), ("/chatter", "second")]
    assert list(driver.publishers) == ["/chatter"]


def test_ros1_adapter_rejects_explicit_ros2_config(tmp_path):
    config = write_config(
        tmp_path,
        """
ros:
  version: 2
topics:
  mode: all
""".strip()
        + "\n",
    )
    adapter = Ros1Adapter(config, driver=FakeRos1Driver(distro="noetic"))

    with pytest.raises(RuntimeUnavailableError, match="ROS 2 was explicitly requested"):
        adapter.start()


def test_ros1_adapter_enforces_topic_selection_on_publish(tmp_path):
    config = write_config(
        tmp_path,
        """
ros:
  version: 1
topics:
  mode: include
  patterns:
    - /chatter
""".strip()
        + "\n",
    )
    adapter = Ros1Adapter(config, driver=FakeRos1Driver(distro="noetic"))
    adapter.start()

    with pytest.raises(AdapterError, match="selection rules"):
        adapter.publish_text("/forbidden", "blocked")


def test_ros1_logging_restore_preserves_existing_handlers():
    logger = logging.getLogger("test.ros1.logging_restore")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_disabled = logger.disabled
    stream_handler = logging.StreamHandler()
    ros_handler = logging.NullHandler()
    try:
        logger.handlers = [stream_handler]
        logger.setLevel(logging.INFO)
        logger.disabled = False
        snapshot = snapshot_logging_handlers(logger)

        logger.handlers = [ros_handler]
        logger.setLevel(logging.ERROR)
        logger.disabled = True
        restore_logging_handlers(snapshot, logger)

        assert stream_handler in logger.handlers
        assert ros_handler in logger.handlers
        assert logger.level == logging.INFO
        assert logger.disabled is False
    finally:
        logger.handlers = original_handlers
        logger.setLevel(original_level)
        logger.disabled = original_disabled
