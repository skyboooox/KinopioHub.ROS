import logging

import pytest

from kinopio_hub_ros.business.configuration import load_config
from kinopio_hub_ros.business.ros2_adapter import Ros2Adapter
from kinopio_hub_ros.errors import AdapterError, RuntimeUnavailableError
from tests.fakes import FakeRos2Driver


def write_config(tmp_path, body):
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return load_config(path)


def test_refresh_subscriptions_include_mode_accepts_any_message_type(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: adapter-test
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
    driver = FakeRos2Driver(
        topics=(
            ("/chatter", ("std_msgs/msg/String",)),
            ("/odom", ("nav_msgs/msg/Odometry",)),
            ("/camera/image", ("sensor_msgs/msg/Image",)),
            ("/robot/head/text", ("std_msgs/msg/String",)),
        )
    )
    adapter = Ros2Adapter(config, on_text_message=messages.append, driver=driver)

    adapter.start()
    selected = adapter.refresh_subscriptions()
    driver.emit("/chatter", "hello")

    assert selected == ("/chatter", "/odom", "/robot/head/text")
    assert sorted(driver.subscriptions) == ["/chatter", "/odom", "/robot/head/text"]
    assert sorted(driver.publishers) == ["/chatter", "/odom", "/robot/head/text"]
    assert driver.subscription_types["/odom"] == "nav_msgs/msg/Odometry"
    assert driver.publisher_types["/odom"] == "nav_msgs/msg/Odometry"
    assert messages[0].topic == "/chatter"
    assert messages[0].text == "hello"
    assert messages[0].message_type == "std_msgs/msg/String"
    assert driver.started_with["node_name"] == "adapter_test"
    assert driver.started_with["qos_config"].depth == 10


def test_refresh_subscriptions_exclude_mode_skips_matching_topics(tmp_path):
    config = write_config(
        tmp_path,
        """
topics:
  mode: exclude
  patterns:
    - /internal/**
""".strip()
        + "\n",
    )
    driver = FakeRos2Driver(
        topics=(
            ("/chatter", ("std_msgs/msg/String",)),
            ("/internal/state", ("std_msgs/msg/String",)),
        )
    )
    adapter = Ros2Adapter(config, driver=driver)

    adapter.start()
    selected = adapter.refresh_subscriptions()

    assert selected == ("/chatter",)
    assert list(driver.subscriptions) == ["/chatter"]
    assert list(driver.publishers) == ["/chatter"]


def test_publish_text_reuses_publishers_and_enforces_topic_selection(tmp_path):
    config = write_config(
        tmp_path,
        """
topics:
  mode: include
  patterns:
    - /chatter
""".strip()
        + "\n",
    )
    driver = FakeRos2Driver()
    adapter = Ros2Adapter(config, driver=driver)

    adapter.start()
    adapter.publish_text("/chatter", "first", message_type="std_msgs/msg/String")
    adapter.publish_text("/chatter", "second", message_type="std_msgs/msg/String")

    with pytest.raises(AdapterError, match="selection rules"):
        adapter.publish_text("/forbidden", "nope")

    assert driver.published == [("/chatter", "first"), ("/chatter", "second")]
    assert list(driver.publishers) == ["/chatter"]


def test_ros2_adapter_rejects_explicit_ros1_config(tmp_path):
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
    adapter = Ros2Adapter(config, driver=FakeRos2Driver())

    with pytest.raises(RuntimeUnavailableError, match="ROS 1 was explicitly requested"):
        adapter.start()


def test_rolling_distro_is_logged_as_best_effort(tmp_path, caplog):
    config = write_config(
        tmp_path,
        """
topics:
  mode: all
""".strip()
        + "\n",
    )
    adapter = Ros2Adapter(
        config,
        driver=FakeRos2Driver(distro="rolling"),
        logger=logging.getLogger("test.ros2_adapter"),
    )

    with caplog.at_level(logging.WARNING):
        adapter.start()

    assert "best-effort" in caplog.text
