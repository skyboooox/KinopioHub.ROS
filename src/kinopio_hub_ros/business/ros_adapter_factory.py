"""ROS runtime selection helpers."""

import importlib.util
import os

from kinopio_hub_ros.business.ros1_adapter import Ros1Adapter
from kinopio_hub_ros.business.ros2_adapter import Ros2Adapter
from kinopio_hub_ros.errors import RuntimeUnavailableError


def resolve_ros_runtime_version(
    configured_version,
    *,
    ros1_available=None,
    ros2_available=None,
    environment=None,
):
    if configured_version in (1, 2):
        return configured_version

    environment = environment or os.environ
    ros1_available = _coalesce_availability(ros1_available, "rospy")
    ros2_available = _coalesce_availability(ros2_available, "rclpy")

    ros_version_env = str(environment.get("ROS_VERSION", "")).strip()
    if ros_version_env == "2" and ros2_available:
        return 2
    if ros_version_env == "1" and ros1_available:
        return 1
    if ros2_available:
        return 2
    if ros1_available:
        return 1

    raise RuntimeUnavailableError(
        "No ROS runtime is available in the current environment. Install ROS 2 (rclpy) or ROS 1 (rospy), or set ros.version explicitly."
    )


def create_ros_adapter(config, **kwargs):
    version = resolve_ros_runtime_version(config.ros.version)
    if version == 1:
        return Ros1Adapter(config, **kwargs)
    return Ros2Adapter(config, **kwargs)


def _coalesce_availability(value, module_name):
    if value is not None:
        return bool(value)
    return importlib.util.find_spec(module_name) is not None
