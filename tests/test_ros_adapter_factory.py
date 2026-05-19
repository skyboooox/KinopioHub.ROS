import io

import pytest

from kinopio_hub_ros.business.configuration import load_config
from kinopio_hub_ros.business.ros_adapter_factory import (
    create_ros_adapter,
    resolve_ros_runtime_version,
)
from kinopio_hub_ros.business.ros1_adapter import Ros1Adapter
from kinopio_hub_ros.business.ros2_adapter import Ros2Adapter
from kinopio_hub_ros.errors import RuntimeUnavailableError


def write_config(tmp_path, body):
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return load_config(path)


def test_resolve_ros_runtime_version_prefers_environment_hint():
    assert (
        resolve_ros_runtime_version(
            "auto",
            ros1_available=True,
            ros2_available=True,
            environment={"ROS_VERSION": "1"},
        )
        == 1
    )
    assert (
        resolve_ros_runtime_version(
            "auto",
            ros1_available=True,
            ros2_available=True,
            environment={"ROS_VERSION": "2"},
        )
        == 2
    )


def test_resolve_ros_runtime_version_falls_back_to_available_runtime():
    assert (
        resolve_ros_runtime_version(
            "auto",
            ros1_available=True,
            ros2_available=False,
            environment={},
        )
        == 1
    )
    assert (
        resolve_ros_runtime_version(
            "auto",
            ros1_available=False,
            ros2_available=True,
            environment={},
        )
        == 2
    )


def test_resolve_ros_runtime_version_errors_when_nothing_is_available():
    with pytest.raises(RuntimeUnavailableError, match="No ROS runtime is available"):
        resolve_ros_runtime_version(
            "auto",
            ros1_available=False,
            ros2_available=False,
            environment={},
        )


def test_create_ros_adapter_uses_explicit_versions(tmp_path, monkeypatch):
    config_ros1 = write_config(
        tmp_path,
        """
ros:
  version: 1
topics:
  mode: all
""".strip()
        + "\n",
    )
    config_ros2 = write_config(
        tmp_path,
        """
ros:
  version: 2
topics:
  mode: all
""".strip()
        + "\n",
    )

    monkeypatch.setattr(
        "kinopio_hub_ros.business.ros_adapter_factory.resolve_ros_runtime_version",
        lambda configured_version, **kwargs: configured_version,
    )

    assert isinstance(create_ros_adapter(config_ros1), Ros1Adapter)
    assert isinstance(create_ros_adapter(config_ros2), Ros2Adapter)
