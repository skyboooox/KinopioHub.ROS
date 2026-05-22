from pathlib import Path

import pytest

from kinopio_hub_ros.business.configuration import DEFAULT_NATS_SERVERS, load_config
from kinopio_hub_ros.errors import ConfigError


def write_config(tmp_path, body):
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def test_defaults_are_applied_to_minimal_config(tmp_path):
    config_path = write_config(
        tmp_path,
        "{}\n",
    )

    config = load_config(config_path)

    assert config.nats.servers == DEFAULT_NATS_SERVERS
    assert config.nats.auth.mode == "none"
    assert config.sync.subject_prefix == "ros"
    assert config.topics.mode == "all"
    assert config.topics.patterns == ()
    assert config.services.subject_prefix == "ros_services"
    assert config.services.calls == ()


def test_include_mode_requires_patterns(tmp_path):
    config_path = write_config(
        tmp_path,
        """
topics:
  mode: include
""".strip()
        + "\n",
    )

    with pytest.raises(ConfigError, match="topics.patterns"):
        load_config(config_path)


def test_all_mode_rejects_patterns(tmp_path):
    config_path = write_config(
        tmp_path,
        """
topics:
  mode: all
  patterns:
    - /chatter
""".strip()
        + "\n",
    )

    with pytest.raises(ConfigError, match="topics.patterns"):
        load_config(config_path)


def test_username_password_mode_requires_password_env(tmp_path):
    config_path = write_config(
        tmp_path,
        """
nats:
  auth:
    mode: username_password
    username: bridge
topics:
  mode: all
""".strip()
        + "\n",
    )

    with pytest.raises(ConfigError, match="nats.auth.password_env"):
        load_config(config_path)


def test_topic_patterns_must_use_ros_topic_format(tmp_path):
    config_path = write_config(
        tmp_path,
        """
topics:
  mode: include
  patterns:
    - chatter
""".strip()
        + "\n",
    )

    with pytest.raises(ConfigError, match="topics.patterns\\[0\\]"):
        load_config(config_path)


def test_services_parse_explicit_allowlist_and_normalize_types(tmp_path):
    config_path = write_config(
        tmp_path,
        """
services:
  subject_prefix: robot_services
  calls:
    - name: /lane_navigation/go_from_to
      type: lane_navigation/GoFromTo
      timeout_ms: 30000
""".strip()
        + "\n",
    )

    config = load_config(config_path)

    assert config.services.subject_prefix == "robot_services"
    assert config.services.calls[0].name == "/lane_navigation/go_from_to"
    assert config.services.calls[0].service_type == "lane_navigation/srv/GoFromTo"
    assert config.services.calls[0].timeout_ms == 30000


def test_service_calls_must_be_list(tmp_path):
    config_path = write_config(
        tmp_path,
        """
services:
  calls:
    name: /lane_navigation/go_from_to
""".strip()
        + "\n",
    )

    with pytest.raises(ConfigError, match="services.calls"):
        load_config(config_path)


def test_service_call_requires_valid_service_type(tmp_path):
    config_path = write_config(
        tmp_path,
        """
services:
  calls:
    - name: /lane_navigation/go_from_to
      type: lane_navigation/msg/GoFromTo
""".strip()
        + "\n",
    )

    with pytest.raises(ConfigError, match="services.calls\\[0\\].type"):
        load_config(config_path)


def test_service_calls_reject_duplicate_names(tmp_path):
    config_path = write_config(
        tmp_path,
        """
services:
  calls:
    - name: /lane_navigation/go_from_to
      type: lane_navigation/srv/GoFromTo
    - name: /lane_navigation/go_from_to
      type: lane_navigation/srv/GoFromTo
""".strip()
        + "\n",
    )

    with pytest.raises(ConfigError, match="duplicates"):
        load_config(config_path)


def test_service_subject_prefix_must_not_be_inside_topic_prefix(tmp_path):
    config_path = write_config(
        tmp_path,
        """
sync:
  subject_prefix: ros
services:
  subject_prefix: ros.services
  calls:
    - name: /lane_navigation/go_from_to
      type: lane_navigation/srv/GoFromTo
""".strip()
        + "\n",
    )

    with pytest.raises(ConfigError, match="services.subject_prefix"):
        load_config(config_path)
