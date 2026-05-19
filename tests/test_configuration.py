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
