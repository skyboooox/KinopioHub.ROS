"""YAML document loading helpers."""

from pathlib import Path

import yaml

from kinopio_hub_ros.errors import ConfigError


def load_yaml_document(path):
    file_path = Path(path)

    try:
        raw_text = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(
            "unable to read configuration file: {0}".format(exc),
            field="config",
        )

    try:
        data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigError("invalid YAML: {0}".format(exc), field="config")

    if data is None:
        return {}
    if not hasattr(data, "items"):
        raise ConfigError("root document must be a mapping/object", field="config")
    return dict(data)
