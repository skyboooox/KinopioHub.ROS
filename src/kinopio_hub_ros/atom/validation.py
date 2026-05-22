"""Small reusable validation helpers."""

import re

from kinopio_hub_ros.atom.topic_tools import normalize_topic_pattern
from kinopio_hub_ros.atom.service_tools import (
    normalize_service_name,
    normalize_service_type,
)
from kinopio_hub_ros.errors import ConfigError

SUBJECT_PREFIX_PATTERN = re.compile(r"^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$")
WHITESPACE_PATTERN = re.compile(r"\s")


def ensure_mapping(value, field):
    if value is None:
        return {}
    if not hasattr(value, "items"):
        raise ConfigError("must be a mapping/object", field=field)
    return dict(value)


def ensure_string(value, field, allow_empty=False):
    if not isinstance(value, str):
        raise ConfigError("must be a string", field=field)
    normalized = value.strip()
    if not allow_empty and not normalized:
        raise ConfigError("must not be empty", field=field)
    return normalized


def ensure_optional_string(value, field):
    if value is None:
        return None
    return ensure_string(value, field)


def ensure_bool(value, field):
    if not isinstance(value, bool):
        raise ConfigError("must be a boolean", field=field)
    return value


def ensure_int(value, field, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError("must be an integer", field=field)
    if value < minimum:
        raise ConfigError("must be greater than or equal to {0}".format(minimum), field=field)
    return value


def ensure_choice(value, field, allowed):
    normalized = ensure_string(value, field).lower()
    if normalized not in allowed:
        raise ConfigError(
            "must be one of: {0}".format(", ".join(sorted(allowed))),
            field=field,
        )
    return normalized


def ensure_list_of_strings(value, field, allow_empty=True):
    if value is None:
        return []
    if not isinstance(value, list):
        raise ConfigError("must be a list", field=field)
    result = []
    for index, item in enumerate(value):
        result.append(ensure_string(item, "{0}[{1}]".format(field, index)))
    if not allow_empty and not result:
        raise ConfigError("must contain at least one item", field=field)
    return result


def validate_topic_pattern(value, field):
    pattern = ensure_string(value, field)
    try:
        return normalize_topic_pattern(pattern)
    except Exception as exc:
        raise ConfigError(str(exc), field=field)


def validate_service_name(value, field):
    name = ensure_string(value, field)
    try:
        return normalize_service_name(name)
    except Exception as exc:
        raise ConfigError(str(exc), field=field)


def validate_service_type(value, field):
    service_type = ensure_string(value, field)
    try:
        return normalize_service_type(service_type)
    except Exception as exc:
        raise ConfigError(str(exc), field=field)


def validate_subject_prefix(value, field):
    prefix = ensure_string(value, field)
    if not SUBJECT_PREFIX_PATTERN.match(prefix):
        raise ConfigError(
            "must contain only dot-separated letters, digits, '-' or '_'",
            field=field,
        )
    return prefix


def normalize_ros_version(value, field):
    if value == "auto":
        return "auto"
    if value in (1, 2):
        return value
    if isinstance(value, str) and value.strip() in ("1", "2"):
        return int(value.strip())
    raise ConfigError("must be one of: auto, 1, 2", field=field)
