"""Helpers for conservative ROS name normalization."""

import re

NON_NODE_NAME_PATTERN = re.compile(r"[^A-Za-z0-9_]")
MULTI_UNDERSCORE_PATTERN = re.compile(r"_+")


def sanitize_ros_node_name(value, fallback="kinopio_hub_ros"):
    text = str(value or "").strip()
    if not text:
        return fallback
    sanitized = NON_NODE_NAME_PATTERN.sub("_", text)
    sanitized = MULTI_UNDERSCORE_PATTERN.sub("_", sanitized).strip("_")
    if not sanitized:
        return fallback
    if sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized
