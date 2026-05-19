"""ROS topic <-> NATS subject mapping helpers."""

import re

from kinopio_hub_ros.atom.topic_tools import split_ros_topic
from kinopio_hub_ros.atom.validation import validate_subject_prefix
from kinopio_hub_ros.errors import ProtocolError

UNSAFE_SUBJECT_SEGMENT_PATTERN = re.compile(r"[^A-Za-z0-9_-]")


def topic_to_subject(topic, subject_prefix="ros"):
    try:
        prefix = validate_subject_prefix(subject_prefix, "subject_prefix")
    except Exception as exc:
        raise ProtocolError(str(exc))
    segments = [_sanitize_subject_segment(segment) for segment in split_ros_topic(topic)]
    return ".".join([prefix] + segments)


def _sanitize_subject_segment(segment):
    sanitized = UNSAFE_SUBJECT_SEGMENT_PATTERN.sub("_", segment)
    return sanitized or "_"
