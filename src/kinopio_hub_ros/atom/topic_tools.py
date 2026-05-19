"""ROS topic normalization and wildcard matching."""

import re

from kinopio_hub_ros.errors import ProtocolError

WHITESPACE_PATTERN = re.compile(r"\s")


def normalize_ros_topic(topic):
    if not isinstance(topic, str):
        raise ProtocolError("ROS topic must be a string")

    normalized = topic.strip()
    if not normalized:
        raise ProtocolError("ROS topic must not be empty")
    if not normalized.startswith("/"):
        raise ProtocolError("ROS topic must start with '/'")
    if WHITESPACE_PATTERN.search(normalized):
        raise ProtocolError("ROS topic must not contain whitespace")

    segments = [segment for segment in normalized.split("/") if segment]
    if not segments:
        raise ProtocolError("ROS topic must contain at least one segment")

    return "/" + "/".join(segments)


def normalize_topic_pattern(pattern):
    normalized = normalize_ros_topic(pattern)
    for segment in _split_normalized_topic(normalized):
        if "*" in segment and segment not in ("*", "**"):
            raise ProtocolError(
                "topic pattern wildcard segments must be exactly '*' or '**'"
            )
    return normalized


def split_ros_topic(topic):
    return _split_normalized_topic(normalize_ros_topic(topic))


def split_topic_pattern(pattern):
    return _split_normalized_topic(normalize_topic_pattern(pattern))


def matches_topic_pattern(pattern, topic):
    pattern_segments = split_topic_pattern(normalize_topic_pattern(pattern))
    topic_segments = split_ros_topic(topic)
    return _match_segments(pattern_segments, topic_segments)


def matches_any_topic_pattern(patterns, topic):
    return any(matches_topic_pattern(pattern, topic) for pattern in patterns)


def _match_segments(pattern_segments, topic_segments):
    if not pattern_segments:
        return not topic_segments

    head = pattern_segments[0]
    tail = pattern_segments[1:]

    if head == "**":
        return _match_double_star(tail, topic_segments)
    if not topic_segments:
        return False
    if head == "*" or head == topic_segments[0]:
        return _match_segments(tail, topic_segments[1:])
    return False


def _match_double_star(remaining_pattern, remaining_topic):
    if _match_segments(remaining_pattern, remaining_topic):
        return True
    if not remaining_topic:
        return False
    return _match_double_star(remaining_pattern, remaining_topic[1:])


def _split_normalized_topic(topic):
    return tuple(topic.strip("/").split("/"))
