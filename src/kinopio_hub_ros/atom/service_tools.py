"""ROS service name and type helpers."""

import re

from kinopio_hub_ros.atom.topic_tools import normalize_ros_topic
from kinopio_hub_ros.errors import ProtocolError

SERVICE_TYPE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def normalize_service_name(name):
    return normalize_ros_topic(name)


def normalize_service_type(service_type):
    package, service = split_service_type(service_type)
    return "{0}/srv/{1}".format(package, service)


def normalize_service_type_for_ros1(service_type):
    package, service = split_service_type(service_type)
    return "{0}/{1}".format(package, service)


def normalize_service_type_for_ros2(service_type):
    return normalize_service_type(service_type)


def split_service_type(service_type):
    if not isinstance(service_type, str):
        raise ProtocolError("ROS service type must be a string")
    normalized = service_type.strip()
    if not normalized:
        raise ProtocolError("ROS service type must not be empty")

    segments = tuple(segment for segment in normalized.split("/") if segment)
    if len(segments) == 2:
        package, service = segments
    elif len(segments) == 3 and segments[1] == "srv":
        package, _, service = segments
    else:
        raise ProtocolError("ROS service type must use pkg/srv/Name or pkg/Name")

    for field, value in (("package", package), ("service", service)):
        if not SERVICE_TYPE_SEGMENT_PATTERN.match(value):
            raise ProtocolError("ROS service type {0} segment is invalid".format(field))
    return package, service
