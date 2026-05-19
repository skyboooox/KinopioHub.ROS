"""ROS message <-> envelope text helpers."""

import json

from collections.abc import Mapping
from array import array
from dataclasses import dataclass

import yaml

from kinopio_hub_ros.business.envelope import ROS1_STRING_MESSAGE_TYPE, ROS2_STRING_MESSAGE_TYPE
from kinopio_hub_ros.errors import AdapterError

STRING_MESSAGE_TYPES = (ROS1_STRING_MESSAGE_TYPE, ROS2_STRING_MESSAGE_TYPE)


@dataclass(frozen=True)
class MessagePayload:
    text: str
    json_value: object = None


def is_string_message_type(message_type):
    return message_type in STRING_MESSAGE_TYPES


def ros_message_to_payload(message, message_type):
    if is_string_message_type(message_type):
        return MessagePayload(text=getattr(message, "data"))

    json_value = _plain_value(message)
    return MessagePayload(
        text=json_value_to_text(json_value),
        json_value=json_value,
    )


def ros_message_to_text(message, message_type):
    return ros_message_to_payload(message, message_type).text


def structured_text_to_json_value(text, message_type):
    if is_string_message_type(message_type):
        return None
    return _mapping_from_structured_text(text, message_type)


def json_value_to_text(value):
    return _json_text(value)


def ros2_text_to_message(text, message_type, message_class, set_message_fields):
    message = message_class()
    if is_string_message_type(message_type):
        message.data = text
        return message

    values = _mapping_from_structured_text(text, message_type)
    set_message_fields(message, values)
    return message


def ros1_text_to_message(text, message_type, message_class, fill_message_args, keys=None):
    message = message_class()
    if is_string_message_type(message_type):
        message.data = text
        return message

    values = _mapping_from_structured_text(text, message_type)
    fill_message_args(message, [values], keys=keys or {})
    return message


def _mapping_from_structured_text(text, message_type):
    stripped = text.strip()
    if not stripped:
        return {}

    try:
        values = json.loads(text)
    except json.JSONDecodeError:
        try:
            values = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise AdapterError(
                "ROS message text for {0} must be valid JSON or YAML".format(message_type)
            ) from exc
    except ValueError as exc:
        raise AdapterError(
            "ROS message text for {0} must be valid JSON or YAML".format(message_type)
        ) from exc

    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise AdapterError(
            "ROS message text for {0} must be a JSON/YAML mapping/object".format(message_type)
        )
    return dict(values)


def _json_text(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
    )


def _plain_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (bytes, bytearray, array)):
        return list(value)
    if isinstance(value, Mapping):
        return {key: _plain_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_value(item) for item in value]

    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return _plain_value(tolist())
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _plain_value(item())
        except ValueError:
            pass

    fields = _message_fields(value)
    if fields:
        return {field: _plain_value(getattr(value, field)) for field in fields}

    return value


def _message_fields(message):
    get_fields_and_field_types = getattr(message, "get_fields_and_field_types", None)
    if callable(get_fields_and_field_types):
        return tuple(get_fields_and_field_types().keys())

    fields = []
    for slot in getattr(message, "__slots__", ()):
        if slot.startswith("_") and hasattr(message, slot[1:]):
            fields.append(slot[1:])
        else:
            fields.append(slot)
    return tuple(fields)
