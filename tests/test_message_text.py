import json

import pytest

from kinopio_hub_ros.business.message_text import (
    ros_message_to_payload,
    ros1_text_to_message,
    ros2_text_to_message,
    ros_message_to_text,
    structured_text_to_json_value,
)
from kinopio_hub_ros.errors import AdapterError


class FakeStringMessage:
    def __init__(self):
        self.data = ""


class FakeNestedMessage:
    __slots__ = ("x", "label")

    def __init__(self):
        self.x = 1.5
        self.label = "ready"


class FakeRos2Message:
    def __init__(self):
        self.child = FakeNestedMessage()
        self.values = [1, 2]

    def get_fields_and_field_types(self):
        return {
            "child": "example_msgs/msg/Nested",
            "values": "int32[]",
        }


def test_std_msgs_string_preserves_plain_text():
    message = FakeStringMessage()
    message.data = "data: this stays plain"

    assert ros_message_to_text(message, "std_msgs/msg/String") == "data: this stays plain"

    decoded = ros2_text_to_message(
        "data: this stays plain",
        "std_msgs/msg/String",
        FakeStringMessage,
        lambda message, values: None,
    )

    assert decoded.data == "data: this stays plain"


def test_structured_message_is_encoded_as_json_text():
    text = ros_message_to_text(FakeRos2Message(), "example_msgs/msg/State")
    payload = json.loads(text)

    assert payload["child"]["x"] == 1.5
    assert payload["child"]["label"] == "ready"
    assert payload["values"] == [1, 2]


def test_structured_message_payload_exposes_direct_json_value():
    payload = ros_message_to_payload(FakeRos2Message(), "example_msgs/msg/State")

    assert payload.json_value == {
        "child": {
            "x": 1.5,
            "label": "ready",
        },
        "values": [1, 2],
    }
    assert json.loads(payload.text) == payload.json_value


def test_ros2_json_text_populates_message_fields():
    def set_message_fields(message, values):
        message.values = values["values"]
        message.child.label = values["child"]["label"]

    message = ros2_text_to_message(
        '{"child":{"label":"active"},"values":[4,5]}',
        "example_msgs/msg/State",
        FakeRos2Message,
        set_message_fields,
    )

    assert message.values == [4, 5]
    assert message.child.label == "active"


def test_ros1_json_text_populates_message_fields():
    def fill_message_args(message, args, keys=None):
        values = args[0]
        message.values = values["values"]

    message = ros1_text_to_message(
        '{"values":[7]}',
        "example_msgs/State",
        FakeRos2Message,
        fill_message_args,
    )

    assert message.values == [7]


def test_structured_message_rejects_non_mapping_json_or_yaml():
    with pytest.raises(AdapterError, match="mapping"):
        ros2_text_to_message(
            "- not\n- a\n- mapping",
            "example_msgs/msg/State",
            FakeRos2Message,
            lambda message, values: None,
        )


def test_structured_message_writeback_still_accepts_yaml():
    def set_message_fields(message, values):
        message.values = values["values"]
        message.child.label = values["child"]["label"]

    message = ros2_text_to_message(
        "child:\n  label: yaml-active\nvalues:\n  - 8\n  - 9",
        "example_msgs/msg/State",
        FakeRos2Message,
        set_message_fields,
    )

    assert message.values == [8, 9]
    assert message.child.label == "yaml-active"


def test_structured_text_to_json_value_parses_json_mapping():
    value = structured_text_to_json_value(
        '{"child":{"label":"active"},"values":[1,2]}',
        "example_msgs/msg/State",
    )

    assert value == {"child": {"label": "active"}, "values": [1, 2]}
