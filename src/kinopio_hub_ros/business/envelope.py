"""Envelope v1 builder and validator."""

import json

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from kinopio_hub_ros.atom.payload_codec import decode_json_utf8, encode_json_utf8
from kinopio_hub_ros.errors import ProtocolError

ENVELOPE_SCHEMA = "kinopio.ros.message.v1"
LEGACY_TEXT_ENVELOPE_SCHEMA = "kinopio.ros.text.v1"
ROS1_STRING_MESSAGE_TYPE = "std_msgs/String"
ROS2_STRING_MESSAGE_TYPE = "std_msgs/msg/String"
STRING_MESSAGE_TYPE = ROS2_STRING_MESSAGE_TYPE
VALID_DIRECTIONS = ("ros_to_nats", "nats_to_ros")


@dataclass(frozen=True)
class RosDescriptor:
    version: int
    distro: str
    message_type: str

    def to_dict(self):
        return {
            "version": self.version,
            "distro": self.distro,
            "type": self.message_type,
        }


@dataclass(frozen=True)
class Stamp:
    source: str
    sec: int
    nanosec: int
    iso: str

    def to_dict(self):
        return {
            "source": self.source,
            "sec": self.sec,
            "nanosec": self.nanosec,
            "iso": self.iso,
        }


@dataclass(frozen=True)
class EnvelopeMeta:
    bridge_id: str
    sequence: int

    def to_dict(self):
        return {
            "bridgeId": self.bridge_id,
            "sequence": self.sequence,
        }


@dataclass(frozen=True)
class RosEnvelope:
    direction: str
    topic: str
    subject: str
    ros: RosDescriptor
    stamp: Stamp
    text: str
    json_value: Optional[object]
    meta: EnvelopeMeta
    schema: str = ENVELOPE_SCHEMA

    def to_dict(self):
        if self.schema == ENVELOPE_SCHEMA:
            data = self.json_value
        else:
            data = {
                "text": self.text,
            }
            if self.json_value is not None:
                data["json"] = self.json_value
        return {
            "schema": self.schema,
            "direction": self.direction,
            "topic": self.topic,
            "subject": self.subject,
            "ros": self.ros.to_dict(),
            "stamp": self.stamp.to_dict(),
            "data": data,
            "meta": self.meta.to_dict(),
        }


TextEnvelope = RosEnvelope


def build_text_envelope(
    *,
    direction,
    topic,
    subject,
    text,
    bridge_id,
    sequence,
    ros_version,
    ros_distro,
    ros_message_type,
    json_value=None,
    timestamp=None,
    stamp_source="bridge",
):
    now = timestamp or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    _require_choice(direction, "direction", VALID_DIRECTIONS)
    _require_string(topic, "topic")
    _require_string(subject, "subject")
    _require_string(text, "data.text")
    _require_string(bridge_id, "meta.bridgeId")
    _require_string(ros_distro, "ros.distro")
    _require_string(ros_message_type, "ros.type")
    _require_string(stamp_source, "stamp.source")
    _require_json_compatible(json_value, "data.json")
    _require_int(sequence, "meta.sequence", minimum=0)
    _require_choice(ros_version, "ros.version", (1, 2))

    return RosEnvelope(
        direction=direction,
        topic=topic,
        subject=subject,
        ros=RosDescriptor(
            version=ros_version,
            distro=ros_distro,
            message_type=ros_message_type,
        ),
        stamp=_stamp_from_datetime(now, stamp_source),
        text=text,
        json_value=json_value,
        meta=EnvelopeMeta(
            bridge_id=bridge_id,
            sequence=sequence,
        ),
        schema=LEGACY_TEXT_ENVELOPE_SCHEMA,
    )


def build_message_envelope(
    *,
    direction,
    topic,
    subject,
    data,
    bridge_id,
    sequence,
    ros_version,
    ros_distro,
    ros_message_type,
    timestamp=None,
    stamp_source="bridge",
):
    now = timestamp or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    _require_choice(direction, "direction", VALID_DIRECTIONS)
    _require_string(topic, "topic")
    _require_string(subject, "subject")
    json_value = _require_json_compatible(_require_mapping(data, "data"), "data")
    _require_string(bridge_id, "meta.bridgeId")
    _require_string(ros_distro, "ros.distro")
    _require_string(ros_message_type, "ros.type")
    _require_string(stamp_source, "stamp.source")
    _require_int(sequence, "meta.sequence", minimum=0)
    _require_choice(ros_version, "ros.version", (1, 2))

    return RosEnvelope(
        direction=direction,
        topic=topic,
        subject=subject,
        ros=RosDescriptor(
            version=ros_version,
            distro=ros_distro,
            message_type=ros_message_type,
        ),
        stamp=_stamp_from_datetime(now, stamp_source),
        text=_json_text_from_value(json_value),
        json_value=json_value,
        meta=EnvelopeMeta(
            bridge_id=bridge_id,
            sequence=sequence,
        ),
        schema=ENVELOPE_SCHEMA,
    )


def encode_envelope(envelope):
    if isinstance(envelope, RosEnvelope):
        return encode_json_utf8(envelope.to_dict())
    if hasattr(envelope, "items"):
        return encode_json_utf8(envelope)
    raise ProtocolError("envelope must be a RosEnvelope or mapping")


def decode_envelope(payload):
    return envelope_from_dict(decode_json_utf8(payload))


def envelope_from_dict(data):
    root = _require_mapping(data, "envelope")
    schema = _require_choice(
        root.get("schema"),
        "schema",
        (ENVELOPE_SCHEMA, LEGACY_TEXT_ENVELOPE_SCHEMA),
    )

    ros = _require_mapping(root.get("ros"), "ros")
    stamp = _stamp_from_dict(root.get("stamp"))
    meta = _require_mapping(root.get("meta"), "meta")

    if schema == ENVELOPE_SCHEMA:
        json_value = _require_mapping(root.get("data"), "data")
        text = _json_text_from_value(json_value)
    else:
        body = _require_mapping(root.get("data"), "data")
        has_json_value = "json" in body
        json_value = body.get("json") if has_json_value else None
        _require_json_compatible(json_value, "data.json")

        text_value = body.get("text")
        if text_value is None:
            if has_json_value:
                text = _json_text_from_value(json_value)
            else:
                raise ProtocolError("data.text must be a string")
        else:
            text = _require_string(text_value, "data.text", allow_empty=True)

    return RosEnvelope(
        schema=schema,
        direction=_require_choice(root.get("direction"), "direction", VALID_DIRECTIONS),
        topic=_require_string(root.get("topic"), "topic"),
        subject=_require_string(root.get("subject"), "subject"),
        ros=RosDescriptor(
            version=_require_choice(ros.get("version"), "ros.version", (1, 2)),
            distro=_require_string(ros.get("distro"), "ros.distro"),
            message_type=_require_string(ros.get("type"), "ros.type"),
        ),
        stamp=stamp,
        text=text,
        json_value=json_value,
        meta=EnvelopeMeta(
            bridge_id=_require_string(meta.get("bridgeId"), "meta.bridgeId"),
            sequence=_require_int(meta.get("sequence"), "meta.sequence", minimum=0),
        ),
    )


def _stamp_from_dict(value):
    if value is None:
        return _stamp_from_datetime(datetime.now(timezone.utc), "received")

    stamp = _require_mapping(value, "stamp")
    return Stamp(
        source=_require_string(stamp.get("source"), "stamp.source"),
        sec=_require_int(stamp.get("sec"), "stamp.sec", minimum=0),
        nanosec=_require_int(stamp.get("nanosec"), "stamp.nanosec", minimum=0),
        iso=_require_string(stamp.get("iso"), "stamp.iso"),
    )


def _stamp_from_datetime(moment, source):
    sec = int(moment.timestamp())
    nanosec = moment.microsecond * 1000
    return Stamp(
        source=source,
        sec=sec,
        nanosec=nanosec,
        iso=moment.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        ),
    )


def _require_mapping(value, field):
    if not hasattr(value, "items"):
        raise ProtocolError("{0} must be a mapping/object".format(field))
    return dict(value)


def _require_string(value, field, allow_empty=False):
    if not isinstance(value, str):
        raise ProtocolError("{0} must be a string".format(field))
    normalized = value.strip()
    if not allow_empty and not normalized:
        raise ProtocolError("{0} must not be empty".format(field))
    return normalized if not allow_empty else value


def _require_int(value, field, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError("{0} must be an integer".format(field))
    if value < minimum:
        raise ProtocolError("{0} must be greater than or equal to {1}".format(field, minimum))
    return value


def _require_choice(value, field, allowed):
    if value not in allowed:
        rendered = ", ".join(str(item) for item in allowed)
        raise ProtocolError("{0} must be one of: {1}".format(field, rendered))
    return value


def _require_json_compatible(value, field):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_require_json_compatible(item, field) for item in value]
    if hasattr(value, "items"):
        return {
            _require_string(str(key), "{0} key".format(field)): _require_json_compatible(
                item, field
            )
            for key, item in dict(value).items()
        }
    raise ProtocolError("{0} must be JSON-compatible".format(field))


def _json_text_from_value(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
    )
