"""Shared UTF-8 and JSON payload helpers."""

import json

from kinopio_hub_ros.errors import ProtocolError


def encode_json_utf8(data):
    try:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except Exception as exc:
        raise ProtocolError("payload is not JSON serializable: {0}".format(exc))


def decode_json_utf8(payload):
    if not payload:
        raise ProtocolError("payload must not be empty")

    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError("payload is not valid UTF-8: {0}".format(exc))

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProtocolError("payload is not valid JSON: {0}".format(exc))


def decode_text_or_json_utf8(payload):
    if not payload:
        return None

    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return payload

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text
