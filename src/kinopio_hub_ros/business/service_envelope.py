"""Service request-reply envelope builder and validator."""

from dataclasses import dataclass

from kinopio_hub_ros.atom.payload_codec import decode_json_utf8, encode_json_utf8
from kinopio_hub_ros.atom.service_tools import (
    normalize_service_name,
    normalize_service_type,
)
from kinopio_hub_ros.errors import ProtocolError

SERVICE_ENVELOPE_SCHEMA = "kinopio.ros.service.v1"
VALID_DIRECTIONS = ("nats_to_ros", "ros_to_nats")
VALID_ERROR_CODES = (
    "invalid_request",
    "service_unavailable",
    "service_timeout",
    "service_error",
)


@dataclass(frozen=True)
class ServiceRosDescriptor:
    version: int
    service_type: str
    distro: str = None

    def to_dict(self):
        data = {
            "version": self.version,
            "type": self.service_type,
        }
        if self.distro:
            data["distro"] = self.distro
        return data


@dataclass(frozen=True)
class ServiceEnvelopeMeta:
    bridge_id: str
    sequence: int

    def to_dict(self):
        return {
            "bridgeId": self.bridge_id,
            "sequence": self.sequence,
        }


@dataclass(frozen=True)
class ServiceError:
    code: str
    message: str

    def to_dict(self):
        return {
            "code": self.code,
            "message": self.message,
        }


@dataclass(frozen=True)
class ServiceEnvelope:
    direction: str
    service: str
    subject: str
    ros: ServiceRosDescriptor
    data: object
    meta: ServiceEnvelopeMeta
    ok: object = None
    error: ServiceError = None
    schema: str = SERVICE_ENVELOPE_SCHEMA

    def to_dict(self):
        data = {
            "schema": self.schema,
            "direction": self.direction,
            "service": self.service,
            "subject": self.subject,
            "ros": self.ros.to_dict(),
            "data": self.data,
            "meta": self.meta.to_dict(),
        }
        if self.ok is not None:
            data["ok"] = self.ok
        if self.error is not None:
            data["error"] = self.error.to_dict()
        return data


def build_service_request_envelope(
    *,
    service,
    subject,
    data,
    bridge_id,
    sequence,
    ros_version,
    ros_service_type,
    ros_distro=None,
):
    return ServiceEnvelope(
        direction="nats_to_ros",
        service=normalize_service_name(service),
        subject=_require_string(subject, "subject"),
        ros=ServiceRosDescriptor(
            version=_require_choice(ros_version, "ros.version", (1, 2)),
            service_type=normalize_service_type(ros_service_type),
            distro=_optional_string(ros_distro, "ros.distro"),
        ),
        data=_require_mapping(data, "data"),
        meta=ServiceEnvelopeMeta(
            bridge_id=_require_string(bridge_id, "meta.bridgeId"),
            sequence=_require_int(sequence, "meta.sequence", minimum=0),
        ),
    )


def build_service_response_envelope(
    *,
    service,
    subject,
    data,
    bridge_id,
    sequence,
    ros_version,
    ros_distro,
    ros_service_type,
):
    return ServiceEnvelope(
        direction="ros_to_nats",
        service=normalize_service_name(service),
        subject=_require_string(subject, "subject"),
        ros=ServiceRosDescriptor(
            version=_require_choice(ros_version, "ros.version", (1, 2)),
            service_type=normalize_service_type(ros_service_type),
            distro=_optional_string(ros_distro, "ros.distro"),
        ),
        data=_require_mapping(data, "data"),
        meta=ServiceEnvelopeMeta(
            bridge_id=_require_string(bridge_id, "meta.bridgeId"),
            sequence=_require_int(sequence, "meta.sequence", minimum=0),
        ),
        ok=True,
    )


def build_service_error_envelope(
    *,
    service,
    subject,
    code,
    message,
    bridge_id,
    sequence,
    ros_version,
    ros_distro,
    ros_service_type,
):
    return ServiceEnvelope(
        direction="ros_to_nats",
        service=normalize_service_name(service),
        subject=_require_string(subject, "subject"),
        ros=ServiceRosDescriptor(
            version=_require_choice(ros_version, "ros.version", (1, 2)),
            service_type=normalize_service_type(ros_service_type),
            distro=_optional_string(ros_distro, "ros.distro"),
        ),
        data=None,
        meta=ServiceEnvelopeMeta(
            bridge_id=_require_string(bridge_id, "meta.bridgeId"),
            sequence=_require_int(sequence, "meta.sequence", minimum=0),
        ),
        ok=False,
        error=ServiceError(
            code=_require_choice(code, "error.code", VALID_ERROR_CODES),
            message=_require_string(message, "error.message"),
        ),
    )


def encode_service_envelope(envelope):
    if isinstance(envelope, ServiceEnvelope):
        return encode_json_utf8(envelope.to_dict())
    if hasattr(envelope, "items"):
        return encode_json_utf8(envelope)
    raise ProtocolError("service envelope must be a ServiceEnvelope or mapping")


def decode_service_envelope(payload):
    return service_envelope_from_dict(decode_json_utf8(payload))


def service_envelope_from_dict(data):
    root = _require_mapping(data, "envelope")
    schema = _require_choice(root.get("schema"), "schema", (SERVICE_ENVELOPE_SCHEMA,))
    direction = _require_choice(root.get("direction"), "direction", VALID_DIRECTIONS)
    ros = _require_mapping(root.get("ros"), "ros")
    meta = _require_mapping(root.get("meta"), "meta")
    ok_value = root.get("ok")

    if direction == "nats_to_ros":
        data_value = _require_mapping(root.get("data"), "data")
        ok = None
        error = None
    else:
        ok = _require_bool(ok_value, "ok")
        if ok:
            data_value = _require_mapping(root.get("data"), "data")
            error = None
        else:
            if root.get("data") is not None:
                raise ProtocolError("data must be null when ok is false")
            error_data = _require_mapping(root.get("error"), "error")
            error = ServiceError(
                code=_require_choice(error_data.get("code"), "error.code", VALID_ERROR_CODES),
                message=_require_string(error_data.get("message"), "error.message"),
            )
            data_value = None

    return ServiceEnvelope(
        schema=schema,
        direction=direction,
        service=normalize_service_name(root.get("service")),
        subject=_require_string(root.get("subject"), "subject"),
        ros=ServiceRosDescriptor(
            version=_require_choice(ros.get("version"), "ros.version", (1, 2)),
            service_type=normalize_service_type(ros.get("type")),
            distro=_optional_string(ros.get("distro"), "ros.distro"),
        ),
        data=data_value,
        meta=ServiceEnvelopeMeta(
            bridge_id=_require_string(meta.get("bridgeId"), "meta.bridgeId"),
            sequence=_require_int(meta.get("sequence"), "meta.sequence", minimum=0),
        ),
        ok=ok,
        error=error,
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


def _optional_string(value, field):
    if value is None:
        return None
    return _require_string(value, field)


def _require_bool(value, field):
    if not isinstance(value, bool):
        raise ProtocolError("{0} must be a boolean".format(field))
    return value


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
