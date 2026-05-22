"""Configuration parsing and normalization."""

from dataclasses import dataclass
from pathlib import Path

from kinopio_hub_ros.atom.validation import (
    ensure_bool,
    ensure_choice,
    ensure_int,
    ensure_list_of_strings,
    ensure_mapping,
    ensure_optional_string,
    ensure_string,
    normalize_ros_version,
    validate_service_name,
    validate_service_type,
    validate_subject_prefix,
    validate_topic_pattern,
)
from kinopio_hub_ros.atom.yaml_loader import load_yaml_document
from kinopio_hub_ros.errors import ConfigError

DEFAULT_NATS_SERVERS = (
    "tls://nats.example.invalid:14222",
)

VALID_DIRECTIONS = ("bidirectional", "ros_to_nats", "nats_to_ros")
VALID_AUTH_MODES = ("none", "username_password", "token", "nkey", "creds")
VALID_TOPIC_MODES = ("include", "exclude", "all")
VALID_QOS_RELIABILITY = ("reliable", "best_effort")
VALID_QOS_DURABILITY = ("volatile", "transient_local")
DEFAULT_SERVICE_SUBJECT_PREFIX = "ros_services"
DEFAULT_SERVICE_TIMEOUT_MS = 30000


@dataclass(frozen=True)
class BridgeConfig:
    bridge_id: str
    direction: str

    def to_dict(self):
        return {
            "id": self.bridge_id,
            "direction": self.direction,
        }


@dataclass(frozen=True)
class NatsTlsConfig:
    enabled: bool
    handshake_first: bool
    ca_file: str
    server_name: str

    def to_dict(self):
        return {
            "enabled": self.enabled,
            "handshake_first": self.handshake_first,
            "ca_file": self.ca_file,
            "server_name": self.server_name,
        }


@dataclass(frozen=True)
class NatsAuthConfig:
    mode: str
    username: str
    password_env: str
    token_env: str
    nkey_file: str
    creds_file: str

    def to_dict(self):
        return {
            "mode": self.mode,
            "username": self.username,
            "password_env": self.password_env,
            "token_env": self.token_env,
            "nkey_file": self.nkey_file,
            "creds_file": self.creds_file,
        }


@dataclass(frozen=True)
class NatsConfig:
    servers: tuple
    tls: NatsTlsConfig
    auth: NatsAuthConfig

    def to_dict(self):
        return {
            "servers": list(self.servers),
            "tls": self.tls.to_dict(),
            "auth": self.auth.to_dict(),
        }


@dataclass(frozen=True)
class RosQosConfig:
    reliability: str
    durability: str
    depth: int

    def to_dict(self):
        return {
            "reliability": self.reliability,
            "durability": self.durability,
            "depth": self.depth,
        }


@dataclass(frozen=True)
class RosConfig:
    version: object
    qos: RosQosConfig

    def to_dict(self):
        return {
            "version": self.version,
            "qos": self.qos.to_dict(),
        }


@dataclass(frozen=True)
class TopicSelectionConfig:
    mode: str
    patterns: tuple

    def to_dict(self):
        return {
            "mode": self.mode,
            "patterns": list(self.patterns),
        }


@dataclass(frozen=True)
class ServiceCallConfig:
    name: str
    service_type: str
    timeout_ms: int

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.service_type,
            "timeout_ms": self.timeout_ms,
        }


@dataclass(frozen=True)
class ServicesConfig:
    subject_prefix: str
    calls: tuple

    def to_dict(self):
        return {
            "subject_prefix": self.subject_prefix,
            "calls": [call.to_dict() for call in self.calls],
        }


@dataclass(frozen=True)
class SyncConfig:
    subject_prefix: str
    throttle_ms: int
    dedupe: bool
    heartbeat_ms: int
    loop_suppression_ms: int

    def to_dict(self):
        return {
            "subject_prefix": self.subject_prefix,
            "throttle_ms": self.throttle_ms,
            "dedupe": self.dedupe,
            "heartbeat_ms": self.heartbeat_ms,
            "loop_suppression_ms": self.loop_suppression_ms,
        }


@dataclass(frozen=True)
class AppConfig:
    bridge: BridgeConfig
    nats: NatsConfig
    ros: RosConfig
    topics: TopicSelectionConfig
    services: ServicesConfig
    sync: SyncConfig

    def to_public_dict(self):
        return {
            "bridge": self.bridge.to_dict(),
            "nats": self.nats.to_dict(),
            "ros": self.ros.to_dict(),
            "topics": self.topics.to_dict(),
            "services": self.services.to_dict(),
            "sync": self.sync.to_dict(),
        }


def load_config(config_path):
    document = load_yaml_document(Path(config_path))
    config = AppConfig(
        bridge=_parse_bridge(document.get("bridge")),
        nats=_parse_nats(document.get("nats")),
        ros=_parse_ros(document.get("ros")),
        topics=_parse_topics(document.get("topics")),
        services=_parse_services(document.get("services")),
        sync=_parse_sync(document.get("sync")),
    )
    _validate_app_config(config)
    return config


def _parse_bridge(value):
    data = ensure_mapping(value, "bridge")
    return BridgeConfig(
        bridge_id=ensure_string(data.get("id", "kinopio-hub-ros"), "bridge.id"),
        direction=ensure_choice(
            data.get("direction", "bidirectional"),
            "bridge.direction",
            VALID_DIRECTIONS,
        ),
    )


def _parse_nats(value):
    data = ensure_mapping(value, "nats")
    servers_value = data.get("servers")
    if servers_value is None:
        servers = DEFAULT_NATS_SERVERS
    else:
        servers = tuple(ensure_list_of_strings(servers_value, "nats.servers", allow_empty=False))
        if not servers:
            raise ConfigError("must contain at least one server URL", field="nats.servers")

    return NatsConfig(
        servers=servers,
        tls=_parse_nats_tls(data.get("tls")),
        auth=_parse_nats_auth(data.get("auth")),
    )


def _parse_nats_tls(value):
    data = ensure_mapping(value, "nats.tls")
    return NatsTlsConfig(
        enabled=ensure_bool(data.get("enabled", True), "nats.tls.enabled"),
        handshake_first=ensure_bool(
            data.get("handshake_first", True), "nats.tls.handshake_first"
        ),
        ca_file=ensure_optional_string(data.get("ca_file"), "nats.tls.ca_file"),
        server_name=ensure_optional_string(data.get("server_name"), "nats.tls.server_name"),
    )


def _parse_nats_auth(value):
    data = ensure_mapping(value, "nats.auth")
    config = NatsAuthConfig(
        mode=ensure_choice(data.get("mode", "none"), "nats.auth.mode", VALID_AUTH_MODES),
        username=ensure_optional_string(data.get("username"), "nats.auth.username"),
        password_env=ensure_optional_string(
            data.get("password_env"), "nats.auth.password_env"
        ),
        token_env=ensure_optional_string(data.get("token_env"), "nats.auth.token_env"),
        nkey_file=ensure_optional_string(data.get("nkey_file"), "nats.auth.nkey_file"),
        creds_file=ensure_optional_string(data.get("creds_file"), "nats.auth.creds_file"),
    )

    if config.mode == "username_password":
        _require_field(config.username, "nats.auth.username")
        _require_field(config.password_env, "nats.auth.password_env")
    elif config.mode == "token":
        _require_field(config.token_env, "nats.auth.token_env")
    elif config.mode == "nkey":
        _require_field(config.nkey_file, "nats.auth.nkey_file")
    elif config.mode == "creds":
        _require_field(config.creds_file, "nats.auth.creds_file")

    return config


def _parse_ros(value):
    data = ensure_mapping(value, "ros")
    qos = ensure_mapping(data.get("qos"), "ros.qos")
    return RosConfig(
        version=normalize_ros_version(data.get("version", "auto"), "ros.version"),
        qos=RosQosConfig(
            reliability=ensure_choice(
                qos.get("reliability", "reliable"),
                "ros.qos.reliability",
                VALID_QOS_RELIABILITY,
            ),
            durability=ensure_choice(
                qos.get("durability", "volatile"),
                "ros.qos.durability",
                VALID_QOS_DURABILITY,
            ),
            depth=ensure_int(qos.get("depth", 10), "ros.qos.depth", minimum=1),
        ),
    )


def _parse_topics(value):
    data = ensure_mapping(value, "topics")
    mode = ensure_choice(data.get("mode", "all"), "topics.mode", VALID_TOPIC_MODES)
    patterns = tuple(
        validate_topic_pattern(pattern, "topics.patterns[{0}]".format(index))
        for index, pattern in enumerate(
            ensure_list_of_strings(data.get("patterns", []), "topics.patterns", allow_empty=True)
        )
    )

    if mode == "all" and patterns:
        raise ConfigError(
            "must be empty or omitted when topics.mode is 'all'",
            field="topics.patterns",
        )
    if mode in ("include", "exclude") and not patterns:
        raise ConfigError(
            "must contain at least one topic pattern when topics.mode is '{0}'".format(mode),
            field="topics.patterns",
        )

    return TopicSelectionConfig(mode=mode, patterns=patterns)


def _parse_services(value):
    data = ensure_mapping(value, "services")
    calls_value = data.get("calls", [])
    if calls_value is None:
        calls_value = []
    if not isinstance(calls_value, list):
        raise ConfigError("must be a list", field="services.calls")

    calls = []
    for index, item in enumerate(calls_value):
        field = "services.calls[{0}]".format(index)
        call = ensure_mapping(item, field)
        service_name = validate_service_name(call.get("name"), "{0}.name".format(field))
        if any(existing.name == service_name for existing in calls):
            raise ConfigError("duplicates an earlier service call", field="{0}.name".format(field))
        calls.append(
            ServiceCallConfig(
                name=service_name,
                service_type=validate_service_type(call.get("type"), "{0}.type".format(field)),
                timeout_ms=ensure_int(
                    call.get("timeout_ms", DEFAULT_SERVICE_TIMEOUT_MS),
                    "{0}.timeout_ms".format(field),
                    minimum=1,
                ),
            )
        )

    return ServicesConfig(
        subject_prefix=validate_subject_prefix(
            ensure_string(
                data.get("subject_prefix", DEFAULT_SERVICE_SUBJECT_PREFIX),
                "services.subject_prefix",
            ),
            "services.subject_prefix",
        ),
        calls=tuple(calls),
    )


def _parse_sync(value):
    data = ensure_mapping(value, "sync")
    return SyncConfig(
        subject_prefix=validate_subject_prefix(
            ensure_string(data.get("subject_prefix", "ros"), "sync.subject_prefix"),
            "sync.subject_prefix",
        ),
        throttle_ms=ensure_int(data.get("throttle_ms", 100), "sync.throttle_ms", minimum=0),
        dedupe=ensure_bool(data.get("dedupe", True), "sync.dedupe"),
        heartbeat_ms=ensure_int(data.get("heartbeat_ms", 0), "sync.heartbeat_ms", minimum=0),
        loop_suppression_ms=ensure_int(
            data.get("loop_suppression_ms", 1000),
            "sync.loop_suppression_ms",
            minimum=0,
        ),
    )


def _validate_app_config(config):
    if not config.services.calls:
        return
    sync_prefix = config.sync.subject_prefix
    service_prefix = config.services.subject_prefix
    if service_prefix == sync_prefix or service_prefix.startswith(sync_prefix + "."):
        raise ConfigError(
            "must not match or be nested under sync.subject_prefix",
            field="services.subject_prefix",
        )


def _require_field(value, field):
    if not value:
        raise ConfigError("is required for the selected authentication mode", field=field)
