"""NATS TCP/TLS adapter for the ROS bridge."""

import asyncio
import inspect
import os
import ssl

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from urllib.parse import urlparse

import nats

from nats import errors as nats_errors
from nats.aio.client import Client as NATSClient

from kinopio_hub_ros.errors import AdapterError

PROBE_ERROR_CATEGORY_DNS = "dns"
PROBE_ERROR_CATEGORY_TCP = "tcp"
PROBE_ERROR_CATEGORY_TLS = "tls"
PROBE_ERROR_CATEGORY_AUTH = "auth"
PROBE_ERROR_CATEGORY_PROTOCOL = "protocol"
PROBE_ERROR_CATEGORY_UNKNOWN = "unknown"


@dataclass(frozen=True)
class NatsProbeResult:
    server: str
    available: bool
    round_trip_ms: float
    category: str
    message: str

    def to_dict(self):
        return {
            "server": self.server,
            "available": self.available,
            "round_trip_ms": self.round_trip_ms,
            "category": self.category,
            "message": self.message,
        }


@dataclass(frozen=True)
class NatsMessage:
    subject: str
    data: bytes
    reply: str
    headers: object


@dataclass(frozen=True)
class NatsHealthStatus:
    state: str
    connected_server: str
    candidate_servers: tuple
    discovered_servers: tuple
    probe_results: tuple
    last_error_category: str
    last_error_message: str
    reconnect_count: int
    disconnect_count: int

    def to_dict(self):
        return {
            "state": self.state,
            "connected_server": self.connected_server,
            "candidate_servers": list(self.candidate_servers),
            "discovered_servers": list(self.discovered_servers),
            "probe_results": [result.to_dict() for result in self.probe_results],
            "last_error_category": self.last_error_category,
            "last_error_message": self.last_error_message,
            "reconnect_count": self.reconnect_count,
            "disconnect_count": self.disconnect_count,
        }


class SubscriptionHandle:
    def __init__(self, subject, subscription):
        self.subject = subject
        self._subscription = subscription

    async def unsubscribe(self):
        if self._subscription is None:
            return
        subscription = self._subscription
        self._subscription = None
        await subscription.unsubscribe()


class NatsAdapter:
    def __init__(
        self,
        config,
        *,
        name=None,
        no_echo=True,
        auto_retry=True,
        connect_timeout=3.0,
        reconnect_time_wait=0.5,
        max_reconnect_attempts=-1,
        ping_interval=5,
        max_ping_out=3,
        flush_timeout=3.0,
        client_factory=None,
    ):
        self._config = config
        self._name = name or config.bridge.bridge_id
        self._no_echo = no_echo
        self._auto_retry = auto_retry
        self._connect_timeout = float(connect_timeout)
        self._reconnect_time_wait = float(reconnect_time_wait)
        self._max_reconnect_attempts = int(max_reconnect_attempts)
        self._ping_interval = int(ping_interval)
        self._max_ping_out = int(max_ping_out)
        self._flush_timeout = float(flush_timeout)
        self._client_factory = client_factory or NATSClient
        self._nc = None
        self._probe_results = ()
        self._last_error = None
        self._last_error_category = None
        self._reconnect_count = 0
        self._disconnect_count = 0
        self._subscriptions = set()
        self._connect_signature = inspect.signature(self._client_factory.connect)

    async def probe_servers(self):
        results = []
        for server in self._config.nats.servers:
            results.append(await self._probe_server(server))
        self._probe_results = tuple(results)
        return self._probe_results

    async def connect(self):
        if self._nc is not None and getattr(self._nc, "is_connected", False):
            return self
        if self._nc is not None:
            await self.close()

        if not self._probe_results:
            await self.probe_servers()

        if not any(result.available for result in self._probe_results):
            failure_summary = "; ".join(
                "{0} -> {1}: {2}".format(
                    result.server,
                    result.category or PROBE_ERROR_CATEGORY_UNKNOWN,
                    result.message or "unavailable",
                )
                for result in self._probe_results
            )
            self._last_error = AdapterError(failure_summary)
            self._last_error_category = (
                self._probe_results[0].category or PROBE_ERROR_CATEGORY_UNKNOWN
            )
            raise AdapterError("no reachable NATS servers: {0}".format(failure_summary))

        ordered_servers = self._order_servers_by_probe_results(
            self._config.nats.servers,
            self._probe_results,
        )
        self._nc = self._client_factory()
        callbacks = self._make_callbacks(self._nc)
        try:
            await asyncio.wait_for(
                self._nc.connect(
                    ordered_servers,
                    **self._connect_kwargs(
                        callbacks=callbacks,
                        allow_reconnect=self._auto_retry,
                    ),
                ),
                timeout=self._initial_connect_timeout(len(ordered_servers)),
            )
            await self.flush()
        except Exception as exc:
            if not isinstance(exc, AdapterError):
                self._remember_error(exc)
            await self._force_close_client(self._nc)
            self._nc = None
            raise AdapterError(
                "NATS connect failed ({0}): {1}".format(
                    self._last_error_category or PROBE_ERROR_CATEGORY_UNKNOWN,
                    self._last_error_message(),
                )
            )
        return self

    async def close(self):
        if self._nc is None:
            return
        await self._force_close_client(self._nc)
        self._nc = None
        self._subscriptions.clear()

    async def reconnect(self):
        await self.close()
        self._probe_results = ()
        return await self.connect()

    async def flush(self):
        try:
            nc = self._require_client()
            await nc.flush(timeout=self._flush_timeout)
        except AdapterError:
            raise
        except Exception as exc:
            raise self._adapter_error("flush", exc) from exc

    async def publish(self, subject, payload, *, headers=None, reply=None):
        try:
            nc = self._require_client()
            await nc.publish(
                subject,
                payload,
                reply=reply or "",
                headers=dict(headers) if headers else None,
            )
        except AdapterError:
            raise
        except Exception as exc:
            raise self._adapter_error("publish", exc) from exc

    async def subscribe(self, subject, callback, *, queue=None, max_messages=None):
        nc = self._require_client()

        async def wrapped(msg):
            result = callback(
                NatsMessage(
                    subject=msg.subject,
                    data=msg.data,
                    reply=msg.reply,
                    headers=getattr(msg, "headers", None) or getattr(msg, "header", None),
                )
            )
            if inspect.isawaitable(result):
                await result

        try:
            subscription = await nc.subscribe(
                subject,
                queue=queue or "",
                cb=wrapped,
                max_msgs=max_messages or 0,
            )
            await nc.flush(timeout=self._flush_timeout)
        except Exception as exc:
            raise self._adapter_error("subscribe", exc) from exc
        handle = SubscriptionHandle(subject, subscription)
        self._subscriptions.add(handle)
        return handle

    def status(self):
        nc = self._nc
        state = "disconnected"
        if nc is not None:
            if getattr(nc, "is_connected", False):
                state = "connected"
            elif getattr(nc, "is_reconnecting", False):
                state = "reconnecting"
            elif getattr(nc, "is_closed", False):
                state = "closed"

        return NatsHealthStatus(
            state=state,
            connected_server=self._resolve_connected_server(nc),
            candidate_servers=tuple(self._config.nats.servers),
            discovered_servers=tuple(self._resolve_discovered_servers(nc)),
            probe_results=tuple(self._probe_results),
            last_error_category=self._last_error_category,
            last_error_message=self._last_error_message(),
            reconnect_count=self._reconnect_count,
            disconnect_count=self._disconnect_count,
        )

    async def _probe_server(self, server):
        probe_client = self._client_factory()
        started = perf_counter()
        observed_errors = []

        async def error_cb(exc):
            observed_errors.append(exc)

        try:
            await probe_client.connect(
                [server],
                **self._connect_kwargs(
                    callbacks={"error_cb": error_cb},
                    allow_reconnect=False,
                    connect_timeout=self._probe_connect_timeout(),
                    reconnect_time_wait=0.0,
                    max_reconnect_attempts=1,
                ),
            )
            await probe_client.flush(timeout=self._flush_timeout)
            return NatsProbeResult(
                server=server,
                available=True,
                round_trip_ms=(perf_counter() - started) * 1000.0,
                category=None,
                message=None,
            )
        except Exception as exc:
            category = None
            message = None
            if observed_errors:
                category, message = classify_nats_exception(observed_errors[-1])
            if category in (None, PROBE_ERROR_CATEGORY_UNKNOWN):
                category, message = classify_nats_exception(exc)
            return NatsProbeResult(
                server=server,
                available=False,
                round_trip_ms=None,
                category=category,
                message=message,
            )
        finally:
            await self._force_close_client(probe_client)

    def _connect_kwargs(
        self,
        *,
        callbacks,
        allow_reconnect,
        connect_timeout=None,
        reconnect_time_wait=None,
        max_reconnect_attempts=None,
    ):
        kwargs = {
            "allow_reconnect": allow_reconnect,
            "connect_timeout": (
                self._connect_timeout if connect_timeout is None else float(connect_timeout)
            ),
            "reconnect_time_wait": (
                self._reconnect_time_wait
                if reconnect_time_wait is None
                else float(reconnect_time_wait)
            ),
            "max_reconnect_attempts": (
                self._max_reconnect_attempts
                if max_reconnect_attempts is None and allow_reconnect
                else 0 if max_reconnect_attempts is None
                else int(max_reconnect_attempts)
            ),
            "ping_interval": self._ping_interval,
            "max_outstanding_pings": self._max_ping_out,
            "dont_randomize": True,
            "no_echo": self._no_echo,
            "name": self._name,
        }
        if callbacks is not None:
            kwargs.update(callbacks)

        tls_context = build_tls_context(self._config)
        if tls_context is not None:
            kwargs["tls"] = tls_context
        if self._config.nats.tls.server_name:
            kwargs["tls_hostname"] = self._config.nats.tls.server_name
        if self._config.nats.tls.handshake_first:
            self._require_connect_param("tls_handshake_first")
            kwargs["tls_handshake_first"] = True

        kwargs.update(self._auth_connect_kwargs())
        return self._filter_supported_connect_kwargs(kwargs)

    def _auth_connect_kwargs(self):
        auth = self._config.nats.auth
        if auth.mode == "none":
            return {}
        if auth.mode == "username_password":
            return {
                "user": auth.username,
                "password": read_required_env(auth.password_env),
            }
        if auth.mode == "token":
            return {
                "token": read_required_env(auth.token_env),
            }
        if auth.mode == "creds":
            self._require_connect_param("user_credentials")
            return {
                "user_credentials": str(Path(auth.creds_file).expanduser()),
            }
        if auth.mode == "nkey":
            seed = Path(auth.nkey_file).expanduser().read_text(encoding="utf-8").strip()
            if "nkeys_seed_str" in self._connect_signature.parameters:
                return {"nkeys_seed_str": seed}
            if "nkeys_seed" in self._connect_signature.parameters:
                return {"nkeys_seed": seed}
            raise AdapterError("installed nats-py does not support nkey seed authentication")
        raise AdapterError("unsupported auth mode: {0}".format(auth.mode))

    def _filter_supported_connect_kwargs(self, kwargs):
        accepts_var_keyword = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in self._connect_signature.parameters.values()
        )
        supported = {}
        for key, value in kwargs.items():
            if value is None:
                continue
            if accepts_var_keyword or key in self._connect_signature.parameters:
                supported[key] = value
        return supported

    def _require_connect_param(self, name):
        accepts_var_keyword = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in self._connect_signature.parameters.values()
        )
        if not accepts_var_keyword and name not in self._connect_signature.parameters:
            raise AdapterError(
                "installed nats-py does not support connect parameter '{0}'".format(name)
            )

    def _make_callbacks(self, client):
        async def error_cb(exc):
            if self._nc is client:
                self._remember_error(exc)

        async def disconnected_cb():
            if self._nc is client:
                self._disconnect_count += 1

        async def closed_cb():
            if self._nc is client and self._last_error is None:
                self._last_error_category = None

        async def reconnected_cb():
            if self._nc is client:
                self._reconnect_count += 1

        return {
            "error_cb": error_cb,
            "disconnected_cb": disconnected_cb,
            "closed_cb": closed_cb,
            "reconnected_cb": reconnected_cb,
        }

    def _order_servers_by_probe_results(self, servers, probe_results):
        healthy = [result for result in probe_results if result.available]
        if not healthy:
            return list(servers)
        ordered = sorted(
            probe_results,
            key=lambda item: (
                0 if item.available else 1,
                item.round_trip_ms if item.round_trip_ms is not None else float("inf"),
                servers.index(item.server),
            ),
        )
        return [item.server for item in ordered]

    def _initial_connect_timeout(self, server_count):
        return (self._connect_timeout * max(1, server_count)) + self._flush_timeout + 1.0

    def _probe_connect_timeout(self):
        return min(self._connect_timeout, 2.0)

    def _remember_error(self, exc):
        self._last_error = exc
        self._last_error_category, _ = classify_nats_exception(exc)

    def _adapter_error(self, operation, exc):
        self._remember_error(exc)
        return AdapterError(
            "NATS {0} failed ({1}): {2}".format(
                operation,
                self._last_error_category or PROBE_ERROR_CATEGORY_UNKNOWN,
                self._last_error_message(),
            )
        )

    def _last_error_message(self):
        if self._last_error is None:
            return None
        return "{0}: {1}".format(type(self._last_error).__name__, self._last_error)

    def _require_client(self):
        if self._nc is None or not getattr(self._nc, "is_connected", False):
            exc = AdapterError("NATS connection is not available")
            self._remember_error(exc)
            raise exc
        return self._nc

    async def _force_close_client(self, client):
        if client is None:
            return
        close = getattr(client, "close", None)
        if callable(close):
            try:
                await close()
            except Exception:
                pass

    def _resolve_connected_server(self, client):
        if client is None:
            return None
        connected_url = getattr(client, "connected_url", None)
        if connected_url is None:
            return None
        if isinstance(connected_url, str):
            return connected_url
        geturl = getattr(connected_url, "geturl", None)
        if callable(geturl):
            value = geturl()
            if isinstance(value, str):
                return value
        return str(connected_url)

    def _resolve_discovered_servers(self, client):
        if client is None:
            return ()
        servers = getattr(client, "discovered_servers", None)
        if not servers:
            return ()
        resolved = []
        for server in servers:
            if isinstance(server, str):
                resolved.append(server)
                continue
            geturl = getattr(server, "geturl", None)
            if callable(geturl):
                value = geturl()
                if isinstance(value, str):
                    resolved.append(value)
                    continue
            resolved.append(str(server))
        return tuple(resolved)


def build_tls_context(config):
    should_enable_tls = bool(config.nats.tls.enabled) or any(
        urlparse(server).scheme == "tls" for server in config.nats.servers
    )
    if not should_enable_tls:
        return None

    context = ssl.create_default_context()
    if config.nats.tls.ca_file:
        context.load_verify_locations(cafile=str(Path(config.nats.tls.ca_file).expanduser()))
    return context


def read_required_env(name):
    value = os.getenv(name or "")
    if value is None or value == "":
        raise AdapterError("required environment variable is missing: {0}".format(name))
    return value


def classify_nats_exception(exc):
    root = unwrap_exception(exc)

    if isinstance(root, OSError) and type(root).__name__ == "gaierror":
        return PROBE_ERROR_CATEGORY_DNS, "{0}: {1}".format(type(root).__name__, root)
    if isinstance(root, ssl.SSLError):
        return PROBE_ERROR_CATEGORY_TLS, "{0}: {1}".format(type(root).__name__, root)
    if isinstance(root, ssl.CertificateError):
        return PROBE_ERROR_CATEGORY_TLS, "{0}: {1}".format(type(root).__name__, root)
    if isinstance(root, (ConnectionRefusedError, TimeoutError, OSError)):
        return PROBE_ERROR_CATEGORY_TCP, "{0}: {1}".format(type(root).__name__, root)
    if isinstance(root, getattr(nats_errors, "AuthorizationError", tuple())):
        return PROBE_ERROR_CATEGORY_AUTH, "{0}: {1}".format(type(root).__name__, root)
    if isinstance(root, getattr(nats_errors, "NoServersError", tuple())):
        return PROBE_ERROR_CATEGORY_TCP, "{0}: {1}".format(type(root).__name__, root)
    if isinstance(
        root,
        tuple(
            item
            for item in (
                getattr(nats_errors, "ProtocolError", None),
                getattr(nats_errors, "BadSubjectError", None),
                getattr(nats_errors, "JsonParseError", None),
            )
            if item is not None
        ),
    ):
        return PROBE_ERROR_CATEGORY_PROTOCOL, "{0}: {1}".format(type(root).__name__, root)
    if isinstance(
        root,
        tuple(
            item
            for item in (
                getattr(nats_errors, "SecureConnRequiredError", None),
                getattr(nats_errors, "SecureConnWantedError", None),
                getattr(nats_errors, "SecureConnFailedError", None),
            )
            if item is not None
        ),
    ):
        return PROBE_ERROR_CATEGORY_TLS, "{0}: {1}".format(type(root).__name__, root)

    text = "{0}: {1}".format(type(root).__name__, root)
    lowered = text.lower()
    if "auth" in lowered or "permission" in lowered or "credential" in lowered:
        return PROBE_ERROR_CATEGORY_AUTH, text
    if "tls" in lowered or "ssl" in lowered or "certificate" in lowered or "handshake" in lowered:
        return PROBE_ERROR_CATEGORY_TLS, text
    if "protocol" in lowered or "parser" in lowered or "subject" in lowered:
        return PROBE_ERROR_CATEGORY_PROTOCOL, text
    if "name or service not known" in lowered or "nodename nor servname provided" in lowered:
        return PROBE_ERROR_CATEGORY_DNS, text
    if (
        "refused" in lowered
        or "timed out" in lowered
        or "timeout" in lowered
        or "unreachable" in lowered
    ):
        return PROBE_ERROR_CATEGORY_TCP, text
    return PROBE_ERROR_CATEGORY_UNKNOWN, text


def unwrap_exception(exc):
    current = exc
    seen = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        next_exc = getattr(current, "__cause__", None) or getattr(current, "__context__", None)
        if next_exc is None:
            return current
        current = next_exc
    return exc
