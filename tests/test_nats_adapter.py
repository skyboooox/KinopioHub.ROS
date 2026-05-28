import asyncio
import ssl
import subprocess
import tempfile
import time

from pathlib import Path

import pytest

from kinopio_hub_ros.business.configuration import DEFAULT_NATS_SERVERS, load_config
from kinopio_hub_ros.business.nats_adapter import (
    NatsAdapter,
    PROBE_ERROR_CATEGORY_AUTH,
    PROBE_ERROR_CATEGORY_DNS,
    PROBE_ERROR_CATEGORY_PROTOCOL,
    PROBE_ERROR_CATEGORY_TCP,
    PROBE_ERROR_CATEGORY_TLS,
    classify_nats_exception,
)
from kinopio_hub_ros.errors import AdapterError


def shutil_which(name):
    import shutil

    return shutil.which(name)


def docker_daemon_available():
    if shutil_which("docker") is None:
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


class _FakeConnectedUrl:
    def __init__(self, value):
        self._value = value

    def geturl(self):
        return self._value


class _FakeClient:
    connect_kwargs_seen = []
    server_outcomes = {}
    flush_exception = None

    def __init__(self):
        self.options = {}
        self.is_connected = False
        self.is_reconnecting = False
        self.is_closed = False
        self.connected_url = None
        self.discovered_servers = []

    async def connect(self, servers, **kwargs):
        _FakeClient.connect_kwargs_seen.append(kwargs)
        self.options.update(kwargs)
        server = servers[0]
        outcome = _FakeClient.server_outcomes.get(server, {})
        exc = outcome.get("exception")
        if exc is not None:
            raise exc
        self.is_connected = True
        self.connected_url = _FakeConnectedUrl(server)

    async def flush(self, timeout=None):
        if _FakeClient.flush_exception is not None:
            raise _FakeClient.flush_exception
        return None

    async def close(self):
        self.is_connected = False
        self.is_closed = True

    async def publish(self, subject, payload, reply="", headers=None):
        self.last_publish = {
            "subject": subject,
            "payload": payload,
            "reply": reply,
            "headers": headers,
        }

    async def subscribe(self, subject, queue="", cb=None, max_msgs=0):
        self.last_subscribe = {
            "subject": subject,
            "queue": queue,
            "max_msgs": max_msgs,
        }
        return _FakeSubscription()


class _FakeSubscription:
    async def unsubscribe(self):
        return None


def test_auth_config_supports_creds_mode(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
nats:
  auth:
    mode: creds
    creds_file: /tmp/example.creds
topics:
  mode: all
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(path)

    assert config.nats.auth.mode == "creds"
    assert config.nats.auth.creds_file == "/tmp/example.creds"


def test_probe_orders_healthy_servers_before_unhealthy(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
nats:
  servers:
    - tls://bad.example:14222
    - tls://good.example:14222
topics:
  mode: all
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = load_config(path)
    _FakeClient.connect_kwargs_seen = []
    _FakeClient.flush_exception = None
    _FakeClient.server_outcomes = {
        "tls://bad.example:14222": {"exception": ConnectionRefusedError("refused")},
        "tls://good.example:14222": {},
    }

    adapter = NatsAdapter(config, client_factory=_FakeClient)

    async def run():
        results = await adapter.probe_servers()
        await adapter.connect()
        return results, adapter.status()

    results, status = asyncio.run(run())

    assert [result.available for result in results] == [False, True]
    assert status.connected_server == "tls://good.example:14222"


def test_connect_passes_tls_first_and_no_echo_options(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
nats:
  tls:
    enabled: true
    handshake_first: true
    server_name: localhost
topics:
  mode: all
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = load_config(path)
    _FakeClient.connect_kwargs_seen = []
    _FakeClient.flush_exception = None
    _FakeClient.server_outcomes = {server: {} for server in DEFAULT_NATS_SERVERS}

    adapter = NatsAdapter(config, client_factory=_FakeClient)
    asyncio.run(adapter.connect())

    connect_kwargs = _FakeClient.connect_kwargs_seen[-1]

    assert connect_kwargs["allow_reconnect"] is True
    assert connect_kwargs["no_echo"] is True
    assert connect_kwargs["tls_handshake_first"] is True
    assert connect_kwargs["tls_hostname"] == "localhost"
    assert isinstance(connect_kwargs["tls"], ssl.SSLContext)
    assert connect_kwargs["max_reconnect_attempts"] == -1


def test_connect_raises_after_probe_failures(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
nats:
  servers:
    - tls://bad-a.example:14222
    - tls://bad-b.example:14222
topics:
  mode: all
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = load_config(path)
    _FakeClient.connect_kwargs_seen = []
    _FakeClient.flush_exception = None
    _FakeClient.server_outcomes = {
        "tls://bad-a.example:14222": {"exception": TimeoutError("timed out")},
        "tls://bad-b.example:14222": {"exception": ConnectionRefusedError("refused")},
    }

    adapter = NatsAdapter(config, client_factory=_FakeClient)

    with pytest.raises(Exception) as exc_info:
        asyncio.run(adapter.connect())

    message = str(exc_info.value)
    assert "no reachable NATS servers" in message
    assert "timed out" in message or "refused" in message


def test_flush_timeout_is_wrapped_as_adapter_error_and_status_is_updated(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
topics:
  mode: all
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = load_config(path)
    _FakeClient.connect_kwargs_seen = []
    _FakeClient.flush_exception = None
    _FakeClient.server_outcomes = {server: {} for server in DEFAULT_NATS_SERVERS}
    adapter = NatsAdapter(config, client_factory=_FakeClient)

    async def run():
        await adapter.connect()
        _FakeClient.flush_exception = TimeoutError("nats: flush timeout")
        try:
            with pytest.raises(AdapterError, match="NATS flush failed"):
                await adapter.flush()
            return adapter.status()
        finally:
            _FakeClient.flush_exception = None
            await adapter.close()

    status = asyncio.run(run())

    assert status.last_error_category == PROBE_ERROR_CATEGORY_TCP
    assert "flush timeout" in status.last_error_message


def test_error_classifier_distinguishes_major_categories():
    class _AuthError(Exception):
        pass

    assert classify_nats_exception(OSError("nodename nor servname provided"))[0] in (
        PROBE_ERROR_CATEGORY_DNS,
        PROBE_ERROR_CATEGORY_TCP,
    )
    assert classify_nats_exception(ConnectionRefusedError("refused"))[0] == PROBE_ERROR_CATEGORY_TCP
    assert classify_nats_exception(ssl.SSLError("handshake failed"))[0] == PROBE_ERROR_CATEGORY_TLS
    assert classify_nats_exception(_AuthError("authorization violation"))[0] == PROBE_ERROR_CATEGORY_AUTH
    assert classify_nats_exception(ValueError("protocol parser exploded"))[0] == PROBE_ERROR_CATEGORY_PROTOCOL


@pytest.mark.skipif(not docker_daemon_available(), reason="docker daemon is not available")
def test_adapter_can_publish_and_subscribe_against_local_tls_nats(tmp_path):
    async def run():
        work_dir = Path(tempfile.mkdtemp(prefix="kinopio-hub-ros-nats."))
        server = None
        try:
            cert_dir = work_dir / "certs"
            cert_dir.mkdir(parents=True, exist_ok=True)
            _generate_tls_material(cert_dir)
            config_path = work_dir / "config.yaml"
            config_path.write_text(
                f"""
bridge:
  id: local-test-bridge
nats:
  servers:
    - tls://127.0.0.1:24222
  tls:
    enabled: true
    handshake_first: true
    ca_file: {str(cert_dir / "ca.pem")}
    server_name: localhost
  auth:
    mode: none
topics:
  mode: all
""".strip()
                + "\n",
                encoding="utf-8",
            )
            (work_dir / "server.conf").write_text(
                f"""
port: 4222
tls {{
  cert_file: "/work/certs/server.pem"
  key_file: "/work/certs/server-key.pem"
  ca_file: "/work/certs/ca.pem"
  verify: false
  handshake_first: true
}}
""".strip()
                + "\n",
                encoding="utf-8",
            )

            server = subprocess.Popen(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--name",
                    "kinopio-hub-ros-test-nats",
                    "-p",
                    "24222:4222",
                    "-v",
                    f"{work_dir}:/work",
                    "nats:2.10-alpine",
                    "-c",
                    "/work/server.conf",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            _wait_for_tcp("127.0.0.1", 24222, timeout_seconds=10.0)

            config = load_config(config_path)
            publisher = NatsAdapter(config)
            subscriber = NatsAdapter(config)
            received = []
            try:
                await publisher.connect()
                await subscriber.connect()
                await subscriber.subscribe(
                    "ros.test.subject",
                    lambda msg: received.append(msg.data.decode("utf-8")),
                    max_messages=1,
                )
                await publisher.publish("ros.test.subject", b"hello tls")
                await publisher.flush()
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline and not received:
                    await asyncio.sleep(0.05)
                assert received == ["hello tls"]
            finally:
                await publisher.close()
                await subscriber.close()
        finally:
            if server is not None:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()

    asyncio.run(run())


def _wait_for_tcp(host, port, timeout_seconds):
    import socket

    deadline = time.monotonic() + timeout_seconds
    last_error = None
    while time.monotonic() < deadline:
        sock = socket.socket()
        sock.settimeout(0.25)
        try:
            sock.connect((host, port))
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.1)
        finally:
            sock.close()
    raise RuntimeError("timed out waiting for TCP port {0}:{1}: {2}".format(host, port, last_error))


def _generate_tls_material(cert_dir):
    subprocess.run(
        [
            "openssl",
            "genrsa",
            "-out",
            str(cert_dir / "ca-key.pem"),
            "2048",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-new",
            "-key",
            str(cert_dir / "ca-key.pem"),
            "-days",
            "1",
            "-subj",
            "/CN=KinopioHub.ROS Test CA",
            "-out",
            str(cert_dir / "ca.pem"),
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-addext",
            "subjectKeyIdentifier=hash",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    (cert_dir / "server.ext").write_text(
        """
subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
""".strip()
        + "\n",
        encoding="utf-8",
    )
    subprocess.run(
        [
            "openssl",
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            str(cert_dir / "server-key.pem"),
            "-out",
            str(cert_dir / "server.csr"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            "openssl",
            "x509",
            "-req",
            "-days",
            "1",
            "-in",
            str(cert_dir / "server.csr"),
            "-CA",
            str(cert_dir / "ca.pem"),
            "-CAkey",
            str(cert_dir / "ca-key.pem"),
            "-CAcreateserial",
            "-out",
            str(cert_dir / "server.pem"),
            "-extfile",
            str(cert_dir / "server.ext"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
