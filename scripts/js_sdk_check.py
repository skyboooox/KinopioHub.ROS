#!/usr/bin/env python3
"""Check KinopioHub.JS interoperability against a remote NATS cluster."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

ROOT_DIR = Path(__file__).resolve().parents[1]
CURRENT_SRC_DIR = ROOT_DIR / "src"
JS_CLIENT_PATH = ROOT_DIR / "scripts" / "kinopiohub_js_client.mjs"
PYTHON_INSTALL_HINT = 'python -m pip install -e ".[test]"'

if str(CURRENT_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_SRC_DIR))

try:
    from kinopio_hub_ros.business.configuration import load_config  # noqa: E402
    from kinopio_hub_ros.business.envelope import (  # noqa: E402
        LEGACY_TEXT_ENVELOPE_SCHEMA,
        ROS2_STRING_MESSAGE_TYPE,
        build_text_envelope,
    )
    from kinopio_hub_ros.business.nats_adapter import NatsAdapter  # noqa: E402
    from kinopio_hub_ros.business.ros2_adapter import Ros2Adapter  # noqa: E402
    from kinopio_hub_ros.business.subject_mapping import topic_to_subject  # noqa: E402
    from kinopio_hub_ros.core.bridge_runtime import BridgeRuntime  # noqa: E402
except ModuleNotFoundError as exc:
    print(
        json.dumps(
            {
                "kind": "js-sdk-check",
                "ok": False,
                "errorType": exc.__class__.__name__,
                "error": str(exc),
                "python_install_hint": PYTHON_INSTALL_HINT,
            },
            indent=2,
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    raise SystemExit(1)


def parse_server_list(value: str | None) -> tuple[str, ...]:
    return tuple(item.strip() for item in (value or "").split(",") if item.strip())


TCP_TLS_SERVERS = parse_server_list(os.environ.get("KINOPIO_HUB_ROS_NATS_TLS_SERVERS"))
ROS_TOPIC = "/chatter"
SUBJECT_PREFIX = "ros"
SUBJECT = topic_to_subject(ROS_TOPIC, SUBJECT_PREFIX)
STRING_ENVELOPE_SCHEMA = LEGACY_TEXT_ENVELOPE_SCHEMA
STARTUP_TIMEOUT_SEC = 20.0
EVENT_TIMEOUT_SEC = 12.0
JS_INSTALL_HINT = (
    "npm install --prefix /tmp/kinopiohub-js-check github:skyboooox/KinopioHub.JS && "
    "export KINOPIOHUB_JS_ENTRYPOINT=/tmp/kinopiohub-js-check/node_modules/kinopio-hub/kinopio.mjs"
)


class EventBuffer:
    def __init__(self) -> None:
        self._items: list[dict[str, Any]] = []
        self._condition = asyncio.Condition()

    @property
    def items(self) -> list[dict[str, Any]]:
        return list(self._items)

    def mark(self) -> int:
        return len(self._items)

    async def append(self, item: dict[str, Any]) -> None:
        async with self._condition:
            self._items.append(item)
            self._condition.notify_all()

    async def wait_for(
        self,
        predicate: Callable[[dict[str, Any]], bool],
        *,
        start: int = 0,
        timeout: float = EVENT_TIMEOUT_SEC,
        description: str,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        index = start

        while True:
            async with self._condition:
                while True:
                    while index < len(self._items):
                        item = self._items[index]
                        index += 1
                        if predicate(item):
                            return item

                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise TimeoutError(f"Timed out waiting for {description}")
                    await asyncio.wait_for(self._condition.wait(), timeout=remaining)


class ScriptRos2Driver:
    def __init__(self, *, distro: str = "humble", topics: tuple[tuple[str, tuple[str, ...]], ...]):
        self.distro = distro
        self._topics = list(topics)
        self.started_with: dict[str, Any] | None = None
        self.shutdown_called = False
        self.subscriptions: dict[str, Callable[..., None]] = {}
        self.subscription_types: dict[str, str] = {}
        self.publishers: dict[str, str] = {}
        self.publisher_types: dict[str, str] = {}
        self.published: list[tuple[str, str]] = []
        self.spin_calls: list[float] = []

    def start(self, *, node_name: str, qos_config: object) -> None:
        self.started_with = {
            "node_name": node_name,
            "qos_config": qos_config,
        }

    def shutdown(self) -> None:
        self.shutdown_called = True

    def list_topics_and_types(self) -> tuple[tuple[str, tuple[str, ...]], ...]:
        return tuple((topic, tuple(types)) for topic, types in self._topics)

    def create_text_subscription(
        self,
        topic: str,
        message_type: str,
        callback: Callable[..., None],
    ) -> str:
        self.subscriptions[topic] = callback
        self.subscription_types[topic] = message_type
        return topic

    def create_text_publisher(self, topic: str, message_type: str) -> str:
        self.publishers[topic] = topic
        self.publisher_types[topic] = message_type
        return topic

    def publish_text(self, publisher: str, text: str) -> None:
        self.published.append((publisher, text))

    def load_message_type(self, message_type: str) -> str:
        return message_type

    def spin_once(self, *, timeout_sec: float) -> None:
        self.spin_calls.append(timeout_sec)

    def emit(self, topic: str, text: str, json_value: object = None) -> None:
        callback = self.subscriptions.get(topic)
        if callback is None:
            raise RuntimeError(f"No subscription registered for {topic}")
        callback(text, json_value)


class JsSdkClient:
    def __init__(self) -> None:
        self._process: asyncio.subprocess.Process | None = None
        self._events = EventBuffer()
        self._stderr_lines: list[str] = []
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None

    @property
    def stderr_lines(self) -> list[str]:
        return list(self._stderr_lines)

    @property
    def events(self) -> list[dict[str, Any]]:
        return self._events.items

    def mark(self) -> int:
        return self._events.mark()

    async def start(self) -> dict[str, Any]:
        if not JS_CLIENT_PATH.is_file():
            raise FileNotFoundError(f"JS client script is missing: {JS_CLIENT_PATH}")
        self._process = await asyncio.create_subprocess_exec(
            "node",
            str(JS_CLIENT_PATH),
            cwd=str(ROOT_DIR),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._stdout_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        return await self.wait_for(
            lambda item: item.get("event") in ("ready", "fatal"),
            description="JS client startup",
            timeout=STARTUP_TIMEOUT_SEC,
        )

    async def _read_stdout(self) -> None:
        assert self._process is not None
        assert self._process.stdout is not None
        while True:
            line = await self._process.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                payload = {
                    "event": "stdout-text",
                    "raw": text,
                }
            await self._events.append(payload)

    async def _read_stderr(self) -> None:
        assert self._process is not None
        assert self._process.stderr is not None
        while True:
            line = await self._process.stderr.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            if text:
                self._stderr_lines.append(text)

    async def send(self, payload: dict[str, Any]) -> None:
        if self._process is None or self._process.stdin is None:
            raise RuntimeError("JS client is not running")
        self._process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
        await self._process.stdin.drain()

    async def wait_for(
        self,
        predicate: Callable[[dict[str, Any]], bool],
        *,
        start: int = 0,
        timeout: float = EVENT_TIMEOUT_SEC,
        description: str,
    ) -> dict[str, Any]:
        return await self._events.wait_for(
            predicate,
            start=start,
            timeout=timeout,
            description=description,
        )

    async def close(self) -> None:
        if self._process is None:
            return
        if self._process.returncode is None:
            try:
                await self.send({"action": "dispose"})
            except Exception:
                pass
        try:
            await asyncio.wait_for(self._process.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            self._process.kill()
            await self._process.wait()
        if self._stdout_task is not None:
            await self._stdout_task
        if self._stderr_task is not None:
            await self._stderr_task


@dataclass
class RuntimeHarness:
    runtime: BridgeRuntime
    ros_driver: ScriptRos2Driver
    nats_adapter: NatsAdapter
    pump_task: asyncio.Task[None]
    stop_event: asyncio.Event


async def pump_runtime(runtime: BridgeRuntime, stop_event: asyncio.Event) -> None:
    try:
        while not stop_event.is_set():
            await runtime.tick(spin_timeout_sec=0.01)
            await asyncio.sleep(0.01)
    except asyncio.CancelledError:
        raise


def build_js_sdk_config_text() -> str:
    server_lines = "\n".join(f"    - {server}" for server in TCP_TLS_SERVERS)
    return f"""
bridge:
  id: js-sdk-check-bridge
  direction: bidirectional

nats:
  servers:
{server_lines}
  tls:
    enabled: true
    handshake_first: true
    ca_file: null
    server_name: null
  auth:
    mode: none
    username: null
    password_env: null
    token_env: null
    nkey_file: null
    creds_file: null

ros:
  version: 2
  qos:
    reliability: reliable
    durability: volatile
    depth: 10

topics:
  mode: include
  patterns:
    - /chatter

sync:
  subject_prefix: ros
  throttle_ms: 0
  dedupe: true
  heartbeat_ms: 0
  loop_suppression_ms: 1000
""".strip() + "\n"


def load_js_sdk_config() -> Any:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as handle:
        handle.write(build_js_sdk_config_text())
        temp_path = Path(handle.name)
    try:
        return load_config(temp_path)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def build_writeback_payload(*, text: str, bridge_id: str, sequence: int) -> dict[str, Any]:
    return build_text_envelope(
        direction="nats_to_ros",
        topic=ROS_TOPIC,
        subject=SUBJECT,
        text=text,
        bridge_id=bridge_id,
        sequence=sequence,
        ros_version=2,
        ros_distro="humble",
        ros_message_type=ROS2_STRING_MESSAGE_TYPE,
    ).to_dict()


def is_matching_envelope(
    payload: dict[str, Any],
    *,
    direction: str,
    text: str,
) -> bool:
    return (
        payload.get("schema") == STRING_ENVELOPE_SCHEMA
        and payload.get("direction") == direction
        and payload.get("topic") == ROS_TOPIC
        and payload.get("subject") == SUBJECT
        and isinstance(payload.get("data"), dict)
        and payload["data"].get("text") == text
    )


async def wait_for_ros_writeback(
    ros_driver: ScriptRos2Driver,
    *,
    text: str,
    timeout: float = EVENT_TIMEOUT_SEC,
) -> tuple[str, str]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        for published_topic, published_text in ros_driver.published:
            if published_topic == ROS_TOPIC and published_text == text:
                return published_topic, published_text
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for ROS writeback text={text!r}")
        await asyncio.sleep(min(0.05, remaining))


async def start_bridge_runtime(config: Any) -> RuntimeHarness:
    ros_driver = ScriptRos2Driver(
        topics=((ROS_TOPIC, (ROS2_STRING_MESSAGE_TYPE,)),),
    )
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    nats_adapter = NatsAdapter(
        config,
        name="js-sdk-check-bridge",
        connect_timeout=3.0,
        flush_timeout=3.0,
    )
    runtime = BridgeRuntime(config, nats_adapter=nats_adapter, ros_adapter=ros_adapter)
    await runtime.start()
    stop_event = asyncio.Event()
    pump_task = asyncio.create_task(pump_runtime(runtime, stop_event), name="js-sdk-check-runtime")
    return RuntimeHarness(
        runtime=runtime,
        ros_driver=ros_driver,
        nats_adapter=nats_adapter,
        pump_task=pump_task,
        stop_event=stop_event,
    )


async def stop_bridge_runtime(harness: RuntimeHarness | None) -> None:
    if harness is None:
        return
    harness.stop_event.set()
    harness.pump_task.cancel()
    try:
        await harness.pump_task
    except asyncio.CancelledError:
        pass
    await harness.runtime.close()


async def run_check() -> dict[str, Any]:
    if not TCP_TLS_SERVERS:
        raise RuntimeError("KINOPIO_HUB_ROS_NATS_TLS_SERVERS is required for the JS SDK check")

    config = load_js_sdk_config()
    js_client = JsSdkClient()
    bridge_harness: RuntimeHarness | None = None
    acceptance_steps: list[dict[str, Any]] = []

    try:
        js_ready = await js_client.start()
        if js_ready.get("event") == "fatal":
            raise RuntimeError(
                "JS client failed during startup: "
                + json.dumps(js_ready, ensure_ascii=False, sort_keys=True)
            )
        bridge_harness = await start_bridge_runtime(config)

        js_initial_mark = js_client.mark()
        bridge_harness.ros_driver.emit(ROS_TOPIC, "hello from ros")
        js_ros_event = await js_client.wait_for(
            lambda item: item.get("event") == "received"
            and isinstance(item.get("data"), dict)
            and is_matching_envelope(item["data"], direction="ros_to_nats", text="hello from ros"),
            start=js_initial_mark,
            description="KinopioHub.JS ros_to_nats envelope",
        )
        acceptance_steps.append(
            {
                "step": "ros_to_nats",
                "consumer": "kinopio-hub-js",
                "subject": SUBJECT,
                "topic": ROS_TOPIC,
                "schema": STRING_ENVELOPE_SCHEMA,
                "direction": "ros_to_nats",
                "text": "hello from ros",
                "server": js_ros_event.get("activeServer"),
            }
        )

        js_publish_mark = js_client.mark()
        await js_client.send(
            {
                "action": "publish",
                "label": "js-writeback",
                "payload": build_writeback_payload(
                    text="js writeback",
                    bridge_id="kinopiohub-js-sdk",
                    sequence=1,
                ),
            }
        )
        js_publish_event = await js_client.wait_for(
            lambda item: item.get("event") == "published"
            and item.get("label") == "js-writeback",
            start=js_publish_mark,
            description="KinopioHub.JS publish acknowledgement",
        )
        await wait_for_ros_writeback(bridge_harness.ros_driver, text="js writeback")
        acceptance_steps.append(
            {
                "step": "nats_to_ros",
                "producer": "kinopio-hub-js",
                "subject": SUBJECT,
                "topic": ROS_TOPIC,
                "schema": STRING_ENVELOPE_SCHEMA,
                "direction": "nats_to_ros",
                "text": "js writeback",
                "server": js_publish_event.get("activeServer"),
            }
        )

        js_reconnect_mark = js_client.mark()
        await js_client.send({"action": "reconnect"})
        js_reconnected_event = await js_client.wait_for(
            lambda item: item.get("event") == "reconnected",
            start=js_reconnect_mark,
            description="KinopioHub.JS reconnect acknowledgement",
        )

        js_recovery_mark = js_client.mark()
        bridge_harness.ros_driver.emit(ROS_TOPIC, "after reconnect")
        js_recovery_event = await js_client.wait_for(
            lambda item: item.get("event") == "received"
            and isinstance(item.get("data"), dict)
            and is_matching_envelope(item["data"], direction="ros_to_nats", text="after reconnect"),
            start=js_recovery_mark,
            description="KinopioHub.JS recovered ros_to_nats envelope",
        )
        acceptance_steps.append(
            {
                "step": "reconnect",
                "consumer": "kinopio-hub-js",
                "subject": SUBJECT,
                "topic": ROS_TOPIC,
                "schema": STRING_ENVELOPE_SCHEMA,
                "direction": "ros_to_nats",
                "text": "after reconnect",
                "server": js_recovery_event.get("activeServer"),
            }
        )

        bridge_status = bridge_harness.nats_adapter.status().to_dict()
        return {
            "kind": "js-sdk-check",
            "ok": True,
            "subject": SUBJECT,
            "topic": ROS_TOPIC,
            "schema": STRING_ENVELOPE_SCHEMA,
            "bridge": {
                "connected_server": bridge_status["connected_server"],
                "candidate_servers": bridge_status["candidate_servers"],
                "probe_results": bridge_status["probe_results"],
            },
            "js": {
                "initial_server": js_ready.get("activeServer"),
                "reconnected_server": js_reconnected_event.get("activeServer"),
                "received_count": sum(
                    1 for item in js_client.events if item.get("event") == "received"
                ),
                "stderr_tail": js_client.stderr_lines[-10:],
            },
            "ros_received_texts": [
                text for topic, text in bridge_harness.ros_driver.published if topic == ROS_TOPIC
            ],
            "steps": acceptance_steps,
        }
    finally:
        await js_client.close()
        await stop_bridge_runtime(bridge_harness)


def failure_payload(exc: Exception) -> dict[str, Any]:
    return {
        "kind": "js-sdk-check",
        "ok": False,
        "errorType": exc.__class__.__name__,
        "error": str(exc),
        "required": {
            "KINOPIO_HUB_ROS_NATS_TLS_SERVERS": "tls:// host list for this bridge",
            "KINOPIOHUB_JS_NATS_WSS_SERVERS": "wss:// host list for KinopioHub.JS",
            "KINOPIOHUB_JS_ENTRYPOINT": "optional explicit JS SDK entrypoint",
        },
        "js_install_hint": JS_INSTALL_HINT,
    }


async def main() -> int:
    try:
        result = await run_check()
    except Exception as exc:
        print(json.dumps(failure_payload(exc), indent=2, sort_keys=True), file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    if not os.environ.get("PYTHONUNBUFFERED"):
        os.environ["PYTHONUNBUFFERED"] = "1"
    raise SystemExit(asyncio.run(main()))
