import asyncio
import inspect
import json

from kinopio_hub_ros.business.configuration import load_config
from kinopio_hub_ros.business.envelope import (
    ENVELOPE_SCHEMA,
    build_message_envelope,
    build_text_envelope,
    decode_envelope,
    encode_envelope,
)
from kinopio_hub_ros.business.service_envelope import (
    SERVICE_ENVELOPE_SCHEMA,
    build_service_request_envelope,
    decode_service_envelope,
    encode_service_envelope,
)
from kinopio_hub_ros.business.nats_adapter import NatsMessage
from kinopio_hub_ros.business.ros1_adapter import Ros1Adapter
from kinopio_hub_ros.business.ros2_adapter import Ros2Adapter
from kinopio_hub_ros.core.bridge_runtime import BridgeRuntime
from kinopio_hub_ros.errors import AdapterError
from tests.fakes import FakeRos1Driver, FakeRos2Driver


class FakeNatsSubscription:
    def __init__(self):
        self.unsubscribed = False

    async def unsubscribe(self):
        self.unsubscribed = True


class FakeNatsStatus:
    def __init__(self, state, connected_server=None):
        self.state = state
        self.connected_server = connected_server
        self.candidate_servers = ("nats://fake",)


class FakeNatsAdapter:
    def __init__(self, *, fail_connect=False, fail_flush_times=0):
        self.connected = False
        self.closed = False
        self.connect_count = 0
        self.reconnect_count = 0
        self.flush_count = 0
        self.published = []
        self.subscription_subject = None
        self.subscription_callback = None
        self.subscription_subjects = []
        self.subscription_callbacks = []
        self.subscription = FakeNatsSubscription()
        self.fail_connect = fail_connect
        self.fail_flush_times = fail_flush_times

    async def connect(self):
        self.connect_count += 1
        if self.fail_connect:
            raise RuntimeError("connect failed")
        self.connected = True
        self.closed = False
        return self

    async def close(self):
        self.connected = False
        self.closed = True

    async def reconnect(self):
        self.reconnect_count += 1
        await self.close()
        return await self.connect()

    async def flush(self):
        self.flush_count += 1
        if self.fail_flush_times > 0:
            self.fail_flush_times -= 1
            raise AdapterError(
                "NATS flush failed (tcp): FlushTimeoutError: nats: flush timeout"
            )

    async def publish(self, subject, payload, *, headers=None, reply=None):
        self.published.append(
            {
                "subject": subject,
                "payload": payload,
                "headers": headers,
                "reply": reply,
            }
        )

    async def subscribe(self, subject, callback, *, queue=None, max_messages=None):
        self.subscription_subject = subject
        self.subscription_callback = callback
        self.subscription_subjects.append(subject)
        self.subscription_callbacks.append((subject, callback))
        return self.subscription

    def status(self):
        if self.connected:
            return FakeNatsStatus("connected", "nats://fake")
        if self.closed:
            return FakeNatsStatus("closed")
        return FakeNatsStatus("disconnected")

    async def emit(self, *, subject, payload, reply=""):
        emitted = False
        for subscription_subject, callback in self.subscription_callbacks:
            if not subject_matches(subscription_subject, subject):
                continue
            emitted = True
            result = callback(
                NatsMessage(subject=subject, data=payload, reply=reply, headers=None)
            )
            if inspect.isawaitable(result):
                await result
        if not emitted and self.subscription_callback is not None:
            result = self.subscription_callback(
                NatsMessage(subject=subject, data=payload, reply=reply, headers=None)
            )
            if inspect.isawaitable(result):
                await result


def subject_matches(pattern, subject):
    if pattern.endswith(".>"):
        return subject.startswith(pattern[:-1])
    return pattern == subject


def write_config(tmp_path, body):
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return load_config(path)


async def tick_until_idle(runtime, count=3):
    for _ in range(count):
        await runtime.tick(spin_timeout_sec=0.0)
        await asyncio.sleep(0)


SERVICE_NAME = "/lane_navigation/go_from_to"
SERVICE_TYPE = "lane_navigation/srv/GoFromTo"
SERVICE_SUBJECT = "ros_services.lane_navigation.go_from_to"


def service_config(tmp_path, *, ros_version=2, timeout_ms=30000, direction="bidirectional"):
    direction_block = "bridge:\n  direction: {0}\n".format(direction) if direction else ""
    return write_config(
        tmp_path,
        (
            direction_block
            + """
ros:
  version: {ros_version}
topics:
  mode: all
services:
  calls:
    - name: {service_name}
      type: {service_type}
      timeout_ms: {timeout_ms}
""".format(
                ros_version=ros_version,
                service_name=SERVICE_NAME,
                service_type=SERVICE_TYPE,
                timeout_ms=timeout_ms,
            )
        ).strip()
        + "\n",
    )


def service_request(data, *, ros_version=2, service=SERVICE_NAME, subject=SERVICE_SUBJECT):
    return encode_service_envelope(
        build_service_request_envelope(
            service=service,
            subject=subject,
            data=data,
            bridge_id="sdk",
            sequence=1,
            ros_version=ros_version,
            ros_service_type=SERVICE_TYPE,
        )
    )


async def wait_for_nats_publish(nats_adapter, *, count=1, timeout_sec=0.1):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec
    while len(nats_adapter.published) < count and loop.time() < deadline:
        await asyncio.sleep(0.001)
    assert len(nats_adapter.published) >= count


def test_bridge_forwards_ros_text_to_nats(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: bridge-test
sync:
  subject_prefix: hub.ros
  throttle_ms: 0
  dedupe: true
topics:
  mode: include
  patterns:
    - /chatter
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(
        topics=(
            ("/chatter", ("std_msgs/msg/String",)),
            ("/image", ("sensor_msgs/msg/Image",)),
        )
    )
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=ros_adapter,
    )

    async def run():
        await runtime.start()
        ros_driver.emit("/chatter", "hello from ros")
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    assert nats_adapter.subscription_subject == "hub.ros.>"
    assert len(nats_adapter.published) == 1
    published = nats_adapter.published[0]
    envelope = decode_envelope(published["payload"])
    assert published["subject"] == "hub.ros.chatter"
    assert envelope.direction == "ros_to_nats"
    assert envelope.topic == "/chatter"
    assert envelope.text == "hello from ros"


def test_bridge_keeps_running_after_transient_nats_flush_failure(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: bridge-test
sync:
  subject_prefix: hub.ros
  throttle_ms: 0
topics:
  mode: include
  patterns:
    - /chatter
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(topics=(("/chatter", ("std_msgs/msg/String",)),))
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    nats_adapter = FakeNatsAdapter(fail_flush_times=1)
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=ros_adapter,
    )

    async def run():
        await runtime.start()
        ros_driver.emit("/chatter", "during outage")
        await runtime.tick(spin_timeout_sec=0.0)

        ros_driver.emit("/chatter", "after recovery")
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    published_texts = [
        decode_envelope(published["payload"]).text
        for published in nats_adapter.published
    ]

    assert nats_adapter.flush_count == 2
    assert nats_adapter.connect_count == 2
    assert nats_adapter.reconnect_count == 1
    assert nats_adapter.subscription_subjects == ["hub.ros.>", "hub.ros.>"]
    assert published_texts[-2:] == ["during outage", "after recovery"]


def test_bridge_forwards_non_string_ros_topic_to_nats_with_actual_type(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: bridge-test
sync:
  subject_prefix: hub.ros
  throttle_ms: 0
topics:
  mode: include
  patterns:
    - /odom
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(topics=(("/odom", ("nav_msgs/msg/Odometry",)),))
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=ros_adapter,
    )

    async def run():
        await runtime.start()
        ros_driver.emit("/odom", "pose:\n  pose:\n    position:\n      x: 1.0")
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    assert len(nats_adapter.published) == 1
    published = nats_adapter.published[0]
    envelope = decode_envelope(published["payload"])
    raw_payload = json.loads(published["payload"].decode("utf-8"))
    assert published["subject"] == "hub.ros.odom"
    assert raw_payload["schema"] == ENVELOPE_SCHEMA
    assert raw_payload["data"] == {"pose": {"pose": {"position": {"x": 1.0}}}}
    assert envelope.topic == "/odom"
    assert envelope.ros.message_type == "nav_msgs/msg/Odometry"
    assert envelope.json_value == {"pose": {"pose": {"position": {"x": 1.0}}}}
    assert json.loads(envelope.text) == {"pose": {"pose": {"position": {"x": 1.0}}}}


def test_bridge_uses_driver_json_value_without_reparsing_text(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: bridge-test
sync:
  subject_prefix: hub.ros
  throttle_ms: 0
topics:
  mode: include
  patterns:
    - /odom
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(topics=(("/odom", ("nav_msgs/msg/Odometry",)),))
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=ros_adapter,
    )

    async def run():
        await runtime.start()
        ros_driver.emit(
            "/odom",
            "not parseable as structured data",
            json_value={"pose": {"position": {"x": 2.0}}},
        )
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    raw_payload = json.loads(nats_adapter.published[0]["payload"].decode("utf-8"))

    assert raw_payload["schema"] == ENVELOPE_SCHEMA
    assert raw_payload["data"] == {"pose": {"position": {"x": 2.0}}}


def test_bridge_writes_nats_envelope_back_to_ros_and_suppresses_loop(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: bridge-test
sync:
  subject_prefix: ros
  throttle_ms: 0
  dedupe: true
  loop_suppression_ms: 1000
topics:
  mode: all
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(topics=(( "/chatter", ("std_msgs/msg/String",)),))
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=ros_adapter,
    )

    async def run():
        await runtime.start()
        payload = encode_envelope(
            build_text_envelope(
                direction="nats_to_ros",
                topic="/chatter",
                subject="ros.chatter",
                text="writeback",
                bridge_id="sdk",
                sequence=7,
                ros_version=2,
                ros_distro="humble",
                ros_message_type="std_msgs/msg/String",
            )
        )
        await nats_adapter.emit(subject="ros.chatter", payload=payload)
        await runtime.tick(spin_timeout_sec=0.0)

        ros_driver.emit("/chatter", "writeback")
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    assert ros_driver.published == [("/chatter", "writeback")]
    assert nats_adapter.published == []


def test_bridge_writes_non_string_nats_envelope_back_to_ros(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: bridge-test
sync:
  subject_prefix: ros
  throttle_ms: 0
topics:
  mode: all
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(topics=(("/goal_pose", ("geometry_msgs/msg/PoseStamped",)),))
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=ros_adapter,
    )

    async def run():
        await runtime.start()
        payload = encode_envelope(
            build_message_envelope(
                direction="nats_to_ros",
                topic="/goal_pose",
                subject="ros.goal_pose",
                data={"pose": {"position": {"x": 1.0}}},
                bridge_id="sdk",
                sequence=8,
                ros_version=2,
                ros_distro="humble",
                ros_message_type="geometry_msgs/msg/PoseStamped",
            )
        )
        await nats_adapter.emit(subject="ros.goal_pose", payload=payload)
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    assert ros_driver.published == [("/goal_pose", '{\n  "pose": {\n    "position": {\n      "x": 1.0\n    }\n  }\n}')]
    assert ros_driver.publisher_types["/goal_pose"] == "geometry_msgs/msg/PoseStamped"


def test_bridge_still_accepts_legacy_non_string_text_envelope(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: bridge-test
sync:
  subject_prefix: ros
  throttle_ms: 0
topics:
  mode: all
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(topics=(("/goal_pose", ("geometry_msgs/msg/PoseStamped",)),))
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=ros_adapter,
    )

    async def run():
        await runtime.start()
        payload = encode_envelope(
            build_text_envelope(
                direction="nats_to_ros",
                topic="/goal_pose",
                subject="ros.goal_pose",
                text="pose:\n  position:\n    x: 1.0",
                bridge_id="sdk",
                sequence=8,
                ros_version=2,
                ros_distro="humble",
                ros_message_type="geometry_msgs/msg/PoseStamped",
            )
        )
        await nats_adapter.emit(subject="ros.goal_pose", payload=payload)
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    assert ros_driver.published == [("/goal_pose", "pose:\n  position:\n    x: 1.0")]


def test_bridge_ignores_invalid_nats_direction_and_subject_mismatch(tmp_path):
    config = write_config(
        tmp_path,
        """
sync:
  subject_prefix: ros
  throttle_ms: 0
topics:
  mode: all
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(topics=(( "/chatter", ("std_msgs/msg/String",)),))
    runtime = BridgeRuntime(
        config,
        nats_adapter=FakeNatsAdapter(),
        ros_adapter=Ros2Adapter(config, driver=ros_driver),
    )

    async def run():
        await runtime.start()
        bad_direction = encode_envelope(
            build_text_envelope(
                direction="ros_to_nats",
                topic="/chatter",
                subject="ros.chatter",
                text="wrong",
                bridge_id="sdk",
                sequence=1,
                ros_version=2,
                ros_distro="humble",
                ros_message_type="std_msgs/msg/String",
            )
        )
        await runtime._nats.emit(subject="ros.chatter", payload=bad_direction)

        wrong_subject = encode_envelope(
            build_text_envelope(
                direction="nats_to_ros",
                topic="/chatter",
                subject="ros.chatter",
                text="wrong-subject",
                bridge_id="sdk",
                sequence=2,
                ros_version=2,
                ros_distro="humble",
                ros_message_type="std_msgs/msg/String",
            )
        )
        await runtime._nats.emit(subject="ros.other", payload=wrong_subject)
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    assert ros_driver.published == []


def test_bridge_cleans_up_ros_state_when_nats_start_fails(tmp_path):
    config = write_config(
        tmp_path,
        """
topics:
  mode: all
""".strip()
        + "\n",
    )
    ros_driver = FakeRos2Driver(topics=(( "/chatter", ("std_msgs/msg/String",)),))
    ros_adapter = Ros2Adapter(config, driver=ros_driver)
    runtime = BridgeRuntime(
        config,
        nats_adapter=FakeNatsAdapter(fail_connect=True),
        ros_adapter=ros_adapter,
    )

    async def run():
        try:
            await runtime.start()
        except RuntimeError as exc:
            assert str(exc) == "connect failed"

    asyncio.run(run())

    assert ros_driver.shutdown_called is True


def test_bridge_forwards_ros1_text_to_nats_and_accepts_writeback(tmp_path):
    config = write_config(
        tmp_path,
        """
bridge:
  id: ros1-bridge
ros:
  version: 1
sync:
  subject_prefix: ros
  throttle_ms: 0
  dedupe: true
  loop_suppression_ms: 1000
topics:
  mode: all
""".strip()
        + "\n",
    )
    ros_driver = FakeRos1Driver(topics=(( "/chatter", ("std_msgs/String",)),))
    ros_adapter = Ros1Adapter(config, driver=ros_driver)
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(config, nats_adapter=nats_adapter, ros_adapter=ros_adapter)

    async def run():
        await runtime.start()
        ros_driver.emit("/chatter", "hello noetic")
        await runtime.tick(spin_timeout_sec=0.0)

        payload = encode_envelope(
            build_text_envelope(
                direction="nats_to_ros",
                topic="/chatter",
                subject="ros.chatter",
                text="writeback noetic",
                bridge_id="sdk",
                sequence=9,
                ros_version=1,
                ros_distro="noetic",
                ros_message_type="std_msgs/String",
            )
        )
        await nats_adapter.emit(subject="ros.chatter", payload=payload)
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    assert decode_envelope(nats_adapter.published[0]["payload"]).ros.version == 1
    assert decode_envelope(nats_adapter.published[0]["payload"]).ros.message_type == "std_msgs/String"
    assert ros_driver.published == [("/chatter", "writeback noetic")]


def test_bridge_replies_to_ros2_service_request(tmp_path):
    config = service_config(tmp_path, ros_version=2)
    ros_driver = FakeRos2Driver(
        services={
            SERVICE_NAME: {
                "accepted": True,
                "message": "started",
            }
        }
    )
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=Ros2Adapter(config, driver=ros_driver),
    )

    async def run():
        await runtime.start()
        await nats_adapter.emit(
            subject=SERVICE_SUBJECT,
            payload=service_request(
                {
                    "start_node": "",
                    "goal_node": "node2",
                    "loop": False,
                    "repeat_count": 1,
                }
            ),
            reply="_INBOX.1",
        )
        await wait_for_nats_publish(nats_adapter)
        await runtime.close()

    asyncio.run(run())

    assert nats_adapter.subscription_subjects == ["ros.>", SERVICE_SUBJECT]
    assert ros_driver.service_calls == [
        (
            SERVICE_NAME,
            SERVICE_TYPE,
            {
                "start_node": "",
                "goal_node": "node2",
                "loop": False,
                "repeat_count": 1,
            },
        )
    ]
    assert len(nats_adapter.published) == 1
    published = nats_adapter.published[0]
    response = decode_service_envelope(published["payload"])
    raw_payload = json.loads(published["payload"].decode("utf-8"))
    assert published["subject"] == "_INBOX.1"
    assert raw_payload["schema"] == SERVICE_ENVELOPE_SCHEMA
    assert response.direction == "ros_to_nats"
    assert response.ok is True
    assert response.data == {"accepted": True, "message": "started"}
    assert response.service == SERVICE_NAME
    assert response.ros.version == 2
    assert response.ros.service_type == SERVICE_TYPE


def test_bridge_reconnects_after_service_reply_flush_failure(tmp_path):
    config = service_config(tmp_path, ros_version=2)
    ros_driver = FakeRos2Driver(
        services={
            SERVICE_NAME: {
                "accepted": True,
            }
        }
    )
    nats_adapter = FakeNatsAdapter(fail_flush_times=1)
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=Ros2Adapter(config, driver=ros_driver),
    )

    async def run():
        await runtime.start()
        await nats_adapter.emit(
            subject=SERVICE_SUBJECT,
            payload=service_request({"goal_node": "node2"}),
            reply="_INBOX.service",
        )
        await asyncio.sleep(0)
        await runtime.tick(spin_timeout_sec=0.0)
        await runtime.close()

    asyncio.run(run())

    assert nats_adapter.reconnect_count == 1
    assert nats_adapter.subscription_subjects == [
        "ros.>",
        SERVICE_SUBJECT,
        "ros.>",
        SERVICE_SUBJECT,
    ]


def test_bridge_replies_to_ros1_service_request_with_ros1_type(tmp_path):
    config = service_config(tmp_path, ros_version=1)
    ros_driver = FakeRos1Driver(
        services={
            SERVICE_NAME: {
                "accepted": True,
            }
        }
    )
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=Ros1Adapter(config, driver=ros_driver),
    )

    async def run():
        await runtime.start()
        await nats_adapter.emit(
            subject=SERVICE_SUBJECT,
            payload=service_request({"goal_node": "node2"}, ros_version=1),
            reply="_INBOX.ros1",
        )
        await wait_for_nats_publish(nats_adapter)
        await runtime.close()

    asyncio.run(run())

    assert ros_driver.service_calls == [
        (
            SERVICE_NAME,
            "lane_navigation/GoFromTo",
            {"goal_node": "node2"},
        )
    ]
    response = decode_service_envelope(nats_adapter.published[0]["payload"])
    assert nats_adapter.published[0]["subject"] == "_INBOX.ros1"
    assert response.ok is True
    assert response.data == {"accepted": True}
    assert response.ros.version == 1


def test_bridge_rejects_mismatched_service_request(tmp_path):
    config = service_config(tmp_path, ros_version=2)
    ros_driver = FakeRos2Driver()
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=Ros2Adapter(config, driver=ros_driver),
    )

    async def run():
        await runtime.start()
        await nats_adapter.emit(
            subject=SERVICE_SUBJECT,
            payload=service_request({"goal_node": "node2"}, service="/lane_navigation/other"),
            reply="_INBOX.bad",
        )
        await wait_for_nats_publish(nats_adapter)
        await runtime.close()

    asyncio.run(run())

    response = decode_service_envelope(nats_adapter.published[0]["payload"])
    assert ros_driver.service_calls == []
    assert response.ok is False
    assert response.error.code == "invalid_request"


def test_bridge_service_unavailable_and_timeout_return_errors(tmp_path):
    unavailable_config = service_config(tmp_path, ros_version=2, timeout_ms=1)

    async def run_case(config, driver, inbox):
        nats_adapter = FakeNatsAdapter()
        runtime = BridgeRuntime(
            config,
            nats_adapter=nats_adapter,
            ros_adapter=Ros2Adapter(config, driver=driver),
        )
        await runtime.start()
        await nats_adapter.emit(
            subject=SERVICE_SUBJECT,
            payload=service_request({"goal_node": "node2"}),
            reply=inbox,
        )
        await wait_for_nats_publish(nats_adapter, timeout_sec=0.05)
        response = decode_service_envelope(nats_adapter.published[0]["payload"])
        await runtime.close()
        return response

    unavailable = asyncio.run(
        run_case(
            unavailable_config,
            FakeRos2Driver(service_ready={SERVICE_NAME: False}),
            "_INBOX.unavailable",
        )
    )
    timed_out = asyncio.run(
        run_case(
            unavailable_config,
            FakeRos2Driver(services={SERVICE_NAME: "__pending__"}),
            "_INBOX.timeout",
        )
    )

    assert unavailable.ok is False
    assert unavailable.error.code == "service_unavailable"
    assert timed_out.ok is False
    assert timed_out.error.code == "service_timeout"


def test_bridge_invalid_service_payload_returns_error(tmp_path):
    config = service_config(tmp_path, ros_version=2)
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=Ros2Adapter(config, driver=FakeRos2Driver()),
    )

    async def run():
        await runtime.start()
        request = json.dumps(
            {
                "schema": "kinopio.ros.service.v1",
                "direction": "nats_to_ros",
                "service": SERVICE_NAME,
                "subject": SERVICE_SUBJECT,
                "ros": {"version": 2, "type": SERVICE_TYPE},
                "data": ["not", "a", "mapping"],
                "meta": {"bridgeId": "sdk", "sequence": 1},
            }
        ).encode("utf-8")
        await nats_adapter.emit(subject=SERVICE_SUBJECT, payload=request, reply="_INBOX.invalid")
        await wait_for_nats_publish(nats_adapter)
        await runtime.close()

    asyncio.run(run())

    response = decode_service_envelope(nats_adapter.published[0]["payload"])
    assert response.ok is False
    assert response.error.code == "invalid_request"


def test_bridge_ros_to_nats_direction_does_not_expose_service_responder(tmp_path):
    config = service_config(tmp_path, ros_version=2, direction="ros_to_nats")
    nats_adapter = FakeNatsAdapter()
    runtime = BridgeRuntime(
        config,
        nats_adapter=nats_adapter,
        ros_adapter=Ros2Adapter(config, driver=FakeRos2Driver()),
    )

    async def run():
        await runtime.start()
        await runtime.close()

    asyncio.run(run())

    assert nats_adapter.subscription_subjects == []
