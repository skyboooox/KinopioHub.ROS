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
from kinopio_hub_ros.business.nats_adapter import NatsMessage
from kinopio_hub_ros.business.ros1_adapter import Ros1Adapter
from kinopio_hub_ros.business.ros2_adapter import Ros2Adapter
from kinopio_hub_ros.core.bridge_runtime import BridgeRuntime
from tests.fakes import FakeRos1Driver, FakeRos2Driver


class FakeNatsSubscription:
    def __init__(self):
        self.unsubscribed = False

    async def unsubscribe(self):
        self.unsubscribed = True


class FakeNatsAdapter:
    def __init__(self, *, fail_connect=False):
        self.connected = False
        self.closed = False
        self.flush_count = 0
        self.published = []
        self.subscription_subject = None
        self.subscription_callback = None
        self.subscription = FakeNatsSubscription()
        self.fail_connect = fail_connect

    async def connect(self):
        if self.fail_connect:
            raise RuntimeError("connect failed")
        self.connected = True
        return self

    async def close(self):
        self.closed = True

    async def flush(self):
        self.flush_count += 1

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
        return self.subscription

    async def emit(self, *, subject, payload):
        result = self.subscription_callback(
            NatsMessage(subject=subject, data=payload, reply="", headers=None)
        )
        if inspect.isawaitable(result):
            await result


def write_config(tmp_path, body):
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return load_config(path)


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
