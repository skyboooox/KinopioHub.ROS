"""ROS 1 topic adapter for the bridge runtime."""

import importlib
import logging
import os
import time

from dataclasses import dataclass

from kinopio_hub_ros.atom.ros_names import sanitize_ros_node_name
from kinopio_hub_ros.atom.topic_tools import matches_any_topic_pattern, normalize_ros_topic
from kinopio_hub_ros.business.envelope import ROS1_STRING_MESSAGE_TYPE
from kinopio_hub_ros.business.message_text import ros1_text_to_message, ros_message_to_payload
from kinopio_hub_ros.errors import AdapterError, RuntimeUnavailableError

SUPPORTED_ROS1_DISTROS = ("noetic",)


@dataclass(frozen=True)
class RosTextMessage:
    topic: str
    text: str
    message_type: str
    received_at_ms: int
    json_value: object = None


@dataclass(frozen=True)
class Ros1PublisherHandle:
    publisher: object
    message_class: object
    message_type: str


@dataclass(frozen=True)
class LoggingSnapshot:
    handlers: tuple
    level: int
    disabled: bool


class RospyRos1Driver:
    def __init__(self, *, logger=None):
        self._logger = logger or logging.getLogger(__name__)
        self._rospy = None
        self._message_module = None
        self._fill_message_args = None
        self._fill_keys = {}
        self._message_classes_by_type = {}
        self._publisher_queue_size = 10
        self._publisher_latch = False
        self._distro = "unknown"

    @property
    def distro(self):
        return self._distro

    def start(self, *, node_name, qos_config):
        try:
            rospy = importlib.import_module("rospy")
            roslib_message = importlib.import_module("roslib.message")
            genpy_message = importlib.import_module("genpy.message")
            std_msgs_module = importlib.import_module("std_msgs.msg")
        except ImportError as exc:
            raise RuntimeUnavailableError(
                "ROS 1 runtime requires rospy, roslib, genpy, and std_msgs to be installed in the current environment."
            ) from exc

        self._rospy = rospy
        self._message_module = roslib_message
        self._fill_message_args = getattr(genpy_message, "fill_message_args")
        self._fill_keys = {
            "now": self._rospy.get_rostime,
            "auto": getattr(std_msgs_module, "Header"),
        }
        self._publisher_queue_size = qos_config.depth
        self._publisher_latch = qos_config.durability == "transient_local"
        self._distro = (os.getenv("ROS_DISTRO") or "unknown").strip().lower() or "unknown"

        logging_snapshot = snapshot_logging_handlers()
        try:
            self._rospy.init_node(
                sanitize_ros_node_name(node_name),
                anonymous=False,
                disable_signals=True,
            )
        finally:
            restore_logging_handlers(logging_snapshot)

    def shutdown(self):
        if self._rospy is None:
            return
        if not self._rospy.is_shutdown():
            self._rospy.signal_shutdown("kinopio-hub-ros shutdown")

    def list_topics_and_types(self):
        return tuple(
            (topic, (topic_type,))
            for topic, topic_type in self._rospy.get_published_topics("/")
        )

    def create_text_subscription(self, topic, message_type, callback):
        message_class = self._message_class(message_type)
        return self._rospy.Subscriber(
            topic,
            message_class,
            lambda message: self._handle_subscription_message(
                topic,
                message_type,
                callback,
                message,
            ),
            queue_size=self._publisher_queue_size,
        )

    def create_text_publisher(self, topic, message_type):
        message_class = self._message_class(message_type)
        publisher = self._rospy.Publisher(
            topic,
            message_class,
            queue_size=self._publisher_queue_size,
            latch=self._publisher_latch,
        )
        return Ros1PublisherHandle(
            publisher=publisher,
            message_class=message_class,
            message_type=message_type,
        )

    def publish_text(self, publisher, text):
        try:
            message = ros1_text_to_message(
                text,
                publisher.message_type,
                publisher.message_class,
                self._fill_message_args,
                keys=self._fill_keys,
            )
        except Exception as exc:
            raise AdapterError(
                "Failed to decode envelope text as ROS 1 message type {0}".format(
                    publisher.message_type
                )
            ) from exc
        publisher.publisher.publish(message)

    def load_message_type(self, message_type):
        return self._message_class(message_type)

    def spin_once(self, *, timeout_sec):
        if timeout_sec > 0:
            self._rospy.sleep(timeout_sec)

    def _message_class(self, message_type):
        message_class = self._message_classes_by_type.get(message_type)
        if message_class is not None:
            return message_class
        message_class = self._message_module.get_message_class(message_type)
        if message_class is None:
            raise AdapterError("Unable to load ROS 1 message type {0}".format(message_type))
        self._message_classes_by_type[message_type] = message_class
        return message_class

    def _handle_subscription_message(self, topic, message_type, callback, message):
        try:
            payload = ros_message_to_payload(message, message_type)
        except Exception:
            self._logger.exception(
                "Failed to encode ROS 1 message topic=%s type=%s as envelope text",
                topic,
                message_type,
            )
            return
        callback(payload.text, payload.json_value)


def snapshot_logging_handlers(root_logger=None):
    root_logger = root_logger or logging.getLogger()
    return LoggingSnapshot(
        handlers=tuple(root_logger.handlers),
        level=root_logger.level,
        disabled=root_logger.disabled,
    )


def restore_logging_handlers(snapshot, root_logger=None):
    root_logger = root_logger or logging.getLogger()
    known_handlers = {id(handler) for handler in root_logger.handlers}
    for handler in snapshot.handlers:
        if id(handler) not in known_handlers:
            root_logger.addHandler(handler)
            known_handlers.add(id(handler))
    root_logger.setLevel(snapshot.level)
    root_logger.disabled = snapshot.disabled


class Ros1Adapter:
    def __init__(
        self,
        config,
        *,
        on_text_message=None,
        driver=None,
        logger=None,
        node_name=None,
        clock=None,
    ):
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._on_text_message = on_text_message or (lambda message: None)
        self._driver = driver or RospyRos1Driver(logger=self._logger)
        self._node_name = node_name or config.bridge.bridge_id
        self._clock = clock or time.monotonic
        self._started = False
        self._subscriptions_by_topic = {}
        self._publishers_by_topic = {}
        self._message_types_by_topic = {}
        self._selected_topics = ()
        self._logged_unloadable_topics = set()

    def set_on_text_message(self, callback):
        self._on_text_message = callback or (lambda message: None)

    @property
    def distro(self):
        return self._driver.distro

    @property
    def version(self):
        return 1

    @property
    def message_type(self):
        return ROS1_STRING_MESSAGE_TYPE

    @property
    def selected_topics(self):
        return self._selected_topics

    def start(self):
        if self._started:
            return
        if self._config.ros.version == 2:
            raise RuntimeUnavailableError(
                "ROS 2 was explicitly requested; ROS 1 adapter will not start for ros.version: 2."
            )

        self._driver.start(
            node_name=sanitize_ros_node_name(self._node_name),
            qos_config=self._config.ros.qos,
        )
        self._started = True
        self._log_distro_status()

    def close(self):
        if not self._started:
            return
        self._driver.shutdown()
        self._subscriptions_by_topic.clear()
        self._publishers_by_topic.clear()
        self._message_types_by_topic.clear()
        self._selected_topics = ()
        self._started = False

    def refresh_subscriptions(self):
        self._require_started()
        selected_topics = []

        for topic_name, message_types in self._driver.list_topics_and_types():
            try:
                topic = normalize_ros_topic(topic_name)
            except Exception as exc:
                self._logger.debug("Skipping invalid ROS topic name %r: %s", topic_name, exc)
                continue

            if not self.topic_allowed(topic):
                continue

            message_type = self._select_message_type(topic, message_types)
            if message_type is None:
                continue

            selected_topics.append(topic)
            self._message_types_by_topic[topic] = message_type
            subscription = self._subscriptions_by_topic.get(topic)
            if subscription is None:
                self._subscriptions_by_topic[topic] = (
                    message_type,
                    self._driver.create_text_subscription(
                        topic,
                        message_type,
                        self._make_subscription_callback(topic, message_type),
                    ),
                )
                self._logger.info(
                    "Subscribed to ROS 1 topic %s type=%s (distro=%s)",
                    topic,
                    message_type,
                    self.distro,
                )
            elif subscription[0] != message_type:
                self._logger.info(
                    "ROS 1 topic %s changed type from %s to %s; keeping existing subscription until restart",
                    topic,
                    subscription[0],
                    message_type,
                )
            self._ensure_text_publisher(topic, message_type)

        self._selected_topics = tuple(sorted(selected_topics))
        return self._selected_topics

    def spin_once(self, *, timeout_sec):
        self._require_started()
        self._driver.spin_once(timeout_sec=timeout_sec)

    def publish_text(self, topic, text, message_type=None):
        self._require_started()
        normalized_topic = normalize_ros_topic(topic)
        if not self.topic_allowed(normalized_topic):
            raise AdapterError(
                "ROS topic is not allowed by the configured selection rules: {0}".format(
                    normalized_topic
                )
            )

        message_type = message_type or self._message_types_by_topic.get(normalized_topic)
        if message_type is None:
            message_type = self.message_type
        publisher_key = (normalized_topic, message_type)
        publisher = self._publishers_by_topic.get(publisher_key)
        if publisher is None:
            publisher = self._ensure_text_publisher(normalized_topic, message_type)
        self._driver.publish_text(publisher, text)

    def topic_allowed(self, topic):
        normalized_topic = normalize_ros_topic(topic)
        mode = self._config.topics.mode
        patterns = self._config.topics.patterns
        if mode == "all":
            return True
        matched = matches_any_topic_pattern(patterns, normalized_topic)
        if mode == "include":
            return matched
        return not matched

    def _make_subscription_callback(self, topic, message_type):
        def callback(text, json_value=None):
            self._on_text_message(
                RosTextMessage(
                    topic=topic,
                    text=text,
                    message_type=message_type,
                    received_at_ms=int(self._clock() * 1000),
                    json_value=json_value,
                )
            )

        return callback

    def _ensure_text_publisher(self, topic, message_type):
        publisher_key = (topic, message_type)
        publisher = self._publishers_by_topic.get(publisher_key)
        if publisher is None:
            publisher = self._driver.create_text_publisher(topic, message_type)
            self._publishers_by_topic[publisher_key] = publisher
            self._logger.info(
                "Prepared ROS 1 publisher for %s type=%s (distro=%s)",
                topic,
                message_type,
                self.distro,
            )
        return publisher

    def _select_message_type(self, topic, message_types):
        candidates = tuple(message_type for message_type in message_types if message_type)
        if not candidates:
            return None
        message_type = candidates[0]
        try:
            load_message_type = getattr(self._driver, "load_message_type", None)
            if callable(load_message_type):
                load_message_type(message_type)
        except AdapterError as exc:
            if topic not in self._logged_unloadable_topics:
                self._logged_unloadable_topics.add(topic)
                self._logger.warning(
                    "Skipping ROS 1 topic %s with unloadable type %s: %s",
                    topic,
                    message_type,
                    exc,
                )
            return None
        if len(candidates) > 1:
            self._logger.warning(
                "ROS 1 topic %s has multiple advertised types %s; using %s",
                topic,
                list(candidates),
                message_type,
            )
        return message_type

    def _log_distro_status(self):
        distro = self.distro
        if distro != "unknown" and distro not in SUPPORTED_ROS1_DISTROS:
            self._logger.warning("ROS 1 distro '%s' is outside the tested distro set.", distro)

    def _require_started(self):
        if not self._started:
            raise AdapterError("ROS 1 adapter has not been started")
