"""Async bridge runtime for ROS <-> NATS message synchronization."""

import asyncio
import logging
import time

from collections import deque

from kinopio_hub_ros.business.envelope import (
    build_message_envelope,
    build_text_envelope,
    decode_envelope,
    encode_envelope,
)
from kinopio_hub_ros.business.message_text import (
    is_string_message_type,
    structured_text_to_json_value,
)
from kinopio_hub_ros.business.nats_adapter import NatsAdapter
from kinopio_hub_ros.business.ros_adapter_factory import create_ros_adapter
from kinopio_hub_ros.business.subject_mapping import topic_to_subject
from kinopio_hub_ros.core.service_responder import ServiceResponder
from kinopio_hub_ros.core.sync_policy import LatestStatePolicy
from kinopio_hub_ros.errors import AdapterError, ProtocolError


class BridgeRuntime:
    def __init__(
        self,
        config,
        *,
        nats_adapter=None,
        ros_adapter=None,
        logger=None,
        monotonic_clock=None,
    ):
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._nats = nats_adapter or NatsAdapter(config)
        self._ros_events = deque()
        self._nats_events = deque()
        self._ros = ros_adapter or create_ros_adapter(
            config,
            on_text_message=self._ros_events.append,
            logger=self._logger,
        )
        set_on_text_message = getattr(self._ros, "set_on_text_message", None)
        if callable(set_on_text_message):
            set_on_text_message(self._ros_events.append)
        self._policy = LatestStatePolicy(
            throttle_ms=config.sync.throttle_ms,
            dedupe=config.sync.dedupe,
            loop_suppression_ms=config.sync.loop_suppression_ms,
        )
        self._subject_prefix = config.sync.subject_prefix
        self._started = False
        self._next_sequence = 0
        self._discovery_interval_ms = 1000
        self._next_discovery_at_ms = 0
        self._nats_subscription = None
        self._service_responder = ServiceResponder(
            config,
            ros_adapter=self._ros,
            nats_adapter=self._nats,
            next_sequence=self._take_sequence,
            logger=self._logger,
            on_nats_error=self._mark_nats_reconnect_required,
        )
        self._stop_event = asyncio.Event()
        self._selected_topics = None
        self._next_nats_publish_error_log_at_ms = 0
        self._nats_publish_error_log_interval_ms = 10000
        self._nats_reconnect_required = False
        self._next_nats_reconnect_at_ms = 0
        self._nats_reconnect_interval_ms = 1000

    async def start(self):
        if self._started:
            return

        try:
            self._logger.info(
                "Starting bridge runtime bridge_id=%s direction=%s subject_prefix=%s ros_version=%s topic_mode=%s patterns=%s qos=%s/%s depth=%s",
                self._config.bridge.bridge_id,
                self._config.bridge.direction,
                self._subject_prefix,
                self._config.ros.version,
                self._config.topics.mode,
                list(self._config.topics.patterns),
                self._config.ros.qos.reliability,
                self._config.ros.qos.durability,
                self._config.ros.qos.depth,
            )
            self._ros.start()
            await self._nats.connect()
            nats_status = getattr(self._nats, "status", None)
            if callable(nats_status):
                status = nats_status()
                self._logger.info(
                    "Connected NATS adapter server=%s candidates=%s",
                    status.connected_server,
                    list(status.candidate_servers),
                )
            else:
                self._logger.info("Connected NATS adapter")

            await self._subscribe_nats_consumers()

            self._refresh_ros_subscriptions()
            self._started = True
        except Exception:
            await self._close_nats_consumers()
            await self._nats.close()
            self._ros.close()
            raise

    async def close(self):
        self._logger.info("Closing bridge runtime bridge_id=%s", self._config.bridge.bridge_id)
        await self._close_nats_consumers()
        await self._nats.close()
        self._ros.close()
        self._started = False
        self._selected_topics = None
        self._nats_reconnect_required = False

    async def tick(self, *, spin_timeout_sec=0.05):
        self._require_started()
        await self._recover_nats_if_due()

        now_ms = self._now_ms()
        if now_ms >= self._next_discovery_at_ms:
            self._refresh_ros_subscriptions()
            self._next_discovery_at_ms = now_ms + self._discovery_interval_ms

        self._ros.spin_once(timeout_sec=spin_timeout_sec)
        await asyncio.sleep(0)
        await self._drain_ros_events()
        await self._drain_nats_events()
        await self._flush_due_ros_messages()

    async def run_forever(self):
        await self.start()
        try:
            while not self._stop_event.is_set():
                await self.tick()
        finally:
            await self.close()

    def stop(self):
        self._stop_event.set()

    def _refresh_ros_subscriptions(self):
        if self._should_forward_ros_to_nats() or self._should_forward_nats_to_ros():
            selected_topics = tuple(self._ros.refresh_subscriptions())
            if self._selected_topics is None or selected_topics != self._selected_topics:
                self._selected_topics = selected_topics
                self._logger.info("Selected ROS topics: %s", list(selected_topics))

    async def _subscribe_nats_consumers(self):
        if not self._should_forward_nats_to_ros():
            return
        self._nats_subscription = await self._nats.subscribe(
            self._subject_wildcard(),
            self._nats_events.append,
        )
        self._logger.info(
            "Subscribed to NATS wildcard %s for writeback envelopes",
            self._subject_wildcard(),
        )
        await self._service_responder.start()

    async def _close_nats_consumers(self):
        if self._nats_subscription is not None:
            try:
                await self._nats_subscription.unsubscribe()
            except Exception as exc:
                self._logger.debug(
                    "Ignoring NATS wildcard subscription cleanup failure: %s",
                    exc,
                )
            self._nats_subscription = None
        await self._service_responder.close()

    async def _recover_nats_if_due(self):
        self._mark_nats_reconnect_required_if_closed()
        if not self._nats_reconnect_required:
            return

        now_ms = self._now_ms()
        if now_ms < self._next_nats_reconnect_at_ms:
            return
        self._next_nats_reconnect_at_ms = now_ms + self._nats_reconnect_interval_ms

        try:
            self._logger.info("Attempting NATS reconnect")
            await self._close_nats_consumers()
            reconnect = getattr(self._nats, "reconnect", None)
            if callable(reconnect):
                await reconnect()
            else:
                await self._nats.close()
                await self._nats.connect()
            await self._subscribe_nats_consumers()
        except Exception as exc:
            self._logger.warning(
                "NATS reconnect attempt failed; bridge will retry: %s",
                exc,
            )
            return

        self._nats_reconnect_required = False
        self._next_nats_reconnect_at_ms = 0
        self._next_nats_publish_error_log_at_ms = 0
        self._log_nats_connected("Reconnected NATS adapter")

    def _mark_nats_reconnect_required(self):
        self._nats_reconnect_required = True
        self._next_nats_reconnect_at_ms = 0

    def _mark_nats_reconnect_required_if_closed(self):
        nats_status = getattr(self._nats, "status", None)
        if not callable(nats_status):
            return
        status = nats_status()
        if status.state in ("closed", "disconnected"):
            self._mark_nats_reconnect_required()

    def _log_nats_connected(self, message):
        nats_status = getattr(self._nats, "status", None)
        if callable(nats_status):
            status = nats_status()
            self._logger.info(
                "%s server=%s candidates=%s",
                message,
                status.connected_server,
                list(status.candidate_servers),
            )
            return
        self._logger.info(message)

    async def _drain_ros_events(self):
        while self._ros_events:
            message = self._ros_events.popleft()
            if not self._should_forward_ros_to_nats():
                continue
            emissions = self._policy.ingest_ros_text(
                message.topic,
                message.text,
                message.received_at_ms,
                message_type=message.message_type,
                json_value=getattr(message, "json_value", None),
            )
            await self._publish_emissions(emissions)

    async def _flush_due_ros_messages(self):
        if not self._should_forward_ros_to_nats():
            return
        if self._nats_reconnect_required:
            return
        await self._publish_emissions(self._policy.flush_due(self._now_ms()))

    async def _publish_emissions(self, emissions):
        emissions = tuple(emissions)
        if not emissions:
            return
        if self._nats_reconnect_required:
            self._policy.requeue_emissions(emissions, self._now_ms())
            return
        try:
            for emission in emissions:
                subject = topic_to_subject(emission.topic, self._subject_prefix)
                envelope = self._build_ros_to_nats_envelope(emission, subject)
                self._logger.debug(
                    "Forwarding ROS message to NATS topic=%s subject=%s text=%r",
                    emission.topic,
                    subject,
                    emission.text,
                )
                await self._nats.publish(subject, encode_envelope(envelope))
            await self._nats.flush()
            self._next_nats_publish_error_log_at_ms = 0
        except AdapterError as exc:
            self._policy.requeue_emissions(emissions, self._now_ms())
            self._mark_nats_reconnect_required()
            self._log_nats_publish_failure(exc, emission_count=len(emissions))

    def _log_nats_publish_failure(self, exc, *, emission_count):
        now_ms = self._now_ms()
        if now_ms < self._next_nats_publish_error_log_at_ms:
            return
        self._next_nats_publish_error_log_at_ms = (
            now_ms + self._nats_publish_error_log_interval_ms
        )
        nats_status = getattr(self._nats, "status", None)
        status = nats_status() if callable(nats_status) else None
        if status is None:
            self._logger.warning(
                "Unable to publish %s ROS message(s) to NATS; bridge will keep running: %s",
                emission_count,
                exc,
            )
            return
        self._logger.warning(
            "Unable to publish %s ROS message(s) to NATS; "
            "bridge will keep running state=%s server=%s error=%s",
            emission_count,
            status.state,
            status.connected_server,
            exc,
        )

    def _build_ros_to_nats_envelope(self, emission, subject):
        message_type = emission.message_type or self._ros.message_type
        if message_type and not is_string_message_type(message_type):
            json_value = emission.json_value
            if json_value is None:
                json_value = self._parse_structured_emission(emission)
            if json_value is not None:
                return build_message_envelope(
                    direction="ros_to_nats",
                    topic=emission.topic,
                    subject=subject,
                    data=json_value,
                    bridge_id=self._config.bridge.bridge_id,
                    sequence=self._take_sequence(),
                    ros_version=self._ros.version,
                    ros_distro=self._ros.distro,
                    ros_message_type=message_type,
                )

        return build_text_envelope(
            direction="ros_to_nats",
            topic=emission.topic,
            subject=subject,
            text=emission.text,
            bridge_id=self._config.bridge.bridge_id,
            sequence=self._take_sequence(),
            ros_version=self._ros.version,
            ros_distro=self._ros.distro,
            ros_message_type=message_type,
        )

    def _parse_structured_emission(self, emission):
        try:
            return structured_text_to_json_value(
                emission.text,
                emission.message_type,
            )
        except Exception:
            self._logger.exception(
                "Failed to convert ROS message text back into structured JSON topic=%s type=%s",
                emission.topic,
                emission.message_type,
            )
            return None

    async def _drain_nats_events(self):
        while self._nats_events:
            message = self._nats_events.popleft()
            await self._process_nats_message(message)

    async def _process_nats_message(self, message):
        if not self._should_forward_nats_to_ros():
            return
        try:
            envelope = decode_envelope(message.data)
        except ProtocolError as exc:
            self._logger.warning("Ignoring invalid NATS envelope on %s: %s", message.subject, exc)
            return

        expected_subject = topic_to_subject(envelope.topic, self._subject_prefix)
        if message.subject != expected_subject or envelope.subject != expected_subject:
            self._logger.warning(
                "Ignoring NATS envelope with mismatched subject/topic (%s != %s)",
                message.subject,
                expected_subject,
            )
            return
        if envelope.direction != "nats_to_ros":
            self._logger.debug(
                "Ignoring NATS envelope with direction %s on %s",
                envelope.direction,
                message.subject,
            )
            return
        if envelope.ros.version != self._ros.version:
            self._logger.warning(
                "Ignoring NATS envelope for unsupported ROS target version %s",
                envelope.ros.version,
            )
            return
        if not self._ros.topic_allowed(envelope.topic):
            self._logger.debug(
                "Ignoring NATS envelope for filtered ROS topic %s",
                envelope.topic,
            )
            return

        self._policy.record_nats_writeback(
            envelope.topic,
            envelope.text,
            self._now_ms(),
            message_type=envelope.ros.message_type,
        )
        self._logger.debug(
            "Forwarding NATS message to ROS topic=%s subject=%s text=%r",
            envelope.topic,
            message.subject,
            envelope.text,
        )
        self._ros.publish_text(
            envelope.topic,
            envelope.text,
            message_type=envelope.ros.message_type,
        )

    def _subject_wildcard(self):
        return "{0}.>".format(self._subject_prefix)

    def _should_forward_ros_to_nats(self):
        return self._config.bridge.direction in ("bidirectional", "ros_to_nats")

    def _should_forward_nats_to_ros(self):
        return self._config.bridge.direction in ("bidirectional", "nats_to_ros")

    def _take_sequence(self):
        sequence = self._next_sequence
        self._next_sequence += 1
        return sequence

    def _now_ms(self):
        return int(self._monotonic_clock() * 1000)

    def _require_started(self):
        if not self._started:
            raise RuntimeError("BridgeRuntime.start() must be called before tick()")
