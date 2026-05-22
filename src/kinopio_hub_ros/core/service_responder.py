"""NATS request-reply responder for configured ROS services."""

import asyncio
import logging

from kinopio_hub_ros.business.service_envelope import (
    build_service_error_envelope,
    build_service_response_envelope,
    decode_service_envelope,
    encode_service_envelope,
)
from kinopio_hub_ros.business.subject_mapping import service_to_subject
from kinopio_hub_ros.errors import AdapterError, ProtocolError, ServiceCallError


class ServiceResponder:
    def __init__(self, config, *, ros_adapter, nats_adapter, next_sequence, logger=None):
        self._config = config
        self._ros = ros_adapter
        self._nats = nats_adapter
        self._next_sequence = next_sequence
        self._logger = logger or logging.getLogger(__name__)
        self._subscriptions = []
        self._tasks = set()
        self._calls_by_subject = {}

    async def start(self):
        for call in self._config.services.calls:
            subject = service_to_subject(call.name, self._config.services.subject_prefix)
            self._calls_by_subject[subject] = call
            subscription = await self._nats.subscribe(subject, self._make_callback())
            self._subscriptions.append(subscription)
            self._logger.info(
                "Subscribed to NATS service request subject %s for ROS service %s type=%s",
                subject,
                call.name,
                call.service_type,
            )

    async def close(self):
        for subscription in tuple(self._subscriptions):
            await subscription.unsubscribe()
        self._subscriptions.clear()
        self._calls_by_subject.clear()
        await self._cancel_tasks()

    def _make_callback(self):
        def callback(message):
            task = asyncio.create_task(self._process_request(message))
            self._tasks.add(task)
            task.add_done_callback(self._forget_task)

        return callback

    async def _process_request(self, message):
        call = self._calls_by_subject.get(message.subject)
        if call is None:
            self._logger.warning(
                "Ignoring request on unconfigured service subject %s",
                message.subject,
            )
            return
        if not message.reply:
            self._logger.warning(
                "Ignoring service request on %s without NATS reply subject",
                message.subject,
            )
            return

        try:
            envelope = decode_service_envelope(message.data)
            self._validate_request(message, envelope, call)
        except ProtocolError as exc:
            await self._reply(
                message.reply,
                self._build_error(call, message.subject, "invalid_request", str(exc)),
            )
            return

        try:
            response_data = await self._ros.call_service(
                call.name,
                call.service_type,
                envelope.data,
                call.timeout_ms,
            )
            response = build_service_response_envelope(
                service=call.name,
                subject=message.subject,
                data=response_data,
                bridge_id=self._config.bridge.bridge_id,
                sequence=self._next_sequence(),
                ros_version=self._ros.version,
                ros_distro=self._ros.distro,
                ros_service_type=call.service_type,
            )
        except ServiceCallError as exc:
            response = self._build_error(call, message.subject, exc.code, str(exc))
        except AdapterError as exc:
            response = self._build_error(call, message.subject, "service_error", str(exc))
        except Exception as exc:
            self._logger.exception(
                "Unexpected ROS service request failure service=%s subject=%s",
                call.name,
                message.subject,
            )
            response = self._build_error(
                call,
                message.subject,
                "service_error",
                "{0}: {1}".format(type(exc).__name__, exc),
            )

        await self._reply(message.reply, response)

    def _validate_request(self, message, envelope, call):
        expected_subject = service_to_subject(call.name, self._config.services.subject_prefix)
        if message.subject != expected_subject or envelope.subject != expected_subject:
            raise ProtocolError("service subject does not match configured service")
        if envelope.direction != "nats_to_ros":
            raise ProtocolError("service request direction must be nats_to_ros")
        if envelope.service != call.name:
            raise ProtocolError("service request name does not match configured service")
        if envelope.ros.version != self._ros.version:
            raise ProtocolError(
                "service request targets unsupported ROS version {0}".format(
                    envelope.ros.version
                )
            )
        if envelope.ros.service_type != call.service_type:
            raise ProtocolError("service request type does not match configured service")

    def _build_error(self, call, subject, code, message):
        return build_service_error_envelope(
            service=call.name,
            subject=subject,
            code=code,
            message=message or code,
            bridge_id=self._config.bridge.bridge_id,
            sequence=self._next_sequence(),
            ros_version=self._ros.version,
            ros_distro=self._ros.distro,
            ros_service_type=call.service_type,
        )

    async def _reply(self, reply, envelope):
        await self._nats.publish(reply, encode_service_envelope(envelope))
        await self._nats.flush()

    def _forget_task(self, task):
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._logger.error(
                "Service request task failed",
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    async def _cancel_tasks(self):
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
