"""Latest-state sync policy used by the bridge core."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TextEmission:
    topic: str
    text: str
    message_type: Optional[str]
    emitted_at_ms: int
    first_observed_at_ms: int
    last_observed_at_ms: int
    json_value: object = None


@dataclass
class _PendingText:
    topic: str
    text: str
    message_type: Optional[str]
    emit_at_ms: int
    first_observed_at_ms: int
    last_observed_at_ms: int
    json_value: object = None


class LatestStatePolicy:
    def __init__(self, *, throttle_ms, dedupe, loop_suppression_ms):
        self._throttle_ms = throttle_ms
        self._dedupe = dedupe
        self._loop_suppression_ms = loop_suppression_ms
        self._pending_by_topic = {}
        self._latest_seen_by_topic = {}
        self._latest_published_by_topic = {}
        self._recent_writebacks = {}

    def ingest_ros_text(self, topic, text, now_ms, message_type=None, json_value=None):
        emissions = list(self.flush_due(now_ms))
        self._prune_recent_writebacks(now_ms)
        self._latest_seen_by_topic[topic] = text

        if self._is_loop_suppressed(topic, text, message_type, now_ms):
            return tuple(emissions)

        pending = self._pending_by_topic.get(topic)
        if pending is not None:
            pending.text = text
            pending.message_type = message_type
            pending.json_value = json_value
            pending.last_observed_at_ms = now_ms
            return tuple(emissions)

        if self._dedupe and self._latest_published_by_topic.get(topic) == (message_type, text):
            return tuple(emissions)

        if self._throttle_ms == 0:
            emissions.append(
                self._emit_now(
                    topic,
                    text,
                    message_type,
                    now_ms,
                    now_ms,
                    json_value=json_value,
                )
            )
            return tuple(emissions)

        self._pending_by_topic[topic] = _PendingText(
            topic=topic,
            text=text,
            message_type=message_type,
            emit_at_ms=now_ms + self._throttle_ms,
            first_observed_at_ms=now_ms,
            last_observed_at_ms=now_ms,
            json_value=json_value,
        )
        return tuple(emissions)

    def flush_due(self, now_ms):
        self._prune_recent_writebacks(now_ms)
        emissions = []
        due_topics = sorted(
            topic
            for topic, pending in self._pending_by_topic.items()
            if pending.emit_at_ms <= now_ms
        )
        for topic in due_topics:
            pending = self._pending_by_topic.pop(topic)
            if self._dedupe and self._latest_published_by_topic.get(topic) == (
                pending.message_type,
                pending.text,
            ):
                continue
            emissions.append(
                self._emit_now(
                    topic=pending.topic,
                    text=pending.text,
                    message_type=pending.message_type,
                    first_observed_at_ms=pending.first_observed_at_ms,
                    emitted_at_ms=now_ms,
                    last_observed_at_ms=pending.last_observed_at_ms,
                    json_value=pending.json_value,
                )
            )
        return tuple(emissions)

    def record_nats_writeback(self, topic, text, now_ms, message_type=None):
        self._recent_writebacks[(topic, message_type, text)] = now_ms + self._loop_suppression_ms

    def latest_seen_text(self, topic):
        return self._latest_seen_by_topic.get(topic)

    def latest_published_text(self, topic):
        value = self._latest_published_by_topic.get(topic)
        return None if value is None else value[1]

    def pending_text(self, topic):
        pending = self._pending_by_topic.get(topic)
        return None if pending is None else pending.text

    def _emit_now(
        self,
        topic,
        text,
        message_type,
        emitted_at_ms,
        first_observed_at_ms,
        last_observed_at_ms=None,
        json_value=None,
    ):
        self._latest_published_by_topic[topic] = (message_type, text)
        return TextEmission(
            topic=topic,
            text=text,
            message_type=message_type,
            emitted_at_ms=emitted_at_ms,
            first_observed_at_ms=first_observed_at_ms,
            last_observed_at_ms=last_observed_at_ms or emitted_at_ms,
            json_value=json_value,
        )

    def _is_loop_suppressed(self, topic, text, message_type, now_ms):
        expires_at_ms = self._recent_writebacks.get((topic, message_type, text))
        if expires_at_ms is None and message_type is None:
            expires_at_ms = self._recent_writebacks.get((topic, None, text))
        return expires_at_ms is not None and expires_at_ms > now_ms

    def _prune_recent_writebacks(self, now_ms):
        expired = [
            key for key, expires_at_ms in self._recent_writebacks.items() if expires_at_ms <= now_ms
        ]
        for key in expired:
            self._recent_writebacks.pop(key, None)
