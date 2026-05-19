import asyncio

from kinopio_hub_ros.business.configuration import load_config
from kinopio_hub_ros.business.nats_adapter import (
    NatsHealthStatus,
    NatsProbeResult,
)
from kinopio_hub_ros.business.preflight import run_preflight_checks


def write_config(tmp_path, body):
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return load_config(path)


class PassingNatsAdapter:
    def __init__(self, config):
        self._config = config
        self.connected = False
        self.closed = False

    async def probe_servers(self):
        results = [
            NatsProbeResult(
                server=self._config.nats.servers[0],
                available=True,
                round_trip_ms=11.0,
                category=None,
                message=None,
            ),
        ]
        if len(self._config.nats.servers) > 1:
            results.append(
                NatsProbeResult(
                    server=self._config.nats.servers[1],
                    available=False,
                    round_trip_ms=None,
                    category="tcp",
                    message="timeout",
                )
            )
        return tuple(results)

    async def connect(self):
        self.connected = True
        return self

    async def close(self):
        self.closed = True

    def status(self):
        return NatsHealthStatus(
            state="connected",
            connected_server=self._config.nats.servers[0],
            candidate_servers=self._config.nats.servers,
            discovered_servers=(),
            probe_results=(),
            last_error_category=None,
            last_error_message=None,
            reconnect_count=0,
            disconnect_count=0,
        )


class FailingNatsAdapter(PassingNatsAdapter):
    async def probe_servers(self):
        return (
            NatsProbeResult(
                server=self._config.nats.servers[0],
                available=False,
                round_trip_ms=None,
                category="tcp",
                message="refused",
            ),
        )

    async def connect(self):
        raise AssertionError("connect() should not be called when every probe fails")


def test_preflight_reports_ok_when_ros_and_nats_are_available(tmp_path):
    config = write_config(
        tmp_path,
        """
ros:
  version: auto
topics:
  mode: all
""".strip()
        + "\n",
    )

    report = asyncio.run(
        run_preflight_checks(
            config,
            environment={"ROS_VERSION": "2", "ROS_DISTRO": "humble"},
            ros1_available=False,
            ros2_available=True,
            nats_adapter_factory=PassingNatsAdapter,
        )
    )

    assert report["ok"] is True
    assert report["checks"]["ros"]["resolved_version"] == 2
    assert report["checks"]["nats"]["status"] == "ok"
    assert report["checks"]["nats"]["reachable_server_count"] == 1


def test_preflight_reports_missing_explicit_ros_runtime(tmp_path):
    config = write_config(
        tmp_path,
        """
ros:
  version: 2
topics:
  mode: all
""".strip()
        + "\n",
    )

    report = asyncio.run(
        run_preflight_checks(
            config,
            environment={},
            ros1_available=False,
            ros2_available=False,
            nats_adapter_factory=PassingNatsAdapter,
        )
    )

    assert report["ok"] is False
    assert report["checks"]["ros"]["status"] == "error"
    assert "ROS 2 was explicitly requested" in report["checks"]["ros"]["message"]


def test_preflight_reports_nats_probe_failure(tmp_path):
    config = write_config(
        tmp_path,
        """
topics:
  mode: all
""".strip()
        + "\n",
    )

    report = asyncio.run(
        run_preflight_checks(
            config,
            environment={"ROS_VERSION": "2"},
            ros1_available=False,
            ros2_available=True,
            nats_adapter_factory=FailingNatsAdapter,
        )
    )

    assert report["ok"] is False
    assert report["checks"]["nats"]["status"] == "error"
    assert report["checks"]["nats"]["reachable_server_count"] == 0
