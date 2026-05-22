"""Preflight checks for deployment and operational diagnostics."""

import asyncio
import importlib.util
import os

from datetime import datetime, timezone

from kinopio_hub_ros.business.nats_adapter import NatsAdapter
from kinopio_hub_ros.business.ros_adapter_factory import resolve_ros_runtime_version
from kinopio_hub_ros.errors import AdapterError, RuntimeUnavailableError


async def run_preflight_checks(
    config,
    *,
    environment=None,
    ros1_available=None,
    ros2_available=None,
    nats_adapter_factory=NatsAdapter,
):
    environment = environment or os.environ
    report = {
        "ok": False,
        "checked_at": _utcnow_iso(),
        "config": {
            "bridge": config.bridge.to_dict(),
            "ros": config.ros.to_dict(),
            "topics": config.topics.to_dict(),
            "services": config.services.to_dict(),
            "sync": config.sync.to_dict(),
            "nats": {
                "servers": list(config.nats.servers),
                "tls": config.nats.tls.to_dict(),
                "auth": config.nats.auth.to_dict(),
            },
        },
        "checks": {
            "config": {
                "status": "ok",
                "message": "Configuration parsed successfully.",
            },
            "ros": _check_ros_environment(
                config,
                environment=environment,
                ros1_available=ros1_available,
                ros2_available=ros2_available,
            ),
            "nats": await _check_nats_connectivity(
                config,
                nats_adapter_factory=nats_adapter_factory,
            ),
        },
    }
    report["ok"] = (
        report["checks"]["config"]["status"] == "ok"
        and report["checks"]["ros"]["status"] == "ok"
        and report["checks"]["nats"]["status"] == "ok"
    )
    return report


def _check_ros_environment(
    config,
    *,
    environment,
    ros1_available=None,
    ros2_available=None,
):
    ros1_available = _coalesce_availability(ros1_available, "rospy")
    ros2_available = _coalesce_availability(ros2_available, "rclpy")
    requested_version = config.ros.version
    environment_summary = {
        "ROS_VERSION": _optional_env(environment.get("ROS_VERSION")),
        "ROS_DISTRO": _optional_env(environment.get("ROS_DISTRO")),
    }
    report = {
        "status": "error",
        "requested_version": requested_version,
        "resolved_version": None,
        "available": {
            "ros1_module": ros1_available,
            "ros2_module": ros2_available,
        },
        "environment": environment_summary,
        "message": None,
    }

    if requested_version == 1 and not ros1_available:
        report["message"] = (
            "ROS 1 was explicitly requested, but rospy is not importable in the current environment."
        )
        return report
    if requested_version == 2 and not ros2_available:
        report["message"] = (
            "ROS 2 was explicitly requested, but rclpy is not importable in the current environment."
        )
        return report

    try:
        resolved_version = resolve_ros_runtime_version(
            requested_version,
            ros1_available=ros1_available,
            ros2_available=ros2_available,
            environment=environment,
        )
    except RuntimeUnavailableError as exc:
        report["message"] = str(exc)
        return report

    report["status"] = "ok"
    report["resolved_version"] = resolved_version
    report["message"] = (
        "ROS {0} runtime is available for this configuration.".format(resolved_version)
    )
    return report


async def _check_nats_connectivity(config, *, nats_adapter_factory):
    adapter = nats_adapter_factory(config)
    report = {
        "status": "error",
        "candidate_servers": list(config.nats.servers),
        "reachable_server_count": 0,
        "connected_server": None,
        "probe_results": [],
        "message": None,
    }

    try:
        probe_results = await adapter.probe_servers()
        report["probe_results"] = [item.to_dict() for item in probe_results]
        reachable_server_count = sum(1 for item in probe_results if item.available)
        report["reachable_server_count"] = reachable_server_count

        if reachable_server_count == 0:
            report["message"] = "No reachable NATS servers were found for the configured candidate list."
            return report

        await adapter.connect()
        status = adapter.status()
        report["status"] = "ok"
        report["connected_server"] = status.connected_server
        report["message"] = (
            "Connected to {0}; {1}/{2} candidate servers reachable.".format(
                status.connected_server,
                reachable_server_count,
                len(config.nats.servers),
            )
        )
        return report
    except AdapterError as exc:
        report["message"] = str(exc)
        return report
    except Exception as exc:
        report["message"] = "{0}: {1}".format(type(exc).__name__, exc)
        return report
    finally:
        close = getattr(adapter, "close", None)
        if close is not None:
            result = close()
            if asyncio.iscoroutine(result):
                await result


def _coalesce_availability(value, module_name):
    if value is not None:
        return bool(value)
    return importlib.util.find_spec(module_name) is not None


def _optional_env(value):
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _utcnow_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
