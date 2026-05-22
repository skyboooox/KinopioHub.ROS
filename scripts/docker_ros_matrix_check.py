#!/usr/bin/env python3
"""Run Docker checks for the ROS 2 distro matrix and ROS 1 Noetic."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT_DIR / "docker" / "compose.check.yaml"
ROS2_DISTROS = ("foxy", "humble", "jazzy", "kilted")
ROS2_IMAGE_BY_DISTRO = {
    "foxy": "ros:foxy-ros-base-focal",
    "humble": "ros:humble-ros-base-jammy",
    "jazzy": "ros:jazzy-ros-base-noble",
    "kilted": "ros:kilted-ros-base-noble",
}
ROS1_IMAGE_BY_DISTRO = {
    "noetic": "ros:noetic-ros-base-focal",
}


class CheckFailure(RuntimeError):
    """Raised when a Docker check step fails."""


def _run(command, *, env=None, capture_output=False, check=True, cwd=ROOT_DIR):
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=capture_output,
        check=check,
    )


def _retry(label, func, *, attempts=3, delay_sec=3):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:  # pragma: no cover - exercised in live Docker checks only
            last_error = exc
            if attempt == attempts:
                raise
            print(
                json.dumps(
                    {
                        "retry": label,
                        "attempt": attempt,
                        "remaining": attempts - attempt,
                        "error": str(exc),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            time.sleep(delay_sec)
    raise last_error


def _compose(project_name, env, *args, capture_output=False, check=True):
    return _run(
        ["docker", "compose", "-p", project_name, "-f", str(COMPOSE_FILE), *args],
        env=env,
        capture_output=capture_output,
        check=check,
    )


def _compose_exec(project_name, env, *args, capture_output=False, check=True):
    return _compose(
        project_name,
        env,
        "exec",
        "-T",
        "bridge",
        *args,
        capture_output=capture_output,
        check=check,
    )


def _compose_exec_detached(project_name, env, *args):
    _compose(project_name, env, "exec", "-d", "bridge", *args)


def _docker_available():
    return shutil.which("docker") is not None


def _ensure_file_contains(project_name, env, path, expected, *, timeout_sec=20):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        result = _compose_exec(
            project_name,
            env,
            "bash",
            "-lc",
            f"test -f {path} && cat {path} || true",
            capture_output=True,
            check=True,
        )
        content = (result.stdout or "").strip()
        if expected in content:
            return content
        time.sleep(1)
    raise CheckFailure(f"Timed out waiting for {path} to contain {expected!r}")


def _ensure_service_reply_ok(reply):
    if not reply.get("ok"):
        raise CheckFailure(f"Service request failed: {reply}")
    data = reply.get("data") or {}
    if data.get("success") is not True:
        raise CheckFailure(f"Service response did not contain success=true: {reply}")


def _ensure_bridge_ready(project_name, env, *, timeout_sec=20):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        result = _compose_exec(
            project_name,
            env,
            "bash",
            "-lc",
            "test -f /tmp/bridge.log && cat /tmp/bridge.log || true",
            capture_output=True,
            check=True,
        )
        log_text = result.stdout or ""
        if "Connected NATS adapter" in log_text and "Selected ROS topics" in log_text:
            return log_text
        if "RuntimeUnavailableError" in log_text or "Traceback" in log_text:
            raise CheckFailure(f"Bridge failed to start:\n{log_text}")
        time.sleep(1)
    raise CheckFailure("Timed out waiting for bridge runtime to become ready.")


def _start_bridge(project_name, env, config_path):
    _compose_exec(
        project_name,
        env,
        "bash",
        "-lc",
        "pkill -f '[k]inopio_hub_ros' >/dev/null 2>&1 || true; rm -f /tmp/bridge.log",
        check=False,
    )
    _compose_exec_detached(
        project_name,
        env,
        "bash",
        "-lc",
        f"source /opt/ros/${{ROS_DISTRO}}/setup.bash && python -m kinopio_hub_ros --config {config_path} > /tmp/bridge.log 2>&1",
    )
    return _ensure_bridge_ready(project_name, env)


def _start_ros1_core(project_name, env):
    _compose_exec(
        project_name,
        env,
        "bash",
        "-lc",
        "pkill -f '[r]oscore' >/dev/null 2>&1 || true; rm -f /tmp/roscore.log",
        check=False,
    )
    _compose_exec_detached(
        project_name,
        env,
        "bash",
        "-lc",
        "source /opt/ros/${ROS_DISTRO}/setup.bash && roscore > /tmp/roscore.log 2>&1",
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        result = _compose_exec(
            project_name,
            env,
            "bash",
            "-lc",
            "test -f /tmp/roscore.log && cat /tmp/roscore.log || true",
            capture_output=True,
            check=True,
        )
        if "started core service [/rosout]" in (result.stdout or ""):
            return
        time.sleep(1)
    raise CheckFailure("Timed out waiting for roscore to start.")


def _start_nats_subscriber(project_name, env, *, output_path):
    _compose_exec(
        project_name,
        env,
        "bash",
        "-lc",
        f"rm -f {output_path}",
    )
    _compose_exec_detached(
        project_name,
        env,
        "bash",
        "-lc",
        f"""python - <<'PY'
import asyncio
import json
from pathlib import Path

from nats.aio.client import Client
from kinopio_hub_ros.business.envelope import decode_envelope

OUTPUT = Path("{output_path}")

async def main():
    client = Client()
    received = {{}}
    done = asyncio.Event()

    async def callback(message):
        envelope = decode_envelope(message.data)
        received["text"] = envelope.text
        received["subject"] = message.subject
        done.set()

    await client.connect(servers=["nats://nats:4222"])
    await client.subscribe("ros.chatter", cb=callback)
    await asyncio.wait_for(done.wait(), timeout=20)
    OUTPUT.write_text(json.dumps(received), encoding="utf-8")
    await client.close()

asyncio.run(main())
PY""",
    )


def _publish_to_nats(project_name, env, *, ros_version, message_type, text):
    _compose_exec(
        project_name,
        env,
        "bash",
        "-lc",
        f"""python - <<'PY'
import asyncio

from nats.aio.client import Client
from kinopio_hub_ros.business.envelope import build_text_envelope, encode_envelope

async def main():
    client = Client()
    await client.connect(servers=["nats://nats:4222"])
    envelope = build_text_envelope(
        direction="nats_to_ros",
        topic="/chatter",
        subject="ros.chatter",
        text={text!r},
        bridge_id="docker-check-publisher",
        sequence=1,
        ros_version={ros_version},
        ros_distro="{env['ROS_DISTRO']}",
        ros_message_type="{message_type}",
    )
    await client.publish("ros.chatter", encode_envelope(envelope))
    await client.flush()
    await client.close()

asyncio.run(main())
PY""",
    )


def _request_set_bool_via_nats(project_name, env, *, ros_version):
    result = _compose_exec(
        project_name,
        env,
        "bash",
        "-lc",
        f"""python - <<'PY'
import asyncio
import json

from nats.aio.client import Client
from kinopio_hub_ros.business.service_envelope import (
    build_service_request_envelope,
    decode_service_envelope,
    encode_service_envelope,
)

SUBJECT = "ros_services.docker_matrix.set_bool"

async def main():
    client = Client()
    await client.connect(servers=["nats://nats:4222"])
    request = build_service_request_envelope(
        service="/docker_matrix/set_bool",
        subject=SUBJECT,
        data={{"data": True}},
        bridge_id="docker-check-service-client",
        sequence=1,
        ros_version={ros_version},
        ros_service_type="std_srvs/srv/SetBool",
    )
    message = await client.request(
        SUBJECT,
        encode_service_envelope(request),
        timeout=20,
    )
    response = decode_service_envelope(message.data)
    print(json.dumps({{
        "ok": response.ok,
        "data": response.data,
        "error": response.error.to_dict() if response.error else None,
    }}, ensure_ascii=False))
    await client.close()

asyncio.run(main())
PY""",
        capture_output=True,
    )
    return json.loads(result.stdout)


def _run_ros2_publish(project_name, env, *, text):
    _compose_exec(
        project_name,
        env,
        "bash",
        "-lc",
        f"""source /opt/ros/${{ROS_DISTRO}}/setup.bash && python - <<'PY'
import time
import rclpy
from std_msgs.msg import String

rclpy.init(args=None)
node = rclpy.create_node("docker_matrix_ros2_publisher")
publisher = node.create_publisher(String, "/chatter", 10)
message = String()
message.data = {text!r}
deadline = time.time() + 4
while time.time() < deadline:
    publisher.publish(message)
    rclpy.spin_once(node, timeout_sec=0.1)
    time.sleep(0.4)
node.destroy_node()
rclpy.shutdown()
PY""",
    )


def _start_ros2_service_server(project_name, env):
    _compose_exec_detached(
        project_name,
        env,
        "bash",
        "-lc",
        f"""source /opt/ros/${{ROS_DISTRO}}/setup.bash && python - <<'PY'
import rclpy
from std_srvs.srv import SetBool

rclpy.init(args=None)
node = rclpy.create_node("docker_matrix_ros2_set_bool_server")

def callback(request, response):
    response.success = bool(request.data)
    response.message = "ros2 {env['ROS_DISTRO']} set_bool " + str(bool(request.data))
    return response

node.create_service(SetBool, "/docker_matrix/set_bool", callback)
rclpy.spin(node)
node.destroy_node()
rclpy.shutdown()
PY""",
    )


def _start_ros2_subscriber(project_name, env, *, output_path):
    _compose_exec(project_name, env, "bash", "-lc", f"rm -f {output_path}")
    _compose_exec_detached(
        project_name,
        env,
        "bash",
        "-lc",
        f"""source /opt/ros/${{ROS_DISTRO}}/setup.bash && python - <<'PY'
import json
import time
from pathlib import Path

import rclpy
from std_msgs.msg import String

OUTPUT = Path("{output_path}")
rclpy.init(args=None)
node = rclpy.create_node("docker_matrix_ros2_subscriber")
received = {{"text": None}}

def callback(message):
    received["text"] = message.data

node.create_subscription(String, "/chatter", callback, 10)
deadline = time.time() + 20
while received["text"] is None and time.time() < deadline:
    rclpy.spin_once(node, timeout_sec=0.2)

if received["text"] is None:
    raise SystemExit(2)

OUTPUT.write_text(json.dumps(received), encoding="utf-8")
node.destroy_node()
rclpy.shutdown()
PY""",
    )


def _run_ros1_publish(project_name, env, *, text):
    _compose_exec(
        project_name,
        env,
        "bash",
        "-lc",
        f"""source /opt/ros/${{ROS_DISTRO}}/setup.bash && python - <<'PY'
import time
import rospy
from std_msgs.msg import String

rospy.init_node("docker_matrix_ros1_publisher", anonymous=False, disable_signals=True)
publisher = rospy.Publisher("/chatter", String, queue_size=10)
deadline = time.time() + 4
while time.time() < deadline:
    publisher.publish(String(data={text!r}))
    rospy.sleep(0.4)
PY""",
    )


def _start_ros1_service_server(project_name, env):
    _compose_exec_detached(
        project_name,
        env,
        "bash",
        "-lc",
        """source /opt/ros/${ROS_DISTRO}/setup.bash && python - <<'PY'
import rospy
from std_srvs.srv import SetBool, SetBoolResponse

def callback(request):
    return SetBoolResponse(
        success=bool(request.data),
        message="ros1 noetic set_bool " + str(bool(request.data)),
    )

rospy.init_node("docker_matrix_ros1_set_bool_server", anonymous=False, disable_signals=True)
rospy.Service("/docker_matrix/set_bool", SetBool, callback)
rospy.spin()
PY""",
    )


def _start_ros1_subscriber(project_name, env, *, output_path):
    _compose_exec(project_name, env, "bash", "-lc", f"rm -f {output_path}")
    _compose_exec_detached(
        project_name,
        env,
        "bash",
        "-lc",
        f"""source /opt/ros/${{ROS_DISTRO}}/setup.bash && python - <<'PY'
import json
import time
from pathlib import Path

import rospy
from std_msgs.msg import String

OUTPUT = Path("{output_path}")
received = {{"text": None}}

def callback(message):
    received["text"] = message.data

rospy.init_node("docker_matrix_ros1_subscriber", anonymous=False, disable_signals=True)
rospy.Subscriber("/chatter", String, callback, queue_size=10)
deadline = time.time() + 20
while received["text"] is None and time.time() < deadline and not rospy.is_shutdown():
    rospy.sleep(0.2)

if received["text"] is None:
    raise SystemExit(2)

OUTPUT.write_text(json.dumps(received), encoding="utf-8")
PY""",
    )


def _cleanup_processes(project_name, env):
    _compose_exec(
        project_name,
        env,
        "bash",
        "-lc",
        "pkill -f '[k]inopio_hub_ros|[d]ocker_matrix_ros|[d]ocker-check-publisher|[r]oscore' >/dev/null 2>&1 || true",
        check=False,
    )


def _run_ros2_distro(distro):
    project_name = f"kinopio-ros2-{distro}"
    env = os.environ.copy()
    env["ROS_BASE_IMAGE"] = ROS2_IMAGE_BY_DISTRO[distro]
    env["ROS_DISTRO"] = distro
    env["ROS_DOMAIN_ID"] = "42"
    ros_to_nats_text = f"hello from ros2 {distro}"
    nats_to_ros_text = f"hello from nats {distro}"
    try:
        _retry(
            f"docker compose up ros2 {distro}",
            lambda: _compose(project_name, env, "up", "-d", "--build", "nats", "bridge"),
        )
        _start_bridge(project_name, env, "docker/ros2.check.yaml")
        _start_nats_subscriber(project_name, env, output_path="/tmp/ros_to_nats.json")
        time.sleep(2)
        _run_ros2_publish(project_name, env, text=ros_to_nats_text)
        ros_to_nats = _ensure_file_contains(
            project_name,
            env,
            "/tmp/ros_to_nats.json",
            ros_to_nats_text,
        )
        _start_ros2_subscriber(project_name, env, output_path="/tmp/nats_to_ros.json")
        time.sleep(2)
        _publish_to_nats(
            project_name,
            env,
            ros_version=2,
            message_type="std_msgs/msg/String",
            text=nats_to_ros_text,
        )
        nats_to_ros = _ensure_file_contains(
            project_name,
            env,
            "/tmp/nats_to_ros.json",
            nats_to_ros_text,
        )
        _start_ros2_service_server(project_name, env)
        service_reply = _request_set_bool_via_nats(
            project_name,
            env,
            ros_version=2,
        )
        _ensure_service_reply_ok(service_reply)
        return {
            "status": "passed",
            "ros_to_nats": json.loads(ros_to_nats),
            "nats_to_ros": json.loads(nats_to_ros),
            "service_reply": service_reply,
        }
    finally:
        _cleanup_processes(project_name, env)
        _compose(project_name, env, "down", "-v", "--remove-orphans", check=False)


def _run_ros1_noetic():
    project_name = "kinopio-ros1-noetic"
    env = os.environ.copy()
    env["ROS_BASE_IMAGE"] = ROS1_IMAGE_BY_DISTRO["noetic"]
    env["ROS_DISTRO"] = "noetic"
    ros_to_nats_text = "hello from ros1 noetic"
    nats_to_ros_text = "hello from nats noetic"
    try:
        _retry(
            "docker compose up ros1 noetic",
            lambda: _compose(project_name, env, "up", "-d", "--build", "nats", "bridge"),
        )
        _start_ros1_core(project_name, env)
        _start_bridge(project_name, env, "docker/ros1.check.yaml")
        _start_nats_subscriber(project_name, env, output_path="/tmp/ros1_to_nats.json")
        time.sleep(2)
        _run_ros1_publish(project_name, env, text=ros_to_nats_text)
        ros_to_nats = _ensure_file_contains(
            project_name,
            env,
            "/tmp/ros1_to_nats.json",
            ros_to_nats_text,
        )
        _start_ros1_subscriber(project_name, env, output_path="/tmp/nats_to_ros1.json")
        time.sleep(2)
        _publish_to_nats(
            project_name,
            env,
            ros_version=1,
            message_type="std_msgs/String",
            text=nats_to_ros_text,
        )
        nats_to_ros = _ensure_file_contains(
            project_name,
            env,
            "/tmp/nats_to_ros1.json",
            nats_to_ros_text,
        )
        _start_ros1_service_server(project_name, env)
        service_reply = _request_set_bool_via_nats(
            project_name,
            env,
            ros_version=1,
        )
        _ensure_service_reply_ok(service_reply)
        return {
            "status": "passed",
            "ros_to_nats": json.loads(ros_to_nats),
            "nats_to_ros": json.loads(nats_to_ros),
            "service_reply": service_reply,
        }
    finally:
        _cleanup_processes(project_name, env)
        _compose(project_name, env, "down", "-v", "--remove-orphans", check=False)


def main():
    parser = argparse.ArgumentParser(
        description="Run Docker checks for the ROS distro matrix."
    )
    parser.add_argument(
        "--ros2-only",
        action="store_true",
        help="Run only the ROS 2 Foxy/Humble/Jazzy/Kilted matrix.",
    )
    parser.add_argument(
        "--ros2-distros",
        default=",".join(ROS2_DISTROS),
        help="Comma-separated ROS 2 distros to run. Default: all validated distros.",
    )
    parser.add_argument(
        "--ros1-only",
        action="store_true",
        help="Run only the ROS 1 Noetic check.",
    )
    args = parser.parse_args()

    if args.ros2_only and args.ros1_only:
        parser.error("--ros2-only and --ros1-only are mutually exclusive")

    if not _docker_available():
        raise SystemExit("docker is required to run docker_ros_matrix_check.py")

    selected_ros2_distros = tuple(
        distro.strip().lower()
        for distro in args.ros2_distros.split(",")
        if distro.strip()
    )
    unknown_ros2_distros = sorted(set(selected_ros2_distros) - set(ROS2_DISTROS))
    if unknown_ros2_distros:
        parser.error(
            "--ros2-distros contains unsupported values: {0}".format(
                ", ".join(unknown_ros2_distros)
            )
        )

    report = {
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ros2": {},
        "ros1": {},
    }
    failed = False

    if not args.ros1_only:
        for distro in selected_ros2_distros:
            try:
                report["ros2"][distro] = _run_ros2_distro(distro)
            except Exception as exc:  # pragma: no cover - exercised via live Docker checks
                failed = True
                report["ros2"][distro] = {
                    "status": "failed",
                    "error": str(exc),
                }

    if not args.ros2_only:
        try:
            report["ros1"]["noetic"] = _run_ros1_noetic()
        except Exception as exc:  # pragma: no cover - exercised via live Docker checks
            failed = True
            report["ros1"]["noetic"] = {
                "status": "failed",
                "error": str(exc),
            }

    print(json.dumps(report, indent=2, ensure_ascii=False))
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
