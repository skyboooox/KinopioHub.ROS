import json
import shutil
import subprocess

import pytest

from kinopio_hub_ros.business.envelope import (
    ENVELOPE_SCHEMA,
    LEGACY_TEXT_ENVELOPE_SCHEMA,
    build_message_envelope,
    build_text_envelope,
    encode_envelope,
)

JS_INSTALL_HINT = (
    "npm install --prefix /tmp/kinopiohub-js-check github:skyboooox/KinopioHub.JS && "
    "export KINOPIOHUB_JS_ENTRYPOINT=/tmp/kinopiohub-js-check/node_modules/kinopio-hub/kinopio.mjs"
)
JS_RESOLVER_PROGRAM = """
const spec = process.env.KINOPIOHUB_JS_ENTRYPOINT
  ? new URL(process.env.KINOPIOHUB_JS_ENTRYPOINT, `file://${process.cwd()}/`).href
  : (process.env.KINOPIOHUB_JS_PACKAGE ?? "kinopio-hub");
const module = await import(spec);
const KinopioHub = module.KinopioHub ?? module.default?.KinopioHub ?? module.default;
if (typeof KinopioHub !== "function") {
  throw new Error("KinopioHub constructor export is missing");
}
"""


def js_sdk_available():
    if shutil.which("node") is None:
        return False
    result = subprocess.run(
        ["node", "--input-type=module", "-e", JS_RESOLVER_PROGRAM],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def test_envelope_is_compatible_with_python_default_json_behavior():
    payload = encode_envelope(
        build_text_envelope(
            direction="ros_to_nats",
            topic="/chatter",
            subject="ros.chatter",
            text="hello from ros",
            bridge_id="bridge-a",
            sequence=1,
            ros_version=2,
            ros_distro="humble",
            ros_message_type="std_msgs/msg/String",
        )
    )

    decoded = json.loads(payload.decode("utf-8"))

    assert decoded["schema"] == LEGACY_TEXT_ENVELOPE_SCHEMA
    assert decoded["data"]["text"] == "hello from ros"


def test_message_envelope_is_compatible_with_python_default_json_behavior():
    payload = encode_envelope(
        build_message_envelope(
            direction="ros_to_nats",
            topic="/odom",
            subject="ros.odom",
            data={"pose": {"position": {"x": 1.0}}},
            bridge_id="bridge-structured",
            sequence=3,
            ros_version=2,
            ros_distro="humble",
            ros_message_type="nav_msgs/msg/Odometry",
        )
    )

    decoded = json.loads(payload.decode("utf-8"))

    assert decoded["schema"] == ENVELOPE_SCHEMA
    assert decoded["data"] == {"pose": {"position": {"x": 1.0}}}


@pytest.mark.skipif(
    not js_sdk_available(),
    reason="KinopioHub.JS SDK is not installed; " + JS_INSTALL_HINT,
)
def test_envelope_is_compatible_with_kinopiohub_js_default_deserializer():
    payload = encode_envelope(
        build_text_envelope(
            direction="ros_to_nats",
            topic="/robot/status/text",
            subject="ros.robot.status.text",
            text="nominal",
            bridge_id="bridge-b",
            sequence=2,
            ros_version=2,
            ros_distro="jazzy",
            ros_message_type="std_msgs/msg/String",
        )
    )
    node_program = JS_RESOLVER_PROGRAM + """
const hub = new KinopioHub({ autoConnect: false });
const payload = new Uint8Array(%s);
const decoded = hub.deserializeData(payload);
process.stdout.write(JSON.stringify(decoded));
""" % json.dumps(list(payload))

    result = subprocess.run(
        ["node", "--input-type=module", "-e", node_program],
        check=True,
        capture_output=True,
        text=True,
    )

    decoded = json.loads(result.stdout)

    assert decoded["schema"] == LEGACY_TEXT_ENVELOPE_SCHEMA
    assert decoded["data"]["text"] == "nominal"
