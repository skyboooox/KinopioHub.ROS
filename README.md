# KinopioHub.ROS

KinopioHub.ROS is a bidirectional ROS topic bridge plus an explicit ROS service request-reply bridge for KinopioHub-compatible NATS subjects.
It discovers ROS 1 or ROS 2 topics, serializes ROS messages into JSON envelopes, publishes them to KinopioHub, accepts matching KinopioHub envelopes for ROS writeback, and exposes configured ROS services over NATS request-reply.

中文说明见 [README_CN.md](./README_CN.md).

## Status

- Package name: `kinopio-hub-ros`
- CLI: `kinopio-hub-ros`
- Python: `>=3.8`
- Runtime dependencies: `nats-py[nkeys]`, `PyYAML`
- ROS 2 adapter: Foxy, Humble, Jazzy, Kilted; Rolling is best-effort
- ROS 1 adapter: Noetic
- Default topic mode: `all`, meaning every discoverable topic whose message type can be imported
- Service calls: explicit allowlist only; no service is exposed unless configured
- Runtime config: copy `config.example.yaml` to an untracked `config.yaml`

Actions are out of scope.

## Quick Start

Use an isolated virtualenv so the repository root stays clean:

```bash
python3 -m venv /tmp/kinopio-hub-ros-venv
source /tmp/kinopio-hub-ros-venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[test]"

cp config.example.yaml config.yaml
kinopio-hub-ros --config config.yaml --dry-run
```

`--dry-run` validates YAML and prints normalized config. It does not connect to ROS or KinopioHub and does not read secret environment-variable values.

To run against an installed ROS environment:

```bash
source /opt/ros/humble/setup.bash
kinopio-hub-ros --config config.yaml --check
kinopio-hub-ros --config config.yaml --log-level INFO
```

Use `ros.version: 1` for ROS 1 Noetic, `ros.version: 2` for ROS 2, or `ros.version: auto` to select from the current environment.

## Configuration

Start from [config.example.yaml](./config.example.yaml). Keep real servers, credentials, CA paths, and private overrides in an untracked `config.yaml`.

```yaml
bridge:
  id: ubuntu22-ros-bridge
  direction: bidirectional

nats:
  servers:
    - tls://nats.example.invalid:14222
  tls:
    enabled: true
    handshake_first: true
    ca_file: null
    server_name: null
  auth:
    mode: none

ros:
  version: auto
  qos:
    reliability: reliable
    durability: volatile
    depth: 10

topics:
  mode: all

services:
  subject_prefix: ros_services
  calls:
    - name: /lane_navigation/go_from_to
      type: lane_navigation/srv/GoFromTo
      timeout_ms: 30000

sync:
  subject_prefix: ros
  throttle_ms: 100
  dedupe: true
  heartbeat_ms: 0
  loop_suppression_ms: 1000
```

Topic selection supports `all`, `include`, and `exclude`. `include` and `exclude` require ROS topic patterns such as `/chatter`, `/robot/*/state`, or `/robot/**/text`.

`services.calls` defaults to empty. Each configured service is exposed as a NATS request-reply responder. Service types may use ROS 2 style `pkg/srv/Name`; ROS 1 runtime calls normalize that to `pkg/Name`.

Supported auth modes are `none`, `username_password`, `token`, `nkey`, and `creds`. Secret values should be supplied through environment variables or untracked private files.

## Wire Format

ROS topics map to KinopioHub subjects by stripping the leading slash, replacing `/` with `.`, and prefixing with `sync.subject_prefix`:

```text
/chatter           -> ros.chatter
/robot/status/text -> ros.robot.status.text
```

Structured messages use `kinopio.ros.message.v1`; `data` is the ROS message as a JSON object. `std_msgs/String` and `std_msgs/msg/String` keep the legacy `kinopio.ros.text.v1` envelope for SDK compatibility. The decoder still accepts legacy text envelopes for writeback.

Every envelope carries `topic`, `subject`, ROS version/distro/type, timestamp, `meta.bridgeId`, and `meta.sequence`. `bridgeId` plus recent-writeback tracking prevents infinite loops.

Configured ROS services map to `services.subject_prefix` with the same slash-to-dot rule:

```text
/lane_navigation/go_from_to -> ros_services.lane_navigation.go_from_to
```

Service request-reply uses `kinopio.ros.service.v1`. A NATS client sends a request to the mapped subject with `direction: nats_to_ros`, `service`, `subject`, `ros.version`, `ros.type`, `data`, and `meta`; the bridge replies to the NATS reply subject with `direction: ros_to_nats`, `ok: true` and response `data`, or `ok: false` plus `error.code`/`error.message`.

## Verification

```bash
./scripts/pytest_clean.sh tests
kinopio-hub-ros --config config.example.yaml --dry-run
kinopio-hub-ros --config examples/config.minimal.yaml --dry-run
./scripts/check.sh --skip-docker
```

`scripts/check.sh` creates its virtualenv under `/tmp`, installs the package with test extras, runs tests, validates public configs, and can optionally validate Docker Compose.

Docker checks are available when Docker is installed:

```bash
python scripts/docker_ros_matrix_check.py --ros2-only --ros2-distros foxy,humble,jazzy,kilted
python scripts/docker_ros_matrix_check.py --ros1-only
```

Remote SDK checks exercise this bridge with the public KinopioHub.JS package. Install the JS SDK into an isolated prefix and pass the installed entrypoint explicitly:

```bash
rm -rf /tmp/kinopiohub-js-check
mkdir -p /tmp/kinopiohub-js-check
npm install --prefix /tmp/kinopiohub-js-check github:skyboooox/KinopioHub.JS
```

Then run the check with explicit NATS endpoints:

```bash
export KINOPIO_HUB_ROS_NATS_TLS_SERVERS="tls://nats.example.invalid:14222"
export KINOPIOHUB_JS_NATS_WSS_SERVERS="wss://nats.example.invalid:55588"
export KINOPIOHUB_JS_ENTRYPOINT="/tmp/kinopiohub-js-check/node_modules/kinopio-hub/kinopio.mjs"
python scripts/js_sdk_check.py
```

`js_sdk_check.py` is JS-only: Python uses this project's `NatsAdapter` plus an in-process fake ROS 2 driver, while KinopioHub.JS verifies `ROS -> bridge -> NATS -> JS` receive and `JS -> NATS -> bridge -> ROS` writeback. If the JS SDK is missing, the script returns a JSON failure with the required variables and the npm install command. The Python and C++ SDKs share the same subject and JSON envelope contract, but they are not executable targets for this check.

## Troubleshooting

- Run `kinopio-hub-ros --config config.yaml --check` before long-running deployment.
- Check `checks.ros` for missing `rclpy` or `rospy`.
- Check `checks.nats.probe_results` for DNS, TCP, TLS, auth, or protocol failures.
- Look for `Selected ROS topics` to confirm discovery and filtering.
- Look for `Connected NATS adapter` to confirm the selected server.
- `Ignoring invalid NATS envelope` means the payload does not match the envelope contract.
- `Ignoring NATS envelope with mismatched subject/topic` means the subject does not match the envelope topic mapping.
- Service request errors return `kinopio.ros.service.v1` replies with `ok: false`.

This repository provides commands and verification scripts only. It does not install a systemd service, modify firewalls, write certificates, or change ROS environment files.
