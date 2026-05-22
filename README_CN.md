# KinopioHub.ROS

KinopioHub.ROS 是一个面向 KinopioHub 协议的 ROS topic 双向桥，也支持显式配置的 ROS service request-reply。它发现 ROS 1 或 ROS 2 topic，把 ROS message 序列化为 JSON envelope 发布到 KinopioHub，接收匹配的 KinopioHub envelope 写回 ROS，并把 allowlist 中的 ROS service 暴露为 NATS request-reply responder。

English version: [README.md](./README.md).

## 当前状态

- 包名：`kinopio-hub-ros`
- CLI：`kinopio-hub-ros`
- Python：`>=3.8`
- 运行依赖：`nats-py[nkeys]`、`PyYAML`
- ROS 2 adapter：Foxy、Humble、Jazzy、Kilted；Rolling 为 best-effort
- ROS 1 adapter：Noetic
- 默认 topic 模式：`all`，即桥接当前 ROS graph 中可导入 message type 的 topic
- Service 调用：只暴露 `services.calls` 中显式声明的 allowlist
- 运行配置：把 `config.example.yaml` 复制为未跟踪的 `config.yaml`

本项目不处理 action。

## 快速开始

使用 `/tmp` 下的隔离虚拟环境，避免污染仓库根目录：

```bash
python3 -m venv /tmp/kinopio-hub-ros-venv
source /tmp/kinopio-hub-ros-venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[test]"

cp config.example.yaml config.yaml
kinopio-hub-ros --config config.yaml --dry-run
```

`--dry-run` 只校验 YAML 并输出归一化配置，不连接 ROS 或 KinopioHub，也不会读取认证环境变量的真实值。

在已安装 ROS 的环境中运行：

```bash
source /opt/ros/humble/setup.bash
kinopio-hub-ros --config config.yaml --check
kinopio-hub-ros --config config.yaml --log-level INFO
```

ROS 1 Noetic 使用 `ros.version: 1`，ROS 2 使用 `ros.version: 2`，也可以用 `ros.version: auto` 按当前环境自动选择。

## 配置

从 [config.example.yaml](./config.example.yaml) 开始。真实服务器、凭据、CA 路径和私有覆盖项放进未跟踪的 `config.yaml`。

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

Topic 选择支持 `all`、`include`、`exclude`。`include` 和 `exclude` 必须提供 ROS topic pattern，例如 `/chatter`、`/robot/*/state`、`/robot/**/text`。

`services.calls` 默认空列表。每个声明的 service 会暴露为 NATS request-reply responder。Service type 可用 ROS 2 风格 `pkg/srv/Name`；ROS 1 运行时会归一化为 `pkg/Name`。

认证模式支持 `none`、`username_password`、`token`、`nkey`、`creds`。密钥值应通过环境变量或未跟踪的私有文件提供。

## 线协议

ROS topic 到 KinopioHub subject 的默认映射规则：去掉开头 `/`，把 `/` 转为 `.`，再加上 `sync.subject_prefix`。

```text
/chatter           -> ros.chatter
/robot/status/text -> ros.robot.status.text
```

结构化消息使用 `kinopio.ros.message.v1`，`data` 直接承载 ROS message 的 JSON 对象。`std_msgs/String` 和 `std_msgs/msg/String` 保持旧的 `kinopio.ros.text.v1` envelope，以兼容现有 SDK。写回路径仍接受旧文本 envelope。

每个 envelope 都包含 `topic`、`subject`、ROS version/distro/type、时间戳、`meta.bridgeId` 和 `meta.sequence`。`bridgeId` 与近期写回记录一起用于避免无限回环。

显式配置的 ROS service 使用 `services.subject_prefix` 和同样的 slash-to-dot 规则：

```text
/lane_navigation/go_from_to -> ros_services.lane_navigation.go_from_to
```

Service request-reply 使用 `kinopio.ros.service.v1`。NATS client 向映射后的 subject 发送 request，payload 包含 `direction: nats_to_ros`、`service`、`subject`、`ros.version`、`ros.type`、`data`、`meta`；bridge 向 NATS reply subject 返回 `direction: ros_to_nats`、`ok: true` 和响应 `data`，失败时返回 `ok: false`、`error.code`、`error.message`。

## 开发与验证

```bash
./scripts/pytest_clean.sh tests
kinopio-hub-ros --config config.example.yaml --dry-run
kinopio-hub-ros --config examples/config.minimal.yaml --dry-run
./scripts/check.sh --skip-docker
```

`scripts/check.sh` 会在 `/tmp` 下创建虚拟环境，安装测试依赖，运行测试，校验公开配置，并可选校验 Docker Compose。

安装 Docker 后可以跑容器检查：

```bash
python scripts/docker_ros_matrix_check.py --ros2-only --ros2-distros foxy,humble,jazzy,kilted
python scripts/docker_ros_matrix_check.py --ros1-only
```

远端 SDK 检查只实际拉起公开的 KinopioHub.JS 包。把 JS SDK 安装到隔离 prefix，并显式传入安装后的入口：

```bash
rm -rf /tmp/kinopiohub-js-check
mkdir -p /tmp/kinopiohub-js-check
npm install --prefix /tmp/kinopiohub-js-check github:skyboooox/KinopioHub.JS
```

然后显式传入 NATS endpoint 后运行：

```bash
export KINOPIO_HUB_ROS_NATS_TLS_SERVERS="tls://nats.example.invalid:14222"
export KINOPIOHUB_JS_NATS_WSS_SERVERS="wss://nats.example.invalid:55588"
export KINOPIOHUB_JS_ENTRYPOINT="/tmp/kinopiohub-js-check/node_modules/kinopio-hub/kinopio.mjs"
python scripts/js_sdk_check.py
```

`js_sdk_check.py` 是 JS-only：Python 侧只使用本项目的 `NatsAdapter` 和进程内 fake ROS 2 driver；KinopioHub.JS 负责验证 `ROS -> bridge -> NATS -> JS` 接收，以及 `JS -> NATS -> bridge -> ROS` 写回。如果 JS SDK 缺失，脚本会输出 JSON failure，包含必需变量和 npm 安装命令。Python 与 C++ SDK 共享同一 subject 和 JSON envelope 线协议，但不作为此检查的可执行目标。

## 排障

- 长期运行前先执行 `kinopio-hub-ros --config config.yaml --check`。
- 查看 `checks.ros` 判断是否缺少 `rclpy` 或 `rospy`。
- 查看 `checks.nats.probe_results` 区分 DNS、TCP、TLS、认证或协议错误。
- 运行日志中的 `Selected ROS topics` 用于确认 discovery 和过滤结果。
- 运行日志中的 `Connected NATS adapter` 用于确认实际连接的 NATS server。
- `Ignoring invalid NATS envelope` 表示 payload 不符合 envelope 契约。
- `Ignoring NATS envelope with mismatched subject/topic` 表示 subject 与 envelope topic 映射不一致。
- Service request 失败会返回 `kinopio.ros.service.v1`，其中 `ok: false`。

本仓库只提供命令和验证脚本，不安装 systemd service，不修改防火墙，不写入证书库，也不修改 ROS 环境文件。
