ARG ROS_BASE_IMAGE=python:3.12-slim
FROM ${ROS_BASE_IMAGE}

SHELL ["/bin/bash", "-lc"]

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=5 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/kinopio-hub-ros-venv \
    PYTHONPATH=/workspace/src \
    PATH=/opt/kinopio-hub-ros-venv/bin:$PATH

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        python3 \
        python3-pip \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv --system-site-packages "${VIRTUAL_ENV}"

COPY pyproject.toml README.md LICENSE config.example.yaml /workspace/
RUN python -m pip install \
    "nats-py[nkeys]>=2.14,<3" \
    "PyYAML>=6.0.2,<7"

COPY docker /workspace/docker
COPY src /workspace/src
COPY scripts /workspace/scripts
COPY tests /workspace/tests

CMD ["bash", "-lc", "sleep infinity"]
