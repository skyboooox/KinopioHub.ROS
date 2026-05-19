#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

cleanup() {
  rm -rf "${ROOT_DIR}/.pytest_cache"
  find "${ROOT_DIR}/src" "${ROOT_DIR}/tests" "${ROOT_DIR}/scripts" \
    -type d -name '__pycache__' \
    -prune -exec rm -rf {} + >/dev/null 2>&1 || true
}

trap cleanup EXIT

cd "${ROOT_DIR}"
export PYTHONDONTWRITEBYTECODE=1

"${PYTHON_BIN}" -m pytest "$@"
