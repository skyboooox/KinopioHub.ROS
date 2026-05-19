#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$(mktemp -d /tmp/kinopio-hub-ros-check.XXXXXX)"
KEEP_VENV=0
SKIP_DOCKER=0

cleanup() {
  if [[ "${KEEP_VENV}" -eq 0 ]]; then
    rm -rf "${VENV_DIR}"
  fi
  rm -rf "${ROOT_DIR}/.pytest_cache"
  find "${ROOT_DIR}/src" "${ROOT_DIR}/tests" "${ROOT_DIR}/scripts" \
    -type d \( -name '__pycache__' -o -name '*.egg-info' \) \
    -prune -exec rm -rf {} + >/dev/null 2>&1 || true
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-venv)
      KEEP_VENV=1
      shift
      ;;
    --skip-docker)
      SKIP_DOCKER=1
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 2
      ;;
  esac
done

trap cleanup EXIT

cd "${ROOT_DIR}"
export PYTHONDONTWRITEBYTECODE=1

echo "[1/4] create isolated virtualenv: ${VENV_DIR}"
python3 -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"

echo "[2/4] install package and test extras"
python -m pip install --upgrade pip
python -m pip install -e ".[test]"

echo "[3/4] run unit tests and dry-run validation"
./scripts/pytest_clean.sh tests
kinopio-hub-ros --config config.example.yaml --dry-run >/tmp/kinopio-hub-ros-dry-run.json
kinopio-hub-ros --config examples/config.minimal.yaml --dry-run >/tmp/kinopio-hub-ros-minimal-dry-run.json

if [[ "${SKIP_DOCKER}" -eq 0 ]]; then
  echo "[4/4] validate docker compose skeleton"
  docker compose -f docker/compose.check.yaml config >/tmp/kinopio-hub-ros-compose.config.yaml
else
  echo "[4/4] skip docker compose validation (--skip-docker)"
fi

echo "check completed"
