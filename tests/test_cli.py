import io
import json

import pytest

from kinopio_hub_ros.entry.cli import main
from kinopio_hub_ros.errors import AdapterError


def test_dry_run_prints_normalized_config_without_secret_values(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
bridge:
  id: ubuntu22-ros-bridge
nats:
  auth:
    mode: username_password
    username: bridge
    password_env: KINOPIO_HUB_NATS_PASSWORD
topics:
  mode: include
  patterns:
    - /chatter
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("KINOPIO_HUB_NATS_PASSWORD", "super-secret-value")

    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = main(
        ["--config", str(config_path), "--dry-run"],
        stdout=stdout,
        stderr=stderr,
    )

    payload = json.loads(stdout.getvalue())

    assert exit_code == 0
    assert stderr.getvalue() == ""
    assert payload["bridge"]["id"] == "ubuntu22-ros-bridge"
    assert payload["nats"]["auth"]["password_env"] == "KINOPIO_HUB_NATS_PASSWORD"
    assert "super-secret-value" not in stdout.getvalue()


def test_cli_reports_adapter_errors(monkeypatch):
    stdout = io.StringIO()
    stderr = io.StringIO()

    monkeypatch.setattr(
        "kinopio_hub_ros.entry.cli.run_application",
        lambda *args, **kwargs: (_ for _ in ()).throw(AdapterError("adapter exploded")),
    )

    exit_code = main(["--config", "config.yaml"], stdout=stdout, stderr=stderr)

    assert exit_code == 1
    assert stdout.getvalue() == ""
    assert "adapter exploded" in stderr.getvalue()


def test_check_mode_passes_through_and_prints_report(monkeypatch):
    stdout = io.StringIO()
    stderr = io.StringIO()
    captured = {}

    def fake_run_application(*, config_path, dry_run, check, stdout, logger):
        captured["config_path"] = str(config_path)
        captured["dry_run"] = dry_run
        captured["check"] = check
        captured["logger_name"] = logger.name
        stdout.write('{"ok": true}\n')
        return 0

    monkeypatch.setattr("kinopio_hub_ros.entry.cli.run_application", fake_run_application)

    exit_code = main(
        ["--config", "config.yaml", "--check", "--log-level", "DEBUG"],
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 0
    assert json.loads(stdout.getvalue())["ok"] is True
    assert captured["config_path"] == "config.yaml"
    assert captured["dry_run"] is False
    assert captured["check"] is True
    assert captured["logger_name"] == "kinopio_hub_ros"


def test_cli_rejects_dry_run_and_check_together():
    with pytest.raises(SystemExit) as excinfo:
        main(["--config", "config.yaml", "--dry-run", "--check"])

    assert excinfo.value.code == 2
