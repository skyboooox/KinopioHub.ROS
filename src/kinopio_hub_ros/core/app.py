"""Core application flow for the bridge runtime."""

import asyncio
import json
import logging
import signal

from kinopio_hub_ros.business.configuration import load_config
from kinopio_hub_ros.business.preflight import run_preflight_checks
from kinopio_hub_ros.core.bridge_runtime import BridgeRuntime


def run_application(config_path, dry_run, check, stdout, logger=None):
    config = load_config(config_path)
    logger = logger or logging.getLogger(__name__)

    if dry_run:
        stdout.write(json.dumps(config.to_public_dict(), indent=2, sort_keys=True))
        stdout.write("\n")
        return 0

    if check:
        report = asyncio.run(run_preflight_checks(config))
        stdout.write(json.dumps(report, indent=2, sort_keys=True))
        stdout.write("\n")
        return 0 if report["ok"] else 1

    return asyncio.run(_run_runtime(config, logger=logger))


async def _run_runtime(config, *, logger):
    runtime = BridgeRuntime(config, logger=logger)
    loop = asyncio.get_running_loop()
    registered_signals = []

    def request_stop(signal_name):
        logger.info("Received %s; stopping bridge runtime.", signal_name)
        runtime.stop()

    for candidate in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(candidate, request_stop, candidate.name)
            registered_signals.append(candidate)
        except (NotImplementedError, RuntimeError, ValueError):
            continue

    try:
        await runtime.run_forever()
        return 0
    finally:
        for candidate in registered_signals:
            loop.remove_signal_handler(candidate)
