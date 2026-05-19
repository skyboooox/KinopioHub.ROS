"""CLI entrypoint for the bridge."""

import argparse
import logging
import sys

from pathlib import Path

from kinopio_hub_ros import __version__
from kinopio_hub_ros.core.app import run_application
from kinopio_hub_ros.errors import AdapterError, ConfigError, RuntimeUnavailableError


def build_parser():
    parser = argparse.ArgumentParser(
        prog="kinopio-hub-ros",
        description="Validate and run the KinopioHub ROS message bridge.",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the configuration and print the normalized result.",
    )
    mode_group.add_argument(
        "--check",
        action="store_true",
        help="Run deployment preflight checks for config, ROS runtime availability, and NATS connectivity.",
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Path to the YAML configuration file.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"),
        help="Set the stderr log level for runtime messages.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s {0}".format(__version__),
    )
    return parser


def main(argv=None, stdout=None, stderr=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    logger = _configure_logging(args.log_level, stderr)

    try:
        return run_application(
            config_path=args.config,
            dry_run=args.dry_run,
            check=args.check,
            stdout=stdout,
            logger=logger,
        )
    except ConfigError as exc:
        stderr.write("Configuration error: {0}\n".format(exc))
        return 2
    except AdapterError as exc:
        stderr.write("{0}\n".format(exc))
        return 1
    except RuntimeUnavailableError as exc:
        stderr.write("{0}\n".format(exc))
        return 1


def _configure_logging(level_name, stream):
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        stream=stream,
        force=True,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    return logging.getLogger("kinopio_hub_ros")
