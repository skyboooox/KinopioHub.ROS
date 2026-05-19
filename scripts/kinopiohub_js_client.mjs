#!/usr/bin/env node

import { createInterface } from "node:readline";
import process from "node:process";

const DEFAULT_SERVERS = (process.env.KINOPIOHUB_JS_NATS_WSS_SERVERS ?? "")
  .split(",")
  .map((server) => server.trim())
  .filter(Boolean);
const EXPLICIT_JS_ENTRYPOINT = process.env.KINOPIOHUB_JS_ENTRYPOINT?.trim() ?? "";
const JS_PACKAGE = process.env.KINOPIOHUB_JS_PACKAGE?.trim() || "kinopio-hub";
const INSTALL_HINT = [
  "npm install --prefix /tmp/kinopiohub-js-check github:skyboooox/KinopioHub.JS",
  "export KINOPIOHUB_JS_ENTRYPOINT=/tmp/kinopiohub-js-check/node_modules/kinopio-hub/kinopio.mjs",
].join(" && ");

function emit(payload) {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
}

function getActiveServer(hub) {
  return hub.nats?.getServer?.() ?? null;
}

function moduleToConstructor(module) {
  const candidate = module.KinopioHub ?? module.default?.KinopioHub ?? module.default;
  if (typeof candidate !== "function") {
    throw new Error("KinopioHub.JS module does not export a KinopioHub constructor");
  }
  return candidate;
}

function entrypointToUrl(value) {
  return new URL(value, `file://${process.cwd()}/`).href;
}

async function loadKinopioHub() {
  if (EXPLICIT_JS_ENTRYPOINT) {
    try {
      return moduleToConstructor(await import(entrypointToUrl(EXPLICIT_JS_ENTRYPOINT)));
    } catch (error) {
      throw new Error(
        [
          `Failed to load KinopioHub.JS from KINOPIOHUB_JS_ENTRYPOINT=${EXPLICIT_JS_ENTRYPOINT}.`,
          `Original error: ${error?.message ?? String(error)}`,
          `Install hint: ${INSTALL_HINT}`,
        ].join(" ")
      );
    }
  }

  try {
    return moduleToConstructor(await import(JS_PACKAGE));
  } catch (error) {
    throw new Error(
      [
        `KinopioHub.JS SDK is not available as package "${JS_PACKAGE}".`,
        `Original error: ${error?.message ?? String(error)}`,
        `Install hint: ${INSTALL_HINT}`,
      ].join(" ")
    );
  }
}

function createHub(KinopioHub) {
  if (DEFAULT_SERVERS.length === 0) {
    throw new Error("KINOPIOHUB_JS_NATS_WSS_SERVERS is required for the JS SDK check");
  }
  return new KinopioHub({
    servers: DEFAULT_SERVERS,
    serverSelectionMode: "latency",
    noEcho: true,
    autoRetry: false,
    reconnectTimeout: 3000,
    timeout: 3000,
    healthReport: 0,
  });
}

async function main() {
  const KinopioHub = await loadKinopioHub();
  const hub = createHub(KinopioHub);
  const variable = hub.getScope("ros").getVariable("chatter");
  let shuttingDown = false;
  let commandChain = Promise.resolve();

  const shutdown = async (reason) => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    try {
      await hub.dispose();
    } catch (error) {
      emit({
        event: "shutdown-error",
        reason,
        errorType: error?.name ?? "Error",
        error: error?.message ?? String(error),
      });
    }
    emit({ event: "disposed", reason });
  };

  try {
    await hub.connected(20000);
    await variable.sub((data) => {
      emit({
        event: "received",
        activeServer: getActiveServer(hub),
        data,
      });
    });
    emit({
      event: "ready",
      activeServer: getActiveServer(hub),
    });
  } catch (error) {
    emit({
      event: "fatal",
      step: "startup",
      errorType: error?.name ?? "Error",
      error: error?.message ?? String(error),
    });
    process.exit(1);
    return;
  }

  const readline = createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });

  const handleCommand = async (line) => {
    if (!line.trim()) {
      return;
    }

    let command;
    try {
      command = JSON.parse(line);
    } catch (error) {
      emit({
        event: "error",
        step: "parse-command",
        errorType: error?.name ?? "Error",
        error: error?.message ?? String(error),
        raw: line,
      });
      return;
    }

    const action = command?.action;

    try {
      if (action === "publish") {
        await variable.pub(command.payload);
        emit({
          event: "published",
          label: command.label ?? null,
          activeServer: getActiveServer(hub),
        });
        return;
      }

      if (action === "reconnect") {
        await hub.reconnect();
        emit({
          event: "reconnected",
          activeServer: getActiveServer(hub),
        });
        return;
      }

      if (action === "status") {
        emit({
          event: "status",
          state: hub.state,
          activeServer: getActiveServer(hub),
        });
        return;
      }

      if (action === "dispose") {
        await shutdown("command");
        readline.close();
        process.exit(0);
        return;
      }

      emit({
        event: "error",
        step: "unknown-command",
        errorType: "CommandError",
        error: `Unsupported action: ${String(action)}`,
      });
    } catch (error) {
      emit({
        event: "error",
        step: action ?? "unknown",
        errorType: error?.name ?? "Error",
        error: error?.message ?? String(error),
      });
    }
  };

  readline.on("line", (line) => {
    commandChain = commandChain.then(() => handleCommand(line));
  });

  readline.on("close", async () => {
    await commandChain.catch(() => undefined);
    await shutdown("stdin-closed");
    process.exit(0);
  });

  process.on("SIGINT", async () => {
    readline.close();
  });
  process.on("SIGTERM", async () => {
    readline.close();
  });
}

main().catch((error) => {
  emit({
    event: "fatal",
    step: "top-level",
    errorType: error?.name ?? "Error",
    error: error?.message ?? String(error),
  });
  process.exit(1);
});
