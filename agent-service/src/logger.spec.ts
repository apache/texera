/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { describe, expect, mock, test } from "bun:test";
import * as realPinoModule from "pino";
import * as realEnvModule from "./config/env";
import { createLogger, logger } from "./logger";

// Snapshot the real modules eagerly: `mock.module` swaps the live module
// namespaces process-wide, so these references are what the helper below
// restores once it is done.
const REAL_ENV = realEnvModule.env;
const REAL_LOG_LEVEL = REAL_ENV.TEXERA_SERVICE_LOG_LEVEL;
const REAL_PINO = { ...realPinoModule };

type CapturedPinoOptions = {
  level?: string;
  base?: unknown;
  transport?: { target?: string; options?: Record<string, unknown> };
};

type LoadedLogger = {
  /** The options object logger.ts handed to the pino factory. */
  options: CapturedPinoOptions;
  /** Bindings passed to `rootLogger.child(...)`, in call order. */
  childBindings: Record<string, unknown>[];
  module: typeof import("./logger");
  fakeRoot: any;
};

/**
 * Re-evaluate logger.ts against a stubbed `env` and a stubbed `pino` factory.
 *
 * logger.ts builds its root logger at module-evaluation time, so the LOG_PRETTY
 * branch is only observable by forcing a fresh evaluation. The `?tag` suffix
 * makes Bun build a new module instance instead of returning the cached one.
 */
async function loadLogger(envOverrides: Record<string, unknown>, tag: string): Promise<LoadedLogger> {
  const captured: CapturedPinoOptions[] = [];
  const childBindings: Record<string, unknown>[] = [];
  const fakeRoot: any = {
    child(bindings: Record<string, unknown>) {
      childBindings.push(bindings);
      return { ...fakeRoot, bindings: () => bindings };
    },
  };
  const fakePino: any = (options: CapturedPinoOptions) => {
    captured.push(options);
    fakeRoot.level = options.level;
    return fakeRoot;
  };

  mock.module("./config/env", () => ({ env: { ...REAL_ENV, ...envOverrides } }));
  mock.module("pino", () => ({ ...REAL_PINO, default: fakePino, pino: fakePino }));
  let loaded: typeof import("./logger");
  try {
    loaded = await import(`./logger.ts?${tag}`);
  } finally {
    mock.module("./config/env", () => ({ env: REAL_ENV }));
    mock.module("pino", () => REAL_PINO);
  }

  expect(captured).toHaveLength(1);
  return { options: captured[0]!, childBindings, module: loaded, fakeRoot };
}

describe("root logger construction", () => {
  test("uses plain JSON output with no base fields when LOG_PRETTY is off", async () => {
    const { options, module } = await loadLogger({ TEXERA_SERVICE_LOG_LEVEL: "warn", LOG_PRETTY: false }, "plain");

    expect(options.level).toBe("warn");
    // `base: undefined` is explicit so pino omits pid/hostname from every record.
    expect("base" in options).toBe(true);
    expect(options.base).toBeUndefined();
    // The non-pretty branch spreads `{}`, so the key must be absent entirely -
    // a `transport: undefined` would make pino throw.
    expect("transport" in options).toBe(false);
    // The module exports exactly the instance it built.
    expect(module.logger.level).toBe("warn");
  });

  test("attaches the pino-pretty transport when LOG_PRETTY is on", async () => {
    const { options, module, childBindings, fakeRoot } = await loadLogger(
      { TEXERA_SERVICE_LOG_LEVEL: "debug", LOG_PRETTY: true },
      "pretty"
    );

    expect(options.level).toBe("debug");
    expect(options.base).toBeUndefined();
    expect(options.transport?.target).toBe("pino-pretty");
    expect(options.transport?.options).toEqual({
      colorize: true,
      translateTime: "HH:MM:ss.l",
      ignore: "pid,hostname",
    });
    expect(module.logger).toBe(fakeRoot);

    // createLogger delegates to the root logger's child(), regardless of transport.
    module.createLogger("Ingest", { agentId: "agent-1" });
    expect(childBindings).toEqual([{ module: "Ingest", agentId: "agent-1" }]);
  });
});

describe("exported root logger", () => {
  test("is wired to the configured level rather than a hardcoded one", () => {
    expect(logger.level).toBe(REAL_LOG_LEVEL);
  });

  test("carries no bindings of its own", () => {
    expect(logger.bindings()).toEqual({});
  });
});

describe("createLogger", () => {
  test("returns a child logger tagged with the module name", () => {
    const child = createLogger("Server");

    expect(child).not.toBe(logger);
    expect(child.bindings()).toEqual({ module: "Server" });
    expect(child.level).toBe(logger.level);
  });

  test("merges extra bindings alongside the module name", () => {
    const child = createLogger("WS", { agentId: "agent-1", userId: 7 });

    expect(child.bindings()).toEqual({ module: "WS", agentId: "agent-1", userId: 7 });
  });

  test("lets explicit bindings override the module name", () => {
    // `{ module, ...bindings }` means a `module` key inside bindings wins.
    const child = createLogger("Original", { module: "Override" });

    expect(child.bindings()).toEqual({ module: "Override" });
  });

  test("does not leak child bindings back onto the root logger", () => {
    createLogger("Transient", { agentId: "agent-2" });

    expect(logger.bindings()).toEqual({});
  });

  test("gives sibling children independent bindings", () => {
    const first = createLogger("A", { agentId: "a" });
    const second = createLogger("B");

    expect(first.bindings()).toEqual({ module: "A", agentId: "a" });
    expect(second.bindings()).toEqual({ module: "B" });
  });

  test("honours a level threshold set on the child alone", () => {
    const child = createLogger("Levels");
    child.level = "warn";

    expect(child.isLevelEnabled("error")).toBe(true);
    expect(child.isLevelEnabled("warn")).toBe(true);
    expect(child.isLevelEnabled("info")).toBe(false);
    expect(child.isLevelEnabled("debug")).toBe(false);
    // Changing the child must not disturb the shared root logger.
    expect(logger.level).toBe(REAL_LOG_LEVEL);
  });
});
