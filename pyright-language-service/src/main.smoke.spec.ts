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

// End-to-end smoke test for the Pyright LSP bridge. Boots `src/main.ts` as a
// real subprocess — exactly how the service runs in production — and drives it
// over a WebSocket, so this covers the whole chain that CI otherwise never
// exercises: express opens the port, the "upgrade" handler matches the client
// path, `pyright-langserver` is spawned per connection, and JSON-RPC is
// forwarded in both directions.
//
// Nothing in src/ is imported. `main.ts` runs its work at import time and
// exports nothing, and `runLanguageServer` returns no handle to the http/ws
// servers, so there is no in-process seam to test against; the subprocess IS
// the seam. That also keeps this change additive — no source edits.
//
// Messages on the wire are bare JSON, not Content-Length framed: WebSocket is
// already message-oriented, so vscode-ws-jsonrpc writes one JSON.stringify'd
// message per frame with no LSP headers.

import { after, afterEach, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// The port and path are the deployed wire contract, not values read back from
// the service's own config, so a drift in src/config.json fails this test
// instead of silently changing the contract. Both are pinned by the k8s
// gateway route (bin/k8s/templates/base/gateway/gateway-routes.yaml) and match
// main.ts's default port.
const PORT = 3000;
const CLIENT_PATH = "/python-language-server";

const SERVICE_ROOT = path.resolve(fileURLToPath(import.meta.url), "..", "..");
const ENTRY_POINT = path.join(SERVICE_ROOT, "src", "main.ts");
// main.ts hardcodes this path (it does NOT read config.json's languageServerDir).
const PYRIGHT_LANGSERVER = path.join(SERVICE_ROOT, "node_modules", "pyright", "dist", "pyright-langserver.js");

// Pyright cold start has to load typeshed before it publishes anything, which
// is comfortably the slowest step on a cold CI runner.
const BOOT_TIMEOUT_MS = 30_000;
const DIAGNOSTICS_TIMEOUT_MS = 60_000;
// Window for letting late messages arrive before asserting one did NOT.
const SETTLE_MS = 2_000;
// How long an unknown-path upgrade is watched before calling it silent. A real
// handshake completes in single-digit milliseconds, so this is generous.
const UNKNOWN_PATH_WINDOW_MS = 3_000;

// `undefined_variable_for_smoke_test` is never bound, so pyright reports
// reportUndefinedVariable — an error under its default ruleset.
const PYTHON_SOURCE = "result = undefined_variable_for_smoke_test\n";
// Every name is bound, so this must analyse without a single diagnostic.
const CLEAN_SOURCE = [
  "def add(left: int, right: int) -> int:",
  "    return left + right",
  "",
  "total = add(1, 2)",
  "",
].join("\n");
// Astral-plane emoji on line 0, undefined unicode identifier on line 1.
const UNICODE_SOURCE = ['mensaje = "héllo wörld 🐍"', "resultado = variable_ñö_definida", ""].join("\n");

let child: ChildProcess | undefined;
let workspaceDir: string;
let documentUri: string;
const openSockets: WebSocket[] = [];

// server-commons.ts logs every forwarded message to stdout, so these pipes must
// be drained or the child eventually blocks writing to a full pipe. We keep a
// bounded tail purely to attach to a failure message.
let childLog = "";
function captureChildOutput(chunk: Buffer): void {
  childLog = (childLog + chunk.toString()).slice(-16_000);
}

const settle = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Fails fast if something already holds the port.
 *
 * This matters more than usual here: runLanguageServer installs a process-wide
 * `uncaughtException` handler that logs and swallows, so an EADDRINUSE from
 * app.listen() does NOT kill the child. It would stay alive but not listening,
 * the readiness poll below would connect to the *other* process, and the test
 * would silently talk to the wrong server.
 */
function assertPortFree(port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.once("error", (err: NodeJS.ErrnoException) => {
      reject(
        new Error(
          err.code === "EADDRINUSE"
            ? `port ${port} is already in use — stop whatever holds it (a stale service, or bin/local-dev.sh) before running this test`
            : `could not probe port ${port}: ${err.message}`
        )
      );
    });
    probe.once("listening", () => probe.close(() => resolve()));
    probe.listen(port, "127.0.0.1");
  });
}

/** Polls until the port accepts a TCP connection. The service prints no startup banner, so there is no log line to wait on. */
async function waitForPort(port: number, deadlineMs: number): Promise<void> {
  const deadline = Date.now() + deadlineMs;
  let lastError = "connection never attempted";
  while (Date.now() < deadline) {
    // Fail fast rather than burning the full timeout if the child died on boot.
    if (child?.exitCode !== null && child?.exitCode !== undefined) {
      throw new Error(
        `service exited with code ${child.exitCode} before listening.\n--- child output ---\n${childLog}`
      );
    }
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = net.connect({ port, host: "127.0.0.1" });
        socket.once("connect", () => socket.end(() => resolve()));
        socket.once("error", reject);
      });
      return;
    } catch (err) {
      lastError = (err as Error).message;
      await settle(100);
    }
  }
  throw new Error(
    `service did not listen on ${port} within ${deadlineMs}ms (${lastError}).\n--- child output ---\n${childLog}`
  );
}

/**
 * Kills the bridge and the pyright-langserver it spawned per connection.
 *
 * A plain child.kill() can orphan that grandchild, leaving a process attached
 * to the port. On POSIX the child is spawned detached so it leads its own
 * process group and the negative pid kills the group; on Windows taskkill /T
 * walks the tree instead.
 */
function killTreeSync(target: ChildProcess): void {
  if (target.pid === undefined || target.exitCode !== null || target.signalCode !== null) return;
  if (process.platform === "win32") {
    spawnSync("taskkill", ["/pid", String(target.pid), "/T", "/F"], { stdio: "ignore" });
    return;
  }
  try {
    process.kill(-target.pid, "SIGKILL");
  } catch {
    try {
      target.kill("SIGKILL");
    } catch {
      // already gone
    }
  }
}

/**
 * Removes the temp workspace, retrying while Windows drains file handles.
 *
 * Pyright's watchers keep the directory locked for a few seconds after their
 * processes exit — verified transient, it deletes cleanly once they drain.
 * rmSync's own maxRetries/retryDelay do not cover this EPERM, hence the
 * explicit loop, which also yields to the event loop between attempts. POSIX
 * unlinks regardless, so this costs nothing on the CI runner.
 */
async function removeWorkspace(directory: string): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      fs.rmSync(directory, { recursive: true, force: true });
      return;
    } catch {
      await settle(250);
    }
  }
  // A stray directory under the OS temp dir must not fail an otherwise green
  // run; the OS reclaims it.
  console.warn(`could not remove temp workspace ${directory}`);
}

// Backstop for the paths a test hook cannot cover — an uncaught error in the
// runner itself, or the process being interrupted. Must stay synchronous.
process.on("exit", () => {
  if (child) killTreeSync(child);
});

interface Collector {
  waitFor(predicate: (message: any) => boolean, timeoutMs?: number): Promise<any>;
  /** Everything buffered so far that matches — for asserting a message did NOT arrive. */
  matches(predicate: (message: any) => boolean): any[];
}

/**
 * Buffers every frame from the moment the socket is created — before `open` —
 * so nothing sent early is missed, then resolves waiters out of that buffer.
 *
 * It also auto-answers server-to-client requests (a frame carrying both `id`
 * and `method`, e.g. client/registerCapability) with a null result. Pyright
 * awaits those responses, and an unanswered one can stall analysis before any
 * diagnostic is ever published.
 */
function collect(ws: WebSocket): Collector {
  const buffer: any[] = [];
  const waiters: { predicate: (message: any) => boolean; resolve: (message: any) => void }[] = [];
  ws.addEventListener("message", event => {
    let data: any;
    try {
      data = JSON.parse(event.data as string);
    } catch {
      return;
    }
    if (data.id !== undefined && data.method !== undefined) {
      ws.send(JSON.stringify({ jsonrpc: "2.0", id: data.id, result: null }));
    }
    buffer.push(data);
    const index = waiters.findIndex(waiter => waiter.predicate(data));
    if (index >= 0) {
      waiters[index].resolve(data);
      waiters.splice(index, 1);
    }
  });
  return {
    waitFor(predicate, timeoutMs = 5_000) {
      const found = buffer.find(predicate);
      if (found) return Promise.resolve(found);
      return new Promise((resolve, reject) => {
        let timer: ReturnType<typeof setTimeout>;
        const waiter = {
          predicate,
          resolve: (message: any) => {
            clearTimeout(timer);
            resolve(message);
          },
        };
        waiters.push(waiter);
        timer = setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index >= 0) waiters.splice(index, 1);
          reject(new Error(`timed out after ${timeoutMs}ms waiting for a matching LSP message`));
        }, timeoutMs);
      });
    },
    matches(predicate) {
      return buffer.filter(predicate);
    },
  };
}

function connect(clientPath: string = CLIENT_PATH): { ws: WebSocket; messages: Collector } {
  const ws = new WebSocket(`ws://localhost:${PORT}${clientPath}`);
  openSockets.push(ws);
  return { ws, messages: collect(ws) };
}

function waitOpen(ws: WebSocket): Promise<void> {
  if (ws.readyState === WebSocket.OPEN) return Promise.resolve();
  return new Promise((resolve, reject) => {
    ws.addEventListener("open", () => resolve(), { once: true });
    ws.addEventListener("error", () => reject(new Error("WS connection error")), { once: true });
  });
}

/** Pyright re-encodes URIs, so compare resolved paths rather than raw strings (Windows drive letters and casing differ). */
function isSameDocument(a: string, b: string): boolean {
  if (a === b) return true;
  try {
    const left = path.resolve(fileURLToPath(a));
    const right = path.resolve(fileURLToPath(b));
    return process.platform === "win32" ? left.toLowerCase() === right.toLowerCase() : left === right;
  } catch {
    return false;
  }
}

/** Writes a Python file into the temp workspace and returns its document URI. */
function writeDocument(fileName: string, source: string): string {
  const documentPath = path.join(workspaceDir, fileName);
  fs.writeFileSync(documentPath, source, "utf-8");
  return pathToFileURL(documentPath).href;
}

/**
 * Runs the initialize/initialized handshake and resolves once the server has
 * replied. Every session needs this before pyright will answer anything, so
 * both the positive and negative cases share it.
 */
async function initializeSession(ws: WebSocket, messages: Collector, requestId: number): Promise<any> {
  const workspaceUri = pathToFileURL(workspaceDir).href;
  ws.send(
    JSON.stringify({
      jsonrpc: "2.0",
      id: requestId,
      method: "initialize",
      params: {
        processId: process.pid,
        clientInfo: { name: "texera-pyright-smoke-test", version: "0.0.1" },
        locale: "en",
        rootUri: workspaceUri,
        workspaceFolders: [{ uri: workspaceUri, name: "smoke" }],
        // Deliberately minimal. `workspace.configuration: false` keeps pyright
        // from issuing workspace/configuration requests that this client would
        // have to answer with a real settings array, and workDoneProgress:
        // false suppresses progress-token creation.
        capabilities: {
          textDocument: {
            synchronization: { dynamicRegistration: false, didSave: false, willSave: false },
            publishDiagnostics: { relatedInformation: true },
          },
          workspace: { workspaceFolders: true, configuration: false },
          window: { workDoneProgress: false },
        },
      },
    })
  );

  const initializeResult = await messages.waitFor(
    message => message.id === requestId && message.result !== undefined,
    BOOT_TIMEOUT_MS
  );
  ws.send(JSON.stringify({ jsonrpc: "2.0", method: "initialized", params: {} }));
  return initializeResult;
}

function didOpen(ws: WebSocket, uri: string, source: string): void {
  ws.send(
    JSON.stringify({
      jsonrpc: "2.0",
      method: "textDocument/didOpen",
      params: { textDocument: { uri, languageId: "python", version: 1, text: source } },
    })
  );
}

before(async () => {
  // Turns an otherwise silent hang into an actionable error: with the bundle
  // missing, createServerProcess never starts a server, the bridge accepts the
  // socket and forwards nothing, and the test just times out.
  assert.ok(
    fs.existsSync(PYRIGHT_LANGSERVER),
    `missing ${PYRIGHT_LANGSERVER} — run \`yarn install\` in pyright-language-service first`
  );

  await assertPortFree(PORT);

  // A real workspace root, with the file on disk as well as sent via didOpen,
  // so pyright analyses it as a normal workspace file. Pyright needs no Python
  // interpreter for this — undefined-variable detection comes from its own
  // binder, not from an installed environment.
  workspaceDir = fs.mkdtempSync(path.join(os.tmpdir(), "texera-pyright-smoke-"));
  documentUri = writeDocument("smoke_undefined_variable.py", PYTHON_SOURCE);

  // Booted through ts-node's ESM loader, matching the `start` script exactly —
  // there is no build output to run instead (allowImportingTsExtensions forces
  // --noEmit).
  //
  // Node's own TypeScript support cannot load this source, so it is not an
  // option here. server-commons.ts imports the type-only export `IWebSocket`
  // in a regular value import clause; Node's transpiler does no type
  // resolution, so it emits a runtime import for a binding that does not exist
  // and the child dies with "does not provide an export named 'IWebSocket'".
  // (`server-commons.ts` also exports an `enum`, which strip-only mode rejects
  // separately.) ts-node type-checks and elides type-only imports, so it is
  // the only way to run this package unmodified.
  child = spawn(process.execPath, ["--loader", "ts-node/esm", ENTRY_POINT], {
    cwd: SERVICE_ROOT,
    // Own process group on POSIX so killTreeSync can take out the spawned
    // pyright-langserver too. Not on Windows, where it would open a console.
    detached: process.platform !== "win32",
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout?.on("data", captureChildOutput);
  child.stderr?.on("data", captureChildOutput);

  await waitForPort(PORT, BOOT_TIMEOUT_MS);
});

// Each connection makes the bridge spawn its own pyright-langserver, so
// sockets are closed between tests rather than only at the end of the file —
// otherwise those processes accumulate for the whole run.
afterEach(() => {
  while (openSockets.length) {
    try {
      openSockets.pop()?.close();
    } catch {
      // ignore
    }
  }
});

after(async () => {
  while (openSockets.length) {
    try {
      openSockets.pop()?.close();
    } catch {
      // ignore
    }
  }

  if (child) {
    // Resolve immediately if it is already gone — `once("exit")` on an
    // already-exited child never fires, which would hang teardown.
    const alreadyExited = child.exitCode !== null || child.signalCode !== null;
    const exited = alreadyExited
      ? Promise.resolve()
      : new Promise<void>(resolve => child?.once("exit", () => resolve()));
    killTreeSync(child);
    await exited;
    child = undefined;
  }

  if (workspaceDir) await removeWorkspace(workspaceDir);
});

describe(`WS ${CLIENT_PATH} (pyright bridge)`, () => {
  it(
    "publishes diagnostics for an opened document containing an undefined variable",
    { timeout: DIAGNOSTICS_TIMEOUT_MS + 30_000 },
    async () => {
      const { ws, messages } = connect();
      await waitOpen(ws);

      // Proves the bridge forwards server -> client as well as client -> server.
      const initializeResult = await initializeSession(ws, messages, 1);
      assert.ok(initializeResult.result.capabilities, "initialize result should advertise server capabilities");

      didOpen(ws, documentUri, PYTHON_SOURCE);

      // Pyright often publishes an empty array for a document first and fills
      // it in once analysis completes, so require a non-empty payload rather
      // than taking the first publishDiagnostics that arrives.
      const published = await messages.waitFor(
        message =>
          message.method === "textDocument/publishDiagnostics" &&
          isSameDocument(message.params?.uri ?? "", documentUri) &&
          (message.params?.diagnostics?.length ?? 0) > 0,
        DIAGNOSTICS_TIMEOUT_MS
      );

      const diagnostics = published.params.diagnostics as {
        code?: string | number;
        severity?: number;
        message: string;
      }[];

      // Assert on severity and rule code, never on wording — pyright rephrases
      // messages between versions, but `reportUndefinedVariable` is a public,
      // documented configuration rule name.
      assert.ok(
        diagnostics.some(diagnostic => diagnostic.severity === 1),
        `expected at least one Error-severity diagnostic, got ${JSON.stringify(diagnostics)}`
      );
      assert.ok(
        diagnostics.some(diagnostic => String(diagnostic.code) === "reportUndefinedVariable"),
        `expected a reportUndefinedVariable diagnostic, got codes ${JSON.stringify(diagnostics.map(d => d.code))}`
      );
    }
  );

  it("reports no diagnostics for a valid document", { timeout: DIAGNOSTICS_TIMEOUT_MS + 30_000 }, async () => {
    const uri = writeDocument("smoke_clean.py", CLEAN_SOURCE);
    const { ws, messages } = connect();
    await waitOpen(ws);
    await initializeSession(ws, messages, 10);
    didOpen(ws, uri, CLEAN_SOURCE);

    await messages.waitFor(
      message =>
        message.method === "textDocument/publishDiagnostics" &&
        isSameDocument(message.params?.uri ?? "", uri) &&
        (message.params?.diagnostics?.length ?? 0) === 0,
      DIAGNOSTICS_TIMEOUT_MS
    );

    // An empty publish alone proves little, since pyright emits one before
    // analysis finishes. Wait out a settle window and require that no
    // non-empty publish ever lands for this document.
    await settle(SETTLE_MS);
    const spurious = messages.matches(
      message =>
        message.method === "textDocument/publishDiagnostics" &&
        isSameDocument(message.params?.uri ?? "", uri) &&
        (message.params?.diagnostics?.length ?? 0) > 0
    );
    assert.equal(spurious.length, 0, `clean document should produce no diagnostics, got ${JSON.stringify(spurious)}`);
  });

  it("reports no diagnostics for an empty document", { timeout: DIAGNOSTICS_TIMEOUT_MS + 30_000 }, async () => {
    const uri = writeDocument("smoke_empty.py", "");
    const { ws, messages } = connect();
    await waitOpen(ws);
    await initializeSession(ws, messages, 20);
    didOpen(ws, uri, "");

    const published = await messages.waitFor(
      message => message.method === "textDocument/publishDiagnostics" && isSameDocument(message.params?.uri ?? "", uri),
      DIAGNOSTICS_TIMEOUT_MS
    );
    assert.deepEqual(published.params.diagnostics, [], "an empty file should analyse cleanly");
  });

  it(
    "reports the undefined variable in a document containing unicode",
    { timeout: DIAGNOSTICS_TIMEOUT_MS + 30_000 },
    async () => {
      const uri = writeDocument("smoke_unicode.py", UNICODE_SOURCE);
      const { ws, messages } = connect();
      await waitOpen(ws);
      await initializeSession(ws, messages, 30);
      didOpen(ws, uri, UNICODE_SOURCE);

      const published = await messages.waitFor(
        message =>
          message.method === "textDocument/publishDiagnostics" &&
          isSameDocument(message.params?.uri ?? "", uri) &&
          (message.params?.diagnostics?.length ?? 0) > 0,
        DIAGNOSTICS_TIMEOUT_MS
      );

      const diagnostics = published.params.diagnostics as {
        code?: string | number;
        range: { start: { line: number } };
      }[];
      const undefinedVariable = diagnostics.find(diagnostic => String(diagnostic.code) === "reportUndefinedVariable");
      assert.ok(
        undefinedVariable,
        `expected reportUndefinedVariable, got codes ${JSON.stringify(diagnostics.map(d => d.code))}`
      );
      // Line 0 holds an astral-plane emoji (two UTF-16 code units). The error is
      // on line 1, so a mangled offset calculation shows up as a wrong line.
      assert.equal(undefinedVariable.range.start.line, 1, "diagnostic should be positioned on the second line");
    }
  );

  it("answers an unknown LSP method with a MethodNotFound error", { timeout: BOOT_TIMEOUT_MS + 30_000 }, async () => {
    const { ws, messages } = connect();
    await waitOpen(ws);
    await initializeSession(ws, messages, 40);

    ws.send(JSON.stringify({ jsonrpc: "2.0", id: 41, method: "textDocument/thisMethodDoesNotExist", params: {} }));

    const response = await messages.waitFor(message => message.id === 41, BOOT_TIMEOUT_MS);
    assert.ok(response.error, `expected a JSON-RPC error, got ${JSON.stringify(response)}`);
    // -32601 is the JSON-RPC MethodNotFound constant — a stable protocol
    // value, unlike the human-readable message beside it.
    assert.equal(response.error.code, -32601);
  });

  it(
    "survives a malformed non-JSON frame and still serves new sessions",
    { timeout: BOOT_TIMEOUT_MS + 30_000 },
    async () => {
      const first = connect();
      await waitOpen(first.ws);
      await initializeSession(first.ws, first.messages, 50);

      first.ws.send("this is not json");
      await settle(SETTLE_MS);

      // The bridge must not take the whole service down with the session: a
      // parse failure inside one connection has to stay inside it.
      assert.equal(child?.exitCode, null, "service process should still be running after a malformed frame");

      const second = connect();
      await waitOpen(second.ws);
      const initializeResult = await initializeSession(second.ws, second.messages, 51);
      assert.ok(initializeResult.result.capabilities, "a new session should still initialize");
    }
  );

  it("does not complete a WebSocket handshake on an unknown path", { timeout: BOOT_TIMEOUT_MS + 30_000 }, async () => {
    const { ws } = connect("/not-the-language-server");

    const outcome = await Promise.race([
      new Promise<string>(resolve => ws.addEventListener("open", () => resolve("open"), { once: true })),
      new Promise<string>(resolve => ws.addEventListener("error", () => resolve("error"), { once: true })),
      settle(UNKNOWN_PATH_WINDOW_MS).then(() => "silent"),
    ]);

    // Characterization, not endorsement. upgradeWsServer only acts when the
    // path matches and has no else branch, so a bad path gets no 404 and no
    // socket.destroy() — the client just hangs and the server-side socket
    // leaks. If that is ever fixed, this assertion should flip to "error".
    assert.equal(
      outcome,
      "silent",
      "unknown path currently neither upgrades nor rejects; update this test if that behaviour is fixed"
    );
  });

  it("remains available after an unknown-path upgrade attempt", { timeout: BOOT_TIMEOUT_MS + 30_000 }, async () => {
    connect("/not-the-language-server");
    await settle(SETTLE_MS);

    const { ws, messages } = connect();
    await waitOpen(ws);
    const initializeResult = await initializeSession(ws, messages, 60);
    assert.ok(initializeResult.result.capabilities, "a stranded upgrade attempt must not block real clients");
  });
});
