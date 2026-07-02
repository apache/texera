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

// Exercises the /agents/:id/react WebSocket protocol end to end: the snapshot
// sent on connect, the status lifecycle frames, the stop command, the prompt
// request (with a stubbed run), and the error paths. These drive the real
// socket via app.listen + a WebSocket client, since app.handle() does not
// perform WS upgrades.

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { buildApp, _resetAgentStoreForTests, _getAgentForTests } from "./server";
import { env } from "./config/env";

const API = env.API_PREFIX;

let app: ReturnType<typeof buildApp>;
let port: number;
const openSockets: WebSocket[] = [];

function mintTestToken(): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const payload = Buffer.from(
    JSON.stringify({
      sub: "tester",
      userId: 1,
      email: "tester@example.com",
      role: "REGULAR",
      exp: Math.floor(Date.now() / 1000) + 3600,
    })
  ).toString("base64url");
  return `${header}.${payload}.test-signature`;
}

const TOKEN = mintTestToken();

async function createAgent(): Promise<string> {
  const res = await app.handle(
    new Request(`http://localhost${API}/agents`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${TOKEN}` },
      body: JSON.stringify({ modelType: "test-model" }),
    })
  );
  const body = (await res.json()) as { id: string };
  return body.id;
}

interface Collector {
  waitFor(predicate: (m: any) => boolean, timeoutMs?: number): Promise<any>;
}

// Attaches a message listener immediately (before `open`) so no frame — not even
// the snapshot the server sends on connect — is missed, then resolves waiters
// from a buffer.
function collect(ws: WebSocket): Collector {
  const buffer: any[] = [];
  const waiters: { predicate: (m: any) => boolean; resolve: (m: any) => void }[] = [];
  ws.addEventListener("message", ev => {
    let data: any;
    try {
      data = JSON.parse(ev.data as string);
    } catch {
      return;
    }
    buffer.push(data);
    const i = waiters.findIndex(w => w.predicate(data));
    if (i >= 0) {
      waiters[i].resolve(data);
      waiters.splice(i, 1);
    }
  });
  return {
    waitFor(predicate, timeoutMs = 2000) {
      const found = buffer.find(predicate);
      if (found) return Promise.resolve(found);
      return new Promise((resolve, reject) => {
        let timer: ReturnType<typeof setTimeout>;
        const w = {
          predicate,
          resolve: (m: any) => {
            clearTimeout(timer);
            resolve(m);
          },
        };
        waiters.push(w);
        timer = setTimeout(() => {
          const idx = waiters.indexOf(w);
          if (idx >= 0) {
            waiters.splice(idx, 1);
            reject(new Error("timed out waiting for a matching WS frame"));
          }
        }, timeoutMs);
      });
    },
  };
}

function connect(agentId: string): { ws: WebSocket; messages: Collector } {
  const ws = new WebSocket(`ws://localhost:${port}${API}/agents/${agentId}/react`);
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

beforeAll(() => {
  app = buildApp();
  app.listen(0);
  port = app.server?.port ?? 0;
});

afterAll(() => {
  app.stop();
});

beforeEach(() => {
  _resetAgentStoreForTests();
});

afterEach(() => {
  while (openSockets.length) {
    try {
      openSockets.pop()?.close();
    } catch {
      // ignore
    }
  }
});

describe(`WS ${API}/agents/:id/react`, () => {
  test("sends a results-free snapshot frame on connect", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);

    const snapshot = await messages.waitFor(m => m.type === "WsServerSnapshotEvent");
    expect(snapshot.state).toBe("AVAILABLE");
    expect(Array.isArray(snapshot.steps)).toBe(true);
    expect(typeof snapshot.headId).toBe("string");
    // Results are pulled on demand, never pushed on the snapshot.
    expect("operatorResults" in snapshot).toBe(false);
  });

  test("errors and closes when connecting to an unknown agent", async () => {
    const { messages } = connect("agent-does-not-exist");
    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Agent not found");
  });

  test("a stop command broadcasts a STOPPING status frame", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientStopCommand" }));

    const status = await messages.waitFor(m => m.type === "WsServerStatusEvent");
    expect(status.state).toBe("STOPPING");
  });

  test("a prompt with empty content yields an error frame", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientPromptCommand", content: "" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Message content is required");
  });

  test("a malformed (non-JSON) frame yields an error frame", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send("this is not json");

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Invalid message format");
  });

  test("an unknown message type yields an error frame", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "bogus" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Unknown message type: bogus");
  });

  test("a prompt run streams GENERATING -> step -> resting status (no result frames)", async () => {
    const id = await createAgent();

    // Stub the agent's run so no live LLM is needed: emit one ending step via
    // the registered step callback, then return.
    const agent = _getAgentForTests(id)!;
    (agent as any).sendMessage = async function (this: any) {
      this.stepCallback?.({
        id: "step-1",
        parentId: "init",
        messageId: "m1",
        stepId: 1,
        timestamp: 0,
        role: "agent",
        content: "done",
        isBegin: true,
        isEnd: true,
      });
      return {
        response: "done",
        messages: [],
        usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
        stopped: false,
      };
    };
    // The server re-broadcasts the final step (with isEnd) after the run.
    (agent as any).getReActSteps = () => [
      {
        id: "step-1",
        parentId: "init",
        messageId: "m1",
        stepId: 1,
        timestamp: 0,
        role: "agent",
        content: "done",
        isBegin: true,
        isEnd: true,
      },
    ];

    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientPromptCommand", content: "hello" }));

    const generating = await messages.waitFor(m => m.type === "WsServerStatusEvent" && m.state === "GENERATING");
    expect(generating.state).toBe("GENERATING");

    const step = await messages.waitFor(m => m.type === "WsServerStepEvent");
    expect(step.step.content).toBe("done");
    expect("operatorResults" in step).toBe(false);

    const resting = await messages.waitFor(m => m.type === "WsServerStatusEvent" && m.state === "AVAILABLE");
    expect(resting.state).toBe("AVAILABLE");
  });

  test("a failed run emits an error frame and still returns to a resting status", async () => {
    const id = await createAgent();

    const agent = _getAgentForTests(id)!;
    (agent as any).sendMessage = async function () {
      throw new Error("boom");
    };

    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientPromptCommand", content: "hello" }));

    await messages.waitFor(m => m.type === "WsServerStatusEvent" && m.state === "GENERATING");

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("boom");

    // The end-of-run status frame must still fire after a failure, so the client
    // is not left stuck on GENERATING.
    const resting = await messages.waitFor(m => m.type === "WsServerStatusEvent" && m.state === "AVAILABLE");
    expect(resting.state).toBe("AVAILABLE");
  });

  test("a message for an agent that no longer exists yields an error frame", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    // Drop the agent while the socket stays open; the message handler re-looks it up.
    _resetAgentStoreForTests();
    ws.send(JSON.stringify({ type: "WsClientPromptCommand", content: "hello" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Agent not found");
  });

  test("runs the close handler when the client disconnects", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    const closed = new Promise<void>(resolve => ws.addEventListener("close", () => resolve(), { once: true }));
    ws.close();
    await closed;
    // Let the server process the disconnect (its close handler runs here).
    await new Promise(resolve => setTimeout(resolve, 50));

    expect(ws.readyState).toBe(WebSocket.CLOSED);
  });
});
