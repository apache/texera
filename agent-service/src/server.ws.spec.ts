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
    // The snapshot must NOT carry the agent's workflow copy: it can lag the
    // backend (manual canvas edits never reach it), so pushing it on connect
    // would overwrite the user's canvas. Backend polling owns canvas sync.
    expect("workflowContent" in snapshot).toBe(false);
    // Results are pulled on demand, never pushed on the snapshot.
    expect("operatorResults" in snapshot).toBe(false);
  });

  test("a client connecting after a revert gets the moved HEAD + canRedo in its snapshot", async () => {
    const id = await createAgent();
    const { agent } = seedRevertableTurn(id);
    await agent.revertToTurnStart("turn-1"); // HEAD -> initial, redo available

    const { ws, messages } = connect(id);
    await waitOpen(ws);
    const snapshot = await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    expect(snapshot.headId).toBe("step-initial");
    expect(snapshot.canRedo).toBe(true);
    expect("workflowContent" in snapshot).toBe(false);
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

  // --- Revert (WsClientRevertCommand) ---------------------------------------

  const EMPTY_CONTENT = { operators: [], operatorPositions: {}, links: [], commentBoxes: [], settings: {} };
  const ONE_OP_CONTENT = {
    operators: [{ operatorID: "op1" }],
    operatorPositions: {},
    links: [],
    commentBoxes: [],
    settings: {},
  };

  // Seed a single completed turn ("turn-1") whose pre-edit workflow was empty and
  // whose post-edit workflow has one operator; leave HEAD at the turn's last step.
  function seedRevertableTurn(id: string) {
    const agent = _getAgentForTests(id)! as any;
    const priorHead = agent.getHead(); // INITIAL_STEP_ID on a fresh agent
    const userStep = {
      id: "u1",
      parentId: priorHead,
      messageId: "turn-1",
      stepId: 0,
      timestamp: Date.now(),
      role: "user",
      content: "add an operator",
      isBegin: true,
      isEnd: true,
      beforeWorkflowContent: EMPTY_CONTENT,
      afterWorkflowContent: EMPTY_CONTENT,
    };
    const agentStep = {
      id: "a1",
      parentId: "u1",
      messageId: "turn-1",
      stepId: 1,
      timestamp: Date.now(),
      role: "agent",
      content: "done",
      isBegin: true,
      isEnd: true,
      afterWorkflowContent: ONE_OP_CONTENT,
    };
    agent.reActStepsByMessageId.set("turn-1", [userStep, agentStep]);
    agent.stepsById.set("u1", userStep);
    agent.stepsById.set("a1", agentStep);
    agent.head = "a1";
    agent.getWorkflowState().setWorkflowContent(ONE_OP_CONTENT);
    return { agent, priorHead };
  }

  test("a revert command rewinds HEAD and broadcasts a head-change frame", async () => {
    const id = await createAgent();
    const { agent, priorHead } = seedRevertableTurn(id);
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientRevertCommand", messageId: "turn-1" }));

    const head = await messages.waitFor(m => m.type === "WsServerHeadChangeEvent");
    expect(head.headId).toBe(priorHead);
    expect(head.workflowContent.operators).toHaveLength(0);
    // The agent truly rewound: HEAD moved and its working workflow is the pre-turn state.
    expect(agent.getHead()).toBe(priorHead);
    expect(agent.getWorkflowState().getWorkflowContent().operators).toHaveLength(0);
  });

  test("a revert command without a messageId yields an error frame", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientRevertCommand" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("messageId is required to revert");
  });

  test("a revert command for an unknown turn yields an error frame", async () => {
    const id = await createAgent();
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientRevertCommand", messageId: "no-such-turn" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Unknown turn: no-such-turn");
  });

  test("a revert command while generating is rejected", async () => {
    const id = await createAgent();
    seedRevertableTurn(id);
    const agent = _getAgentForTests(id)! as any;
    agent.state = "GENERATING";
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientRevertCommand", messageId: "turn-1" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Cannot revert while the agent is busy");
    // HEAD must not have moved.
    expect(agent.getHead()).toBe("a1");
  });

  test("a revert command for a turn no longer on the HEAD path yields an error frame", async () => {
    const id = await createAgent();
    const { agent } = seedRevertableTurn(id);
    await agent.revertToTurnStart("turn-1"); // turn-1 drops off the visible path
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    // A duplicate/stale revert (e.g. from a second client) must not execute.
    ws.send(JSON.stringify({ type: "WsClientRevertCommand", messageId: "turn-1" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Turn is not on the current conversation path");
    expect(agent.getHead()).toBe("step-initial");
  });

  test("revert and redo are rejected while STOPPING (aborted run still unwinding)", async () => {
    const id = await createAgent();
    const { agent } = seedRevertableTurn(id);
    await agent.revertToTurnStart("turn-1"); // make redo available
    (agent as any).state = "STOPPING";
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientRevertCommand", messageId: "turn-1" }));
    const rerr = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(rerr.error).toBe("Cannot revert while the agent is busy");

    ws.send(JSON.stringify({ type: "WsClientRedoCommand" }));
    const derr = await messages.waitFor(m => m.type === "WsServerErrorEvent" && m.error.includes("redo"));
    expect(derr.error).toBe("Cannot redo while the agent is busy");
  });

  // --- Redo (WsClientRedoCommand) -------------------------------------------

  test("revert then redo returns HEAD and workflow to the post-turn state", async () => {
    const id = await createAgent();
    const { agent } = seedRevertableTurn(id);
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    // Revert: HEAD -> parent, canRedo becomes true.
    ws.send(JSON.stringify({ type: "WsClientRevertCommand", messageId: "turn-1" }));
    const reverted = await messages.waitFor(m => m.type === "WsServerHeadChangeEvent" && m.headId === "step-initial");
    expect(reverted.canRedo).toBe(true);

    // Redo: HEAD -> back to the turn's leaf, workflow restored.
    ws.send(JSON.stringify({ type: "WsClientRedoCommand" }));
    const redone = await messages.waitFor(m => m.type === "WsServerHeadChangeEvent" && m.headId === "a1");
    expect(redone.workflowContent.operators).toHaveLength(1);
    expect(redone.canRedo).toBe(false);
    expect(agent.getHead()).toBe("a1");
    expect(agent.getWorkflowState().getWorkflowContent().operators).toHaveLength(1);
  });

  test("redo restores the canvas even when the leaf step has no snapshot (error/stopped turn)", async () => {
    const id = await createAgent();
    const { agent } = seedRevertableTurn(id);
    // Simulate a turn that ended on an error/stopped step: a leaf with no
    // afterWorkflowContent, whose parent (a1) holds the real snapshot.
    const errorLeaf = {
      id: "err1",
      parentId: "a1",
      messageId: "turn-1",
      stepId: 2,
      timestamp: Date.now(),
      role: "agent",
      content: "Error: rate limit",
      isBegin: false,
      isEnd: true,
    };
    (agent as any).reActStepsByMessageId.get("turn-1").push(errorLeaf);
    (agent as any).stepsById.set("err1", errorLeaf);
    (agent as any).head = "err1";

    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientRevertCommand", messageId: "turn-1" }));
    await messages.waitFor(m => m.type === "WsServerHeadChangeEvent" && m.headId === "step-initial");

    ws.send(JSON.stringify({ type: "WsClientRedoCommand" }));
    const redone = await messages.waitFor(m => m.type === "WsServerHeadChangeEvent" && m.headId === "err1");
    // The leaf carries no snapshot, but redo walks up to a1's snapshot.
    expect(redone.workflowContent?.operators).toHaveLength(1);
    expect(agent.getWorkflowState().getWorkflowContent().operators).toHaveLength(1);
  });

  test("a redo with nothing to redo yields an error frame", async () => {
    const id = await createAgent();
    seedRevertableTurn(id);
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientRedoCommand" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Nothing to redo");
  });

  test("a redo while generating is rejected", async () => {
    const id = await createAgent();
    const { agent } = seedRevertableTurn(id);
    // Make redo available, then flip to GENERATING.
    await agent.revertToTurnStart("turn-1");
    agent.state = "GENERATING";
    const { ws, messages } = connect(id);
    await waitOpen(ws);
    await messages.waitFor(m => m.type === "WsServerSnapshotEvent");

    ws.send(JSON.stringify({ type: "WsClientRedoCommand" }));

    const err = await messages.waitFor(m => m.type === "WsServerErrorEvent");
    expect(err.error).toBe("Cannot redo while the agent is busy");
  });
});
