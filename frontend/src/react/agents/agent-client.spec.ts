/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient } from "../core/api-client";
import { AgentClient, visibleAgentSteps, type ReActStep } from "./agent-client";
class Socket {
  readyState = 1;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  sent: string[] = [];
  closed = false;
  send(value: string) {
    this.sent.push(value);
  }
  close() {
    this.closed = true;
  }
  receive(value: unknown) {
    this.onmessage?.({ data: JSON.stringify(value) });
  }
}
const step = (id: string, parentId?: string, extra = {}) => ({
  id,
  parentId,
  messageId: id,
  stepId: 0,
  timestamp: 1,
  role: "agent",
  content: id,
  isBegin: true,
  isEnd: true,
  ...extra,
});
function build() {
  const sockets: Socket[] = [];
  let warehouse: number | undefined = 4;
  const fetcher = vi.fn(async (path: string) =>
    Response.json(
      path.endsWith("/agents")
        ? {
            agents: [
              {
                id: "agent/one",
                name: "Agent",
                modelType: "model",
                state: "AVAILABLE",
                createdAt: "2026-01-01",
                delegate: { workflowId: 7 },
              },
            ],
          }
        : path.endsWith("/models")
          ? { data: [{ id: "model" }] }
          : path.includes("/workflow/")
            ? { wid: 7, content: { operators: [], links: [], commentBoxes: [], operatorPositions: {}, settings: {} } }
            : { status: "ok" }
    )
  );
  const onWorkflow = vi.fn(),
    onGenerating = vi.fn();
  const client = new AgentClient(new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => "token" }), {
    workflowId: 7,
    getWarehouseId: () => warehouse,
    onWorkflow,
    onGenerating,
    createSocket: () => {
      const socket = new Socket();
      sockets.push(socket);
      return socket;
    },
  });
  return {
    client,
    sockets,
    fetcher,
    onWorkflow,
    onGenerating,
    setWarehouse: (value: number | undefined) => {
      warehouse = value;
    },
  };
}
afterEach(() => vi.useRealTimers());
describe("agent service protocol", () => {
  it("retains existing agents when the independent model catalog is unavailable", async () => {
    const { client, fetcher } = build();
    const existing = {
      id: "agent/one",
      name: "Agent",
      modelType: "m",
      state: "AVAILABLE",
      delegate: { workflowId: 7 },
    };
    fetcher.mockImplementation(async path =>
      path.endsWith("/models")
        ? new Response("Model catalog unavailable", { status: 503 })
        : Response.json({ agents: [existing] })
    );
    await client.load();
    expect(client.getSnapshot().agents).toHaveLength(1);
    expect(client.getSnapshot().models).toEqual([]);
    expect(client.getSnapshot().error).toContain("Model catalog unavailable");
    expect(() => client.activate("agent/one")).not.toThrow();
    client.dispose();
  });
  it("closes an active agent removed during backend synchronization", async () => {
    const { client, fetcher, sockets } = build();
    await client.load();
    client.activate("agent/one");
    fetcher.mockResolvedValueOnce(Response.json({ agents: [] }));
    await client.load();
    expect(client.getSnapshot().activeId).toBeUndefined();
    expect(sockets[0].closed).toBe(true);
    client.dispose();
  });
  it("does not clear a newly selected agent after the outgoing clear request completes", async () => {
    const { client, fetcher, sockets } = build();
    fetcher.mockResolvedValueOnce(
      Response.json({
        agents: [
          { id: "agent/one", name: "One", modelType: "m", state: "AVAILABLE", delegate: { workflowId: 7 } },
          { id: "two", name: "Two", modelType: "m", state: "AVAILABLE", delegate: { workflowId: 7 } },
        ],
      })
    );
    await client.load();
    client.activate("agent/one");
    let complete!: (response: Response) => void;
    fetcher.mockImplementationOnce(
      () =>
        new Promise<Response>(resolve => {
          complete = resolve;
        })
    );
    const pending = client.clear();
    client.activate("two");
    sockets[1].receive({ type: "WsServerSnapshotEvent", state: "AVAILABLE", headId: "b", steps: [step("b")] });
    complete(Response.json({ status: "cleared" }));
    await pending;
    expect(client.getSnapshot().steps.map(step => step.id)).toEqual(["b"]);
    client.dispose();
  });
  it("persists settings as PATCH JSON with workflow routing and current unit on create", async () => {
    const { client, fetcher } = build();
    await client.load();
    client.activate("agent/one");
    fetcher.mockResolvedValueOnce(Response.json({ maxSteps: 4 }));
    expect(await client.updateSettings({ maxSteps: 4, disabledTools: ["run"] })).toEqual({ maxSteps: 4 });
    const call = fetcher.mock.calls.at(-1) as unknown as [string, RequestInit];
    expect(call[0]).toBe("/api/agents/agent%2Fone/settings");
    expect(call[1].method).toBe("PATCH");
    expect(JSON.parse(String(call[1].body))).toEqual({ maxSteps: 4, disabledTools: ["run"] });
    client.dispose();
  });
  it("lists models/agents, sends prompts with current warehouse, and encodes agent path/routing header", async () => {
    const { client, sockets, fetcher, setWarehouse } = build();
    await client.load();
    client.activate("agent/one");
    sockets[0].onopen?.();
    sockets[0].receive({ type: "WsServerSnapshotEvent", state: "AVAILABLE", steps: [], headId: null });
    client.send("ü");
    expect(JSON.parse(sockets[0].sent[0])).toEqual({
      type: "WsClientPromptCommand",
      content: "ü",
      messageSource: "chat",
      warehouseId: 4,
    });
    sockets[0].receive({ type: "WsServerStatusEvent", state: "AVAILABLE" });
    setWarehouse(undefined);
    client.send("feedback", "feedback");
    expect(JSON.parse(sockets[0].sent[1])).not.toHaveProperty("warehouseId");
    await client.clear();
    expect(fetcher.mock.calls.at(-1)?.[0]).toBe("/api/agents/agent%2Fone/clear");
    expect(
      new Headers((fetcher.mock.calls.at(-1) as unknown as [string, RequestInit])[1].headers).get("X-Agent-Workflow-Id")
    ).toBe("7");
    client.dispose();
  });
  it("deduplicates streamed steps and HEAD branches, handles workflow snapshots, errors and unavailable agents", async () => {
    const { client, sockets, onWorkflow, onGenerating } = build();
    await client.load();
    client.activate("agent/one");
    const content = { operators: [], links: [], commentBoxes: [], operatorPositions: {}, settings: {} };
    sockets[0].receive({
      type: "WsServerSnapshotEvent",
      state: "GENERATING",
      steps: [step("a"), step("orphan")],
      headId: "a",
      workflowContent: content,
    });
    expect(onGenerating).toHaveBeenLastCalledWith(true);
    expect(onWorkflow).toHaveBeenCalledWith(content);
    sockets[0].receive({ type: "WsServerStepEvent", step: step("b", "a", { content: "draft", isEnd: false }) });
    sockets[0].receive({
      type: "WsServerStepEvent",
      step: step("b", "a", { content: "final", afterWorkflowContent: content }),
    });
    expect(client.getSnapshot().steps).toHaveLength(3);
    expect(visibleAgentSteps(client.getSnapshot().steps, client.getSnapshot().headId).map(s => s.content)).toEqual([
      "a",
      "final",
    ]);
    sockets[0].receive({ type: "WsServerErrorEvent", error: "failed <script>" });
    expect(client.getSnapshot().error).toBe("failed <script>");
    sockets[0].receive({ type: "WsServerStatusEvent", state: "AVAILABLE" });
    expect(onGenerating).toHaveBeenLastCalledWith(false);
    sockets[0].receive({ type: "WsServerErrorEvent", error: "Agent not found" });
    expect(client.getSnapshot().agents).toEqual([]);
    expect(sockets[0].closed).toBe(true);
    client.dispose();
  });
  it("bounds HEAD cycles and rejects malformed messages/settings/blank prompts", async () => {
    const { client, sockets } = build();
    await client.load();
    client.activate("agent/one");
    sockets[0].receive({ type: "WsServerSnapshotEvent", state: "AVAILABLE", steps: [], headId: null });
    expect(() => client.send(" ")).toThrow("message");
    sockets[0].onmessage?.({ data: "bad json" });
    expect(client.getSnapshot().error).toContain("Malformed");
    sockets[0].receive({ type: "WsServerStepEvent", step: { id: "bad" } });
    expect(client.getSnapshot().steps).toEqual([]);
    await expect(client.updateSettings({ maxSteps: 0 })).rejects.toThrow("maxSteps");
    expect(visibleAgentSteps([step("a", "b"), step("b", "a")] as unknown as ReActStep[], "a")).toHaveLength(2);
    client.dispose();
  });
  it("reconnects with snapshot reconciliation, ignores detached socket frames and uses stop HTTP fallback", async () => {
    vi.useFakeTimers();
    const { client, sockets, fetcher } = build();
    await client.load();
    client.activate("agent/one");
    const late = sockets[0].onmessage;
    sockets[0].onclose?.();
    await client.stop();
    expect(fetcher.mock.calls.at(-1)?.[0]).toBe("/api/agents/agent%2Fone/stop");
    await vi.advanceTimersByTimeAsync(3000);
    expect(sockets.length).toBe(2);
    late?.({ data: JSON.stringify({ type: "WsServerStepEvent", step: step("late") }) });
    expect(client.getSnapshot().steps).toEqual([]);
    client.dispose();
    await vi.advanceTimersByTimeAsync(10000);
    expect(sockets).toHaveLength(2);
  });
  it("refuses agents attached to a different workflow and keeps deletion failure visible", async () => {
    const { client, fetcher } = build();
    fetcher.mockResolvedValueOnce(
      Response.json({
        agents: [{ id: "other", name: "Other", modelType: "m", state: "AVAILABLE", delegate: { workflowId: 8 } }],
      })
    );
    await client.load();
    expect(() => client.activate("other")).toThrow("different workflow");
    fetcher.mockResolvedValueOnce(new Response("Forbidden", { status: 403 }));
    await expect(client.remove("other")).rejects.toThrow("Forbidden");
    expect(client.getSnapshot().agents).toHaveLength(1);
    client.dispose();
  });
});
