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
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { WorkspaceAssistant } from "./workspace-assistant";
import { ApiClient } from "../core/api-client";
import type { WorkflowSession } from "../workspace/workflow-session";
import type { WorkflowRuntime } from "../workspace/workflow-runtime";
class Socket {
  static instances: Socket[] = [];
  readyState = 1;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  sent: string[] = [];
  constructor() {
    Socket.instances.push(this);
  }
  send(value: string) {
    this.sent.push(value);
  }
  close() {}
  receive(value: unknown) {
    act(() => this.onmessage?.({ data: JSON.stringify(value) }));
  }
}
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  Socket.instances = [];
});
function build() {
  const workflow = { wid: 7, name: "Flow", content: { operators: [{ operatorID: "a" }], links: [] } },
    snapshot = { workflow, canEdit: true };
  const session = {
    getSnapshot: () => snapshot,
    subscribe: () => () => {},
    save: vi.fn(async () => workflow),
    applyRemoteWorkflow: vi.fn(),
    setAutosaveSuspended: vi.fn(),
  } as unknown as WorkflowSession;
  const runtimeSnapshot = { state: "Completed" };
  const runtime = { getSnapshot: () => runtimeSnapshot, subscribe: () => () => {} } as unknown as WorkflowRuntime;
  const fetcher = vi.fn(async (path: string) =>
    Response.json(
      path === "/api/agents"
        ? {
            agents: [
              { id: "a1", name: "Assistant", modelType: "model", state: "AVAILABLE", delegate: { workflowId: 7 } },
            ],
          }
        : path === "/api/models"
          ? { data: [{ id: "model" }] }
          : path.endsWith("fetch-notebook-and-mapping")
            ? {
                exists: true,
                mapping: { cell_to_operator: { cell: ["a"] }, operator_to_cell: { a: ["cell"] } },
                notebook: { cells: [] },
              }
            : path.includes("get-jupyter-iframe")
              ? { success: true, url: "https://jupyter.test/notebook" }
              : path.includes("get-jupyter-url")
                ? { success: true, url: "https://jupyter.test/" }
                : {}
    )
  );
  return {
    session,
    runtime,
    fetcher,
    api: new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => null }),
    config: { copilotEnabled: true, pythonNotebookMigrationEnabled: true },
    user: { uid: 1, name: "User", email: "user@test.org", role: "REGULAR" as const, expiresAt: 999999999999 },
    getToken: () => null,
  };
}
describe("workspace assistant integration", () => {
  it("drops outgoing notebook frame and keeps subscriptions working across workflow changes", async () => {
    const props = build();
    const { rerender } = render(
      <WorkspaceAssistant
        {...props}
        config={{ pythonNotebookMigrationEnabled: true }}
      />
    );
    await screen.findByTitle("Uploaded Source Code");
    const original = props.session.getSnapshot();
    const nextSnapshot = { ...original, workflow: { ...original.workflow, wid: 8 } };
    const nextSession = { ...props.session, getSnapshot: () => nextSnapshot } as WorkflowSession;
    props.fetcher.mockResolvedValueOnce(Response.json({ exists: false }));
    rerender(
      <WorkspaceAssistant
        {...props}
        session={nextSession}
        config={{ pythonNotebookMigrationEnabled: true }}
      />
    );
    await waitFor(() => expect(screen.queryByTitle("Uploaded Source Code")).toBeNull());
    expect(props.fetcher.mock.calls.some(([path]) => path.includes("delete"))).toBe(false);
  });
  it("makes no agent or notebook requests when deployment features are disabled", () => {
    const props = build();
    render(
      <WorkspaceAssistant
        {...props}
        config={{ copilotEnabled: false, pythonNotebookMigrationEnabled: false }}
      />
    );
    expect(props.fetcher).not.toHaveBeenCalled();
    expect(screen.queryByText("Agents")).toBeNull();
  });
  it("saves before sending, streams safe rich text and applies server edits without remounting workspace", async () => {
    vi.stubGlobal("WebSocket", Socket);
    const props = build();
    render(
      <WorkspaceAssistant
        {...props}
        config={{ copilotEnabled: true }}
      />
    );
    await screen.findByRole("option", { name: "Assistant" });
    fireEvent.change(screen.getByLabelText("Active agent"), { target: { value: "a1" } });
    Socket.instances[0].receive({ type: "WsServerSnapshotEvent", state: "AVAILABLE", steps: [], headId: null });
    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "Build a workflow" } });
    fireEvent.click(screen.getByRole("button", { name: "Send message" }));
    await waitFor(() => expect(Socket.instances[0].sent).toHaveLength(1));
    expect(props.session.save).toHaveBeenCalledOnce();
    Socket.instances[0].receive({
      type: "WsServerStepEvent",
      step: {
        id: "s1",
        messageId: "s1",
        stepId: 0,
        timestamp: 1,
        role: "agent",
        content: "**Ready** <img src=x onerror=evil>",
        isBegin: true,
        isEnd: true,
        afterWorkflowContent: { operators: [], links: [], commentBoxes: [], operatorPositions: {}, settings: {} },
      },
    });
    expect(screen.getByText("Ready").tagName).toBe("STRONG");
    expect(document.querySelector("[onerror]")).toBeNull();
    expect(props.session.applyRemoteWorkflow).toHaveBeenCalledOnce();
    Socket.instances[0].receive({ type: "WsServerStatusEvent", state: "AVAILABLE" });
    expect(props.session.setAutosaveSuspended).toHaveBeenLastCalledWith(false, "agent");
  });
  it("validates notebook message source AND origin and posts selected operator mapped cells to that origin", async () => {
    const props = build(),
      onHighlight = vi.fn();
    const { rerender } = render(
      <WorkspaceAssistant
        {...props}
        config={{ pythonNotebookMigrationEnabled: true }}
        onHighlight={onHighlight}
      />
    );
    const frame = (await screen.findByTitle("Uploaded Source Code")) as HTMLIFrameElement;
    act(() =>
      window.dispatchEvent(
        new MessageEvent("message", {
          origin: "https://jupyter.test",
          source: window,
          data: { action: "cellClicked", cellUUID: "cell" },
        })
      )
    );
    expect(onHighlight).not.toHaveBeenCalled();
    act(() =>
      window.dispatchEvent(
        new MessageEvent("message", {
          origin: "https://evil.test",
          source: frame.contentWindow,
          data: { action: "cellClicked", cellUUID: "cell" },
        })
      )
    );
    expect(onHighlight).not.toHaveBeenCalled();
    act(() =>
      window.dispatchEvent(
        new MessageEvent("message", {
          origin: "https://jupyter.test",
          source: frame.contentWindow,
          data: { action: "cellClicked", cellUUID: "cell" },
        })
      )
    );
    expect(onHighlight).toHaveBeenCalledWith(["a"], []);
    const post = vi.spyOn(frame.contentWindow!, "postMessage");
    rerender(
      <WorkspaceAssistant
        {...props}
        config={{ pythonNotebookMigrationEnabled: true }}
        selectedOperatorIds={["a"]}
        onHighlight={onHighlight}
      />
    );
    await waitFor(() =>
      expect(post).toHaveBeenCalledWith({ action: "triggerCellClick", operators: ["cell"] }, "https://jupyter.test")
    );
    fireEvent.click(screen.getByRole("button", { name: "Delete uploaded source code" }));
    expect(props.fetcher.mock.calls.some(([path]) => path.includes("delete"))).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Cancel notebook deletion" }));
  });
});
