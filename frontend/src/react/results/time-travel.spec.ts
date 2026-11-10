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

import { describe, expect, it, vi } from "vitest";
import { ApiClient } from "../core/api-client";
import { TimeTravelController } from "./time-travel";
import type { WorkflowSession } from "../workspace/workflow-session";
import type { WorkflowRuntime } from "../workspace/workflow-runtime";
import type { WorkflowExecutionsEntry } from "./results-repository";
import { WorkflowSession as RealSession } from "../workspace/workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import * as Y from "yjs";
import type { Workflow } from "../../core/common/type/workflow";
const content = {
  operators: [],
  links: [],
  commentBoxes: [],
  operatorPositions: {},
  settings: { dataTransferBatchSize: 100, executionMode: "PIPELINED" },
};
const entry = { eId: 5, vId: 2, cuId: 9, logLocation: "s3://log", name: "Old run" } as WorkflowExecutionsEntry;
function build() {
  const fetcher = vi.fn(async (path: string) =>
    Response.json(
      path === "/api/executions/7"
        ? [entry, { ...entry, eId: 6, logLocation: "" }]
        : path.includes("interactions")
          ? ["checkpoint α", "pause"]
          : { wid: 7, name: "Flow", content }
    )
  );
  const workflow = {
    wid: 7,
    name: "Flow",
    content: { ...content, settings: { ...content.settings, dataTransferBatchSize: 400 } },
  };
  const session = {
    getSnapshot: () => ({ workflow }),
    save: vi.fn(async () => workflow),
    setAutosaveSuspended: vi.fn(),
    setExecutionLocked: vi.fn(),
    applyRemoteWorkflow: vi.fn(),
    enterPreview: vi.fn(),
    exitPreview: vi.fn(),
  } as unknown as WorkflowSession;
  const runtime = {
    getSnapshot: vi.fn(() => ({ state: "Completed" })),
    subscribe: () => () => {},
    run: vi.fn(async () => undefined),
    kill: vi.fn(),
  } as unknown as WorkflowRuntime;
  return {
    fetcher,
    session,
    runtime,
    controller: new TimeTravelController(
      new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => null }),
      session,
      runtime
    ),
  };
}
describe("execution time travel", () => {
  it("keeps historical preview out of the live room and restores current collaborator edits on exit", async () => {
    const { fetcher, runtime } = build();
    const workflow = {
      wid: 7,
      name: "Flow",
      readonly: false,
      content: { ...content, settings: { ...content.settings, dataTransferBatchSize: 400 } },
    } as Workflow;
    const live = new SharedModel(7, undefined, undefined, { connect: false });
    const session = new RealSession(workflow, { shared: live, autosaveMs: 0, save: async value => value });
    const peer = new SharedModel(7, undefined, undefined, { connect: false });
    Y.applyUpdate(peer.yDoc, Y.encodeStateAsUpdate(live.yDoc));
    const peerSession = new RealSession(workflow, { shared: peer, autosaveMs: 0, save: async value => value });
    const forward = (update: Uint8Array) => Y.applyUpdate(peer.yDoc, update),
      backward = (update: Uint8Array) => Y.applyUpdate(live.yDoc, update);
    live.yDoc.on("update", forward);
    peer.yDoc.on("update", backward);
    const controller = new TimeTravelController(
      new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => null }),
      session,
      runtime
    );
    try {
      await controller.replay(entry, "pause");
      expect(session.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(100);
      expect(peerSession.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(400);
      peerSession.setSettings({ ...workflow.content.settings, dataTransferBatchSize: 999 });
      expect(session.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(100);
      await controller.exit();
      expect(session.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(999);
      expect(peerSession.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(999);
    } finally {
      controller.dispose();
      session.dispose();
      peerSession.dispose();
    }
  });
  it("finishes the working save before entering suspended historical preview", async () => {
    const { controller, session } = build();
    let suspended = false;
    vi.mocked(session.setAutosaveSuspended).mockImplementation(value => {
      suspended = value;
    });
    vi.mocked(session.save).mockImplementation(async () => {
      if (suspended) throw new Error("Historical previews cannot be saved.");
      return session.getSnapshot().workflow;
    });
    await controller.replay(entry, "pause");
    expect(suspended).toBe(true);
    await controller.exit();
    expect(suspended).toBe(false);
    controller.dispose();
  });
  it("serializes replay requests and restores the working graph when closed during an in-flight run", async () => {
    const { controller, session, runtime } = build();
    let complete!: () => void;
    vi.mocked(runtime.run).mockImplementationOnce(
      () =>
        new Promise<void>(resolve => {
          complete = resolve;
        })
    );
    const pending = controller.replay(entry, "pause");
    await vi.waitFor(() => expect(runtime.run).toHaveBeenCalled());
    await expect(controller.replay(entry, "checkpoint α")).rejects.toThrow("already running");
    vi.mocked(runtime.getSnapshot).mockReturnValue({ state: "Running" } as ReturnType<WorkflowRuntime["getSnapshot"]>);
    controller.dispose();
    expect(runtime.kill).toHaveBeenCalledOnce();
    expect(session.exitPreview).toHaveBeenCalled();
    expect(session.setAutosaveSuspended).toHaveBeenLastCalledWith(false, "replay");
    expect(session.setExecutionLocked).toHaveBeenLastCalledWith(false, "replay");
    complete();
    await pending;
    expect(session.save).toHaveBeenCalledOnce();
  });
  it("lists only logged runs and fetches exact interaction endpoint with validation", async () => {
    const { controller, fetcher } = build();
    await controller.refresh();
    expect(controller.getSnapshot().entries).toHaveLength(1);
    expect(await controller.interactions(5)).toEqual(["checkpoint α", "pause"]);
    expect(fetcher.mock.calls.at(-1)?.[0]).toBe("/api/executions/7/interactions/5");
    await expect(controller.interactions(-1)).rejects.toThrow("execution");
    controller.dispose();
  });
  it("saves current graph once, runs exact historical version/replay label and restores current edits on exit", async () => {
    const { controller, session, runtime, fetcher } = build();
    await controller.replay(entry, "checkpoint α");
    expect(session.save).toHaveBeenCalledOnce();
    expect(fetcher.mock.calls[0][0]).toBe("/api/version/7/2");
    expect(session.setAutosaveSuspended).toHaveBeenCalledWith(true, "replay");
    expect(session.enterPreview).toHaveBeenCalledWith(content);
    expect(runtime.run).toHaveBeenCalledWith({
      replayFromExecution: { eid: 5, interaction: "checkpoint α" },
      executionName: "Replay run of 5 to checkpoint α",
    });
    expect(controller.getSnapshot().replay).toEqual({ eid: 5, interaction: "checkpoint α" });
    await controller.exit();
    expect(session.exitPreview).toHaveBeenCalled();
    expect(session.setAutosaveSuspended).toHaveBeenLastCalledWith(false, "replay");
    expect(session.save).toHaveBeenCalledOnce();
    controller.dispose();
  });
  it("rolls back graph/autosave after unavailable unit or failed historical fetch without persisting preview", async () => {
    const { controller, session, runtime } = build();
    vi.mocked(runtime.run).mockRejectedValueOnce(new Error("Select a computing unit before running."));
    await expect(controller.replay(entry, "pause")).rejects.toThrow("computing unit");
    expect(session.exitPreview).toHaveBeenCalled();
    expect(session.setAutosaveSuspended).toHaveBeenLastCalledWith(false, "replay");
    await expect(controller.replay(entry, " ")).rejects.toThrow("interaction");
    controller.dispose();
  });
  it("rejects malformed interaction lists and active unrelated execution before changing graph", async () => {
    const { controller, fetcher, session, runtime } = build();
    fetcher.mockResolvedValueOnce(Response.json([null]));
    await expect(controller.interactions(5)).rejects.toThrow("interaction");
    vi.mocked(runtime.getSnapshot).mockReturnValue({ state: "Running" } as ReturnType<WorkflowRuntime["getSnapshot"]>);
    await expect(controller.replay(entry, "pause")).rejects.toThrow("Stop the current execution");
    expect(session.save).not.toHaveBeenCalled();
    controller.dispose();
  });
});
