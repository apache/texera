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
import { WorkflowRuntime } from "./workflow-runtime";
import { WorkflowSocket } from "./workflow-socket";
import { WorkflowSession } from "./workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { ExecutionMode, type Workflow } from "../../core/common/type/workflow";

const disposals: (() => void)[] = [];
function setup(options: { warehouse?: boolean; fetch?: typeof fetch } = {}) {
  const workflow: Workflow = {
    wid: 3,
    name: "Workflow",
    description: "",
    isPublished: 0,
    readonly: false,
    creationTime: 1,
    lastModifiedTime: 1,
    content: {
      operators: [],
      links: [],
      commentBoxes: [],
      operatorPositions: {},
      settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
    },
  };
  const session = new WorkflowSession(workflow, {
    shared: new SharedModel(3, undefined, undefined, { connect: false }),
    save: async value => value,
    autosaveMs: 0,
  });
  const wire = {
    readyState: 1,
    onopen: null as (() => void) | null,
    onmessage: null as ((event: { data: string }) => void) | null,
    onerror: null,
    onclose: null,
    send: vi.fn(),
    close: vi.fn(),
  };
  const socket = new WorkflowSocket({ wid: 3, uid: 7, getToken: () => "token", createSocket: () => wire });
  socket.connect();
  const unit: { cuid: number; status: string; isOwner?: boolean; accessPrivilege?: string } = {
    cuid: 9,
    status: "Running",
  };
  let warehouseId: number | undefined;
  const fetcher =
    options.fetch ??
    vi.fn(async () => Response.json({ physicalPlan: {}, operatorOutputSchemas: {}, operatorErrors: {} }));
  const runtime = new WorkflowRuntime(session, socket, new ApiClient({ fetch: fetcher }), {
    engineVersion: "test-build",
    config: { warehouseEnabled: options.warehouse },
    getComputingUnit: () => unit,
    getWarehouseId: () => warehouseId,
  });
  const message = (event: unknown) => wire.onmessage?.({ data: JSON.stringify(event) });
  message({ type: "ClusterStatusUpdateEvent", numWorkers: 2 });
  disposals.push(() => {
    runtime.dispose();
    session.dispose();
  });
  return {
    session,
    socket,
    wire,
    runtime,
    unit,
    message,
    fetcher,
    selectWarehouse: (id: number) => {
      warehouseId = id;
    },
  };
}
afterEach(() => {
  disposals.splice(0).forEach(dispose => dispose());
  vi.useRealTimers();
});

describe("React workflow execution session", () => {
  it("does not send kill or checkpoint commands after any terminal state", () => {
    const { runtime, message } = setup();
    for (const state of ["Uninitialized", "Completed", "Killed", "Terminated"]) {
      message({ type: "WorkflowStateEvent", state });
      expect(() => runtime.kill()).toThrow(/state/i);
      expect(() => runtime.checkpoint()).toThrow(/state/i);
    }
    message({ type: "WorkflowErrorEvent", fatalErrors: [] });
    expect(() => runtime.kill()).toThrow(/state/i);
  });
  it("rejects mutating execution controls for a read-only computing unit while retaining result access", () => {
    const { runtime, unit, message, wire } = setup();
    unit.accessPrivilege = "READ";
    unit.isOwner = false;
    message({ type: "WorkflowStateEvent", state: "Running" });
    expect(() => runtime.pause()).toThrow(/permission/i);
    expect(() => runtime.kill()).toThrow(/permission/i);
    expect(() => runtime.checkpoint()).toThrow(/permission/i);
    message({ type: "WorkflowStateEvent", state: "Paused" });
    expect(() => runtime.resume()).toThrow(/permission/i);
    expect(() => runtime.skip(["w"])).toThrow(/permission/i);
    expect(() => runtime.retry(["w"])).toThrow(/permission/i);
    expect(wire.send).not.toHaveBeenCalled();
    unit.isOwner = true;
    expect(() => runtime.resume()).not.toThrow();
  });
  it("rejects a normal run while a historical replay preview owns the working graph", async () => {
    const { session, runtime, wire } = setup();
    session.setAutosaveSuspended(true, "replay");
    await expect(runtime.run()).rejects.toThrow(/replay/i);
    expect(wire.send).not.toHaveBeenCalled();
  });
  it("preserves the replay execution label while retaining the current workflow name", async () => {
    vi.useFakeTimers();
    const { runtime, wire, session } = setup();
    const running = runtime.run({
      executionName: "Replay run of 7 to 日本語",
      replayFromExecution: { eid: 7, interaction: "日本語" },
    });
    await vi.advanceTimersByTimeAsync(150);
    await running;
    expect(JSON.parse(wire.send.mock.calls.at(-1)![0])).toMatchObject({
      executionName: "Replay run of 7 to 日本語",
      replayFromExecution: { eid: 7, interaction: "日本語" },
    });
    expect(session.getSnapshot().workflow.name).toBe("Workflow");
  });
  it("retains snapshot visualization rows across delta messages until a table transition and tracks assigned workers", () => {
    const { runtime, message } = setup();
    message({
      type: "WebResultUpdateEvent",
      updates: { op: { mode: { type: "SetSnapshotMode" }, table: [{ a: 1 }] } },
      tableStats: {},
    });
    message({
      type: "WebResultUpdateEvent",
      updates: { op: { mode: { type: "SetDeltaMode" }, table: [{ a: 2 }] } },
      tableStats: {},
    });
    expect(runtime.getSnapshot().materializedResults.op).toEqual([{ a: 1 }]);
    expect(runtime.getSnapshot().results.op).toMatchObject({ table: [{ a: 2 }] });
    message({
      type: "WebResultUpdateEvent",
      updates: { op: { mode: { type: "PaginationMode" }, totalNumTuples: 3, dirtyPageIndices: [] } },
      tableStats: {},
    });
    expect(runtime.getSnapshot().materializedResults.op).toBeUndefined();
    message({
      type: "WebResultUpdateEvent",
      updates: { op: { mode: { type: "SetDeltaMode" }, table: [{ a: 3 }] } },
      tableStats: {},
    });
    expect(runtime.getSnapshot().materializedResults.op).toBeUndefined();
    message({ type: "WorkerAssignmentUpdateEvent", operatorId: "op", workerIds: ["worker-1"] });
    expect(runtime.getSnapshot().workers.op).toEqual(["worker-1"]);
    message({ type: "RegionUpdateEvent", regions: [[1, ["op"]]] });
    message({ type: "RegionStateEvent", id: 1, state: "Completed" });
    expect(runtime.getSnapshot().regions).toEqual([[1, ["op"]]]);
    expect(runtime.getSnapshot().regionStates[1]).toBe("Completed");
    message({
      type: "WebResultUpdateEvent",
      updates: { op: { mode: { type: "SetSnapshotMode" }, table: [] } },
      tableStats: {},
    });
    expect(runtime.getSnapshot().materializedResults.op).toEqual([]);
  });
  it("sends the same execution body after the form debounce, including unit and warehouse selection", async () => {
    vi.useFakeTimers();
    const { runtime, wire, selectWarehouse } = setup({ warehouse: true });
    selectWarehouse(11);
    const sent = runtime.run({ emailNotificationEnabled: true });
    await vi.advanceTimersByTimeAsync(150);
    await sent;
    expect(JSON.parse(wire.send.mock.calls.at(-1)![0])).toEqual({
      type: "WorkflowExecuteRequest",
      executionName: "Workflow",
      engineVersion: "test-build",
      logicalPlan: { operators: [], links: [], opsToViewResult: [], opsToReuseResult: [] },
      workflowSettings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
      emailNotificationEnabled: true,
      computingUnitId: 9,
      warehouseId: 11,
    });
  });
  it("rejects unavailable units and missing warehouses before clearing prior runtime state", async () => {
    const { runtime, unit, message } = setup({ warehouse: true });
    message({ type: "WorkflowStateEvent", state: "Completed" });
    await expect(runtime.run()).rejects.toThrow(/warehouse/i);
    expect(runtime.getSnapshot().state).toBe("Completed");
    unit.status = "Terminating";
    await expect(runtime.run()).rejects.toThrow(/terminating/i);
  });
  it("locks editing in active states, validates controls and unlocks after completion", () => {
    const { runtime, session, wire, message } = setup();
    expect(() => runtime.pause()).toThrow(/pause/i);
    message({ type: "WorkflowStateEvent", state: "Running" });
    expect(() => session.rename("Blocked")).toThrow(/execution/i);
    runtime.pause();
    expect(JSON.parse(wire.send.mock.calls.at(-1)![0]).type).toBe("WorkflowPauseRequest");
    message({ type: "WorkflowStateEvent", state: "Paused" });
    runtime.resume();
    message({ type: "WorkflowStateEvent", state: "Completed" });
    expect(() => session.rename("Allowed")).not.toThrow();
  });
  it("keeps the clock advancing for a view mounted after the backend duration event", async () => {
    vi.useFakeTimers();
    const { runtime, message } = setup();
    message({ type: "ExecutionDurationUpdateEvent", duration: 7, isRunning: true });
    await vi.advanceTimersByTimeAsync(2000);
    expect(runtime.getSnapshot().duration).toBe(2007);
    message({ type: "ExecutionDurationUpdateEvent", duration: 10, isRunning: false });
    await vi.advanceTimersByTimeAsync(2000);
    expect(runtime.getSnapshot().duration).toBe(10);
  });
  it("compiles the original plan contract and preserves only the latest response", async () => {
    let resolveFirst!: (value: Response) => void;
    let count = 0;
    const fetcher = vi.fn(async () =>
      ++count === 1
        ? new Promise<Response>(resolve => {
            resolveFirst = resolve;
          })
        : Response.json({ physicalPlan: { operators: ["new"] }, operatorOutputSchemas: {}, operatorErrors: {} })
    );
    const { runtime } = setup({ fetch: fetcher });
    const first = runtime.compile();
    const second = runtime.compile();
    await second;
    resolveFirst(
      Response.json({ physicalPlan: { operators: ["old"] }, operatorOutputSchemas: {}, operatorErrors: {} })
    );
    await first;
    expect(runtime.getSnapshot().compilation?.physicalPlan).toEqual({ operators: ["new"] });
    expect(JSON.parse(String(fetcher.mock.calls[0][1]?.body))).toMatchObject({
      opsToReuseResult: [],
      opsToViewResult: [],
    });
  });
});
