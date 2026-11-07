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
import { SharedModel } from "../../app/workspace/service/workflow-graph/model/shared-model";
import { ExecutionMode, type Workflow } from "../../app/common/type/workflow";

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
  const unit: { cuid: number; status: string } = { cuid: 9, status: "Running" };
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
