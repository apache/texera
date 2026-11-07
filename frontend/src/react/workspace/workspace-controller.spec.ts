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
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { ExecutionMode, type Workflow } from "../../core/common/type/workflow";
import type { ComputingUnit, Warehouse } from "../resources/api";
import { WorkflowSocket } from "./workflow-socket";
import { WorkspaceController } from "./workspace-controller";

const workflow = (): Workflow => ({
  wid: 42,
  name: "日本語 workflow",
  description: "description",
  readonly: false,
  creationTime: 1,
  lastModifiedTime: 2,
  isPublished: 0,
  content: {
    operators: [],
    links: [],
    operatorPositions: {},
    commentBoxes: [],
    settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
  },
});
const unit = (
  cuid: number,
  status: ComputingUnit["status"] = "Running",
  accessPrivilege: ComputingUnit["accessPrivilege"] = "WRITE"
): ComputingUnit => ({
  computingUnit: {
    cuid,
    uid: 7,
    name: `Unit ${cuid}`,
    creationTime: 1,
    type: "local",
    uri: "",
    resource: {
      cpuLimit: "1",
      memoryLimit: "1Gi",
      gpuLimit: "0",
      jvmMemorySize: "1Gi",
      shmSize: "1Gi",
      nodeAddresses: [],
    },
  },
  status,
  accessPrivilege,
  isOwner: true,
  ownerAvatar: "",
  ownerName: "User",
  metrics: { cpuUsage: "0", memoryUsage: "0" },
});
const warehouse = (whid: number): Warehouse => ({
  whid,
  name: `Warehouse ${whid}`,
  lakekeeperWarehouseName: "",
  flavor: "local",
  createdAtMillis: 1,
  ownerName: null,
  ownerAvatar: null,
});
const controllers: WorkspaceController[] = [];
const flush = async () => {
  for (let i = 0; i < 15; i++) await Promise.resolve();
};
function setup(
  options: {
    units?: ComputingUnit[];
    warehouses?: Warehouse[];
    latest?: { cuId?: number; whId?: number };
    versionId?: number;
    override?: (path: string, init?: RequestInit) => Response | Promise<Response> | undefined;
  } = {}
) {
  let units = options.units ?? [unit(9), unit(10)];
  let warehouses = options.warehouses ?? [warehouse(11), warehouse(12)];
  const events: string[] = [];
  const wires: {
    readyState: number;
    onopen: (() => void) | null;
    onmessage: ((event: { data: string }) => void) | null;
    onerror: (() => void) | null;
    onclose: (() => void) | null;
    send: ReturnType<typeof vi.fn>;
    close: ReturnType<typeof vi.fn>;
  }[] = [];
  const fetcher = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const path = String(input).replace(/^\/api\//, "");
    const overridden = options.override?.(path, init);
    if (overridden) return overridden;
    if (path === "resources/operator-metadata") return Response.json({ operators: [], groups: [] });
    if (path === "workflow/42" || path === "version/42/3") return Response.json(workflow());
    if (path === "computing-unit") return Response.json(units);
    if (path === "warehouse/status") return Response.json({ enabled: true, warehouses });
    if (path === "executions/42/latest")
      return options.latest ? Response.json(options.latest) : new Response("No execution", { status: 404 });
    if (path === "workflow/persist")
      return Response.json({ ...workflow(), ...JSON.parse(String(init?.body)), lastModifiedTime: 4 });
    if (path === "compile") return Response.json({ physicalPlan: {}, operatorOutputSchemas: {}, operatorErrors: {} });
    throw new Error(`Unexpected ${path}`);
  });
  const sharedModels: SharedModel[] = [];
  const controller = new WorkspaceController(
    new ApiClient({ fetch: fetcher }),
    { uid: 7, name: "User", email: "a@example.test", role: "REGULAR", expiresAt: Infinity },
    { warehouseEnabled: true },
    {
      workflowId: 42,
      versionId: options.versionId,
      engineVersion: "engine",
      getToken: () => "secret",
      autosaveMs: 1000,
      createSharedModel: (wid, user) => {
        const shared = new SharedModel(wid, user, undefined, { connect: false });
        sharedModels.push(shared);
        return shared;
      },
      createSocket: socketOptions =>
        new WorkflowSocket({
          ...socketOptions,
          createSocket: url => {
            events.push(`open:${new URL(url).searchParams.get("cuid")}`);
            const wire = {
              readyState: 1,
              onopen: null,
              onmessage: null,
              onerror: null,
              onclose: null,
              send: vi.fn(),
              close: vi.fn(() => {
                events.push("close");
              }),
            };
            wires.push(wire);
            return wire;
          },
        }),
    }
  );
  controllers.push(controller);
  return {
    controller,
    fetcher,
    wires,
    events,
    sharedModels,
    setUnits: (value: ComputingUnit[]) => {
      units = value;
    },
    setWarehouses: (value: Warehouse[]) => {
      warehouses = value;
    },
  };
}
afterEach(async () => {
  await Promise.allSettled(controllers.splice(0).map(controller => controller.dispose()));
  localStorage.clear();
  vi.useRealTimers();
});

describe("WorkspaceController", () => {
  it("loads metadata and one shared workflow session with user presence and server-derived selections", async () => {
    const { controller, fetcher, sharedModels } = setup({ latest: { cuId: 10, whId: 12 } });
    const listener = vi.fn();
    const stop = controller.subscribe(listener);
    await Promise.all([controller.load(), controller.load()]);
    const snapshot = controller.getSnapshot();
    expect(snapshot).toMatchObject({
      loading: false,
      metadata: { operators: [], groups: [] },
      selectedCuid: 10,
      selectedWarehouseId: 12,
    });
    expect(snapshot.session?.getSnapshot().workflow.name).toBe("日本語 workflow");
    expect(sharedModels[0].wid).toBe(42);
    expect(sharedModels[0].awareness.getLocalState()).toMatchObject({
      user: { uid: 7, name: "User", email: "a@example.test" },
      isActive: false,
    });
    expect(controller.session).toBe(snapshot.session);
    expect(controller.runtime).toBe(snapshot.runtime);
    expect(fetcher.mock.calls.filter(call => String(call[0]).endsWith("workflow/42"))).toHaveLength(1);
    expect(listener).toHaveBeenCalled();
    stop();
  });
  it("recalls only existing remembered units and lets later explicit picks survive refresh", async () => {
    vi.useFakeTimers();
    localStorage.setItem("computing-unit-of-workflow-42", "9");
    const { controller, setWarehouses } = setup({ latest: { cuId: 10, whId: 12 } });
    await controller.load();
    expect(controller.getSnapshot().selectedCuid).toBe(9);
    controller.selectComputingUnit(10);
    controller.selectWarehouse(11);
    setWarehouses([warehouse(12), warehouse(11)]);
    await vi.advanceTimersByTimeAsync(2000);
    expect(controller.getSnapshot()).toMatchObject({ selectedCuid: 10, selectedWarehouseId: 11 });
    expect(localStorage.getItem("computing-unit-of-workflow-42")).toBe("10");
  });
  it("drops stale remembered ids and chooses a running writable unit, with no fabricated fallback", async () => {
    localStorage.setItem("computing-unit-of-workflow-42", "999");
    const { controller } = setup({ units: [unit(8, "Running", "READ"), unit(9, "Pending"), unit(10)] });
    await controller.load();
    expect(controller.getSnapshot().selectedCuid).toBe(10);
    expect(localStorage.getItem("computing-unit-of-workflow-42")).toBeNull();
    const empty = setup({ units: [], warehouses: [] });
    await empty.controller.load();
    expect(empty.controller.getSnapshot().selectedCuid).toBeUndefined();
    expect(empty.wires).toHaveLength(0);
    expect(empty.controller.getSnapshot().selectedWarehouseId).toBeUndefined();
  });
  it("closes and resets the previous runtime before switching sockets without replacing the graph session", async () => {
    const { controller, wires, events } = setup();
    await controller.load();
    const session = controller.session;
    const runtime = controller.runtime;
    wires[0].onmessage?.({ data: JSON.stringify({ type: "WorkflowStateEvent", state: "Running" }) });
    controller.selectComputingUnit(9);
    expect(wires).toHaveLength(1);
    controller.selectComputingUnit(10);
    expect(events).toEqual(["open:9", "close", "open:10"]);
    expect(controller.session).toBe(session);
    expect(controller.runtime).not.toBe(runtime);
    expect(controller.runtime?.getSnapshot().state).toBe("Uninitialized");
    expect(session?.getSnapshot().canEdit).toBe(true);
    expect(() => controller.selectComputingUnit(999)).toThrow(/not available/i);
    expect(() => controller.selectWarehouse(999)).toThrow(/not available/i);
    expect(() => controller.selectComputingUnit(NaN)).toThrow(/identifier/i);
  });
  it("polls every two seconds, clears disappeared selections and preserves the graph through recoverable errors", async () => {
    vi.useFakeTimers();
    let broken = false;
    const { controller, setUnits, setWarehouses, wires } = setup({
      override: path => (broken && path === "computing-unit" ? new Response("Offline", { status: 503 }) : undefined),
    });
    await controller.load();
    const session = controller.session;
    broken = true;
    await vi.advanceTimersByTimeAsync(2000);
    expect(controller.getSnapshot().error).toMatch(/Offline/);
    expect(controller.session).toBe(session);
    broken = false;
    setUnits([]);
    setWarehouses([]);
    await vi.advanceTimersByTimeAsync(2000);
    expect(controller.getSnapshot()).toMatchObject({
      units: [],
      warehouses: [],
      selectedCuid: undefined,
      selectedWarehouseId: undefined,
      error: undefined,
    });
    expect(wires[0].close).toHaveBeenCalledOnce();
    expect(controller.session).toBe(session);
    const requests = vi.fn();
    controller.subscribe(requests);
    await controller.dispose();
    requests.mockClear();
    await vi.advanceTimersByTimeAsync(10000);
    expect(requests).not.toHaveBeenCalled();
  });
  it("keeps historical versions read-only and isolated from the current collaboration room", async () => {
    const { controller, sharedModels, fetcher } = setup({ versionId: 3 });
    await controller.load();
    expect(fetcher).toHaveBeenCalledWith("/api/version/42/3", expect.anything());
    expect(controller.session?.getSnapshot().canEdit).toBe(false);
    expect(sharedModels[0].wid).toBeUndefined();
    expect(sharedModels[0].wsProvider.shouldConnect).toBe(false);
    expect(() => controller.session?.rename("Should not save")).toThrow(/read.only/i);
  });
  it("retains computing-unit ownership and access restrictions when creating the execution runtime", async () => {
    vi.useFakeTimers();
    const readOnly = { ...unit(9, "Running", "READ"), isOwner: false };
    const { controller, wires } = setup({ units: [readOnly], latest: { cuId: 9 } });
    await controller.load();
    wires[0].onmessage?.({ data: JSON.stringify({ type: "ClusterStatusUpdateEvent", numWorkers: 1 }) });
    const run = controller.runtime!.run().then(
      () => undefined,
      error => error as Error
    );
    await vi.advanceTimersByTimeAsync(150);
    expect(await run).toBeInstanceOf(Error);
    expect((await run)?.message).toMatch(/write|permission|read.only/i);
    expect(wires[0].send.mock.calls.some(call => JSON.parse(call[0]).type === "WorkflowExecuteRequest")).toBe(false);
  });
  it("reports malformed metadata and allows retry without creating a stale session", async () => {
    let broken = true;
    const { controller, sharedModels } = setup({
      override: path =>
        path === "resources/operator-metadata" && broken ? Response.json({ operators: null }) : undefined,
    });
    await controller.load();
    expect(controller.getSnapshot()).toMatchObject({ loading: false, error: expect.stringMatching(/metadata/i) });
    expect(sharedModels).toHaveLength(0);
    broken = false;
    await controller.load();
    expect(controller.session).toBeDefined();
    expect(controller.getSnapshot().error).toBeUndefined();
  });
  it("flushes edits on graceful dispose behind in-flight saves and never reopens disposed controllers", async () => {
    let finish: ((value: Response) => void) | undefined;
    let saves = 0;
    const { controller, fetcher, wires } = setup({
      override: path =>
        path === "workflow/persist" && ++saves === 1
          ? new Promise(resolve => {
              finish = resolve;
            })
          : undefined,
    });
    await controller.load();
    controller.session!.rename("first");
    const first = controller.session!.save();
    await flush();
    controller.session!.rename("second");
    const disposed = controller.dispose();
    await flush();
    expect(wires[0].close).toHaveBeenCalledOnce();
    expect(saves).toBe(1);
    finish!(Response.json(workflow()));
    await first;
    await disposed;
    const payloads = fetcher.mock.calls
      .filter(call => String(call[0]).endsWith("workflow/persist"))
      .map(call => JSON.parse(String(call[1]?.body)));
    expect(payloads.map(payload => payload.name)).toEqual(["first", "second"]);
    await expect(controller.load()).rejects.toThrow(/closed/i);
  });
  it("aborts in-flight loads on dispose and ignores late responses", async () => {
    let finish: ((value: Response) => void) | undefined;
    let signal: AbortSignal | undefined;
    const { controller, sharedModels } = setup({
      override: (path, init) => {
        if (path !== "workflow/42") return;
        signal = init?.signal ?? undefined;
        return new Promise(resolve => {
          finish = resolve;
        });
      },
    });
    const load = controller.load();
    await flush();
    await controller.dispose();
    expect(signal?.aborted).toBe(true);
    finish!(Response.json(workflow()));
    await load;
    expect(sharedModels).toHaveLength(0);
    expect(controller.session).toBeUndefined();
  });
});
