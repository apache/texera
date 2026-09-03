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

import { TestBed } from "@angular/core/testing";
import {
  OperatorPaginationResultService,
  OperatorResultService,
  WorkflowResultService,
} from "./workflow-result.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { firstValueFrom, of, Subject } from "rxjs";
import { SchemaAttribute } from "../../types/workflow-compiling.interface";
import { WebDataUpdate, WebPaginationUpdate } from "../../types/execute-workflow.interface";
import { PaginatedResultEvent } from "../../types/workflow-websocket.interface";
import { commonTestProviders } from "../../../common/testing/test-utils";
import type { Mocked } from "vitest";

/**
 * Push a raw websocket event through the real WorkflowWebsocketService's response subject so the
 * WorkflowResultService's constructor subscriptions fire exactly as they would against a live socket.
 */
function pushWsEvent(ws: WorkflowWebsocketService, event: Record<string, unknown>): void {
  (ws as any).webSocketResponseSubject.next(event);
}

function paginationUpdate(totalNumTuples: number, dirtyPageIndices: number[] = []): WebPaginationUpdate {
  return { mode: { type: "PaginationMode" }, totalNumTuples, dirtyPageIndices };
}

function snapshotUpdate(table: ReadonlyArray<object>): WebDataUpdate {
  return { mode: { type: "SetSnapshotMode" }, table };
}

describe("WorkflowResultService", () => {
  let service: WorkflowResultService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowResultService, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowResultService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("clearResults() drops cached operator results", () => {
    (service as any).operatorResultServices.set("op1", {});
    (service as any).paginatedResultServices.set("op2", {});
    expect(service.hasAnyResult("op1")).toBe(true);
    expect(service.hasAnyResult("op2")).toBe(true);

    service.clearResults();

    expect(service.hasAnyResult("op1")).toBe(false);
    expect(service.hasAnyResult("op2")).toBe(false);
  });

  it("clearResults() resets table stats to empty for subscribers", () => {
    const pairs: [unknown, unknown][] = [];
    service.getResultTableStats().subscribe(p => pairs.push(p));
    (service as any).resultTableStats.next({ op1: {} });
    service.clearResults();
    expect(pairs[pairs.length - 1][1]).toEqual({});
  });

  it("clearResults() emits on the cleared stream so the UI tears down stale frames", () => {
    let clearedCount = 0;
    service.getResultClearedStream().subscribe(() => clearedCount++);
    service.clearResults();
    expect(clearedCount).toBe(1);
  });

  it("routes pagination updates to a paginated service and data updates to a result service", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    const updateEvents: Record<string, unknown>[] = [];
    // Sampled from INSIDE the subscriber so the emit point is pinned, not just the payload:
    // consumers (visualization-frame-content) read the operator's result service back out of this
    // service while handling the event, so routing must be finished before the stream fires.
    const routedAtEmit: boolean[] = [];
    service.getResultUpdateStream().subscribe(u => {
      updateEvents.push(u);
      routedAtEmit.push(service.hasResult("dataOp"), service.hasPaginatedResult("pagOp"));
    });

    const updates = { pagOp: paginationUpdate(42, [2]), dataOp: snapshotUpdate([{ a: 1 }]) };
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates, tableStats: {} });

    expect(routedAtEmit).toEqual([true, true]);
    expect(service.hasPaginatedResult("pagOp")).toBe(true);
    expect(service.hasResult("pagOp")).toBe(false);
    expect(service.hasResult("dataOp")).toBe(true);
    expect(service.hasPaginatedResult("dataOp")).toBe(false);
    expect(service.hasAnyResult("pagOp")).toBe(true);
    expect(service.getPaginatedResultService("pagOp")!.getCurrentTotalNumTuples()).toBe(42);
    expect(service.getResultService("dataOp")!.getCurrentResultSnapshot()).toEqual([{ a: 1 }]);
    // the raw update record is republished verbatim on the result-update stream
    expect(updateEvents).toEqual([updates]);
  });

  it("announces newly-created operators on the result-initiate stream", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { pagOp: paginationUpdate(1), dataOp: snapshotUpdate([]) },
      tableStats: {},
    });

    expect(initiated).toEqual(["pagOp", "dataOp"]);
  });

  it("creates no result service for a cleared operator entry but still forwards the update record", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    const updateEvents: Record<string, unknown>[] = [];
    service.getResultUpdateStream().subscribe(u => updateEvents.push(u));
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    // An undefined entry means the operator's result was cleared: it is neither a pagination
    // nor a data update, so no service of either kind may be created for it.
    const updates = { clearedOp: undefined };
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates, tableStats: {} });

    expect(service.hasResult("clearedOp")).toBe(false);
    expect(service.hasPaginatedResult("clearedOp")).toBe(false);
    expect(service.hasAnyResult("clearedOp")).toBe(false);
    expect(initiated).toEqual([]);
    // the record itself is still republished verbatim so consumers can drop the stale frame
    expect(updateEvents.length).toBe(1);
    expect(updateEvents[0]).toBe(updates);
  });

  it("reuses the existing result service for a second data update and announces the operator only once", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: snapshotUpdate([{ a: 1 }]) }, tableStats: {} });
    const firstService = service.getResultService("op");
    expect(firstService).toBeDefined();

    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: snapshotUpdate([{ b: 2 }]) }, tableStats: {} });

    // the SAME instance is reused for the second update -- this identity check carries the whole
    // reuse claim on its own
    expect(service.getResultService("op")).toBe(firstService);
    // a plain shape check on the routed payload; SetSnapshotMode REPLACES the snapshot rather than
    // accumulating, so this value alone would also hold for a freshly-constructed service
    expect(service.getResultService("op")!.getCurrentResultSnapshot()).toEqual([{ b: 2 }]);
    // and the operator is announced only on first creation, not on every update
    expect(initiated).toEqual(["op"]);
  });

  it("reuses the existing paginated service for a second pagination update and announces it only once", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    // tableStats also names pagOp, so handleTableStatsUpdate reaches the same get-or-init path in
    // the same frame; neither that nor the second frame may re-announce the operator.
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { pagOp: paginationUpdate(3) },
      tableStats: { pagOp: { colA: { count: 1 } } },
    });
    const firstService = service.getPaginatedResultService("pagOp");
    expect(firstService).toBeDefined();

    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { pagOp: paginationUpdate(9) }, tableStats: {} });

    expect(service.getPaginatedResultService("pagOp")).toBe(firstService);
    expect(service.getPaginatedResultService("pagOp")!.getCurrentTotalNumTuples()).toBe(9);
    expect(initiated).toEqual(["pagOp"]);
  });

  it("feeds table stats to the matching paginated service and republishes the snapshot", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    const statsPairs: [unknown, unknown][] = [];
    service.getResultTableStats().subscribe(p => statsPairs.push(p));

    const tableStats = { pagOp: { colA: { count: 5 } } };
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { pagOp: paginationUpdate(3) }, tableStats });

    expect(service.getPaginatedResultService("pagOp")!.getStats()).toEqual({ colA: { count: 5 } });
    expect(statsPairs[statsPairs.length - 1][1]).toEqual(tableStats);
  });

  it("switches an operator from data output to paginated output and clears the stale service", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: snapshotUpdate([{ a: 1 }]) }, tableStats: {} });
    expect(service.hasResult("op")).toBe(true);
    expect(service.hasPaginatedResult("op")).toBe(false);

    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: paginationUpdate(9) }, tableStats: {} });
    expect(service.hasPaginatedResult("op")).toBe(true);
    expect(service.hasResult("op")).toBe(false);

    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: snapshotUpdate([{ b: 2 }]) }, tableStats: {} });
    expect(service.hasResult("op")).toBe(true);
    expect(service.hasPaginatedResult("op")).toBe(false);
  });

  it("determineOutputTypes distinguishes binary from non-binary paginated output", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { binOp: paginationUpdate(1), tblOp: paginationUpdate(1) },
      tableStats: {},
    });
    (service.getPaginatedResultService("binOp") as any).schema = [{ attributeName: "b", attributeType: "binary" }];
    (service.getPaginatedResultService("tblOp") as any).schema = [{ attributeName: "s", attributeType: "string" }];

    expect(service.determineOutputTypes("binOp")).toEqual({
      hasAnyResult: true,
      isTableOutput: true,
      containsBinaryData: true,
      isVisualizationOutput: false,
    });
    expect(service.determineOutputTypes("tblOp")).toEqual({
      hasAnyResult: true,
      isTableOutput: true,
      containsBinaryData: false,
      isVisualizationOutput: false,
    });
  });

  it("determineOutputTypes marks a data-only operator as visualization output", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { vizOp: snapshotUpdate([{ a: 1 }]) }, tableStats: {} });

    expect(service.determineOutputTypes("vizOp")).toEqual({
      hasAnyResult: true,
      isTableOutput: false,
      containsBinaryData: false,
      isVisualizationOutput: true,
    });
  });

  it("determineOutputTypes returns all-false for an operator with no results", () => {
    expect(service.determineOutputTypes("missing")).toEqual({
      hasAnyResult: false,
      isTableOutput: false,
      containsBinaryData: false,
      isVisualizationOutput: false,
    });
  });

  it("determineOutputExtension short-circuits the 'data' extension", () => {
    expect(service.determineOutputExtension("anything", "data")).toBe("data");
  });

  it("determineOutputExtension returns html for visualization output", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { vizOp: snapshotUpdate([{ a: 1 }]) }, tableStats: {} });
    expect(service.determineOutputExtension("vizOp", "csv")).toBe("html");
  });

  it("determineOutputExtension keeps csv for table output and passes other extensions through", () => {
    const ws = TestBed.inject(WorkflowWebsocketService);
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { tblOp: paginationUpdate(1) }, tableStats: {} });

    expect(service.determineOutputExtension("tblOp", "csv")).toBe("csv");
    expect(service.determineOutputExtension("tblOp", "json")).toBe("json");
    // no result at all -> the (defaulted) extension is returned unchanged
    expect(service.determineOutputExtension("missing")).toBe("csv");
  });
});

describe("OperatorPaginationResultService", () => {
  let service: OperatorPaginationResultService;
  let mockWorkflowWebsocketService: Mocked<WorkflowWebsocketService>;

  beforeEach(() => {
    mockWorkflowWebsocketService = {
      subscribeToEvent: vi.fn(),
      send: vi.fn(),
    } as unknown as Mocked<WorkflowWebsocketService>;
    mockWorkflowWebsocketService.subscribeToEvent.mockReturnValue(new Subject());

    service = new OperatorPaginationResultService("testOperator", mockWorkflowWebsocketService);
  });

  describe("getSchema", () => {
    it("should return the current schema", () => {
      const testSchema: SchemaAttribute[] = [
        { attributeName: "id", attributeType: "integer" },
        { attributeName: "name", attributeType: "string" },
      ];
      service["schema"] = testSchema;

      expect(service.getSchema()).toEqual(testSchema);
    });
  });

  describe("selectTuple", () => {
    it("should return the correct tuple and schema", async () => {
      const testSchema: SchemaAttribute[] = [
        { attributeName: "id", attributeType: "integer" },
        { attributeName: "name", attributeType: "string" },
      ];
      service["schema"] = testSchema;

      const testTable = [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
        { id: 3, name: "Charlie" },
      ];

      vi.spyOn(service, "selectPage").mockReturnValue(
        of({
          requestID: "test",
          operatorID: "testOperator",
          pageIndex: 1,
          table: testTable,
          schema: testSchema,
        })
      );

      const result = await firstValueFrom(service.selectTuple(1, 3));
      expect(result.tuple).toEqual({ id: 2, name: "Bob" });
      expect(result.schema).toEqual(testSchema);
    });

    it("should handle out-of-bounds tuple index", async () => {
      const testSchema: SchemaAttribute[] = [
        { attributeName: "id", attributeType: "integer" },
        { attributeName: "name", attributeType: "string" },
      ];
      service["schema"] = testSchema;

      const testTable = [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ];

      vi.spyOn(service, "selectPage").mockReturnValue(
        of({
          requestID: "test",
          operatorID: "testOperator",
          pageIndex: 1,
          table: testTable,
          schema: testSchema,
        })
      );

      const result = await firstValueFrom(service.selectTuple(2, 3));
      expect(result.tuple).toBeUndefined();
      expect(result.schema).toEqual(testSchema);
    });
  });

  describe("selectPage", () => {
    it("returns the cached page without contacting the server", () => {
      const cachedPage = [{ id: 1 }, { id: 2 }];
      (service as any).resultCache.set(2, cachedPage);
      service["schema"] = [{ attributeName: "id", attributeType: "integer" }];

      let result: PaginatedResultEvent | undefined;
      service.selectPage(2, 10).subscribe(e => (result = e));

      expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
      expect(result).toEqual({
        requestID: "",
        operatorID: "testOperator",
        pageIndex: 2,
        table: cachedPage,
        schema: [{ attributeName: "id", attributeType: "integer" }],
      });
      expect(service.getCurrentPageIndex()).toBe(2);
    });

    it("fetches from the server and resolves once the matching page event arrives", () => {
      const eventSubject = mockWorkflowWebsocketService.subscribeToEvent.mock.results[0]
        .value as unknown as Subject<PaginatedResultEvent>;
      const received: PaginatedResultEvent[] = [];
      service.selectPage(3, 10).subscribe(e => received.push(e));

      expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith(
        "ResultPaginationRequest",
        expect.objectContaining({ operatorID: "testOperator", pageIndex: 3, pageSize: 10 })
      );
      expect(service.getCurrentPageIndex()).toBe(3);

      const requestID = (mockWorkflowWebsocketService.send.mock.calls[0][1] as any).requestID;
      const page: PaginatedResultEvent = {
        requestID,
        operatorID: "testOperator",
        pageIndex: 3,
        table: [{ id: 7 }],
        schema: [{ attributeName: "id", attributeType: "integer" }],
      };
      eventSubject.next(page);

      expect(received).toEqual([page]);
      // schema is refreshed from the event and the pending request is cleared once completed
      expect(service.getSchema()).toEqual([{ attributeName: "id", attributeType: "integer" }]);
      expect((service as any).pendingRequests.size).toBe(0);
    });

    it("bypasses the frontend cache when column filters are set", () => {
      (service as any).resultCache.set(2, [{ id: 1 }]);
      service.selectPage(2, 10, 5).subscribe();

      expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith(
        "ResultPaginationRequest",
        expect.objectContaining({ pageIndex: 2, columnOffset: 5 })
      );
    });
  });

  describe("handleResultUpdate", () => {
    it("records the total tuple count and evicts only the dirty pages", () => {
      (service as any).resultCache.set(1, [{ id: 1 }]);
      (service as any).resultCache.set(2, [{ id: 2 }]);

      service.handleResultUpdate({ mode: { type: "PaginationMode" }, totalNumTuples: 55, dirtyPageIndices: [2] });

      expect(service.getCurrentTotalNumTuples()).toBe(55);
      expect((service as any).resultCache.has(1)).toBe(true);
      expect((service as any).resultCache.has(2)).toBe(false);
    });
  });

  describe("handleStatsUpdate", () => {
    it("replaces the current stats on each update", () => {
      const first = { colA: { count: 1 } };
      const second = { colA: { count: 2 } };

      expect(service.getStats()).toEqual({});

      service.handleStatsUpdate(first);
      expect(service.getStats()).toEqual(first);

      service.handleStatsUpdate(second);
      expect(service.getStats()).toEqual(second);
    });
  });

  describe("reset", () => {
    it("clears caches, pending requests, page index and tuple total", () => {
      (service as any).resultCache.set(1, [{ id: 1 }]);
      (service as any).pendingRequests.set("req", new Subject());
      (service as any).currentPageIndex = 5;
      (service as any).currentTotalNumTuples = 99;

      service.reset();

      expect((service as any).resultCache.size).toBe(0);
      expect((service as any).pendingRequests.size).toBe(0);
      expect(service.getCurrentPageIndex()).toBe(1);
      expect(service.getCurrentTotalNumTuples()).toBe(0);
    });
  });

  describe("incoming page events", () => {
    it("refreshes the schema but ignores events whose request id is unknown", () => {
      const eventSubject = mockWorkflowWebsocketService.subscribeToEvent.mock.results[0]
        .value as unknown as Subject<PaginatedResultEvent>;
      const schema: SchemaAttribute[] = [{ attributeName: "name", attributeType: "string" }];

      eventSubject.next({ requestID: "no-such-request", operatorID: "testOperator", pageIndex: 1, table: [], schema });

      expect(service.getSchema()).toEqual(schema);
    });
  });
});

describe("OperatorResultService", () => {
  it("exposes the latest snapshot and clears it on reset", () => {
    const resultService = new OperatorResultService("op");
    expect(resultService.getCurrentResultSnapshot()).toBeUndefined();

    const table = [{ a: 1 }];
    resultService.handleResultUpdate({ mode: { type: "SetSnapshotMode" }, table });
    expect(resultService.getCurrentResultSnapshot()).toEqual(table);

    resultService.reset();
    expect(resultService.getCurrentResultSnapshot()).toBeUndefined();
  });

  it("ignores delta-mode updates and keeps the last snapshot", () => {
    const resultService = new OperatorResultService("op");
    resultService.handleResultUpdate({ mode: { type: "SetSnapshotMode" }, table: [{ a: 1 }] });
    resultService.handleResultUpdate({ mode: { type: "SetDeltaMode" }, table: [{ b: 2 }] });
    expect(resultService.getCurrentResultSnapshot()).toEqual([{ a: 1 }]);
  });
});
