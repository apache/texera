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
  ALL_COLUMNS,
  OperatorPaginationResultService,
  OperatorResultService,
  WorkflowResultService,
} from "./workflow-result.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { firstValueFrom, of, Subject } from "rxjs";
import { SchemaAttribute } from "../../types/workflow-compiling.interface";
import {
  WebDataUpdate,
  WebPaginationUpdate,
  WebResultUpdate,
  WorkflowResultTableStats,
} from "../../types/execute-workflow.interface";
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
  let service: WorkflowResultService, ws: WorkflowWebsocketService;

  const sendResults = (
    updates: Record<string, WebResultUpdate | undefined>,
    tableStats: WorkflowResultTableStats = {}
  ) => pushWsEvent(ws, { type: "WebResultUpdateEvent", updates, tableStats });

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowResultService, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowResultService);
    ws = TestBed.inject(WorkflowWebsocketService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("clearResults() drops cached operator results", () => {
    sendResults({ op1: snapshotUpdate([]), op2: paginationUpdate(0) });
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
    sendResults(updates);

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

  describe("getPortPaginatedResultService", () => {
    it("gives the first port's service for port 0, and nothing for an operator without a table", () => {
      const ws = TestBed.inject(WorkflowWebsocketService);
      pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { pagOp: paginationUpdate(3) }, tableStats: {} });

      expect(service.getPortPaginatedResultService("pagOp", 0)).toBe(service.getPaginatedResultService("pagOp"));
      expect(service.getPortPaginatedResultService("noResult", 0)).toBeUndefined();
      expect(service.getPortPaginatedResultService("noResult", 1)).toBeUndefined();
    });

    it("makes one service for another port, kept until the results are cleared", () => {
      const ws = TestBed.inject(WorkflowWebsocketService);
      pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { pagOp: paginationUpdate(3) }, tableStats: {} });

      const second = service.getPortPaginatedResultService("pagOp", 1)!;

      expect(second.portIndex).toBe(1);
      expect(second.operatorID).toBe("pagOp");
      expect(second).not.toBe(service.getPaginatedResultService("pagOp"));
      expect(service.getPortPaginatedResultService("pagOp", 1)).toBe(second);

      service.clearResults();
      pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { pagOp: paginationUpdate(3) }, tableStats: {} });
      expect(service.getPortPaginatedResultService("pagOp", 1)).not.toBe(second);
    });
  });

  it("hasNonEmptyResult tells an empty result apart from one with rows", () => {
    sendResults({
      emptyPag: paginationUpdate(0),
      fullPag: paginationUpdate(5),
      emptySnap: snapshotUpdate([]),
      fullSnap: snapshotUpdate([{ a: 1 }]),
    });

    // A service exists for the empty step (hasAnyResult), but it holds nothing.
    expect(service.hasAnyResult("emptyPag")).toBe(true);
    expect(service.hasNonEmptyResult("emptyPag")).toBe(false);
    expect(service.hasNonEmptyResult("fullPag")).toBe(true);
    expect(service.hasNonEmptyResult("emptySnap")).toBe(false);
    expect(service.hasNonEmptyResult("fullSnap")).toBe(true);
    expect(service.hasNonEmptyResult("never-ran")).toBe(false);
  });

  it("announces newly-created operators on the result-initiate stream", () => {
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    sendResults({ pagOp: paginationUpdate(1), dataOp: snapshotUpdate([]) });

    expect(initiated).toEqual(["pagOp", "dataOp"]);
  });

  it("resolves a macro result to the existing leaf service and forwards boundary updates", () => {
    service.setMacroAliases(() => ({ macro: { operatorID: "leaf", portIndex: 0 } }));
    const updates: Record<string, unknown>[] = [];
    service.getResultUpdateStream().subscribe(update => updates.push(update));
    sendResults({ leaf: snapshotUpdate([{ a: 1 }]) });
    expect(service.getResultService("macro")).toBe(service.getResultService("leaf"));
    expect(service.hasNonEmptyResult("macro")).toBe(true);
    expect(updates[0]["macro"]).toBe(updates[0]["leaf"]);
    service.clearResults();
    expect(service.hasAnyResult("macro")).toBe(false);
  });

  it("keeps replayed results when the execution identity arrives after them", () => {
    const send = vi.spyOn(ws, "send").mockImplementation(() => {});
    sendResults({ op: paginationUpdate(3) });
    pushWsEvent(ws, { type: "WorkflowStateEvent", state: "Completed", executionId: 23 });
    expect(service.hasNonEmptyResult("op")).toBe(true);
    service.getPaginatedResultService("op")!.selectPage(1, 10);
    expect(send.mock.calls.at(-1)?.[1]).toMatchObject({ operatorID: "op", executionId: 23 });
  });

  it("pages the exact output of the reported execution and discards its cache on the next run", () => {
    const send = vi.spyOn(ws, "send").mockImplementation(() => {});
    service.setMacroAliases((port = 0) => ({
      macro: port === 0 ? { operatorID: "split", portIndex: 1 } : { operatorID: "leaf", portIndex: 0 },
    }));
    pushWsEvent(ws, { type: "WorkflowStateEvent", state: "Running", executionId: 23 });
    const updates: Record<string, unknown>[] = [];
    service.getResultUpdateStream().subscribe(update => updates.push(update));
    const stats: Record<string, unknown>[] = [];
    service.getResultTableStats().subscribe(([, current]) => stats.push(current));
    sendResults({ split: paginationUpdate(0) }, { split: { firstColumn: { min: 1 } } });
    expect(updates.at(-1)).not.toHaveProperty("macro");
    expect(stats.at(-1)).not.toHaveProperty("macro");
    const selected = service.getPaginatedResultService("macro")!;
    const request = send.mock.calls.at(-1)![1] as { requestID: string };
    expect(request).toMatchObject({ operatorID: "split", portIndex: 1, executionId: 23 });
    pushWsEvent(ws, {
      type: "PaginatedResultEvent",
      requestID: request.requestID,
      operatorID: "split",
      portIndex: 1,
      pageIndex: 1,
      totalTuples: 7,
      table: [],
      schema: [],
    });
    expect(service.hasNonEmptyResult("macro")).toBe(true);
    expect(service.hasNonEmptyResult("split")).toBe(false);
    expect(updates.at(-1)).toEqual({ macro: paginationUpdate(7) });
    sendResults({ leaf: paginationUpdate(11) });
    const second = service.getPortPaginatedResultService("macro", 1)!;
    expect(second).toBe(service.getPaginatedResultService("leaf"));
    second.peekFirstRows(5);
    expect(send.mock.calls.at(-1)![1]).toMatchObject({ operatorID: "leaf", executionId: 23 });
    pushWsEvent(ws, { type: "WorkflowStateEvent", state: "Running", executionId: 24 });
    expect(service.getPaginatedResultService("macro")).toBeUndefined();
    expect(selected.getCurrentTotalNumTuples()).toBe(7);
  });

  it("hides results and updates when the visible macro draft does not match the executed revision", () => {
    sendResults({ leaf: snapshotUpdate([{ a: 1 }]) });
    service.setMacroAliases(() => ({ leaf: null }));
    expect(service.hasAnyResult("leaf")).toBe(false);
    const updates: Record<string, unknown>[] = [];
    service.getResultUpdateStream().subscribe(update => updates.push(update));
    sendResults({ leaf: snapshotUpdate([{ a: 2 }]) });
    expect(updates.at(-1)).toEqual({});
    service.setMacroAliases(() => ({}));
    expect(service.getResultService("leaf")?.getCurrentResultSnapshot()).toEqual([{ a: 2 }]);
  });

  it("creates no result service for a cleared operator entry but still forwards the update record", () => {
    const updateEvents: Record<string, unknown>[] = [];
    service.getResultUpdateStream().subscribe(u => updateEvents.push(u));
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    // An undefined entry means the operator's result was cleared: it is neither a pagination
    // nor a data update, so no service of either kind may be created for it.
    const updates = { clearedOp: undefined };
    sendResults(updates);

    expect(service.hasResult("clearedOp")).toBe(false);
    expect(service.hasPaginatedResult("clearedOp")).toBe(false);
    expect(service.hasAnyResult("clearedOp")).toBe(false);
    expect(initiated).toEqual([]);
    // the record itself is still republished verbatim so consumers can drop the stale frame
    expect(updateEvents.length).toBe(1);
    expect(updateEvents[0]).toBe(updates);
  });

  it("reuses the existing result service for a second data update and announces the operator only once", () => {
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    sendResults({ op: snapshotUpdate([{ a: 1 }]) });
    const firstService = service.getResultService("op");
    expect(firstService).toBeDefined();

    sendResults({ op: snapshotUpdate([{ b: 2 }]) });

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
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    // tableStats also names pagOp, so handleTableStatsUpdate reaches the same get-or-init path in
    // the same frame; neither that nor the second frame may re-announce the operator.
    sendResults({ pagOp: paginationUpdate(3) }, { pagOp: { colA: { count: 1 } } });
    const firstService = service.getPaginatedResultService("pagOp");
    expect(firstService).toBeDefined();

    sendResults({ pagOp: paginationUpdate(9) });

    expect(service.getPaginatedResultService("pagOp")).toBe(firstService);
    expect(service.getPaginatedResultService("pagOp")!.getCurrentTotalNumTuples()).toBe(9);
    expect(initiated).toEqual(["pagOp"]);
  });

  it("feeds table stats to the matching paginated service and republishes the snapshot", () => {
    const statsPairs: [unknown, unknown][] = [];
    service.getResultTableStats().subscribe(p => statsPairs.push(p));

    const tableStats = { pagOp: { colA: { count: 5 } } };
    sendResults({ pagOp: paginationUpdate(3) }, tableStats);

    expect(service.getPaginatedResultService("pagOp")!.getStats()).toEqual({ colA: { count: 5 } });
    expect(statsPairs[statsPairs.length - 1][1]).toEqual(tableStats);
  });

  it("switches an operator from data output to paginated output and clears the stale service", () => {
    sendResults({ op: snapshotUpdate([{ a: 1 }]) });
    expect(service.hasResult("op")).toBe(true);
    expect(service.hasPaginatedResult("op")).toBe(false);

    sendResults({ op: paginationUpdate(9) });
    expect(service.hasPaginatedResult("op")).toBe(true);
    expect(service.hasResult("op")).toBe(false);

    sendResults({ op: snapshotUpdate([{ b: 2 }]) });
    expect(service.hasResult("op")).toBe(true);
    expect(service.hasPaginatedResult("op")).toBe(false);
  });

  it("determineOutputTypes distinguishes binary from non-binary paginated output", () => {
    sendResults({ binOp: paginationUpdate(1), tblOp: paginationUpdate(1) });
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
    sendResults({ vizOp: snapshotUpdate([{ a: 1 }]) });

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
    sendResults({ vizOp: snapshotUpdate([{ a: 1 }]) });
    expect(service.determineOutputExtension("vizOp", "csv")).toBe("html");
  });

  it("determineOutputExtension keeps csv for table output and passes other extensions through", () => {
    sendResults({ tblOp: paginationUpdate(1) });

    expect(service.determineOutputExtension("tblOp", "csv")).toBe("csv");
    expect(service.determineOutputExtension("tblOp", "json")).toBe("json");
    // no result at all -> the (defaulted) extension is returned unchanged
    expect(service.determineOutputExtension("missing")).toBe("csv");
  });
});

describe("OperatorPaginationResultService", () => {
  let service: OperatorPaginationResultService;
  let mockWorkflowWebsocketService: Mocked<WorkflowWebsocketService>;
  let eventSubject: Subject<PaginatedResultEvent>;

  beforeEach(() => {
    eventSubject = new Subject<PaginatedResultEvent>();
    mockWorkflowWebsocketService = {
      subscribeToEvent: vi.fn().mockReturnValue(eventSubject.asObservable()),
      send: vi.fn(),
    } as unknown as Mocked<WorkflowWebsocketService>;

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
    it.each([
      { index: 1, rows: 3, expected: { id: 2, name: "Bob" } },
      { index: 2, rows: 2, expected: undefined },
    ])("reads tuple $index from a page with $rows rows", async ({ index, rows, expected }) => {
      const schema: SchemaAttribute[] = [
        { attributeName: "id", attributeType: "integer" },
        { attributeName: "name", attributeType: "string" },
      ];
      const table = ["Alice", "Bob", "Charlie"].slice(0, rows).map((name, i) => ({ id: i + 1, name }));
      const result = firstValueFrom(service.selectTuple(index, 3));
      const request = mockWorkflowWebsocketService.send.mock.calls[0][1] as { requestID: string };
      eventSubject.next({
        requestID: request.requestID,
        operatorID: "testOperator",
        portIndex: 0,
        pageIndex: 1,
        totalTuples: rows,
        table,
        schema,
      });
      expect(await result).toEqual({ tuple: expected, schema });
    });
  });

  describe("selectPage", () => {
    it("omits a column limit larger than the server can represent", () => {
      const columnLimit = Number.MAX_SAFE_INTEGER;
      service.selectPage(1, 10, 0, columnLimit).subscribe();

      const payload = mockWorkflowWebsocketService.send.mock.calls[0][1];
      expect(JSON.parse(JSON.stringify(payload))).not.toHaveProperty("columnLimit");
    });

    it.each([0, 25])("preserves explicit column limit %s and bypasses the full-page cache", columnLimit => {
      (service as any).resultCache.set(1, [{ id: 1 }]);
      service.selectPage(1, 10, 0, columnLimit).subscribe();

      expect(mockWorkflowWebsocketService.send).toHaveBeenCalledExactlyOnceWith(
        "ResultPaginationRequest",
        expect.objectContaining({ pageIndex: 1, pageSize: 10, columnOffset: 0, columnLimit })
      );
    });

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
        portIndex: 0,
        totalTuples: 0,
      });
      expect(service.getCurrentPageIndex()).toBe(2);
    });

    it("fetches from the server and resolves once the matching page event arrives", () => {
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
        portIndex: 0,
        totalTuples: 1,
      };
      eventSubject.next(page);

      expect(received).toEqual([page]);
      // schema is refreshed from the event and the pending request is cleared once completed
      expect(service.getSchema()).toEqual([{ attributeName: "id", attributeType: "integer" }]);
      expect((service as any).pendingRequests.size).toBe(0);
    });

    it("asks for every column within the server's Int when no column limit is given", () => {
      service.selectPage(3, 10).subscribe();

      expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith(
        "ResultPaginationRequest",
        expect.objectContaining({ columnLimit: ALL_COLUMNS })
      );
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

  describe("peekFirstRows", () => {
    // A hover preview asks for the first rows while the result table may be on any page; the
    // table's page and cache must come through untouched.
    it("fetches the first rows without moving the table's page or reading its cache", () => {
      (service as any).currentPageIndex = 4;
      (service as any).resultCache.set(1, [{ id: "cached" }]);
      const received: PaginatedResultEvent[] = [];

      service.peekFirstRows(5).subscribe(e => received.push(e));

      expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith(
        "ResultPaginationRequest",
        expect.objectContaining({
          operatorID: "testOperator",
          pageIndex: 1,
          pageSize: 5,
          columnOffset: 0,
          // within the server's Int, which a larger number fails to parse as
          columnLimit: 2147483647,
        })
      );
      const requestID = (mockWorkflowWebsocketService.send.mock.calls[0][1] as any).requestID;
      const page: PaginatedResultEvent = {
        requestID,
        operatorID: "testOperator",
        pageIndex: 1,
        table: [{ id: 1 }],
        schema: [{ attributeName: "id", attributeType: "integer" }],
        portIndex: 0,
        totalTuples: 1,
      };
      eventSubject.next(page);

      expect(received).toEqual([page]);
      expect(service.getCurrentPageIndex()).toBe(4);
      expect((service as any).resultCache.get(1)).toEqual([{ id: "cached" }]);
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

  describe("for an output port past the first", () => {
    let secondPort: OperatorPaginationResultService;
    const reply = (requestID: string, totalTuples: number): PaginatedResultEvent => ({
      requestID,
      operatorID: "testOperator",
      pageIndex: 1,
      table: [{ id: 1 }],
      schema: [{ attributeName: "id", attributeType: "integer" }],
      portIndex: 1,
      totalTuples,
    });

    beforeEach(() => {
      secondPort = new OperatorPaginationResultService("testOperator", mockWorkflowWebsocketService, 1);
    });

    it("names the port in every request, and the first port's service leaves it out", () => {
      secondPort.selectPage(1, 10).subscribe();
      secondPort.peekFirstRows(5).subscribe();
      service.selectPage(1, 10).subscribe();

      const sent = mockWorkflowWebsocketService.send.mock.calls.map(call => call[1] as any);
      expect(sent[0].portIndex).toBe(1);
      expect(sent[1].portIndex).toBe(1);
      expect("portIndex" in sent[2]).toBe(false);
    });

    it("takes its row count from each reply, as no live update reports on it", () => {
      secondPort.selectPage(1, 10).subscribe();
      const requestID = (mockWorkflowWebsocketService.send.mock.calls[0][1] as any).requestID;

      eventSubject.next(reply(requestID, 37));

      expect(secondPort.getCurrentTotalNumTuples()).toBe(37);
      expect(secondPort.getSchema()).toEqual([{ attributeName: "id", attributeType: "integer" }]);
      // the first port's service did not ask, so the reply changes nothing there
      expect(service.getCurrentTotalNumTuples()).toBe(0);
      expect(service.getSchema()).toEqual([]);
    });

    it("asks the server again for a page it has read, so the page is never stale", () => {
      (secondPort as any).resultCache.set(1, [{ id: 0 }]);

      secondPort.selectPage(1, 10).subscribe();

      expect(mockWorkflowWebsocketService.send).toHaveBeenCalledTimes(1);
    });
  });

  describe("incoming page events", () => {
    it("keeps its schema when another port or operator receives a reply", () => {
      const schema: SchemaAttribute[] = [{ attributeName: "name", attributeType: "string" }];

      eventSubject.next({
        portIndex: 0,
        totalTuples: 1,
        requestID: "no-such-request",
        operatorID: "testOperator",
        pageIndex: 1,
        table: [],
        schema,
      });

      expect(service.getSchema()).toEqual([]);
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
