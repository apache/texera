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

/** An operator's updates or statistics on its first output port only. */
function onPort0<T>(value: T): Record<number, T> {
  return { 0: value };
}

describe("WorkflowResultService", () => {
  let service: WorkflowResultService;
  let ws: WorkflowWebsocketService;

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
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { op1: onPort0(snapshotUpdate([{ a: 1 }])), op2: { 1: paginationUpdate(3) } },
      tableStats: {},
    });
    expect(service.hasAnyResult("op1")).toBe(true);
    expect(service.hasAnyResult("op2", 1)).toBe(true);

    service.clearResults();

    expect(service.hasAnyResult("op1")).toBe(false);
    expect(service.hasAnyResult("op2", 1)).toBe(false);
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

    const updates = { pagOp: onPort0(paginationUpdate(42, [2])), dataOp: onPort0(snapshotUpdate([{ a: 1 }])) };
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

  describe("an operator's output ports", () => {
    it("gives each port a service of its own, made by that port's updates", () => {
      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        // a port index arrives as a JSON object key
        updates: { split: { "0": paginationUpdate(3), "1": paginationUpdate(12) } },
        tableStats: {},
      });

      const first = service.getPaginatedResultService("split", 0)!;
      const second = service.getPaginatedResultService("split", 1)!;
      expect(first.portIndex).toBe(0);
      expect(second.portIndex).toBe(1);
      expect(second.operatorID).toBe("split");
      expect(first.getCurrentTotalNumTuples()).toBe(3);
      expect(second.getCurrentTotalNumTuples()).toBe(12);
      // a port is asked about by its index, and port 0 is the default
      expect(service.getPaginatedResultService("split")).toBe(first);
      // a port with no result has no service, and asking does not make one
      expect(service.getPaginatedResultService("split", 2)).toBeUndefined();
      expect(service.hasAnyResult("split", 2)).toBe(false);
    });

    it("keeps a port's service across its updates, and an update of one port leaves the others alone", () => {
      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        updates: { split: { 0: paginationUpdate(3), 1: paginationUpdate(4) } },
        tableStats: {},
      });
      const first = service.getPaginatedResultService("split", 0);
      const second = service.getPaginatedResultService("split", 1);

      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        updates: { split: { 1: paginationUpdate(10) } },
        tableStats: {},
      });

      expect(service.getPaginatedResultService("split", 1)).toBe(second);
      expect(second!.getCurrentTotalNumTuples()).toBe(10);
      expect(service.getPaginatedResultService("split", 0)).toBe(first);
      expect(first!.getCurrentTotalNumTuples()).toBe(3);
    });

    it("lets a port be a table while another is a visualization", () => {
      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        updates: { op: { 0: paginationUpdate(4), 1: snapshotUpdate([{ "html-content": "<div></div>" }]) } },
        tableStats: {},
      });

      expect(service.hasPaginatedResult("op", 0)).toBe(true);
      expect(service.hasResult("op", 0)).toBe(false);
      expect(service.hasResult("op", 1)).toBe(true);
      expect(service.hasPaginatedResult("op", 1)).toBe(false);
      expect(service.getResultService("op", 1)!.getCurrentResultSnapshot()).toEqual([
        { "html-content": "<div></div>" },
      ]);
    });

    it("turns one port from a visualization into a table and back without touching another port", () => {
      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        updates: { op: { 0: paginationUpdate(1), 1: snapshotUpdate([{ a: 1 }]) } },
        tableStats: {},
      });
      const first = service.getPaginatedResultService("op", 0);

      pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: { 1: paginationUpdate(5) } }, tableStats: {} });
      expect(service.hasResult("op", 1)).toBe(false);
      expect(service.getPaginatedResultService("op", 1)!.getCurrentTotalNumTuples()).toBe(5);

      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        updates: { op: { 1: snapshotUpdate([{ b: 2 }]) } },
        tableStats: {},
      });
      expect(service.hasPaginatedResult("op", 1)).toBe(false);
      expect(service.getResultService("op", 1)!.getCurrentResultSnapshot()).toEqual([{ b: 2 }]);
      expect(service.getPaginatedResultService("op", 0)).toBe(first);
    });

    it("feeds each port its own table statistics", () => {
      const tableStats = { split: { 0: { colA: { count: 3 } }, 1: { colA: { count: 12 } } } };
      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        updates: { split: { 0: paginationUpdate(3), 1: paginationUpdate(12) } },
        tableStats,
      });

      expect(service.getPaginatedResultService("split", 0)!.getStats()).toEqual({ colA: { count: 3 } });
      expect(service.getPaginatedResultService("split", 1)!.getStats()).toEqual({ colA: { count: 12 } });
    });

    it("keeps the ports of operators whose ids share a prefix apart", () => {
      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        updates: { op1: { 1: paginationUpdate(1) }, op10: { 1: paginationUpdate(2) } },
        tableStats: {},
      });

      expect(service.getPaginatedResultService("op1", 1)!.getCurrentTotalNumTuples()).toBe(1);
      expect(service.getPaginatedResultService("op10", 1)!.getCurrentTotalNumTuples()).toBe(2);
    });

    it("announces an operator when any of its ports gets its first result", () => {
      const initiated: string[] = [];
      service.getResultInitiateStream().subscribe(op => initiated.push(op));

      pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: onPort0(paginationUpdate(1)) }, tableStats: {} });
      pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: { 1: paginationUpdate(1) } }, tableStats: {} });
      pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: { 1: paginationUpdate(2) } }, tableStats: {} });

      expect(initiated).toEqual(["op", "op"]);
    });

    it("determineOutputTypes and determineOutputExtension answer for the port asked about", () => {
      pushWsEvent(ws, {
        type: "WebResultUpdateEvent",
        updates: { op: { 0: snapshotUpdate([{ a: 1 }]), 1: paginationUpdate(4) } },
        tableStats: {},
      });
      (service.getPaginatedResultService("op", 1) as any).schema = [{ attributeName: "b", attributeType: "binary" }];

      expect(service.determineOutputTypes("op", 1)).toEqual({
        hasAnyResult: true,
        isTableOutput: true,
        containsBinaryData: true,
        isVisualizationOutput: false,
      });
      expect(service.determineOutputTypes("op", 0)).toEqual({
        hasAnyResult: true,
        isTableOutput: false,
        containsBinaryData: false,
        isVisualizationOutput: true,
      });
      expect(service.determineOutputTypes("op", 2).hasAnyResult).toBe(false);
      expect(service.determineOutputExtension("op", "csv", 1)).toBe("csv");
      expect(service.determineOutputExtension("op", "csv", 0)).toBe("html");
      // without a port, the first one
      expect(service.determineOutputTypes("op")).toEqual(service.determineOutputTypes("op", 0));
      expect(service.determineOutputExtension("op", "csv")).toBe("html");
    });
  });

  it("hasNonEmptyResult tells an empty result apart from one with rows", () => {
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: {
        emptyPag: onPort0(paginationUpdate(0)),
        fullPag: onPort0(paginationUpdate(5)),
        emptySnap: onPort0(snapshotUpdate([])),
        fullSnap: onPort0(snapshotUpdate([{ a: 1 }])),
        laterPort: { 0: paginationUpdate(0), 1: paginationUpdate(2) },
      },
      tableStats: {},
    });

    // A service exists for the empty step (hasAnyResult), but it holds nothing.
    expect(service.hasAnyResult("emptyPag")).toBe(true);
    expect(service.hasNonEmptyResult("emptyPag")).toBe(false);
    expect(service.hasNonEmptyResult("fullPag")).toBe(true);
    expect(service.hasNonEmptyResult("emptySnap")).toBe(false);
    expect(service.hasNonEmptyResult("fullSnap")).toBe(true);
    expect(service.hasNonEmptyResult("never-ran")).toBe(false);
    expect(service.hasNonEmptyResult("laterPort")).toBe(false);
    expect(service.hasNonEmptyResult("laterPort", 1)).toBe(true);
  });

  it("announces newly-created operators on the result-initiate stream", () => {
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { pagOp: onPort0(paginationUpdate(1)), dataOp: onPort0(snapshotUpdate([])) },
      tableStats: {},
    });

    expect(initiated).toEqual(["pagOp", "dataOp"]);
  });

  it("creates no result service for a cleared port entry but still forwards the update record", () => {
    const updateEvents: Record<string, unknown>[] = [];
    service.getResultUpdateStream().subscribe(u => updateEvents.push(u));
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    // An undefined entry means the port's result was cleared: it is neither a pagination nor a
    // data update, so no service of either kind may be created for it.
    const updates = { clearedOp: { 0: undefined } };
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
    const initiated: string[] = [];
    service.getResultInitiateStream().subscribe(op => initiated.push(op));

    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { op: onPort0(snapshotUpdate([{ a: 1 }])) },
      tableStats: {},
    });
    const firstService = service.getResultService("op");
    expect(firstService).toBeDefined();

    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { op: onPort0(snapshotUpdate([{ b: 2 }])) },
      tableStats: {},
    });

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
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { pagOp: onPort0(paginationUpdate(3)) },
      tableStats: { pagOp: onPort0({ colA: { count: 1 } }) },
    });
    const firstService = service.getPaginatedResultService("pagOp");
    expect(firstService).toBeDefined();

    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { pagOp: onPort0(paginationUpdate(9)) }, tableStats: {} });

    expect(service.getPaginatedResultService("pagOp")).toBe(firstService);
    expect(service.getPaginatedResultService("pagOp")!.getCurrentTotalNumTuples()).toBe(9);
    expect(initiated).toEqual(["pagOp"]);
  });

  it("feeds table stats to the matching paginated service and republishes the snapshot", () => {
    const statsPairs: [unknown, unknown][] = [];
    service.getResultTableStats().subscribe(p => statsPairs.push(p));

    const tableStats = { pagOp: onPort0({ colA: { count: 5 } }) };
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { pagOp: onPort0(paginationUpdate(3)) }, tableStats });

    expect(service.getPaginatedResultService("pagOp")!.getStats()).toEqual({ colA: { count: 5 } });
    expect(statsPairs[statsPairs.length - 1][1]).toEqual(tableStats);
  });

  it("switches an operator from data output to paginated output and clears the stale service", () => {
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { op: onPort0(snapshotUpdate([{ a: 1 }])) },
      tableStats: {},
    });
    expect(service.hasResult("op")).toBe(true);
    expect(service.hasPaginatedResult("op")).toBe(false);

    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { op: onPort0(paginationUpdate(9)) }, tableStats: {} });
    expect(service.hasPaginatedResult("op")).toBe(true);
    expect(service.hasResult("op")).toBe(false);

    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { op: onPort0(snapshotUpdate([{ b: 2 }])) },
      tableStats: {},
    });
    expect(service.hasResult("op")).toBe(true);
    expect(service.hasPaginatedResult("op")).toBe(false);
  });

  it("determineOutputTypes distinguishes binary from non-binary paginated output", () => {
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { binOp: onPort0(paginationUpdate(1)), tblOp: onPort0(paginationUpdate(1)) },
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
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { vizOp: onPort0(snapshotUpdate([{ a: 1 }])) },
      tableStats: {},
    });

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
    pushWsEvent(ws, {
      type: "WebResultUpdateEvent",
      updates: { vizOp: onPort0(snapshotUpdate([{ a: 1 }])) },
      tableStats: {},
    });
    expect(service.determineOutputExtension("vizOp", "csv")).toBe("html");
  });

  it("determineOutputExtension keeps csv for table output and passes other extensions through", () => {
    pushWsEvent(ws, { type: "WebResultUpdateEvent", updates: { tblOp: onPort0(paginationUpdate(1)) }, tableStats: {} });

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
          portIndex: 0,
          totalTuples: testTable.length,
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
          portIndex: 0,
          totalTuples: testTable.length,
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
        portIndex: 0,
        totalTuples: 0,
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
      const eventSubject = mockWorkflowWebsocketService.subscribeToEvent.mock.results[0]
        .value as unknown as Subject<PaginatedResultEvent>;
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

    beforeEach(() => {
      secondPort = new OperatorPaginationResultService("testOperator", mockWorkflowWebsocketService, 1);
    });

    it("names its port in every request, as the first port's service does", () => {
      secondPort.selectPage(1, 10).subscribe();
      secondPort.peekFirstRows(5).subscribe();
      service.selectPage(1, 10).subscribe();

      const sent = mockWorkflowWebsocketService.send.mock.calls.map(call => call[1] as any);
      expect(sent.map(request => request.portIndex)).toEqual([1, 1, 0]);
    });

    it("follows its live updates and pages from its cache, as the first port's service does", () => {
      (secondPort as any).resultCache.set(1, [{ id: 0 }]);
      secondPort.handleResultUpdate({ mode: { type: "PaginationMode" }, totalNumTuples: 37, dirtyPageIndices: [2] });

      secondPort.selectPage(1, 10).subscribe();

      expect(secondPort.getCurrentTotalNumTuples()).toBe(37);
      expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
    });
  });

  describe("incoming page events", () => {
    // Every operator's and every port's replies arrive on one stream, so a reply to another
    // service's request must not hand this one its columns.
    it("ignores an event whose request id is not its own, schema included", () => {
      const eventSubject = mockWorkflowWebsocketService.subscribeToEvent.mock.results[0]
        .value as unknown as Subject<PaginatedResultEvent>;
      const schema: SchemaAttribute[] = [{ attributeName: "name", attributeType: "string" }];

      eventSubject.next({
        requestID: "no-such-request",
        operatorID: "testOperator",
        pageIndex: 1,
        table: [],
        schema,
        portIndex: 0,
        totalTuples: 0,
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
