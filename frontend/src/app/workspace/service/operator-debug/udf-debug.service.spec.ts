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
import { UdfDebugService } from "./udf-debug.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { config as rxjsConfig, Observable, Subject } from "rxjs";
import { OperatorState } from "../../types/execute-workflow.interface";
import { WorkflowGraphReadonly } from "../workflow-graph/model/workflow-graph";
import { mockPoint, mockPythonUDFPredicate } from "../workflow-graph/model/mock-workflow-data";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import * as Y from "yjs";
import { ConsoleMessage, ConsoleUpdateEvent } from "../../types/workflow-common.interface";
import { TexeraWebsocketEvent } from "../../types/workflow-websocket.interface";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { CodeDebuggerComponent } from "../../component/code-editor-dialog/code-debugger.component";
import type { Mocked } from "vitest";
describe("UdfDebugServiceSpec", () => {
  let service: UdfDebugService;
  let workflowActionService: WorkflowActionService;
  let mockWorkflowWebsocketService: Mocked<WorkflowWebsocketService>;
  let mockWorkflowStatusService: Mocked<WorkflowStatusService>;
  let mockExecuteWorkflowService: Mocked<ExecuteWorkflowService>;
  let stateUpdateStream: Subject<Record<string, OperatorState>>;
  let consoleUpdateEventStream: Subject<ConsoleUpdateEvent>;
  let texeraGraph: WorkflowGraphReadonly;
  let stubWorker = "worker1";

  beforeEach(() => {
    // Create mock services
    mockWorkflowWebsocketService = {
      send: vi.fn(),
      subscribeToEvent: vi.fn(),
    } as unknown as Mocked<WorkflowWebsocketService>;
    mockWorkflowStatusService = { getStateUpdateStream: vi.fn() } as unknown as Mocked<WorkflowStatusService>;
    mockExecuteWorkflowService = { getWorkerIds: vi.fn() } as unknown as Mocked<ExecuteWorkflowService>;

    // Initialize the mock streams
    stateUpdateStream = new Subject();
    consoleUpdateEventStream = new Subject();

    // Set mock return values
    mockWorkflowStatusService.getStateUpdateStream.mockReturnValue(stateUpdateStream.asObservable());
    mockWorkflowWebsocketService.subscribeToEvent.mockReturnValue(
      consoleUpdateEventStream.asObservable() as Observable<TexeraWebsocketEvent>
    );
    mockExecuteWorkflowService.getWorkerIds.mockReturnValue([stubWorker]);

    // Configure the TestBed
    TestBed.configureTestingModule({
      providers: [
        UdfDebugService,
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        { provide: WorkflowWebsocketService, useValue: mockWorkflowWebsocketService },
        { provide: WorkflowStatusService, useValue: mockWorkflowStatusService },
        { provide: ExecuteWorkflowService, useValue: mockExecuteWorkflowService },
        ...commonTestProviders,
      ],
    });

    workflowActionService = TestBed.inject(WorkflowActionService);
    texeraGraph = workflowActionService.getTexeraGraph();
    workflowActionService.addOperator(mockPythonUDFPredicate, mockPoint);
    // Spy on the necessary methods
    vi.spyOn(texeraGraph, "createOperatorDebugState");
    vi.spyOn(texeraGraph, "getOperatorDebugState");

    service = TestBed.inject(UdfDebugService);
  });

  afterEach(() => {
    // Clean up the streams after each test
    stateUpdateStream.complete();
    consoleUpdateEventStream.complete();
  });

  it("should give an operator present on service creation a usable debug state", () => {
    service.doModifyBreakpoint(mockPythonUDFPredicate.operatorID, 3);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "break 3",
    });
    expect(texeraGraph.getOperatorDebugState(mockPythonUDFPredicate.operatorID)).toBeInstanceOf(Y.Map);
  });

  it("should retrieve the debug state of an operator", () => {
    const state = service.getDebugState(mockPythonUDFPredicate.operatorID);
    expect(state).toBeInstanceOf(Y.Map);
    expect(state.size).toBe(0); // Initially empty
  });

  it("should get the condition of a breakpoint", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: false });

    const condition = service.getCondition(mockPythonUDFPredicate.operatorID, 1);
    expect(condition).toBe("x > 5");
  });

  it("should return empty string if condition does not exist", () => {
    const condition = service.getCondition(mockPythonUDFPredicate.operatorID, 2);
    expect(condition).toBe("");
  });

  it("should update the breakpoint condition if different", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: false });

    service.doUpdateBreakpointCondition(mockPythonUDFPredicate.operatorID, 1, "x < 10");

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "condition 1 x < 10",
    });

    expect(debugState.get("1")?.condition).toBe("x < 10");
  });

  it("should not update the breakpoint condition if it is the same", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: false });

    service.doUpdateBreakpointCondition(mockPythonUDFPredicate.operatorID, 1, "x > 5");

    expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
  });

  it("should modify a breakpoint (remove existing)", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "", hit: false });

    service.doModifyBreakpoint(mockPythonUDFPredicate.operatorID, 1);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "clear 1",
    });

    expect(debugState.has("1")).toBe(true); // The state is supposed to be cleared later by console update events.
  });

  it("should modify a breakpoint (add new)", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);

    service.doModifyBreakpoint(mockPythonUDFPredicate.operatorID, 10);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "break 10",
    });

    // it should change the state yet
    expect(debugState.has("10")).toBe(false);
  });

  it("should continue the workflow execution", () => {
    service.doContinue(mockPythonUDFPredicate.operatorID, stubWorker);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "continue",
    });
  });

  it("should step through the workflow execution", () => {
    service.doStep(mockPythonUDFPredicate.operatorID, stubWorker);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "next",
    });
  });

  it("should clear the debug state on state change to Uninitialized", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    const operatorId = mockPythonUDFPredicate.operatorID;
    debugState.set(operatorId, { breakpointId: 1, condition: "x > 5", hit: false });
    stateUpdateStream.next({ [operatorId]: OperatorState.Uninitialized });

    expect(debugState.size).toBe(0);
  });

  it("should handle console update events (breakpoint creation)", () => {
    const message: ConsoleUpdateEvent = {
      operatorId: mockPythonUDFPredicate.operatorID,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "Breakpoint 1 at /path/to/file.py:10",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };
    vi.spyOn(service, "doContinue");
    consoleUpdateEventStream.next(message);

    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    expect(debugState.get("10")).toEqual({ breakpointId: 1, condition: "", hit: false });

    // should call doContinue for all workers if no breakpoints are hit
    expect(service.doContinue).toHaveBeenCalled();
    expect(service.doContinue).toHaveBeenCalledWith(mockPythonUDFPredicate.operatorID, "worker1");
  });

  it("should not call doContinue if a breakpoint is hit", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("10", { breakpointId: 1, condition: "", hit: true });

    const message: ConsoleUpdateEvent = {
      operatorId: mockPythonUDFPredicate.operatorID,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "Breakpoint 2 at /path/to/file.py:11",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    vi.spyOn(service, "doContinue");
    consoleUpdateEventStream.next(message);

    expect(service.doContinue).not.toHaveBeenCalled();
  });

  it("should handle breakpoint deletion and remove it from the debug state", () => {
    const operatorId = mockPythonUDFPredicate.operatorID;

    // Pre-set a breakpoint in the debug state
    const debugState = service.getDebugState(operatorId);
    debugState.set("10", { breakpointId: 1, condition: "", hit: false });

    // Simulate a deletion message
    const message: ConsoleUpdateEvent = {
      operatorId,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "Deleted breakpoint 1 at /path/to/file.py:10",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    vi.spyOn(service, "doContinue");
    consoleUpdateEventStream.next(message);

    // Ensure the breakpoint was deleted from the debug state
    expect(debugState.has("10")).toBe(false);

    // Verify that doContinue was called for all workers
    expect(service.doContinue).toHaveBeenCalled();
    expect(service.doContinue).toHaveBeenCalledWith(operatorId, "worker1");
  });

  it("should handle console update events (breakpoint deletion)", () => {
    const operatorId = mockPythonUDFPredicate.operatorID;

    // Pre-set a breakpoint as hit in the debug state
    const debugState = service.getDebugState(operatorId);
    debugState.set("10", { breakpointId: 1, condition: "", hit: true });

    // Simulate a deletion message
    const message: ConsoleUpdateEvent = {
      operatorId,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "Deleted breakpoint 1 at /path/to/file.py:10",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    vi.spyOn(service, "doContinue");
    consoleUpdateEventStream.next(message);

    // Ensure the breakpoint is retained with an undefined breakpointId
    expect(debugState.get("10")).toEqual({ breakpointId: undefined, condition: "", hit: true });

    // Verify that doContinue was not called due to a hit breakpoint
    expect(service.doContinue).not.toHaveBeenCalled();
  });

  it("should handle console update events (breakpoint deletion) without sending continue if a hit breakpoint exists", () => {
    const operatorId = mockPythonUDFPredicate.operatorID;

    // Pre-set a hit breakpoint and another non-hit breakpoint in the debug state
    const debugState = service.getDebugState(operatorId);
    debugState.set("10", { breakpointId: 1, condition: "", hit: true });
    debugState.set("11", { breakpointId: 2, condition: "", hit: false });

    // Simulate a deletion message
    const message: ConsoleUpdateEvent = {
      operatorId,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "Deleted breakpoint 2 at /path/to/file.py:11",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    vi.spyOn(service, "doContinue");
    consoleUpdateEventStream.next(message);

    // Ensure the non-hit breakpoint was deleted
    expect(debugState.has("11")).toBe(false);

    // Verify that doContinue was not called due to the remaining hit breakpoint
    expect(service.doContinue).not.toHaveBeenCalled();
  });

  it("should call doContinue for all workers if no breakpoints are hit", () => {
    const operatorId = mockPythonUDFPredicate.operatorID;

    // Ensure no breakpoints are hit in the debug state
    const debugState = service.getDebugState(operatorId);
    debugState.set("10", { breakpointId: 1, condition: "", hit: false });

    const message: ConsoleUpdateEvent = {
      operatorId,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "*** Blank or comment",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    vi.spyOn(service, "doContinue"); // Spy on the doContinue method

    consoleUpdateEventStream.next(message); // Emit the message
  });

  it("should handle console update events (breakpoint blank message)", () => {
    const operatorId = mockPythonUDFPredicate.operatorID;

    // Set a hit breakpoint in the debug state
    const debugState = service.getDebugState(operatorId);
    debugState.set("10", { breakpointId: 1, condition: "", hit: true });

    const message: ConsoleUpdateEvent = {
      operatorId,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "*** Blank or comment",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    vi.spyOn(service, "doContinue");

    consoleUpdateEventStream.next(message); // Emit the message

    // Ensure doContinue was not called due to a hit breakpoint
    expect(service.doContinue).not.toHaveBeenCalled();

    debugState.delete("10");

    consoleUpdateEventStream.next(message); // Emit the message

    // Ensure doContinue is called for each worker
    expect(service.doContinue).toHaveBeenCalled();
    expect(service.doContinue).toHaveBeenCalledWith(operatorId, "worker1");
  });

  it("should handle console update events (stepping message)", () => {
    vi.spyOn(service as any, "markBreakpointAsHit");

    const message = {
      operatorId: mockPythonUDFPredicate.operatorID,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "> /path/to/file.py(10)<module>()",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    consoleUpdateEventStream.next(message);

    expect((service as UdfDebugService)["markBreakpointAsHit"]).toHaveBeenCalledWith(
      mockPythonUDFPredicate.operatorID,
      10
    );
  });

  it("should mark a breakpoint as hit", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);

    service["markBreakpointAsHit"](mockPythonUDFPredicate.operatorID, 10);

    expect(debugState.get("10")).toEqual({ breakpointId: undefined, condition: "", hit: true });
  });

  it("should mark continue by resetting hit statuses and removing temporary breakpoints", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: false });
    debugState.set("2", { breakpointId: undefined, condition: "", hit: true }); // Temporary breakpoint

    service["markContinue"](mockPythonUDFPredicate.operatorID);

    expect(debugState.get("1")).toEqual({ breakpointId: 1, condition: "x > 5", hit: false });
    expect(debugState.has("2")).toBe(false);
  });

  // Builds the (Pdb) DEBUGGER console event shape that every handler below filters on.
  function pdbEvent(title: string, overrides: Partial<ConsoleMessage> = {}): ConsoleUpdateEvent {
    return {
      operatorId: mockPythonUDFPredicate.operatorID,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title,
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
          ...overrides,
        },
      ],
    };
  }

  it("should not send a condition for a line that has no breakpoint", () => {
    // The condition differs from the empty default, so the early return is passed and
    // the `isDefined(breakpointInfo)` guard is the one that stops the update.
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);

    service.doUpdateBreakpointCondition(mockPythonUDFPredicate.operatorID, 7, "x < 10");

    expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
    expect(debugState.has("7")).toBe(false);
  });

  it("should clear a breakpoint that lost its id with an empty id", () => {
    // State a hit breakpoint is left in once pdb deleted it: still present, so the
    // command is `clear`, but with no id to clear by. The trailing space is pinned
    // deliberately — it is the literal payload that goes over the websocket.
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("10", { breakpointId: undefined, condition: "", hit: true });

    service.doModifyBreakpoint(mockPythonUDFPredicate.operatorID, 10);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "clear ",
    });
  });

  it("should ignore console events from another operator or with no messages", () => {
    vi.spyOn(service as any, "markBreakpointAsHit");

    consoleUpdateEventStream.next({
      ...pdbEvent("> /path/to/file.py(10)<module>()"),
      operatorId: "some-other-operator",
    });
    consoleUpdateEventStream.next({ operatorId: mockPythonUDFPredicate.operatorID, messages: [] });

    expect(service["markBreakpointAsHit"]).not.toHaveBeenCalled();
  });

  it("should ignore console messages that are not from the debugger", () => {
    vi.spyOn(service as any, "markBreakpointAsHit");

    consoleUpdateEventStream.next(pdbEvent("> /path/to/file.py(10)<module>()", { source: "stdout" }));
    consoleUpdateEventStream.next(pdbEvent("> /path/to/file.py(10)<module>()", { msgType: { name: "PRINT" } }));

    expect(service["markBreakpointAsHit"]).not.toHaveBeenCalled();
  });

  it("should keep the debug state on a status update that is not Uninitialized", () => {
    const operatorId = mockPythonUDFPredicate.operatorID;
    const debugState = service.getDebugState(operatorId);
    debugState.set("10", { breakpointId: 1, condition: "", hit: false });

    stateUpdateStream.next({ [operatorId]: OperatorState.Running });
    stateUpdateStream.next({ "some-other-operator": OperatorState.Uninitialized });

    expect(debugState.size).toBe(1);
  });

  it("should ignore a stepping message that carries no line number", () => {
    vi.spyOn(service as any, "markBreakpointAsHit");
    // spied so the assertions below cannot be satisfied by the message never
    // reaching the handler at all
    vi.spyOn(service as any, "extractInfo");

    consoleUpdateEventStream.next(pdbEvent("> <stdin> in the interactive shell"));

    expect(service["extractInfo"]).toHaveBeenCalledWith("> <stdin> in the interactive shell");
    expect(service["markBreakpointAsHit"]).not.toHaveBeenCalled();
  });

  it("should ignore a deletion message that carries no line number", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("10", { breakpointId: 1, condition: "", hit: false });
    vi.spyOn(service, "doContinue");
    vi.spyOn(service as any, "extractInfo");

    consoleUpdateEventStream.next(pdbEvent("Deleted all breakpoints"));

    expect(service["extractInfo"]).toHaveBeenCalledWith("Deleted all breakpoints");
    expect(debugState.has("10")).toBe(true);
    expect(service.doContinue).not.toHaveBeenCalled();
  });

  it("should ignore a deletion message for a line with no debug state", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    vi.spyOn(service, "doContinue");

    consoleUpdateEventStream.next(pdbEvent("Deleted breakpoint 1 at /path/to/file.py:10"));

    expect(debugState.size).toBe(0);
    // the handler returns before reaching the continue check
    expect(service.doContinue).not.toHaveBeenCalled();
  });

  it("should not record a breakpoint whose creation message has no id", () => {
    // `Breakpoint <id> at <file>:<line>` is what carries an id; without one only the
    // line is recovered, so the entry is skipped but the execution still continues.
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    vi.spyOn(service, "doContinue");

    consoleUpdateEventStream.next(pdbEvent("Breakpoint at /path/to/file.py:10"));

    expect(debugState.has("10")).toBe(false);
    expect(service.doContinue).toHaveBeenCalledWith(mockPythonUDFPredicate.operatorID, stubWorker);
  });

  it("should not record a breakpoint whose creation message has neither id nor line", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    vi.spyOn(service, "doContinue");

    consoleUpdateEventStream.next(pdbEvent("Breakpoint already exists"));

    expect(debugState.size).toBe(0);
    expect(service.doContinue).toHaveBeenCalledWith(mockPythonUDFPredicate.operatorID, stubWorker);
  });

  it("should mark an existing breakpoint as hit without discarding its condition", () => {
    // The companion of the existing markBreakpointAsHit test, which starts from an
    // empty state and therefore only creates the placeholder entry.
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("10", { breakpointId: 3, condition: "x > 5", hit: false });

    consoleUpdateEventStream.next(pdbEvent("> /path/to/file.py(10)<module>()"));

    expect(debugState.get("10")).toEqual({ breakpointId: 3, condition: "x > 5", hit: true });
  });

  it("should keep a hit breakpoint that still has an id when continuing", () => {
    // The two combinations the existing markContinue test does not reach: a hit
    // breakpoint with an id is reset rather than dropped, and an idle temporary one
    // is dropped even though it was never hit.
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: true });
    debugState.set("2", { breakpointId: undefined, condition: "", hit: false });

    service["markContinue"](mockPythonUDFPredicate.operatorID);

    expect(debugState.get("1")).toEqual({ breakpointId: 1, condition: "x > 5", hit: false });
    expect(debugState.has("2")).toBe(false);
  });
});

// The service is provided in root and outlives any one workflow, while each workflow (a new one,
// or one opened again) brings a fresh shared document. These cases create the service first and
// only then build the graph, the order a real session takes once any debugger UI has been shown.
describe("UdfDebugService with workflows built after it", () => {
  let workflowActionService: WorkflowActionService;
  let texeraGraph: WorkflowGraphReadonly;
  let mockWorkflowWebsocketService: Mocked<WorkflowWebsocketService>;
  let mockExecuteWorkflowService: Mocked<ExecuteWorkflowService>;
  let stateUpdateStream: Subject<Record<string, OperatorState>>;
  let consoleUpdateEventStream: Subject<ConsoleUpdateEvent>;
  let unhandledErrors: unknown[];

  const preloaded = mockPythonUDFPredicate;
  const addedLater = { ...mockPythonUDFPredicate, operatorID: "python-udf-added-later" };

  function pdbEvent(operatorId: string, title: string): ConsoleUpdateEvent {
    return {
      operatorId,
      messages: [
        {
          workerId: "worker1",
          timestamp: { nanos: 0, seconds: 0 },
          title,
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };
  }

  function sentCommands(): string[] {
    return mockWorkflowWebsocketService.send.mock.calls.map(([, request]) => (request as { cmd: string }).cmd);
  }

  beforeEach(() => {
    // an error thrown inside a subscriber is reported here, on a timer, instead of failing the emit
    unhandledErrors = [];
    rxjsConfig.onUnhandledError = error => unhandledErrors.push(error);

    mockWorkflowWebsocketService = {
      send: vi.fn(),
      subscribeToEvent: vi.fn(),
    } as unknown as Mocked<WorkflowWebsocketService>;
    const mockWorkflowStatusService = {
      getStateUpdateStream: vi.fn(),
      getCurrentState: vi.fn(() => ({})),
    } as unknown as Mocked<WorkflowStatusService>;
    mockExecuteWorkflowService = { getWorkerIds: vi.fn() } as unknown as Mocked<ExecuteWorkflowService>;
    stateUpdateStream = new Subject();
    consoleUpdateEventStream = new Subject();
    mockWorkflowStatusService.getStateUpdateStream.mockReturnValue(stateUpdateStream.asObservable());
    mockWorkflowWebsocketService.subscribeToEvent.mockReturnValue(
      consoleUpdateEventStream.asObservable() as Observable<TexeraWebsocketEvent>
    );
    mockExecuteWorkflowService.getWorkerIds.mockReturnValue(["worker1"]);

    TestBed.configureTestingModule({
      providers: [
        UdfDebugService,
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: WorkflowWebsocketService, useValue: mockWorkflowWebsocketService },
        { provide: WorkflowStatusService, useValue: mockWorkflowStatusService },
        { provide: ExecuteWorkflowService, useValue: mockExecuteWorkflowService },
        ...commonTestProviders,
      ],
    });
    workflowActionService = TestBed.inject(WorkflowActionService);
    texeraGraph = workflowActionService.getTexeraGraph();
  });

  afterEach(() => {
    rxjsConfig.onUnhandledError = null;
    stateUpdateStream.complete();
    consoleUpdateEventStream.complete();
  });

  it("should send a breakpoint for an operator added after the service was created", () => {
    const service = TestBed.inject(UdfDebugService);
    workflowActionService.addOperator(addedLater, mockPoint);

    service.doModifyBreakpoint(addedLater.operatorID, 3);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: addedLater.operatorID,
      workerId: "worker1",
      cmd: "break 3",
    });
  });

  it("should record the reply and continue for an operator added after the service was created", () => {
    const service = TestBed.inject(UdfDebugService);
    workflowActionService.addOperator(addedLater, mockPoint);

    consoleUpdateEventStream.next(pdbEvent(addedLater.operatorID, "Breakpoint 1 at /path/to/udf.py:3"));

    expect(service.getDebugState(addedLater.operatorID).get("3")).toEqual({
      breakpointId: 1,
      condition: "",
      hit: false,
    });
    expect(sentCommands()).toEqual(["continue"]);
  });

  it("should mark the line an operator added later stops at", () => {
    const service = TestBed.inject(UdfDebugService);
    workflowActionService.addOperator(addedLater, mockPoint);

    consoleUpdateEventStream.next(pdbEvent(addedLater.operatorID, "> /path/to/udf.py(12)produce()"));

    expect(service.getDebugState(addedLater.operatorID).get("12")).toEqual({
      breakpointId: undefined,
      condition: "",
      hit: true,
    });
  });

  it("should work in a workflow started with 'create new' after the service was created", () => {
    workflowActionService.addOperator(preloaded, mockPoint);
    const service = TestBed.inject(UdfDebugService);
    workflowActionService.resetAsNewWorkflow();
    workflowActionService.addOperator(addedLater, mockPoint);

    service.doModifyBreakpoint(addedLater.operatorID, 3);

    expect(sentCommands()).toEqual(["break 3"]);
  });

  it("should work when the same workflow is opened again in a new document", () => {
    workflowActionService.addOperator(preloaded, mockPoint);
    const service = TestBed.inject(UdfDebugService);
    // leaving the workspace swaps in a new document; opening the workflow again re-adds its operators
    workflowActionService.clearWorkflow();
    workflowActionService.addOperator(preloaded, mockPoint);

    service.doModifyBreakpoint(preloaded.operatorID, 3);

    expect(sentCommands()).toEqual(["break 3"]);
  });

  it("should ignore a reset for an operator of a previous workflow", async () => {
    workflowActionService.addOperator(preloaded, mockPoint);
    TestBed.inject(UdfDebugService);
    workflowActionService.resetAsNewWorkflow();

    stateUpdateStream.next({ [preloaded.operatorID]: OperatorState.Uninitialized });
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(unhandledErrors).toEqual([]);
  });

  it("should not create debug state when resetting an operator that never used the debugger", () => {
    TestBed.inject(UdfDebugService);
    workflowActionService.addOperator(addedLater, mockPoint);

    stateUpdateStream.next({ [addedLater.operatorID]: OperatorState.Uninitialized });

    expect(() => texeraGraph.getOperatorDebugState(addedLater.operatorID)).toThrow();
  });

  it("should return the same debug state on every lookup and keep its entries", () => {
    const service = TestBed.inject(UdfDebugService);
    workflowActionService.addOperator(addedLater, mockPoint);

    const first = service.getDebugState(addedLater.operatorID);
    first.set("3", { breakpointId: 1, condition: "x > 1", hit: false });
    const second = service.getDebugState(addedLater.operatorID);

    expect(second).toBe(first);
    expect(second.get("3")).toEqual({ breakpointId: 1, condition: "x > 1", hit: false });
  });

  it("should send one continue per reply after an operator is removed and added back", () => {
    workflowActionService.addOperator(preloaded, mockPoint);
    TestBed.inject(UdfDebugService);
    workflowActionService.deleteOperator(preloaded.operatorID);
    workflowActionService.addOperator(preloaded, mockPoint);

    consoleUpdateEventStream.next(pdbEvent(preloaded.operatorID, "Breakpoint 1 at /path/to/udf.py:3"));

    expect(sentCommands()).toEqual(["continue"]);
  });

  it("should send the breakpoint to every assigned worker", () => {
    mockExecuteWorkflowService.getWorkerIds.mockReturnValue(["worker1", "worker2"]);
    const service = TestBed.inject(UdfDebugService);
    workflowActionService.addOperator(addedLater, mockPoint);

    service.doModifyBreakpoint(addedLater.operatorID, 3);

    expect(mockWorkflowWebsocketService.send.mock.calls.map(([, request]) => request)).toEqual([
      { operatorId: addedLater.operatorID, workerId: "worker1", cmd: "break 3" },
      { operatorId: addedLater.operatorID, workerId: "worker2", cmd: "break 3" },
    ]);
  });

  it("should send nothing when no worker is assigned yet", () => {
    mockExecuteWorkflowService.getWorkerIds.mockReturnValue([]);
    const service = TestBed.inject(UdfDebugService);
    workflowActionService.addOperator(addedLater, mockPoint);

    service.doModifyBreakpoint(addedLater.operatorID, 3);

    expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
  });

  it("should reject a breakpoint for an operator that is not in the workflow", () => {
    const service = TestBed.inject(UdfDebugService);

    expect(() => service.doModifyBreakpoint("missing-operator", 3)).toThrow(
      "operator with ID missing-operator doesn't exist"
    );
    expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
  });

  it("should clear the debug state a deleted operator left behind when its run is reset", () => {
    // left in place, it would come back with the old run's breakpoints if the deletion were undone
    workflowActionService.addOperator(preloaded, mockPoint);
    const service = TestBed.inject(UdfDebugService);
    service.getDebugState(preloaded.operatorID).set("3", { breakpointId: 1, condition: "", hit: false });
    workflowActionService.deleteOperator(preloaded.operatorID);

    stateUpdateStream.next({ [preloaded.operatorID]: OperatorState.Uninitialized });

    expect(texeraGraph.getOperatorDebugState(preloaded.operatorID).size).toBe(0);
  });

  it("should ignore debugger replies for a deleted operator whose debug state remains", () => {
    workflowActionService.addOperator(preloaded, mockPoint);
    const service = TestBed.inject(UdfDebugService);
    service.getDebugState(preloaded.operatorID).set("3", { breakpointId: 1, condition: "", hit: false });
    workflowActionService.deleteOperator(preloaded.operatorID);

    consoleUpdateEventStream.next(pdbEvent(preloaded.operatorID, "> /path/to/udf.py(3)produce()"));

    expect(texeraGraph.getOperatorDebugState(preloaded.operatorID).get("3")?.hit).toBe(false);
    expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
  });

  it("should reject a breakpoint for a deleted operator whose debug state remains", () => {
    workflowActionService.addOperator(preloaded, mockPoint);
    const service = TestBed.inject(UdfDebugService);
    service.getDebugState(preloaded.operatorID);
    workflowActionService.deleteOperator(preloaded.operatorID);

    expect(() => service.doModifyBreakpoint(preloaded.operatorID, 3)).toThrow(
      `operator with ID ${preloaded.operatorID} doesn't exist`
    );
    expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
  });

  it("should still continue after a breakpoint reply once an editor opened before the run was closed", () => {
    workflowActionService.addOperator(preloaded, mockPoint);
    const service = TestBed.inject(UdfDebugService);
    // the editor opened to write the code, and closed before the run started
    const editor = TestBed.createComponent(CodeDebuggerComponent);
    editor.componentInstance.currentOperatorId = preloaded.operatorID;
    editor.componentInstance.monacoEditor = { dispose: vi.fn() } as unknown as CodeDebuggerComponent["monacoEditor"];
    editor.detectChanges();
    editor.destroy();

    expect(() =>
      consoleUpdateEventStream.next(pdbEvent(preloaded.operatorID, "Breakpoint 1 at /path/to/udf.py:3"))
    ).not.toThrow();

    expect(service.getDebugState(preloaded.operatorID).get("3")?.breakpointId).toBe(1);
    expect(sentCommands()).toEqual(["continue"]);
  });

  it("should ignore debugger replies for an operator that is not in the workflow", async () => {
    TestBed.inject(UdfDebugService);

    consoleUpdateEventStream.next(pdbEvent("missing-operator", "Breakpoint 1 at /path/to/udf.py:3"));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
    expect(() => texeraGraph.getOperatorDebugState("missing-operator")).toThrow();
    expect(unhandledErrors).toEqual([]);
  });
});
